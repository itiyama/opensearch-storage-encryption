/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.read_ahead.ReadaheadContext;
import org.opensearch.index.store.read_ahead.ReadaheadManager;

/**
 * JMH benchmark measuring block transition costs for the "Pin Once, Read Many, Unpin Once"
 * architecture.
 *
 * <p>In the current architecture, a master IndexInput pins one block and reuses it for all
 * subsequent reads within that block. A block transition occurs when a read crosses into a
 * different block, requiring: Caffeine/L1 cache lookup + retain (AtomicInteger CAS) on the
 * new block + release (AtomicInteger CAS) on the old block.
 *
 * <p>Key performance expectations:
 * <ul>
 *   <li><b>Same-block reads:</b> ~5-10ns (pure field comparison + MemorySegment.get())</li>
 *   <li><b>Block transitions:</b> ~20-30ns (1 retain CAS + 1 release CAS + cache lookup)</li>
 *   <li><b>Raw CAS pair:</b> ~10-15ns (isolated atomic ops without cache lookup)</li>
 * </ul>
 *
 * <p>The transition cost is amortized over ~1024 reads per 8KB block for sequential access.
 * For random access (e.g., HNSW graph traversal), transitions happen frequently. This benchmark
 * quantifies the transition overhead precisely to validate the amortization benefit.
 *
 * <p><b>Profiling recommendations:</b>
 * <ul>
 *   <li>Run with {@code -prof perfnorm} on Linux to count instructions/cycles per op</li>
 *   <li>Run with {@code -prof gc} to confirm zero allocation on the hot path</li>
 *   <li>The gap between {@code refCountRetainRelease} and {@code everyReadTransitions} shows
 *       the Caffeine/L1 cache lookup overhead</li>
 * </ul>
 *
 * <p>Run with: {@code ./gradlew jmh -Pjmh.includes='BlockTransitionBench'}
 */
@State(Scope.Thread)
@BenchmarkMode({ Mode.AverageTime })
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 10, time = 3)
@Fork(value = 3, jvmArgs = { "--enable-preview", "--enable-native-access=ALL-UNNAMED", "-XX:+UseZGC" })
@SuppressWarnings("preview")
public class BlockTransitionBench {

    private static final int BLOCK_SIZE = 8192; // StaticConfigs.CACHE_BLOCK_SIZE
    private static final int NUM_BLOCKS = 2;
    private static final int LONGS_PER_BLOCK = BLOCK_SIZE / Long.BYTES; // 1024
    private static final long TOTAL_LENGTH = (long) NUM_BLOCKS * BLOCK_SIZE;
    private static final ValueLayout.OfLong LAYOUT_LE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    private Arena arena;
    private CachedMemorySegmentIndexInput master;

    /** Standalone RefCountedMemorySegment for the refCountRetainRelease micro-benchmark. */
    private RefCountedMemorySegment isolatedRefCounted;

    // ---------- lightweight fakes ----------

    /**
     * A BlockCache backed by multiple pre-allocated memory segments keyed by block offset.
     * Returns the correct FakeCacheValue based on the key's file offset.
     */
    static final class MultiBlockCache implements BlockCache<RefCountedMemorySegment> {

        private final Map<Long, BlockCacheValue<RefCountedMemorySegment>> blocks;

        MultiBlockCache(Map<Long, BlockCacheValue<RefCountedMemorySegment>> blocks) {
            this.blocks = blocks;
        }

        @Override
        public BlockCacheValue<RefCountedMemorySegment> get(BlockCacheKey key) {
            return blocks.get(key.offset());
        }

        @Override
        public BlockCacheValue<RefCountedMemorySegment> getOrLoad(BlockCacheKey key) {
            return blocks.get(key.offset());
        }

        @Override
        public void prefetch(BlockCacheKey key) {}

        @Override
        public void put(BlockCacheKey key, BlockCacheValue<RefCountedMemorySegment> v) {}

        @Override
        public void invalidate(BlockCacheKey key) {}

        @Override
        public void invalidate(Path normalizedFilePath) {}

        @Override
        public void invalidateByPathPrefix(Path directoryPath) {}

        @Override
        public void clear() {}

        @Override
        public Map<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> loadForPrefetch(
            Path filePath,
            long startOffset,
            long blockCount
        ) {
            return Map.of();
        }

        @Override
        public String cacheStats() {
            return "MultiBlockCache";
        }

        @Override
        public void recordStats() {}

        @Override
        public double getHitRate() {
            return 1.0;
        }

        @Override
        public long getCacheSize() {
            return blocks.size();
        }

        @Override
        public long hitCount() {
            return 0;
        }

        @Override
        public long missCount() {
            return 0;
        }
    }

    /**
     * Minimal BlockCacheValue wrapping a RefCountedMemorySegment.
     */
    static final class FakeCacheValue implements BlockCacheValue<RefCountedMemorySegment> {

        private final RefCountedMemorySegment segment;

        FakeCacheValue(RefCountedMemorySegment segment) {
            this.segment = segment;
        }

        @Override
        public RefCountedMemorySegment value() {
            return segment;
        }

        @Override
        public boolean tryPin() {
            return segment.tryPin();
        }

        @Override
        public void unpin() {
            segment.unpin();
        }

        @Override
        public int length() {
            return BLOCK_SIZE;
        }

        @Override
        public void close() {
            segment.close();
        }

        @Override
        public void decRef() {
            segment.decRef();
        }

        @Override
        public int getGeneration() {
            return segment.getGeneration();
        }
    }

    /**
     * No-op ReadaheadManager -- benchmarks don't need read-ahead.
     */
    static final class NoopReadaheadManager implements ReadaheadManager {

        static final NoopReadaheadManager INSTANCE = new NoopReadaheadManager();

        @Override
        public ReadaheadContext register(Path path, long fileLength) {
            return null;
        }

        @Override
        public void cancel(ReadaheadContext context) {}

        @Override
        public void cancel(Path path) {}

        @Override
        public void close() {}
    }

    @Setup(Level.Trial)
    public void setup() throws IOException {
        arena = Arena.ofShared();

        // Allocate NUM_BLOCKS blocks, each filled with sequential longs
        Map<Long, BlockCacheValue<RefCountedMemorySegment>> blockMap = new HashMap<>();
        for (int b = 0; b < NUM_BLOCKS; b++) {
            MemorySegment seg = arena.allocate(BLOCK_SIZE);
            for (int i = 0; i < LONGS_PER_BLOCK; i++) {
                seg.set(LAYOUT_LE_LONG, (long) i * Long.BYTES, (long) (b * LONGS_PER_BLOCK + i));
            }
            RefCountedMemorySegment ref = new RefCountedMemorySegment(seg, BLOCK_SIZE, s -> {});
            blockMap.put((long) b * BLOCK_SIZE, new FakeCacheValue(ref));
        }

        MultiBlockCache cache = new MultiBlockCache(blockMap);
        Path fakePath = Paths.get("/bench/transition-test.dat");
        BlockSlotTinyCache tinyCache = new BlockSlotTinyCache(cache, fakePath, TOTAL_LENGTH);

        // Create master IndexInput over NUM_BLOCKS * BLOCK_SIZE range
        master = CachedMemorySegmentIndexInput
            .newInstance("bench-transition", fakePath, TOTAL_LENGTH, cache, NoopReadaheadManager.INSTANCE, null, tinyCache);

        // Isolated RefCountedMemorySegment for raw CAS benchmarking
        MemorySegment isolatedSeg = arena.allocate(BLOCK_SIZE);
        isolatedRefCounted = new RefCountedMemorySegment(isolatedSeg, BLOCK_SIZE, s -> {});
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        if (master != null)
            master.close();
        if (arena != null)
            arena.close();
    }

    /**
     * Baseline: read from the same block repeatedly (no transition).
     *
     * <p>Measures the fast path cost: field comparison ({@code blockOffset == currentBlockOffset})
     * + {@code MemorySegment.get()}. No atomic operations, no cache lookups.
     *
     * <p>Expected: ~5-10ns per operation.
     */
    @Benchmark
    public long sameBlockRead() throws IOException {
        // Stay within block 0: positions 0 to 8184 (1024 longs)
        long pos = master.getFilePointer();
        if (pos >= BLOCK_SIZE - Long.BYTES) {
            master.seek(0);
        }
        return master.readLong();
    }

    /**
     * Worst case: every read forces a block transition.
     *
     * <p>Alternates between block 0 (position 0) and block 1 (position 8192).
     * Every {@code readLong()} hits a different block, forcing the full transition path:
     * L1/Caffeine cache lookup + retain new block (CAS) + release old block (CAS).
     *
     * <p>This isolates the pure transition cost without any amortization benefit.
     *
     * <p>Expected: ~20-60ns per operation (cache lookup + two CAS operations).
     */
    @Benchmark
    public long everyReadTransitions() throws IOException {
        // Read from block 0
        master.seek(0);
        long v0 = master.readLong();
        // Read from block 1 -- forces transition
        master.seek(BLOCK_SIZE);
        long v1 = master.readLong();
        return v0 ^ v1;
    }

    /**
     * Micro-benchmark of just the atomic ref-count operations: one {@code incRef()} +
     * one {@code decRef()} on a {@link RefCountedMemorySegment}, bypassing all cache logic.
     *
     * <p>This isolates the raw CAS cost (~10-15ns per retain+release pair on x86) from the
     * cache lookup overhead. Compare with {@code everyReadTransitions} to see how much
     * the L1/Caffeine lookup adds on top of the atomic operations.
     *
     * <p>Expected: ~10-15ns per operation.
     */
    @Benchmark
    public void refCountRetainRelease(Blackhole bh) {
        isolatedRefCounted.incRef();
        isolatedRefCounted.decRef();
    }

    // ---------- amortizedTransitions benchmark ----------

    /**
     * State for the amortized transitions benchmark with parameterized reads-per-block.
     */
    @State(Scope.Thread)
    public static class AmortizedState {

        @Param({ "1", "8", "64", "1024" })
        int readsPerBlock;

        private Arena amortizedArena;
        private CachedMemorySegmentIndexInput amortizedMaster;

        /** Number of blocks to cycle through. Using 4 blocks to show repeated transitions. */
        private static final int AMORTIZED_NUM_BLOCKS = 4;
        private static final long AMORTIZED_TOTAL_LENGTH = (long) AMORTIZED_NUM_BLOCKS * BLOCK_SIZE;

        @Setup(Level.Trial)
        public void setup() throws IOException {
            amortizedArena = Arena.ofShared();

            Map<Long, BlockCacheValue<RefCountedMemorySegment>> blockMap = new HashMap<>();
            for (int b = 0; b < AMORTIZED_NUM_BLOCKS; b++) {
                MemorySegment seg = amortizedArena.allocate(BLOCK_SIZE);
                for (int i = 0; i < LONGS_PER_BLOCK; i++) {
                    seg.set(LAYOUT_LE_LONG, (long) i * Long.BYTES, (long) (b * LONGS_PER_BLOCK + i));
                }
                RefCountedMemorySegment ref = new RefCountedMemorySegment(seg, BLOCK_SIZE, s -> {});
                blockMap.put((long) b * BLOCK_SIZE, new FakeCacheValue(ref));
            }

            MultiBlockCache cache = new MultiBlockCache(blockMap);
            Path fakePath = Paths.get("/bench/amortized-test.dat");
            BlockSlotTinyCache tinyCache = new BlockSlotTinyCache(cache, fakePath, AMORTIZED_TOTAL_LENGTH);

            amortizedMaster = CachedMemorySegmentIndexInput
                .newInstance("bench-amortized", fakePath, AMORTIZED_TOTAL_LENGTH, cache, NoopReadaheadManager.INSTANCE, null, tinyCache);
        }

        @TearDown(Level.Trial)
        public void tearDown() throws IOException {
            if (amortizedMaster != null)
                amortizedMaster.close();
            if (amortizedArena != null)
                amortizedArena.close();
        }
    }

    /**
     * Realistic: read N longs from one block, then transition to the next block, repeat.
     *
     * <p>Parameterized with {@code readsPerBlock} in {1, 8, 64, 1024}:
     * <ul>
     *   <li>N=1: matches {@code everyReadTransitions} (every read pays transition cost)</li>
     *   <li>N=8: ~12.5% of reads pay transition cost</li>
     *   <li>N=64: ~1.6% of reads pay transition cost</li>
     *   <li>N=1024: matches {@code sameBlockRead} (transition cost fully amortized over entire block)</li>
     * </ul>
     *
     * <p>Reports per-read cost, showing how quickly the transition overhead amortizes.
     * At N=1024, cost should approach {@code sameBlockRead}; at N=1, should approach
     * {@code everyReadTransitions}.
     */
    @Benchmark
    public long amortizedTransitions(AmortizedState state) throws IOException {
        CachedMemorySegmentIndexInput input = state.amortizedMaster;
        int readsPerBlock = state.readsPerBlock;
        long sum = 0;

        // Read readsPerBlock longs from block 0, then transition to block 1
        // Position at start of block 0
        input.seek(0);
        for (int i = 0; i < readsPerBlock; i++) {
            sum += input.readLong();
        }

        // Transition to block 1
        input.seek(BLOCK_SIZE);
        for (int i = 0; i < readsPerBlock; i++) {
            sum += input.readLong();
        }

        return sum;
    }
}

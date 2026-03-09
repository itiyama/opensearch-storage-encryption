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
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
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
 * JMH benchmark for end-to-end slice lifecycle throughput, validating the
 * "Pin Once, Read Many, Unpin Once" architecture proposed in issue #150.
 *
 * <p>This benchmark measures realistic Lucene codec patterns:
 * <ul>
 *   <li><b>sequentialSliceReads</b> - 1024 sequential readLong() calls within one 8KB block.
 *       Current architecture pays pin/unpin CAS per read (~200-500ns each = ~200-500us total).
 *       "Pin Once" should pay ~10ns per read = ~10us total (20-50x improvement).</li>
 *   <li><b>multiBlockSequentialSlice</b> - Sequential reads across 4 blocks (32KB).
 *       Measures within-block fast path plus block transition cost.</li>
 *   <li><b>sliceCreationAndAbandon</b> - Fan-out of N slices from a master, each reading one value.
 *       Measures slice lifecycle cost that parent auto-close optimizes.</li>
 *   <li><b>randomAccessSlice</b> - Random seeks across 8 blocks (64KB), worst-case for
 *       "Pin Once" where block transitions dominate.</li>
 * </ul>
 *
 * <p>Run with: {@code ./gradlew jmh -Pjmh.includes='SliceLifecycleBench'}
 */
@State(Scope.Thread)
@BenchmarkMode({ Mode.Throughput, Mode.AverageTime })
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 10, time = 3)
@Fork(value = 3, jvmArgs = { "--enable-preview", "--enable-native-access=ALL-UNNAMED", "-XX:+UseZGC" })
@SuppressWarnings("preview")
public class SliceLifecycleBench {

    private static final int BLOCK_SIZE = 8192; // StaticConfigs.CACHE_BLOCK_SIZE
    private static final ValueLayout.OfLong LAYOUT_LE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    /** How many longs fit in one 8KB block. */
    private static final int LONGS_PER_BLOCK = BLOCK_SIZE / Long.BYTES; // 1024

    private static final Path FAKE_PATH = Paths.get("/bench/lifecycle.dat");

    // ---- Shared state for single-block benchmarks ----

    private Arena arena;
    private CachedMemorySegmentIndexInput singleBlockMaster;

    // ---- Multi-block state (4 blocks for multiBlockSequentialSlice) ----

    private static final int MULTI_BLOCK_COUNT = 4;
    private CachedMemorySegmentIndexInput multiBlockMaster;

    // ---- Random access state (8 blocks for randomAccessSlice) ----

    private static final int RANDOM_BLOCK_COUNT = 8;
    private CachedMemorySegmentIndexInput randomAccessMaster;

    /** Pre-computed random positions for randomAccessSlice. */
    private long[] randomPositions;
    private int randomPosIdx;

    // ---------- lightweight fakes ----------

    /**
     * A BlockCache backed by multiple pre-allocated memory segments.
     * Returns the correct block based on the offset in the BlockCacheKey.
     */
    static final class MultiBlockCache implements BlockCache<RefCountedMemorySegment> {

        private final BlockCacheValue<RefCountedMemorySegment>[] blocks;
        private final int blockCount;

        @SuppressWarnings("unchecked")
        MultiBlockCache(BlockCacheValue<RefCountedMemorySegment>[] blocks) {
            this.blocks = blocks;
            this.blockCount = blocks.length;
        }

        private BlockCacheValue<RefCountedMemorySegment> lookup(BlockCacheKey key) {
            long offset = key.offset();
            int blockIdx = (int) (offset / BLOCK_SIZE);
            if (blockIdx >= 0 && blockIdx < blockCount) {
                return blocks[blockIdx];
            }
            return null;
        }

        @Override
        public BlockCacheValue<RefCountedMemorySegment> get(BlockCacheKey key) {
            return lookup(key);
        }

        @Override
        public BlockCacheValue<RefCountedMemorySegment> getOrLoad(BlockCacheKey key) {
            return lookup(key);
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
            return "MultiBlockCache[" + blockCount + "]";
        }

        @Override
        public void recordStats() {}

        @Override
        public double getHitRate() {
            return 1.0;
        }

        @Override
        public long getCacheSize() {
            return blockCount;
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

    // ---------- helpers ----------

    /**
     * Allocates {@code count} blocks in the given arena, fills them with sequential longs,
     * wraps them in FakeCacheValue[], and returns the array.
     */
    @SuppressWarnings("unchecked")
    private static BlockCacheValue<RefCountedMemorySegment>[] allocateBlocks(Arena arena, int count) {
        BlockCacheValue<RefCountedMemorySegment>[] values = new BlockCacheValue[count];
        for (int b = 0; b < count; b++) {
            MemorySegment seg = arena.allocate(BLOCK_SIZE);
            for (int i = 0; i < LONGS_PER_BLOCK; i++) {
                seg.set(LAYOUT_LE_LONG, (long) i * Long.BYTES, (long) (b * LONGS_PER_BLOCK + i));
            }
            RefCountedMemorySegment refCounted = new RefCountedMemorySegment(seg, BLOCK_SIZE, s -> {});
            values[b] = new FakeCacheValue(refCounted);
        }
        return values;
    }

    /**
     * Creates a master CachedMemorySegmentIndexInput backed by the given block cache
     * with the specified total file length.
     */
    private static CachedMemorySegmentIndexInput createMaster(BlockCache<RefCountedMemorySegment> cache, long fileLength) {
        BlockSlotTinyCache tinyCache = new BlockSlotTinyCache(cache, FAKE_PATH, fileLength);
        return CachedMemorySegmentIndexInput
            .newInstance("bench-master", FAKE_PATH, fileLength, cache, NoopReadaheadManager.INSTANCE, null, tinyCache);
    }

    // ---------- setup / teardown ----------

    @Setup(Level.Trial)
    public void setup() throws IOException {
        arena = Arena.ofShared();

        // Single-block setup (for sequentialSliceReads and sliceCreationAndAbandon)
        BlockCacheValue<RefCountedMemorySegment>[] singleBlocks = allocateBlocks(arena, 1);
        MultiBlockCache singleCache = new MultiBlockCache(singleBlocks);
        singleBlockMaster = createMaster(singleCache, BLOCK_SIZE);

        // Multi-block setup (4 blocks = 32KB)
        BlockCacheValue<RefCountedMemorySegment>[] multiBlocks = allocateBlocks(arena, MULTI_BLOCK_COUNT);
        MultiBlockCache multiCache = new MultiBlockCache(multiBlocks);
        multiBlockMaster = createMaster(multiCache, (long) MULTI_BLOCK_COUNT * BLOCK_SIZE);

        // Random access setup (8 blocks = 64KB)
        BlockCacheValue<RefCountedMemorySegment>[] randomBlocks = allocateBlocks(arena, RANDOM_BLOCK_COUNT);
        MultiBlockCache randomCache = new MultiBlockCache(randomBlocks);
        randomAccessMaster = createMaster(randomCache, (long) RANDOM_BLOCK_COUNT * BLOCK_SIZE);

        // Pre-compute 4096 random long-aligned positions across 8 blocks
        randomPositions = new long[4096];
        long totalSize = (long) RANDOM_BLOCK_COUNT * BLOCK_SIZE;
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        for (int i = 0; i < randomPositions.length; i++) {
            // Random long-aligned position (must leave room for 8 bytes)
            long pos = (rng.nextLong(totalSize - Long.BYTES) / Long.BYTES) * Long.BYTES;
            randomPositions[i] = pos;
        }
        randomPosIdx = 0;
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        if (randomAccessMaster != null)
            randomAccessMaster.close();
        if (multiBlockMaster != null)
            multiBlockMaster.close();
        if (singleBlockMaster != null)
            singleBlockMaster.close();
        if (arena != null)
            arena.close();
    }

    // ---------- State for parameterized sliceCreationAndAbandon ----------

    @State(Scope.Thread)
    public static class SliceCountState {
        @Param({ "10", "100", "1000" })
        public int sliceCount;
    }

    // ---------- benchmarks ----------

    /**
     * Creates a slice, reads 1024 longs sequentially (one full 8KB block), measuring
     * the entire burst. This represents a postings reader consuming a full block.
     *
     * <p>Current architecture: ~200-500ns x 1024 = ~200-500us per invocation.
     * <p>"Pin Once": ~10ns x 1024 = ~10us per invocation (20-50x improvement).
     */
    @Benchmark
    public long sequentialSliceReads(Blackhole bh) throws IOException {
        CachedMemorySegmentIndexInput slice = singleBlockMaster.slice("seq-slice", 0, BLOCK_SIZE);
        long sum = 0;
        for (int i = 0; i < LONGS_PER_BLOCK; i++) {
            sum += slice.readLong();
        }
        slice.close();
        return sum;
    }

    /**
     * Creates a slice over 4 blocks (32KB), reads sequentially across all blocks.
     * Measures both within-block reads (fast) and block transitions (retain new + release old).
     */
    @Benchmark
    public long multiBlockSequentialSlice(Blackhole bh) throws IOException {
        long totalLength = (long) MULTI_BLOCK_COUNT * BLOCK_SIZE;
        CachedMemorySegmentIndexInput slice = multiBlockMaster.slice("multi-slice", 0, totalLength);
        long sum = 0;
        int totalLongs = MULTI_BLOCK_COUNT * LONGS_PER_BLOCK;
        for (int i = 0; i < totalLongs; i++) {
            sum += slice.readLong();
        }
        slice.close();
        return sum;
    }

    /**
     * Creates N slices from a master, each reads one long value, then abandons all slices
     * and closes them. Measures the cost of slice fan-out that parent auto-close optimizes.
     *
     * <p>Parameterized with sliceCount in {10, 100, 1000}.
     */
    @Benchmark
    public long sliceCreationAndAbandon(SliceCountState state, Blackhole bh) throws IOException {
        int n = state.sliceCount;
        CachedMemorySegmentIndexInput[] slices = new CachedMemorySegmentIndexInput[n];
        long sum = 0;

        // Create all slices and read one value each
        for (int i = 0; i < n; i++) {
            slices[i] = singleBlockMaster.slice("abandon-slice-" + i, 0, BLOCK_SIZE);
            sum += slices[i].readLong();
        }

        // Close all slices (simulates master close cleaning up child slices)
        for (int i = 0; i < n; i++) {
            slices[i].close();
        }

        return sum;
    }

    /**
     * Single slice with random seeks across 8 blocks (64KB file). Each seek likely hits
     * a different block, forcing block transitions. Measures worst-case for "Pin Once"
     * where block transitions dominate.
     *
     * <p>Performs 1024 random-position reads per invocation.
     */
    @Benchmark
    public long randomAccessSlice(Blackhole bh) throws IOException {
        long totalLength = (long) RANDOM_BLOCK_COUNT * BLOCK_SIZE;
        CachedMemorySegmentIndexInput slice = randomAccessMaster.slice("random-slice", 0, totalLength);
        long sum = 0;
        for (int i = 0; i < 1024; i++) {
            long pos = randomPositions[randomPosIdx];
            randomPosIdx = (randomPosIdx + 1) & (randomPositions.length - 1);
            slice.seek(pos);
            sum += slice.readLong();
        }
        slice.close();
        return sum;
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.read_ahead.ReadaheadContext;
import org.opensearch.index.store.read_ahead.ReadaheadManager;

/**
 * JMH benchmark validating memory safety of the "Pin Once, Read Many, Unpin Once" architecture
 * under slice churn -- the pattern where thousands of slices are created, used, and abandoned
 * without explicit close() calls (Lucene's standard lifecycle for cloned IndexInputs).
 *
 * <p>Key measurements:
 * <ul>
 *   <li><b>Throughput:</b> How fast can we cycle through create-use-abandon-close?</li>
 *   <li><b>Memory:</b> Does RSS/direct memory stay flat across thousands of iterations?</li>
 *   <li><b>Correctness:</b> Are all pins released after master close? (refcount == 1 = cache ref only)</li>
 * </ul>
 *
 * <p>The current design uses "unpin after every read" for slices ({@code releasePinnedBlockIfSlice()}).
 * This means slices never hold a pinned block across calls, so abandoning them is always safe --
 * there is nothing to leak. The master's close() cleans up its own single pinned block and the
 * shared BlockSlotTinyCache. This benchmark validates that contract under high churn.
 *
 * <p>Run with: {@code ./gradlew jmh -Pjmh.includes='SliceMemoryPressureBench'}
 */
@BenchmarkMode({ Mode.Throughput, Mode.AverageTime })
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 10, time = 3)
@Fork(value = 3, jvmArgs = { "--enable-preview", "--enable-native-access=ALL-UNNAMED", "-XX:+UseZGC" })
@SuppressWarnings("preview")
public class SliceMemoryPressureBench {

    private static final int BLOCK_SIZE = 8192; // StaticConfigs.CACHE_BLOCK_SIZE
    private static final ValueLayout.OfLong LAYOUT_LE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final int LONGS_PER_BLOCK = BLOCK_SIZE / Long.BYTES;

    // ======================== Fakes (self-contained, duplicated from SliceVsMasterReadBench) ========================

    /**
     * Trivial BlockCache backed by a single pre-allocated memory segment.
     * Every get/getOrLoad returns the same block.
     */
    static final class SingleBlockCache implements BlockCache<RefCountedMemorySegment> {

        private final BlockCacheValue<RefCountedMemorySegment> value;

        SingleBlockCache(BlockCacheValue<RefCountedMemorySegment> value) {
            this.value = value;
        }

        @Override
        public BlockCacheValue<RefCountedMemorySegment> get(BlockCacheKey key) {
            return value;
        }

        @Override
        public BlockCacheValue<RefCountedMemorySegment> getOrLoad(BlockCacheKey key) {
            return value;
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
            return "SingleBlockCache";
        }

        @Override
        public void recordStats() {}

        @Override
        public double getHitRate() {
            return 1.0;
        }

        @Override
        public long getCacheSize() {
            return 1;
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

    // ======================== Benchmark 1 & 2: createAndAbandonSlices / sliceChurnWithGC ========================

    @State(Scope.Thread)
    public static class SliceChurnState {

        @Param({ "100", "1000", "10000" })
        int sliceCount;

        Arena arena;
        MemorySegment block;
        RefCountedMemorySegment refCounted;
        FakeCacheValue cacheValue;
        SingleBlockCache blockCache;
        Path fakePath;

        // Memory tracking for sliceChurnWithGC
        long memoryBeforeIteration;
        long directMemoryBeforeIteration;

        @Setup(Level.Trial)
        public void setup() {
            arena = Arena.ofShared();
            block = arena.allocate(BLOCK_SIZE);

            // Fill block with sequential long values so reads return meaningful data
            for (int i = 0; i < LONGS_PER_BLOCK; i++) {
                block.set(LAYOUT_LE_LONG, (long) i * Long.BYTES, (long) i);
            }

            // Wrap in RefCountedMemorySegment with no-op releaser (never fully released in this bench)
            refCounted = new RefCountedMemorySegment(block, BLOCK_SIZE, seg -> {});
            cacheValue = new FakeCacheValue(refCounted);
            blockCache = new SingleBlockCache(cacheValue);
            fakePath = Paths.get("/bench/slice-pressure.dat");
        }

        @Setup(Level.Iteration)
        public void captureMemoryBefore() {
            System.gc();
            memoryBeforeIteration = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            directMemoryBeforeIteration = getDirectMemoryUsed();
        }

        @TearDown(Level.Iteration)
        public void captureMemoryAfter() {
            System.gc();
            long memoryAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            long directMemoryAfter = getDirectMemoryUsed();
            long heapDelta = memoryAfter - memoryBeforeIteration;
            long directDelta = directMemoryAfter - directMemoryBeforeIteration;

            // Log memory deltas for analysis -- JMH will capture stdout
            System.out
                .println(
                    "[MemoryCheck] heapDelta="
                        + heapDelta
                        + " bytes, directDelta="
                        + directDelta
                        + " bytes, refCount="
                        + refCounted.getRefCount()
                );
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            if (arena != null) {
                arena.close();
            }
        }

        private static long getDirectMemoryUsed() {
            for (BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
                if ("direct".equals(pool.getName())) {
                    return pool.getMemoryUsed();
                }
            }
            return -1;
        }
    }

    /**
     * Creates a master, spawns N slices that each read one value (pinning a block),
     * abandons all slices without closing them, then closes the master.
     *
     * <p>Validates that after master.close(), the RefCountedMemorySegment's refcount
     * returns to its base level (1 for the cache's reference). Because slices use
     * "unpin after every read" ({@code releasePinnedBlockIfSlice()}), abandoned slices
     * hold zero pins -- cleanup is deterministic and complete.
     *
     * <p>Measures throughput of the full create-use-abandon-close cycle.
     */
    @Benchmark
    public int createAndAbandonSlices(SliceChurnState state, Blackhole bh) throws IOException {
        BlockSlotTinyCache tinyCache = new BlockSlotTinyCache(state.blockCache, state.fakePath, BLOCK_SIZE);

        // Create master
        CachedMemorySegmentIndexInput master = CachedMemorySegmentIndexInput
            .newInstance("bench-master", state.fakePath, BLOCK_SIZE, state.blockCache, NoopReadaheadManager.INSTANCE, null, tinyCache);

        // Create N slices, each reads one long to trigger block pin/unpin cycle
        for (int i = 0; i < state.sliceCount; i++) {
            CachedMemorySegmentIndexInput slice = master.slice("s" + i, 0, BLOCK_SIZE);
            bh.consume(slice.readLong()); // pins block, reads, unpins (releasePinnedBlockIfSlice)
            // abandon slice -- don't close it. This is Lucene's pattern.
        }

        // Close master -- should release its own pinned block and clear the tiny cache
        master.close();

        // Correctness assertion: refcount should be back to 1 (cache's base reference).
        // If slices leaked pins, this would be > 1.
        int refCount = state.refCounted.getRefCount();
        if (refCount != 1) {
            throw new AssertionError(
                "RefCount leak detected! Expected 1 (cache ref), got "
                    + refCount
                    + " after closing master with "
                    + state.sliceCount
                    + " abandoned slices"
            );
        }

        return state.sliceCount;
    }

    /**
     * Same as {@link #createAndAbandonSlices} but with explicit GC between iterations
     * and memory tracking via {@code @Setup/@TearDown(Level.Iteration)}.
     *
     * <p>Validates that memory does not grow unboundedly across iterations. The
     * iteration-level setup/teardown captures heap and direct memory snapshots,
     * logging deltas to stdout for post-run analysis.
     *
     * <p>If the "unpin after every read" design leaks, direct memory or refcounts
     * would grow monotonically across iterations -- this benchmark makes that visible.
     */
    @Benchmark
    public int sliceChurnWithGC(SliceChurnState state, Blackhole bh) throws IOException {
        BlockSlotTinyCache tinyCache = new BlockSlotTinyCache(state.blockCache, state.fakePath, BLOCK_SIZE);

        CachedMemorySegmentIndexInput master = CachedMemorySegmentIndexInput
            .newInstance("bench-gc-master", state.fakePath, BLOCK_SIZE, state.blockCache, NoopReadaheadManager.INSTANCE, null, tinyCache);

        for (int i = 0; i < state.sliceCount; i++) {
            CachedMemorySegmentIndexInput slice = master.slice("gc-s" + i, 0, BLOCK_SIZE);
            bh.consume(slice.readLong());
            // abandon slice
        }

        master.close();

        int refCount = state.refCounted.getRefCount();
        if (refCount != 1) {
            throw new AssertionError("RefCount leak detected! Expected 1, got " + refCount);
        }

        return state.sliceCount;
    }

    // ======================== Benchmark 3: concurrentSliceCreation ========================

    /**
     * Per-thread state for concurrent slice creation benchmark.
     * Each thread gets its own master, arena, and block -- no shared mutable state
     * between threads except the JMH infrastructure.
     */
    @State(Scope.Thread)
    public static class ConcurrentSliceState {

        @Param({ "100", "1000", "10000" })
        int sliceCount;

        Arena arena;
        RefCountedMemorySegment refCounted;
        SingleBlockCache blockCache;
        Path fakePath;

        @Setup(Level.Trial)
        public void setup() {
            arena = Arena.ofShared();
            MemorySegment block = arena.allocate(BLOCK_SIZE);

            for (int i = 0; i < LONGS_PER_BLOCK; i++) {
                block.set(LAYOUT_LE_LONG, (long) i * Long.BYTES, (long) i);
            }

            refCounted = new RefCountedMemorySegment(block, BLOCK_SIZE, seg -> {});
            FakeCacheValue cacheValue = new FakeCacheValue(refCounted);
            blockCache = new SingleBlockCache(cacheValue);

            // Unique path per thread to avoid FileBlockCacheKey path normalization contention
            fakePath = Paths.get("/bench/concurrent-" + Thread.currentThread().threadId() + ".dat");
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            if (arena != null) {
                arena.close();
            }
        }
    }

    /**
     * Multiple threads simultaneously creating slices from independent masters,
     * reading, and abandoning. Each thread has its own master (via {@code Scope.Thread}
     * state), so this stresses the per-instance slice lifecycle and validates that
     * cleanup is correct under concurrent execution.
     *
     * <p>With 8 threads each creating up to 10,000 slices, this exercises 80,000
     * concurrent slice create-read-abandon cycles. The refcount assertion after
     * each master.close() ensures no cross-thread interference in pin management.
     */
    @Benchmark
    @Threads(8)
    public int concurrentSliceCreation(ConcurrentSliceState state, Blackhole bh) throws IOException {
        BlockSlotTinyCache tinyCache = new BlockSlotTinyCache(state.blockCache, state.fakePath, BLOCK_SIZE);

        CachedMemorySegmentIndexInput master = CachedMemorySegmentIndexInput
            .newInstance("bench-concurrent", state.fakePath, BLOCK_SIZE, state.blockCache, NoopReadaheadManager.INSTANCE, null, tinyCache);

        for (int i = 0; i < state.sliceCount; i++) {
            CachedMemorySegmentIndexInput slice = master.slice("c-s" + i, 0, BLOCK_SIZE);
            bh.consume(slice.readLong());
            // abandon slice
        }

        master.close();

        int refCount = state.refCounted.getRefCount();
        if (refCount != 1) {
            throw new AssertionError("RefCount leak in thread " + Thread.currentThread().getName() + "! Expected 1, got " + refCount);
        }

        return state.sliceCount;
    }
}

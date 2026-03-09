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
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.read_ahead.ReadaheadContext;
import org.opensearch.index.store.read_ahead.ReadaheadManager;

/**
 * JMH benchmark measuring readLong() throughput for sequential reads within a single 8KB block,
 * comparing master IndexInput vs slice IndexInput.
 *
 * <p>This quantifies the latency penalty on slices caused by {@code releasePinnedBlockIfSlice()}
 * which nulls the cached block after every read for slices. Master inputs reuse the cached block
 * (fast path ~10-20ns) while slices must go through L1 cache lookup + pin/unpin CAS every time
 * (~180-600ns).
 *
 * <p>Run with: {@code ./gradlew jmh -Pjmh.includes='SliceVsMasterReadBench'}
 */
@State(Scope.Thread)
@BenchmarkMode({ Mode.Throughput, Mode.AverageTime })
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 10, time = 3)
@Fork(value = 3, jvmArgs = { "--enable-preview", "--enable-native-access=ALL-UNNAMED", "-XX:+UseZGC" })
@SuppressWarnings("preview")
public class SliceVsMasterReadBench {

    private static final int BLOCK_SIZE = 8192; // StaticConfigs.CACHE_BLOCK_SIZE
    private static final ValueLayout.OfLong LAYOUT_LE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    /** How many longs fit in one 8KB block. */
    private static final int LONGS_PER_BLOCK = BLOCK_SIZE / Long.BYTES; // 1024

    /** We read this many longs before wrapping back to position 0. */
    private static final long BLOCK_END = (long) LONGS_PER_BLOCK * Long.BYTES; // 8192

    private Arena arena;
    private CachedMemorySegmentIndexInput master;
    private CachedMemorySegmentIndexInput slice;

    // ---------- lightweight fakes (no encryption, no Caffeine, no disk I/O) ----------

    /**
     * A trivial BlockCache backed by a single pre-allocated memory segment.
     * Every get/getOrLoad returns the same block -- good enough for a single-block benchmark.
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

    @Setup(Level.Trial)
    public void setup() throws IOException {
        arena = Arena.ofShared();

        // Allocate one 8KB block and fill with sequential long values
        MemorySegment segment = arena.allocate(BLOCK_SIZE);
        for (int i = 0; i < LONGS_PER_BLOCK; i++) {
            segment.set(LAYOUT_LE_LONG, (long) i * Long.BYTES, (long) i);
        }

        // Wrap in RefCountedMemorySegment with no-op releaser
        RefCountedMemorySegment refCounted = new RefCountedMemorySegment(segment, BLOCK_SIZE, seg -> {});

        FakeCacheValue cacheValue = new FakeCacheValue(refCounted);
        SingleBlockCache blockCache = new SingleBlockCache(cacheValue);
        Path fakePath = Paths.get("/bench/test.dat");

        // BlockSlotTinyCache needs a real BlockCache that returns pinnable values
        BlockSlotTinyCache tinyCache = new BlockSlotTinyCache(blockCache, fakePath, BLOCK_SIZE);

        // Create master IndexInput (isSlice=false) over the single block
        master = CachedMemorySegmentIndexInput
            .newInstance(
                "bench-master",
                fakePath,
                BLOCK_SIZE,
                blockCache,
                NoopReadaheadManager.INSTANCE,
                null, // no readahead context
                tinyCache
            );

        // Create slice IndexInput (isSlice=true) covering the same range
        slice = master.slice("bench-slice", 0, BLOCK_SIZE);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        if (slice != null)
            slice.close();
        if (master != null)
            master.close();
        if (arena != null)
            arena.close();
    }

    /**
     * Master readLong: exercises the fast path where currentBlock is reused
     * across calls (no unpin/re-pin overhead).
     * Expected: ~10-20ns per operation.
     */
    @Benchmark
    public long masterReadLong() throws IOException {
        if (master.getFilePointer() >= BLOCK_END) {
            master.seek(0);
        }
        return master.readLong();
    }

    /**
     * Slice readLong: exercises the slow path where releasePinnedBlockIfSlice()
     * nulls the cached block after every read, forcing L1 cache lookup + pin/unpin CAS.
     * Expected: ~180-600ns per operation (10-30x slower than master).
     */
    @Benchmark
    public long sliceReadLong() throws IOException {
        if (slice.getFilePointer() >= BLOCK_END) {
            slice.seek(0);
        }
        return slice.readLong();
    }
}

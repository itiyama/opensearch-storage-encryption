/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.pool;

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

/**
 * JMH benchmark measuring {@link MemorySegmentPool} throughput under concurrent load.
 *
 * <p>The pool uses a single {@link java.util.concurrent.locks.ReentrantLock} for both
 * {@code acquire()} and {@code release()}. Under concurrent search with readahead, this
 * lock is contested between block loader threads (acquiring), search threads (releasing
 * via unpin), and readahead workers. Security zeroing ({@code segment.fill((byte) 0)})
 * happens inside the lock, extending hold time by ~200ns per release.
 *
 * <p>This benchmark measures:
 * <ul>
 *   <li>Throughput scaling (or collapse) as thread count increases from 1 to 16</li>
 *   <li>Impact of {@code requiresZeroing=true} (zeroing inside lock) vs {@code false}</li>
 * </ul>
 *
 * <p>Run with profilers for deeper insight:
 * <ul>
 *   <li>{@code -prof stack} to see lock contention frames</li>
 *   <li>{@code -prof gc} to confirm zero allocation overhead</li>
 * </ul>
 *
 * <p>Expected result: throughput degrades at higher thread counts due to lock
 * serialization, and zeroing amplifies contention by extending critical section duration.
 */
@State(Scope.Benchmark)
@BenchmarkMode({ Mode.Throughput, Mode.AverageTime })
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 10, time = 3)
@Fork(value = 2, jvmArgs = { "--enable-preview", "-XX:+UseZGC", "--enable-native-access=ALL-UNNAMED" })
public class PoolContentionBench {

    private static final int SEGMENT_SIZE = 8192;
    private static final int NUM_SEGMENTS = 1000;
    private static final long TOTAL_MEMORY = (long) NUM_SEGMENTS * SEGMENT_SIZE;

    private MemorySegmentPool pool;

    @Param({ "true", "false" })
    boolean requiresZeroing;

    @Setup(Level.Trial)
    public void setup() {
        pool = new MemorySegmentPool(TOTAL_MEMORY, SEGMENT_SIZE, requiresZeroing);
        // Pre-fill the pool so acquire() hits the freelist path (warm),
        // not the cold malloc allocation path.
        pool.warmUp(NUM_SEGMENTS);
    }

    @TearDown(Level.Trial)
    public void teardown() {
        pool.close();
    }

    /**
     * Baseline: single-threaded acquire/release cycle.
     * No contention -- establishes the uncontested lock cost.
     */
    @Threads(1)
    @Benchmark
    public void acquireRelease_1thread(Blackhole bh) throws Exception {
        RefCountedMemorySegment seg = pool.tryAcquire(5000, TimeUnit.MILLISECONDS);
        if (seg != null) {
            bh.consume(seg);
            pool.release(seg);
        }
    }

    /**
     * 4 threads: moderate contention, typical of a small search cluster node.
     */
    @Threads(4)
    @Benchmark
    public void acquireRelease_4threads(Blackhole bh) throws Exception {
        RefCountedMemorySegment seg = pool.tryAcquire(5000, TimeUnit.MILLISECONDS);
        if (seg != null) {
            bh.consume(seg);
            pool.release(seg);
        }
    }

    /**
     * 8 threads: heavy contention simulating concurrent search + readahead.
     */
    @Threads(8)
    @Benchmark
    public void acquireRelease_8threads(Blackhole bh) throws Exception {
        RefCountedMemorySegment seg = pool.tryAcquire(5000, TimeUnit.MILLISECONDS);
        if (seg != null) {
            bh.consume(seg);
            pool.release(seg);
        }
    }

    /**
     * 16 threads: extreme contention to expose lock serialization bottleneck.
     */
    @Threads(16)
    @Benchmark
    public void acquireRelease_16threads(Blackhole bh) throws Exception {
        RefCountedMemorySegment seg = pool.tryAcquire(5000, TimeUnit.MILLISECONDS);
        if (seg != null) {
            bh.consume(seg);
            pool.release(seg);
        }
    }
}

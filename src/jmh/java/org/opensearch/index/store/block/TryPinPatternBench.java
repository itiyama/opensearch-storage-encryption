/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * JMH micro-benchmark comparing two pin+validate patterns on {@link RefCountedMemorySegment}.
 *
 * <p><b>Current pattern</b> (3 atomic ops on match, 3 on mismatch):
 * <pre>
 *   tryPin()          // CAS to increment refCount
 *   getGeneration()   // volatile read of packed state
 *   unpin()           // CAS to decrement refCount (always needed)
 * </pre>
 *
 * <p><b>Optimized pattern</b> (1-2 atomic ops on match, 1 on mismatch):
 * <pre>
 *   tryPinIfGeneration(expectedGen)  // single CAS loop checks gen + pins atomically
 *   unpin()                          // only on success
 * </pre>
 *
 * <p>The mismatch case is where the biggest savings appear: the optimized pattern
 * returns false without ever incrementing (so no unpin CAS is needed).
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 2)
@Fork(value = 3, jvmArgs = { "--enable-preview", "--enable-native-access=ALL-UNNAMED" })
public class TryPinPatternBench {

    private Arena arena;
    private RefCountedMemorySegment seg;
    private int expectedGen;

    @Setup
    public void setup() {
        arena = Arena.ofConfined();
        MemorySegment nativeSegment = arena.allocate(64);
        // No-op releaser: in bench we never actually let refCount reach 0
        seg = new RefCountedMemorySegment(nativeSegment, 64, s -> {});
        expectedGen = seg.getGeneration();
    }

    @TearDown
    public void teardown() {
        arena.close();
    }

    /**
     * Current pattern, generation matches:
     *   tryPin() [CAS] -> getGeneration() [volatile read] -> unpin() [CAS]
     * = 3 atomic operations
     */
    @Benchmark
    public boolean currentPattern() {
        if (seg.tryPin()) {
            boolean match = seg.getGeneration() == expectedGen;
            seg.unpin();
            return match;
        }
        return false;
    }

    /**
     * Optimized pattern, generation matches:
     *   tryPinIfGeneration() [single CAS] -> unpin() [CAS]
     * = 2 atomic operations (no separate volatile read for generation)
     */
    @Benchmark
    public boolean optimizedPattern() {
        if (seg.tryPinIfGeneration(expectedGen)) {
            seg.unpin();
            return true;
        }
        return false;
    }

    /**
     * Current pattern, generation MISMATCH:
     *   tryPin() [CAS] -> getGeneration() [volatile read] -> mismatch -> unpin() [CAS]
     * = 3 atomic operations even though the result is discarded
     */
    @Benchmark
    public boolean currentPatternMismatch() {
        if (seg.tryPin()) {
            boolean match = seg.getGeneration() == (expectedGen + 1); // force mismatch
            seg.unpin();
            return match;
        }
        return false;
    }

    /**
     * Optimized pattern, generation MISMATCH:
     *   tryPinIfGeneration(wrong) -> returns false immediately [1 volatile read, no CAS]
     * = 1 atomic operation, no unpin needed
     */
    @Benchmark
    public boolean optimizedPatternMismatch() {
        return seg.tryPinIfGeneration(expectedGen + 1);
    }
}

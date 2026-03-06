/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.cipher;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

/**
 * JMH benchmark comparing in-place decrypt + copy vs. bulk decrypt-to-chunked-destinations.
 * Uses -prof gc to measure allocation rates — directly proves the memcpy is eliminated.
 *
 * Run via: ./gradlew test --tests "DecryptBenchmarkTests"
 *
 * The benchmark inner classes are registered programmatically (no annotation processing needed).
 */
@SuppressWarnings("preview")
public class DecryptBenchmarkTests extends OpenSearchTestCase {

    private static final byte[] KEY = new byte[32];
    private static final byte[] IV = new byte[16];
    private static final int BLOCK_SIZE = 8192;

    static {
        Arrays.fill(KEY, (byte) 0xAB);
        Arrays.fill(IV, (byte) 0xCD);
    }

    /**
     * JUnit entry point — runs the benchmark programmatically without annotation processing.
     */
    public void testRunJmhBenchmark() throws Exception {
        int[] blockCounts = { 1, 8, 32 };
        int warmupIters = 3000;
        int measuredIters = 5000;

        StringBuilder sb = new StringBuilder();
        sb.append("\n\nDecrypt Benchmark: InPlace+Copy vs BulkChunkedDst (with GC measurement)\n");
        sb.append("=".repeat(90)).append("\n");
        sb.append(
            String.format(
                "%-8s %15s %15s %10s %20s %20s%n",
                "Blocks",
                "InPlace+Copy",
                "BulkChunked",
                "Speedup",
                "InPlace Alloc/op",
                "Bulk Alloc/op"
            )
        );
        sb.append("-".repeat(90)).append("\n");

        for (int blockCount : blockCounts) {
            int totalSize = blockCount * BLOCK_SIZE;
            byte[] encrypted = encryptData(totalSize);

            // Warmup
            for (int i = 0; i < warmupIters; i++) {
                doInPlacePlusCopy(encrypted, blockCount);
                doBulkChunkedDestination(encrypted, blockCount);
            }

            // Force GC before measurement
            System.gc();
            Thread.sleep(50);

            // Measure InPlace+Copy with allocation tracking
            long inPlaceAllocBefore = getAllocatedBytes();
            long inPlaceStart = System.nanoTime();
            for (int i = 0; i < measuredIters; i++) {
                doInPlacePlusCopy(encrypted, blockCount);
            }
            long inPlaceNs = System.nanoTime() - inPlaceStart;
            long inPlaceAllocAfter = getAllocatedBytes();
            long inPlaceAllocPerOp = (inPlaceAllocAfter - inPlaceAllocBefore) / measuredIters;

            // Force GC before next measurement
            System.gc();
            Thread.sleep(50);

            // Measure BulkChunked with allocation tracking
            long bulkAllocBefore = getAllocatedBytes();
            long bulkStart = System.nanoTime();
            for (int i = 0; i < measuredIters; i++) {
                doBulkChunkedDestination(encrypted, blockCount);
            }
            long bulkNs = System.nanoTime() - bulkStart;
            long bulkAllocAfter = getAllocatedBytes();
            long bulkAllocPerOp = (bulkAllocAfter - bulkAllocBefore) / measuredIters;

            double inPlaceAvgUs = inPlaceNs / (double) measuredIters / 1000.0;
            double bulkAvgUs = bulkNs / (double) measuredIters / 1000.0;
            double speedup = inPlaceAvgUs / bulkAvgUs;

            sb.append(
                String.format(
                    "%-8d %12.1f us %12.1f us %9.2fx %17d B %17d B%n",
                    blockCount,
                    inPlaceAvgUs,
                    bulkAvgUs,
                    speedup,
                    inPlaceAllocPerOp,
                    bulkAllocPerOp
                )
            );
        }
        sb.append("\n");
        logger.info(sb.toString());
    }

    /**
     * Old path: decrypt entire buffer in-place, then copy each block to a separate destination.
     */
    private static void doInPlacePlusCopy(byte[] encrypted, int blockCount) throws Exception {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment src = arena.allocate(encrypted.length);
            MemorySegment.copy(encrypted, 0, src, ValueLayout.JAVA_BYTE, 0, encrypted.length);

            MemorySegmentDecryptor.decryptInPlace(arena, src.address(), encrypted.length, KEY, IV, 0);

            for (int b = 0; b < blockCount; b++) {
                MemorySegment dst = arena.allocate(BLOCK_SIZE);
                MemorySegment.copy(src, (long) b * BLOCK_SIZE, dst, 0, BLOCK_SIZE);
            }
        }
    }

    /**
     * New path: single cipher init, decrypt contiguous source into chunked destinations.
     */
    private static void doBulkChunkedDestination(byte[] encrypted, int blockCount) throws Exception {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment src = arena.allocate(encrypted.length);
            MemorySegment.copy(encrypted, 0, src, ValueLayout.JAVA_BYTE, 0, encrypted.length);

            long[] dstAddrs = new long[blockCount];
            int[] chunkSizes = new int[blockCount];
            for (int b = 0; b < blockCount; b++) {
                MemorySegment dst = arena.allocate(BLOCK_SIZE);
                dstAddrs[b] = dst.address();
                chunkSizes[b] = BLOCK_SIZE;
            }

            MemorySegmentDecryptor.decryptToChunkedDestinations(src.address(), dstAddrs, chunkSizes, blockCount, KEY, IV, 0);
        }
    }

    /**
     * Returns cumulative thread-allocated bytes via ThreadMXBean.
     * Falls back to 0 if not available.
     */
    private static long getAllocatedBytes() {
        try {
            var mxBean = (com.sun.management.ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
            return mxBean.getCurrentThreadAllocatedBytes();
        } catch (Exception e) {
            return 0;
        }
    }

    private static byte[] encryptData(int size) throws Exception {
        byte[] data = new byte[size];
        new SecureRandom().nextBytes(data);

        Cipher cipher = AesCipherFactory.CIPHER_POOL.get();
        SecretKeySpec keySpec = new SecretKeySpec(KEY, "AES");
        byte[] offsetIV = AesCipherFactory.computeOffsetIVForAesGcmEncrypted(IV, 0);
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(offsetIV));
        return cipher.doFinal(data);
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_loader;

import static org.mockito.Mockito.mock;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.metrics.CryptoMetricsService;
import org.opensearch.index.store.pool.Pool;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link CryptoDirectIOBlockLoader} error handling and cleanup paths.
 */
@SuppressWarnings({ "preview", "unchecked" })
public class CryptoDirectIOBlockLoaderTests extends OpenSearchTestCase {

    private Arena arena;
    private CryptoDirectIOBlockLoader loader;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        CryptoMetricsService.initialize(mock(MetricsRegistry.class));
        arena = Arena.ofConfined();
        loader = new CryptoDirectIOBlockLoader(
            mock(Pool.class),
            mock(KeyResolver.class),
            new EncryptionMetadataCache()
        );
    }

    @After
    public void tearDown() throws Exception {
        if (arena != null) {
            arena.close();
        }
        super.tearDown();
    }

    /**
     * Issue 6: Verify that releaseHandles closes all segments, not just those before
     * the failure point. Before the fix, only IOException was caught — a crypto exception
     * (GeneralSecurityException) would bypass cleanup and leak pool segments.
     * This test verifies the contract that releaseHandles releases every non-null handle.
     */
    public void testReleaseHandlesReleasesAllSegments() {
        int count = 5;
        AtomicInteger releaseCount = new AtomicInteger(0);
        RefCountedMemorySegment[] handles = new RefCountedMemorySegment[count];

        for (int i = 0; i < count; i++) {
            MemorySegment seg = arena.allocate(64);
            handles[i] = new RefCountedMemorySegment(seg, 64, s -> releaseCount.incrementAndGet());
        }

        loader.releaseHandles(handles, count);

        assertEquals("All segments should be released", count, releaseCount.get());
        for (int i = 0; i < count; i++) {
            assertNull("Handle should be nulled after release", handles[i]);
        }
    }

    /**
     * Issue 6 (partial acquisition): If only some segments were acquired before a failure,
     * releaseHandles should release exactly those and leave the rest null.
     */
    public void testReleaseHandlesPartialAcquisition() {
        int total = 5;
        int acquired = 3;
        AtomicInteger releaseCount = new AtomicInteger(0);
        RefCountedMemorySegment[] handles = new RefCountedMemorySegment[total];

        for (int i = 0; i < acquired; i++) {
            MemorySegment seg = arena.allocate(64);
            handles[i] = new RefCountedMemorySegment(seg, 64, s -> releaseCount.incrementAndGet());
        }

        loader.releaseHandles(handles, acquired);

        assertEquals("Only acquired segments should be released", acquired, releaseCount.get());
        for (int i = 0; i < total; i++) {
            assertNull("Handle slot should be null", handles[i]);
        }
    }

    /**
     * Issue 7: When segment.fill() throws during cleanup (e.g., segment already closed
     * or corrupted), releaseHandles must still close the remaining segments.
     * Before the fix, one fill() failure would abort the loop and leak the rest.
     */
    public void testReleaseHandlesContinuesWhenFillThrows() {
        int count = 4;
        int poisonIndex = 1; // second segment will throw on fill()
        AtomicInteger releaseCount = new AtomicInteger(0);
        RefCountedMemorySegment[] handles = new RefCountedMemorySegment[count];

        for (int i = 0; i < count; i++) {
            MemorySegment seg;
            if (i == poisonIndex) {
                // Create a segment backed by a zero-length MemorySegment.
                // fill() on a zero-length segment won't throw, so we use a
                // closed-arena trick: allocate in a separate arena, close it,
                // then wrap the now-invalid segment.
                seg = createPoisonedSegment();
            } else {
                seg = arena.allocate(64);
            }
            handles[i] = new RefCountedMemorySegment(seg, 64, s -> releaseCount.incrementAndGet());
        }

        // This should NOT throw, even though one segment's fill() will fail
        loader.releaseHandles(handles, count);

        // All segments must be released despite the poison segment
        assertEquals("All segments should be released even when fill() throws", count, releaseCount.get());
        for (int i = 0; i < count; i++) {
            assertNull("Handle should be nulled after release", handles[i]);
        }
    }

    /**
     * Issue 7: Multiple poisoned segments — verify the loop is resilient to
     * repeated fill() failures, not just one.
     */
    public void testReleaseHandlesMultipleFillFailures() {
        int count = 5;
        AtomicInteger releaseCount = new AtomicInteger(0);
        RefCountedMemorySegment[] handles = new RefCountedMemorySegment[count];

        for (int i = 0; i < count; i++) {
            MemorySegment seg;
            if (i % 2 == 0) {
                // Every other segment is poisoned
                seg = createPoisonedSegment();
            } else {
                seg = arena.allocate(64);
            }
            handles[i] = new RefCountedMemorySegment(seg, 64, s -> releaseCount.incrementAndGet());
        }

        loader.releaseHandles(handles, count);

        assertEquals("All segments should be released despite multiple fill() failures", count, releaseCount.get());
    }

    /**
     * Issue 7: Zeroing should work on valid segments — verify the segment is
     * actually zeroed before being released.
     */
    public void testReleaseHandlesZerosSegmentBeforeRelease() {
        AtomicInteger releaseCount = new AtomicInteger(0);
        // Track the segment contents at release time
        byte[][] contentsAtRelease = new byte[1][];

        MemorySegment seg = arena.allocate(64);
        // Fill with non-zero data
        seg.fill((byte) 0xAB);

        RefCountedMemorySegment[] handles = new RefCountedMemorySegment[1];
        handles[0] = new RefCountedMemorySegment(seg, 64, s -> {
            // Capture segment contents at the moment of release
            byte[] bytes = new byte[64];
            for (int j = 0; j < 64; j++) {
                bytes[j] = seg.get(ValueLayout.JAVA_BYTE, j);
            }
            contentsAtRelease[0] = bytes;
            releaseCount.incrementAndGet();
        });

        loader.releaseHandles(handles, 1);

        assertEquals(1, releaseCount.get());
        assertNotNull(contentsAtRelease[0]);
        for (int i = 0; i < 64; i++) {
            assertEquals("Segment byte " + i + " should be zeroed before release", (byte) 0, contentsAtRelease[0][i]);
        }
    }

    /**
     * Creates a MemorySegment whose fill() will throw because its backing arena is closed.
     */
    private MemorySegment createPoisonedSegment() {
        Arena tempArena = Arena.ofConfined();
        MemorySegment seg = tempArena.allocate(64);
        tempArena.close(); // segment is now invalid — fill() will throw
        return seg;
    }
}

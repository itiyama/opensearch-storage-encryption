/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.Randomness;
import org.opensearch.index.store.bufferpoolfs.BufferPoolDirectory;
import org.opensearch.index.store.hybrid.HybridCryptoDirectory;
import org.opensearch.index.store.niofs.CryptoNIOFSDirectory;
import org.opensearch.test.OpenSearchTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

/**
 * Tests encryption footer handling under various edge cases:
 * single byte, large multi-frame files, and round-trip verification
 * for various data sizes.
 */
@ThreadLeakFilters(filters = CaffeineThreadLeakFilter.class)
public class CryptoFooterTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        CryptoTestDirectoryFactory.initMetrics();
    }

    @Override
    public void tearDown() throws Exception {
        CryptoTestDirectoryFactory.shutdownAllExecutors();
        super.tearDown();
    }

    private CryptoNIOFSDirectory createDirectory(Path path) throws IOException {
        return CryptoTestDirectoryFactory.createCryptoNIOFSDirectory(path, FSLockFactory.getDefault());
    }

    /**
     * Write a single byte. Footer should be present. fileLength() returns 1.
     */
    public void testSingleByteFile() throws IOException {
        Path dir = createTempDir("footer-singlebyte");

        try (CryptoNIOFSDirectory cryptoDir = createDirectory(dir)) {
            try (IndexOutput out = cryptoDir.createOutput("single.dat", IOContext.DEFAULT)) {
                out.writeByte((byte) 0x42);
            }

            // Verify logical length is 1
            assertEquals("fileLength should be 1 for single byte file", 1, cryptoDir.fileLength("single.dat"));

            // Verify raw file is larger (has footer)
            long rawSize = Files.size(dir.resolve("single.dat"));
            assertTrue("Raw file should be larger than 1 byte due to footer", rawSize > 1);

            // Verify round-trip
            try (IndexInput in = cryptoDir.openInput("single.dat", IOContext.DEFAULT)) {
                assertEquals("File pointer should start at 0", 0, in.getFilePointer());
                assertEquals("Should read back the single byte", 0x42, in.readByte());
                assertEquals("File pointer should be 1 after reading single byte", 1, in.getFilePointer());
            }
        }
    }

    /**
     * Write a large file spanning multiple encryption frames.
     * Verify all frames decrypt correctly via round-trip read.
     */
    public void testLargeMultiFrameFile() throws IOException {
        Path dir = createTempDir("footer-multiframe");
        // 256KB — should span multiple encryption frames (frame size is typically 64KB)
        int dataSize = 256 * 1024;
        byte[] data = new byte[dataSize];
        Randomness.get().nextBytes(data);

        try (CryptoNIOFSDirectory cryptoDir = createDirectory(dir)) {
            try (IndexOutput out = cryptoDir.createOutput("large.dat", IOContext.DEFAULT)) {
                out.writeBytes(data, data.length);
            }

            assertEquals("fileLength should match data written", dataSize, cryptoDir.fileLength("large.dat"));

            try (IndexInput in = cryptoDir.openInput("large.dat", IOContext.DEFAULT)) {
                byte[] readBack = new byte[dataSize];
                in.readBytes(readBack, 0, dataSize);
                assertArrayEquals("Multi-frame round-trip should produce identical data", data, readBack);
            }
        }
    }

    /**
     * Round-trip test for various data sizes including edge cases around
     * encryption frame boundaries.
     */
    public void testRoundTripVariousSizes() throws IOException {
        Path dir = createTempDir("footer-various");
        // Test sizes around common boundaries
        int[] sizes = { 1, 15, 16, 17, 100, 1023, 1024, 1025, 4096, 8191, 8192, 8193, 65535, 65536, 65537, 100000 };

        try (CryptoNIOFSDirectory cryptoDir = createDirectory(dir)) {
            for (int size : sizes) {
                String fileName = "data_" + size + ".dat";
                byte[] data = new byte[size];
                Randomness.get().nextBytes(data);

                try (IndexOutput out = cryptoDir.createOutput(fileName, IOContext.DEFAULT)) {
                    out.writeBytes(data, data.length);
                }

                assertEquals("fileLength should match for size " + size, size, cryptoDir.fileLength(fileName));

                try (IndexInput in = cryptoDir.openInput(fileName, IOContext.DEFAULT)) {
                    byte[] readBack = new byte[size];
                    in.readBytes(readBack, 0, size);
                    assertArrayEquals("Round-trip failed for size " + size, data, readBack);
                }
            }
        }
    }

    /**
     * Verify that the raw file on disk is larger than the logical data
     * due to the encryption footer, for non-passthrough files.
     */
    public void testFooterAddsOverhead() throws IOException {
        Path dir = createTempDir("footer-overhead");
        int dataSize = 1000;
        byte[] data = new byte[dataSize];
        Randomness.get().nextBytes(data);

        try (CryptoNIOFSDirectory cryptoDir = createDirectory(dir)) {
            try (IndexOutput out = cryptoDir.createOutput("overhead.dat", IOContext.DEFAULT)) {
                out.writeBytes(data, data.length);
            }

            long rawSize = Files.size(dir.resolve("overhead.dat"));
            long logicalSize = cryptoDir.fileLength("overhead.dat");

            assertEquals("Logical size should match data written", dataSize, logicalSize);
            assertTrue("Raw size (" + rawSize + ") should exceed logical size (" + logicalSize + ")", rawSize > logicalSize);
        }
    }

    /**
     * Write data, read partial chunks at various offsets using seek.
     * Verifies footer/frame alignment doesn't break random access.
     */
    public void testRandomAccessAcrossFrames() throws IOException {
        Path dir = createTempDir("footer-randomaccess");
        int dataSize = 128 * 1024; // 128KB
        byte[] data = new byte[dataSize];
        Randomness.get().nextBytes(data);

        try (CryptoNIOFSDirectory cryptoDir = createDirectory(dir)) {
            try (IndexOutput out = cryptoDir.createOutput("random.dat", IOContext.DEFAULT)) {
                out.writeBytes(data, data.length);
            }

            try (IndexInput in = cryptoDir.openInput("random.dat", IOContext.DEFAULT)) {
                // Read at various offsets
                int[] offsets = { 0, 1, 100, 8191, 8192, 8193, 65535, 65536, 65537, dataSize - 1 };
                for (int offset : offsets) {
                    in.seek(offset);
                    assertEquals("File pointer wrong after seek to " + offset, offset, in.getFilePointer());
                    assertEquals("Byte mismatch at offset " + offset, data[offset], in.readByte());
                    assertEquals("File pointer wrong after read at " + offset, offset + 1, in.getFilePointer());
                }

                // Read a chunk spanning a likely frame boundary
                int chunkOffset = 65530;
                int chunkSize = 20;
                in.seek(chunkOffset);
                byte[] chunk = new byte[chunkSize];
                in.readBytes(chunk, 0, chunkSize);
                assertEquals("File pointer wrong after chunk read", chunkOffset + chunkSize, in.getFilePointer());
                for (int i = 0; i < chunkSize; i++) {
                    assertEquals("Chunk byte mismatch at " + (chunkOffset + i), data[chunkOffset + i], chunk[i]);
                }
            }
        }
    }

    /**
     * BufferPoolDirectory.openInput throws IOException for zero-byte files.
     */
    public void testEmptyFileThrowsOnBufferPoolOpen() throws IOException {
        Path dir = createTempDir("footer-empty");

        try (BufferPoolDirectory bpDir = CryptoTestDirectoryFactory.createBufferPoolDirectory(dir, FSLockFactory.getDefault())) {
            // Create a truly empty file on disk (encrypted createOutput writes footer bytes)
            Files.createFile(dir.resolve("empty.dat"));

            IOException ex = expectThrows(IOException.class, () -> bpDir.openInput("empty.dat", IOContext.DEFAULT));
            assertTrue("Should mention empty file", ex.getMessage().contains("Cannot open empty file"));
        }
    }

    /**
     * Verify footer round-trip through BufferPoolDirectory (via HybridCryptoDirectory)
     * for a file that routes to the BufferPool (Direct I/O) path.
     */
    public void testFooterRoundTripViaHybridBufferPoolPath() throws IOException {
        Path dir = createTempDir("footer-hybrid-bp");
        int dataSize = 100_000;
        byte[] data = new byte[dataSize];
        Randomness.get().nextBytes(data);

        try (HybridCryptoDirectory hybridDir = CryptoTestDirectoryFactory.createHybridCryptoDirectory(dir, FSLockFactory.getDefault())) {
            // .tim routes to BufferPool
            try (IndexOutput out = hybridDir.createOutput("_0.tim", IOContext.DEFAULT)) {
                out.writeBytes(data, data.length);
            }

            assertEquals("fileLength should match data written", dataSize, hybridDir.fileLength("_0.tim"));

            long rawSize = Files.size(dir.resolve("_0.tim"));
            assertTrue("Raw size should exceed logical size due to footer", rawSize > dataSize);

            try (IndexInput in = hybridDir.openInput("_0.tim", IOContext.DEFAULT)) {
                byte[] readBack = new byte[dataSize];
                in.readBytes(readBack, 0, dataSize);
                assertArrayEquals("BufferPool-path round-trip should produce identical data", data, readBack);
            }
        }
    }
}

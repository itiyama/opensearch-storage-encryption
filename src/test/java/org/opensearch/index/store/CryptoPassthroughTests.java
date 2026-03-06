/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

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
 * Tests that segments_* and .si files bypass encryption (passthrough)
 * while all other files are properly encrypted on disk.
 */
@ThreadLeakFilters(filters = CaffeineThreadLeakFilter.class)
public class CryptoPassthroughTests extends OpenSearchTestCase {

    private static final String PLAINTEXT_MARKER = "PLAINTEXT_MARKER_FOR_PASSTHROUGH_TEST_12345";

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

    // ==================== CryptoNIOFSDirectory ====================

    public void testNIOFS_SegmentsFileIsNotEncrypted() throws IOException {
        Path dir = createTempDir("passthrough-niofs");
        byte[] data = PLAINTEXT_MARKER.getBytes(StandardCharsets.UTF_8);

        try (CryptoNIOFSDirectory cryptoDir = CryptoTestDirectoryFactory.createCryptoNIOFSDirectory(dir, FSLockFactory.getDefault())) {
            try (IndexOutput out = cryptoDir.createOutput("segments_1", IOContext.DEFAULT)) {
                out.writeBytes(data, data.length);
            }
        }

        // Read raw bytes — should be plaintext since segments_* is passthrough
        byte[] rawBytes = Files.readAllBytes(dir.resolve("segments_1"));
        String rawString = new String(rawBytes, StandardCharsets.UTF_8);
        assertTrue("segments_ file should be plaintext on disk", rawString.contains(PLAINTEXT_MARKER));
    }

    public void testNIOFS_SiFileIsNotEncrypted() throws IOException {
        Path dir = createTempDir("passthrough-niofs-si");
        byte[] data = PLAINTEXT_MARKER.getBytes(StandardCharsets.UTF_8);

        try (CryptoNIOFSDirectory cryptoDir = CryptoTestDirectoryFactory.createCryptoNIOFSDirectory(dir, FSLockFactory.getDefault())) {
            try (IndexOutput out = cryptoDir.createOutput("_0.si", IOContext.DEFAULT)) {
                out.writeBytes(data, data.length);
            }
        }

        byte[] rawBytes = Files.readAllBytes(dir.resolve("_0.si"));
        String rawString = new String(rawBytes, StandardCharsets.UTF_8);
        assertTrue(".si file should be plaintext on disk", rawString.contains(PLAINTEXT_MARKER));
    }

    public void testNIOFS_RegularFileIsEncrypted() throws IOException {
        Path dir = createTempDir("passthrough-niofs-enc");
        byte[] data = PLAINTEXT_MARKER.getBytes(StandardCharsets.UTF_8);

        try (CryptoNIOFSDirectory cryptoDir = CryptoTestDirectoryFactory.createCryptoNIOFSDirectory(dir, FSLockFactory.getDefault())) {
            try (IndexOutput out = cryptoDir.createOutput("_0.cfs", IOContext.DEFAULT)) {
                out.writeBytes(data, data.length);
            }
        }

        byte[] rawBytes = Files.readAllBytes(dir.resolve("_0.cfs"));
        String rawString = new String(rawBytes, StandardCharsets.UTF_8);
        assertFalse("Regular file should be encrypted on disk", rawString.contains(PLAINTEXT_MARKER));
    }

    public void testNIOFS_SegmentsFileLengthIsRawSize() throws IOException {
        Path dir = createTempDir("passthrough-niofs-len");
        byte[] data = new byte[100];
        Randomness.get().nextBytes(data);

        try (CryptoNIOFSDirectory cryptoDir = CryptoTestDirectoryFactory.createCryptoNIOFSDirectory(dir, FSLockFactory.getDefault())) {
            try (IndexOutput out = cryptoDir.createOutput("segments_1", IOContext.DEFAULT)) {
                out.writeBytes(data, data.length);
            }
            // For passthrough files, fileLength should equal the raw file size
            long reportedLength = cryptoDir.fileLength("segments_1");
            long rawLength = Files.size(dir.resolve("segments_1"));
            assertEquals("segments_ fileLength should match data written", data.length, reportedLength);
            assertEquals("segments_ fileLength should match raw file size", rawLength, reportedLength);
        }
    }

    public void testNIOFS_EncryptedFileLengthIsLogicalSize() throws IOException {
        Path dir = createTempDir("passthrough-niofs-enclen");
        byte[] data = new byte[100];
        Randomness.get().nextBytes(data);

        try (CryptoNIOFSDirectory cryptoDir = CryptoTestDirectoryFactory.createCryptoNIOFSDirectory(dir, FSLockFactory.getDefault())) {
            try (IndexOutput out = cryptoDir.createOutput("_0.cfs", IOContext.DEFAULT)) {
                out.writeBytes(data, data.length);
            }
            long reportedLength = cryptoDir.fileLength("_0.cfs");
            long rawLength = Files.size(dir.resolve("_0.cfs"));
            // Encrypted file has footer, so raw > logical
            assertTrue("Encrypted file raw size should be larger than logical size", rawLength > reportedLength);
            assertEquals("Encrypted fileLength should equal data written", data.length, reportedLength);
        }
    }

    public void testNIOFS_ListAllIncludesBothTypes() throws IOException {
        Path dir = createTempDir("passthrough-niofs-list");

        try (CryptoNIOFSDirectory cryptoDir = CryptoTestDirectoryFactory.createCryptoNIOFSDirectory(dir, FSLockFactory.getDefault())) {
            try (IndexOutput out = cryptoDir.createOutput("segments_1", IOContext.DEFAULT)) {
                out.writeByte((byte) 1);
            }
            try (IndexOutput out = cryptoDir.createOutput("_0.cfs", IOContext.DEFAULT)) {
                out.writeByte((byte) 2);
            }

            String[] files = cryptoDir.listAll();
            Set<String> fileSet = Set.of(files);
            assertTrue("listAll should include segments_1", fileSet.contains("segments_1"));
            assertTrue("listAll should include _0.cfs", fileSet.contains("_0.cfs"));
        }
    }

    // ==================== BufferPoolDirectory ====================

    public void testBufferPool_SegmentsFileIsNotEncrypted() throws IOException {
        Path dir = createTempDir("passthrough-bp");
        byte[] data = PLAINTEXT_MARKER.getBytes(StandardCharsets.UTF_8);

        // Note: BufferPoolDirectory.openInput has no passthrough check (by design —
        // it is always used via HybridCryptoDirectory which routes passthrough files
        // to the NIO delegate). So we only verify the write path here.
        try (BufferPoolDirectory bpDir = CryptoTestDirectoryFactory.createBufferPoolDirectory(dir, FSLockFactory.getDefault())) {
            try (IndexOutput out = bpDir.createOutput("segments_1", IOContext.DEFAULT)) {
                out.writeBytes(data, data.length);
            }
        }

        byte[] rawBytes = Files.readAllBytes(dir.resolve("segments_1"));
        String rawString = new String(rawBytes, StandardCharsets.UTF_8);
        assertTrue("BufferPool: segments_ file should be plaintext", rawString.contains(PLAINTEXT_MARKER));
    }

    public void testBufferPool_SiFileIsNotEncrypted() throws IOException {
        Path dir = createTempDir("passthrough-bp-si");
        byte[] data = PLAINTEXT_MARKER.getBytes(StandardCharsets.UTF_8);

        try (BufferPoolDirectory bpDir = CryptoTestDirectoryFactory.createBufferPoolDirectory(dir, FSLockFactory.getDefault())) {
            try (IndexOutput out = bpDir.createOutput("_0.si", IOContext.DEFAULT)) {
                out.writeBytes(data, data.length);
            }
        }

        byte[] rawBytes = Files.readAllBytes(dir.resolve("_0.si"));
        String rawString = new String(rawBytes, StandardCharsets.UTF_8);
        assertTrue("BufferPool: .si file should be plaintext", rawString.contains(PLAINTEXT_MARKER));
    }

    public void testBufferPool_RegularFileIsEncrypted() throws IOException {
        Path dir = createTempDir("passthrough-bp-enc");
        byte[] data = PLAINTEXT_MARKER.getBytes(StandardCharsets.UTF_8);

        try (BufferPoolDirectory bpDir = CryptoTestDirectoryFactory.createBufferPoolDirectory(dir, FSLockFactory.getDefault())) {
            try (IndexOutput out = bpDir.createOutput("_0.cfs", IOContext.DEFAULT)) {
                out.writeBytes(data, data.length);
            }
        }

        byte[] rawBytes = Files.readAllBytes(dir.resolve("_0.cfs"));
        String rawString = new String(rawBytes, StandardCharsets.UTF_8);
        assertFalse("BufferPool: regular file should be encrypted", rawString.contains(PLAINTEXT_MARKER));
    }

    // ==================== HybridCryptoDirectory ====================

    public void testHybrid_SegmentsFileIsNotEncrypted() throws IOException {
        Path dir = createTempDir("passthrough-hybrid");
        byte[] data = PLAINTEXT_MARKER.getBytes(StandardCharsets.UTF_8);

        try (HybridCryptoDirectory hybridDir = CryptoTestDirectoryFactory.createHybridCryptoDirectory(dir, FSLockFactory.getDefault())) {
            try (IndexOutput out = hybridDir.createOutput("segments_1", IOContext.DEFAULT)) {
                out.writeBytes(data, data.length);
            }

            // Verify round-trip read-back through the directory works
            try (IndexInput in = hybridDir.openInput("segments_1", IOContext.DEFAULT)) {
                byte[] readBack = new byte[data.length];
                in.readBytes(readBack, 0, readBack.length);
                assertArrayEquals("Hybrid: passthrough round-trip should work", data, readBack);
            }
        }

        // Verify raw bytes on disk are plaintext
        byte[] rawBytes = Files.readAllBytes(dir.resolve("segments_1"));
        String rawString = new String(rawBytes, StandardCharsets.UTF_8);
        assertTrue("Hybrid: segments_ file should be plaintext", rawString.contains(PLAINTEXT_MARKER));
    }

    public void testHybrid_SiFileIsNotEncrypted() throws IOException {
        Path dir = createTempDir("passthrough-hybrid-si");
        byte[] data = PLAINTEXT_MARKER.getBytes(StandardCharsets.UTF_8);

        try (HybridCryptoDirectory hybridDir = CryptoTestDirectoryFactory.createHybridCryptoDirectory(dir, FSLockFactory.getDefault())) {
            try (IndexOutput out = hybridDir.createOutput("_0.si", IOContext.DEFAULT)) {
                out.writeBytes(data, data.length);
            }

            // Verify round-trip read-back through the directory works
            try (IndexInput in = hybridDir.openInput("_0.si", IOContext.DEFAULT)) {
                byte[] readBack = new byte[data.length];
                in.readBytes(readBack, 0, readBack.length);
                assertArrayEquals("Hybrid: .si passthrough round-trip should work", data, readBack);
            }
        }

        byte[] rawBytes = Files.readAllBytes(dir.resolve("_0.si"));
        String rawString = new String(rawBytes, StandardCharsets.UTF_8);
        assertTrue("Hybrid: .si file should be plaintext", rawString.contains(PLAINTEXT_MARKER));
    }

    public void testHybrid_RegularFileIsEncrypted() throws IOException {
        Path dir = createTempDir("passthrough-hybrid-enc");
        byte[] data = PLAINTEXT_MARKER.getBytes(StandardCharsets.UTF_8);

        try (HybridCryptoDirectory hybridDir = CryptoTestDirectoryFactory.createHybridCryptoDirectory(dir, FSLockFactory.getDefault())) {
            try (IndexOutput out = hybridDir.createOutput("_0.tim", IOContext.DEFAULT)) {
                out.writeBytes(data, data.length);
            }
        }

        byte[] rawBytes = Files.readAllBytes(dir.resolve("_0.tim"));
        String rawString = new String(rawBytes, StandardCharsets.UTF_8);
        assertFalse("Hybrid: regular file should be encrypted", rawString.contains(PLAINTEXT_MARKER));
    }
}

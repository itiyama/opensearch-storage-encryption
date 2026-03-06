/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.hybrid;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;

import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.Randomness;
import org.opensearch.index.store.CaffeineThreadLeakFilter;
import org.opensearch.index.store.CryptoTestDirectoryFactory;
import org.opensearch.test.OpenSearchTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

/**
 * Tests that HybridCryptoDirectory correctly routes file operations based on
 * file extension — files NOT in nioExtensions go to BufferPoolDirectory (Direct I/O),
 * files IN nioExtensions go to CryptoNIOFSDirectory.
 */
@ThreadLeakFilters(filters = CaffeineThreadLeakFilter.class)
public class HybridRoutingIntegTests extends OpenSearchTestCase {

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

    private HybridCryptoDirectory createHybridDirectory(Path path) throws IOException {
        return CryptoTestDirectoryFactory.createHybridCryptoDirectory(path, FSLockFactory.getDefault());
    }

    /**
     * Write files with extensions that should route to BufferPool (Direct I/O path)
     * and verify round-trip read works.
     */
    public void testBufferPoolRoutedFilesRoundTrip() throws IOException {
        Path dir = createTempDir("hybrid-routing-bp");
        String[] bpExtensions = { "_0.tim", "_0.doc", "_0.dvd", "_0.nvd", "_0.cfs" };

        try (HybridCryptoDirectory hybridDir = createHybridDirectory(dir)) {
            for (String fileName : bpExtensions) {
                byte[] data = new byte[64];
                Randomness.get().nextBytes(data);

                try (IndexOutput out = hybridDir.createOutput(fileName, IOContext.DEFAULT)) {
                    out.writeBytes(data, data.length);
                }

                try (IndexInput in = hybridDir.openInput(fileName, IOContext.DEFAULT)) {
                    byte[] readBack = new byte[data.length];
                    in.readBytes(readBack, 0, readBack.length);
                    assertArrayEquals("Round-trip failed for " + fileName, data, readBack);
                }
            }
        }
    }

    /**
     * Write files with NIO extensions and verify round-trip read works.
     */
    public void testNIORoutedFilesRoundTrip() throws IOException {
        Path dir = createTempDir("hybrid-routing-nio");
        String[] nioFiles = { "_0.si", "_0.cfe", "_0.fnm", "_0.fdx", "_0.fdm" };

        try (HybridCryptoDirectory hybridDir = createHybridDirectory(dir)) {
            for (String fileName : nioFiles) {
                byte[] data = new byte[64];
                Randomness.get().nextBytes(data);

                try (IndexOutput out = hybridDir.createOutput(fileName, IOContext.DEFAULT)) {
                    out.writeBytes(data, data.length);
                }

                try (IndexInput in = hybridDir.openInput(fileName, IOContext.DEFAULT)) {
                    byte[] readBack = new byte[data.length];
                    in.readBytes(readBack, 0, readBack.length);
                    assertArrayEquals("Round-trip failed for " + fileName, data, readBack);
                }
            }
        }
    }

    /**
     * listAll() should return files from both delegates without duplicates.
     */
    public void testListAllReturnsBothDelegates() throws IOException {
        Path dir = createTempDir("hybrid-routing-list");

        try (HybridCryptoDirectory hybridDir = createHybridDirectory(dir)) {
            // Write one file per delegate type
            try (IndexOutput out = hybridDir.createOutput("_0.tim", IOContext.DEFAULT)) {
                out.writeByte((byte) 1);
            }
            try (IndexOutput out = hybridDir.createOutput("_0.si", IOContext.DEFAULT)) {
                out.writeByte((byte) 2);
            }

            String[] files = hybridDir.listAll();
            Set<String> fileSet = Set.of(files);

            assertTrue("listAll should include BufferPool file _0.tim", fileSet.contains("_0.tim"));
            assertTrue("listAll should include NIO file _0.si", fileSet.contains("_0.si"));

            // Check no duplicates
            assertEquals("No duplicate files in listAll", fileSet.size(), files.length);
        }
    }

    /**
     * createTempOutput routes to the NIO (CryptoNIOFS) delegate.
     * Verify the temp file can be written, read back, and appears in listAll.
     */
    public void testCreateTempOutputRoutesToNIO() throws IOException {
        Path dir = createTempDir("hybrid-routing-temp");

        try (HybridCryptoDirectory hybridDir = createHybridDirectory(dir)) {
            String tempName;
            try (IndexOutput out = hybridDir.createTempOutput("foo", "bar", IOContext.DEFAULT)) {
                out.writeVInt(42);
                tempName = out.getName();
                assertNotNull("Temp output should have a name", tempName);
            }

            // Verify the temp file appears in listAll
            String[] files = hybridDir.listAll();
            Set<String> fileSet = Set.of(files);
            assertTrue("Temp file should appear in listAll", fileSet.contains(tempName));

            // Verify round-trip read-back
            try (IndexInput in = hybridDir.openInput(tempName, IOContext.DEFAULT)) {
                assertEquals("Should read back written VInt", 42, in.readVInt());
            }
        }
    }

    /**
     * Write files via both routes, delete one, verify it's gone from listAll.
     */
    public void testDeleteFileFromEitherDelegate() throws IOException {
        Path dir = createTempDir("hybrid-routing-delete");

        try (HybridCryptoDirectory hybridDir = createHybridDirectory(dir)) {
            try (IndexOutput out = hybridDir.createOutput("_0.tim", IOContext.DEFAULT)) {
                out.writeByte((byte) 1);
            }
            try (IndexOutput out = hybridDir.createOutput("_0.si", IOContext.DEFAULT)) {
                out.writeByte((byte) 2);
            }

            // Delete the BufferPool-routed file
            hybridDir.deleteFile("_0.tim");

            String[] files = hybridDir.listAll();
            Set<String> fileSet = Set.of(files);
            assertFalse("Deleted BP file should not appear in listAll", fileSet.contains("_0.tim"));
            assertTrue("Non-deleted NIO file should still appear", fileSet.contains("_0.si"));

            // Delete the NIO-routed file
            hybridDir.deleteFile("_0.si");

            String[] filesAfter = hybridDir.listAll();
            Set<String> fileSetAfter = Set.of(filesAfter);
            assertFalse("Deleted NIO file should not appear in listAll", fileSetAfter.contains("_0.si"));
        }
    }
}

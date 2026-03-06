/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.cipher;

import static org.junit.Assert.assertArrayEquals;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Arrays;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.junit.After;
import org.junit.Before;
import org.opensearch.test.OpenSearchTestCase;

@SuppressWarnings("preview")
public class MemorySegmentDecryptorTests extends OpenSearchTestCase {

    private static final byte[] TEST_KEY = new byte[32]; // 256-bit AES key
    private static final byte[] TEST_IV = new byte[16];  // 128-bit IV
    private static final byte[] TEST_DATA = "Hello World Test Data for Encryption!".getBytes(StandardCharsets.UTF_8);

    private Arena arena;

    static {
        Arrays.fill(TEST_KEY, (byte) 0x42);
        Arrays.fill(TEST_IV, (byte) 0x24);
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        arena = Arena.ofConfined();
    }

    @After
    public void tearDown() throws Exception {
        if (arena != null) {
            arena.close();
        }
        super.tearDown();
    }

    public void testDecryptInPlaceWithArena() throws Exception {
        // Encrypt test data first
        Cipher cipher = AesCipherFactory.CIPHER_POOL.get();
        SecretKeySpec keySpec = new SecretKeySpec(TEST_KEY, "AES");
        byte[] offsetIV = AesCipherFactory.computeOffsetIVForAesGcmEncrypted(TEST_IV, 0);
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(offsetIV));
        byte[] encrypted = cipher.update(TEST_DATA);

        // Allocate memory segment and copy encrypted data
        MemorySegment segment = arena.allocate(encrypted.length);
        for (int i = 0; i < encrypted.length; i++) {
            segment.set(ValueLayout.JAVA_BYTE, i, encrypted[i]);
        }

        // Decrypt in place
        MemorySegmentDecryptor.decryptInPlace(arena, segment.address(), encrypted.length, TEST_KEY, TEST_IV, 0);

        // Verify decrypted data
        byte[] decrypted = new byte[TEST_DATA.length];
        for (int i = 0; i < decrypted.length; i++) {
            decrypted[i] = segment.get(ValueLayout.JAVA_BYTE, i);
        }

        assertArrayEquals(TEST_DATA, decrypted);
    }

    public void testDecryptInPlaceWithGlobalScope() throws Exception {
        // Encrypt test data first
        Cipher cipher = AesCipherFactory.CIPHER_POOL.get();
        SecretKeySpec keySpec = new SecretKeySpec(TEST_KEY, "AES");
        byte[] offsetIV = AesCipherFactory.computeOffsetIVForAesGcmEncrypted(TEST_IV, 0);
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(offsetIV));
        byte[] encrypted = cipher.update(TEST_DATA);

        // Allocate memory segment and copy encrypted data
        MemorySegment segment = arena.allocate(encrypted.length);
        for (int i = 0; i < encrypted.length; i++) {
            segment.set(ValueLayout.JAVA_BYTE, i, encrypted[i]);
        }

        // Decrypt in place using global scope
        MemorySegmentDecryptor.decryptInPlace(segment.address(), encrypted.length, TEST_KEY, TEST_IV, 0);

        // Verify decrypted data
        byte[] decrypted = new byte[TEST_DATA.length];
        for (int i = 0; i < decrypted.length; i++) {
            decrypted[i] = segment.get(ValueLayout.JAVA_BYTE, i);
        }

        assertArrayEquals(TEST_DATA, decrypted);
    }

    public void testDecryptSegment() throws Exception {
        // Encrypt test data first
        Cipher cipher = AesCipherFactory.CIPHER_POOL.get();
        SecretKeySpec keySpec = new SecretKeySpec(TEST_KEY, "AES");
        byte[] offsetIV = AesCipherFactory.computeOffsetIVForAesGcmEncrypted(TEST_IV, 0);
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(offsetIV));
        byte[] encrypted = cipher.update(TEST_DATA);

        // Allocate memory segment and copy encrypted data
        MemorySegment segment = arena.allocate(encrypted.length);
        for (int i = 0; i < encrypted.length; i++) {
            segment.set(ValueLayout.JAVA_BYTE, i, encrypted[i]);
        }

        // Decrypt segment
        MemorySegmentDecryptor.decryptSegment(segment, 0, TEST_KEY, TEST_IV, encrypted.length);

        // Verify decrypted data
        byte[] decrypted = new byte[TEST_DATA.length];
        for (int i = 0; i < decrypted.length; i++) {
            decrypted[i] = segment.get(ValueLayout.JAVA_BYTE, i);
        }

        assertArrayEquals(TEST_DATA, decrypted);
    }

    public void testDecryptWithFileOffset() throws Exception {
        long fileOffset = 1024;

        // Encrypt test data with offset-aware IV
        Cipher cipher = AesCipherFactory.CIPHER_POOL.get();
        SecretKeySpec keySpec = new SecretKeySpec(TEST_KEY, "AES");
        byte[] offsetIV = AesCipherFactory.computeOffsetIVForAesGcmEncrypted(TEST_IV, fileOffset);
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(offsetIV));

        // Advance cipher for non-aligned offset
        if (fileOffset % (1 << AesCipherFactory.AES_BLOCK_SIZE_BYTES_IN_POWER) > 0) {
            byte[] skip = new byte[(int) (fileOffset % (1 << AesCipherFactory.AES_BLOCK_SIZE_BYTES_IN_POWER))];
            cipher.update(skip);
        }

        byte[] encrypted = cipher.update(TEST_DATA);

        // Allocate memory segment and copy encrypted data
        MemorySegment segment = arena.allocate(encrypted.length);
        for (int i = 0; i < encrypted.length; i++) {
            segment.set(ValueLayout.JAVA_BYTE, i, encrypted[i]);
        }

        // Decrypt with file offset
        MemorySegmentDecryptor.decryptInPlace(arena, segment.address(), encrypted.length, TEST_KEY, TEST_IV, fileOffset);

        // Verify decrypted data
        byte[] decrypted = new byte[TEST_DATA.length];
        for (int i = 0; i < decrypted.length; i++) {
            decrypted[i] = segment.get(ValueLayout.JAVA_BYTE, i);
        }

        assertArrayEquals(TEST_DATA, decrypted);
    }

    public void testDecryptLargeData() throws Exception {
        // Test with data larger than default chunk size
        byte[] largeData = new byte[32768]; // 32KB
        new SecureRandom().nextBytes(largeData);

        // Encrypt
        Cipher cipher = AesCipherFactory.CIPHER_POOL.get();
        SecretKeySpec keySpec = new SecretKeySpec(TEST_KEY, "AES");
        byte[] offsetIV = AesCipherFactory.computeOffsetIVForAesGcmEncrypted(TEST_IV, 0);
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(offsetIV));
        byte[] encrypted = cipher.doFinal(largeData);

        // Allocate memory segment and copy encrypted data
        MemorySegment segment = arena.allocate(encrypted.length);
        for (int i = 0; i < encrypted.length; i++) {
            segment.set(ValueLayout.JAVA_BYTE, i, encrypted[i]);
        }

        // Decrypt
        MemorySegmentDecryptor.decryptInPlace(arena, segment.address(), encrypted.length, TEST_KEY, TEST_IV, 0);

        // Verify
        byte[] decrypted = new byte[largeData.length];
        for (int i = 0; i < decrypted.length; i++) {
            decrypted[i] = segment.get(ValueLayout.JAVA_BYTE, i);
        }

        assertArrayEquals(largeData, decrypted);
    }

    public void testDecryptWithNonAlignedOffset() throws Exception {
        long fileOffset = 17; // Non-aligned offset

        // Encrypt with offset
        Cipher cipher = AesCipherFactory.CIPHER_POOL.get();
        SecretKeySpec keySpec = new SecretKeySpec(TEST_KEY, "AES");
        byte[] offsetIV = AesCipherFactory.computeOffsetIVForAesGcmEncrypted(TEST_IV, fileOffset);
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(offsetIV));

        // Advance cipher for non-aligned offset
        int skipBytes = (int) (fileOffset % (1 << AesCipherFactory.AES_BLOCK_SIZE_BYTES_IN_POWER));
        if (skipBytes > 0) {
            cipher.update(new byte[skipBytes]);
        }

        byte[] encrypted = cipher.update(TEST_DATA);

        // Allocate and decrypt
        MemorySegment segment = arena.allocate(encrypted.length);
        for (int i = 0; i < encrypted.length; i++) {
            segment.set(ValueLayout.JAVA_BYTE, i, encrypted[i]);
        }

        MemorySegmentDecryptor.decryptInPlace(arena, segment.address(), encrypted.length, TEST_KEY, TEST_IV, fileOffset);

        // Verify
        byte[] decrypted = new byte[TEST_DATA.length];
        for (int i = 0; i < decrypted.length; i++) {
            decrypted[i] = segment.get(ValueLayout.JAVA_BYTE, i);
        }

        assertArrayEquals(TEST_DATA, decrypted);
    }

    public void testDecryptEmptyData() throws Exception {
        byte[] emptyData = new byte[0];

        // Encrypt empty data
        Cipher cipher = AesCipherFactory.CIPHER_POOL.get();
        SecretKeySpec keySpec = new SecretKeySpec(TEST_KEY, "AES");
        byte[] offsetIV = AesCipherFactory.computeOffsetIVForAesGcmEncrypted(TEST_IV, 0);
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(offsetIV));
        byte[] encrypted = cipher.update(emptyData);

        if (encrypted == null || encrypted.length == 0) {
            // Empty encryption is valid
            return;
        }

        // Allocate and decrypt
        MemorySegment segment = arena.allocate(Math.max(1, encrypted.length));
        MemorySegmentDecryptor.decryptInPlace(arena, segment.address(), encrypted.length, TEST_KEY, TEST_IV, 0);

        // Should complete without error
    }

    public void testDecryptMultipleSegments() throws Exception {
        int segmentCount = 5;
        byte[][] originalData = new byte[segmentCount][];
        MemorySegment[] segments = new MemorySegment[segmentCount];

        for (int i = 0; i < segmentCount; i++) {
            originalData[i] = ("Segment " + i + " data content").getBytes(StandardCharsets.UTF_8);

            // Encrypt
            Cipher cipher = AesCipherFactory.CIPHER_POOL.get();
            SecretKeySpec keySpec = new SecretKeySpec(TEST_KEY, "AES");
            byte[] offsetIV = AesCipherFactory.computeOffsetIVForAesGcmEncrypted(TEST_IV, i * 1024L);
            cipher.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(offsetIV));

            if ((i * 1024L) % (1 << AesCipherFactory.AES_BLOCK_SIZE_BYTES_IN_POWER) > 0) {
                cipher.update(new byte[(int) ((i * 1024L) % (1 << AesCipherFactory.AES_BLOCK_SIZE_BYTES_IN_POWER))]);
            }

            byte[] encrypted = cipher.update(originalData[i]);

            // Allocate segment
            segments[i] = arena.allocate(encrypted.length);
            for (int j = 0; j < encrypted.length; j++) {
                segments[i].set(ValueLayout.JAVA_BYTE, j, encrypted[j]);
            }

            // Decrypt
            MemorySegmentDecryptor.decryptSegment(segments[i], i * 1024L, TEST_KEY, TEST_IV, encrypted.length);

            // Verify
            byte[] decrypted = new byte[originalData[i].length];
            for (int j = 0; j < decrypted.length; j++) {
                decrypted[j] = segments[i].get(ValueLayout.JAVA_BYTE, j);
            }

            assertArrayEquals("Segment " + i + " mismatch", originalData[i], decrypted);
        }
    }

    public void testDecryptToDestinationMatchesInPlace() throws Exception {
        // Verify decryptToDestination produces byte-identical output to decryptInPlace
        int dataSize = 32768;
        byte[] testData = new byte[dataSize];
        new SecureRandom().nextBytes(testData);

        // Encrypt
        Cipher cipher = AesCipherFactory.CIPHER_POOL.get();
        SecretKeySpec keySpec = new SecretKeySpec(TEST_KEY, "AES");
        byte[] offsetIV = AesCipherFactory.computeOffsetIVForAesGcmEncrypted(TEST_IV, 0);
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(offsetIV));
        byte[] encrypted = cipher.doFinal(testData);

        // Path A: decryptInPlace
        MemorySegment segA = arena.allocate(encrypted.length);
        for (int i = 0; i < encrypted.length; i++) {
            segA.set(ValueLayout.JAVA_BYTE, i, encrypted[i]);
        }
        MemorySegmentDecryptor.decryptInPlace(arena, segA.address(), encrypted.length, TEST_KEY, TEST_IV, 0);

        // Path B: decryptToDestination (separate src and dst)
        MemorySegment srcSeg = arena.allocate(encrypted.length);
        MemorySegment dstSeg = arena.allocate(encrypted.length);
        for (int i = 0; i < encrypted.length; i++) {
            srcSeg.set(ValueLayout.JAVA_BYTE, i, encrypted[i]);
        }
        MemorySegmentDecryptor.decryptToDestination(srcSeg.address(), dstSeg.address(), encrypted.length, TEST_KEY, TEST_IV, 0);

        // Compare byte-for-byte
        byte[] resultA = new byte[testData.length];
        byte[] resultB = new byte[testData.length];
        for (int i = 0; i < testData.length; i++) {
            resultA[i] = segA.get(ValueLayout.JAVA_BYTE, i);
            resultB[i] = dstSeg.get(ValueLayout.JAVA_BYTE, i);
        }
        assertArrayEquals(resultA, resultB);
        assertArrayEquals(testData, resultB);
    }

    public void testDecryptToDestinationWithOffset() throws Exception {
        long fileOffset = 1024;
        byte[] testData = new byte[8192];
        new SecureRandom().nextBytes(testData);

        // Encrypt with offset
        Cipher cipher = AesCipherFactory.CIPHER_POOL.get();
        SecretKeySpec keySpec = new SecretKeySpec(TEST_KEY, "AES");
        byte[] offsetIV = AesCipherFactory.computeOffsetIVForAesGcmEncrypted(TEST_IV, fileOffset);
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(offsetIV));
        int skipBytes = (int) (fileOffset % (1 << AesCipherFactory.AES_BLOCK_SIZE_BYTES_IN_POWER));
        if (skipBytes > 0) {
            cipher.update(new byte[skipBytes]);
        }
        byte[] encrypted = cipher.doFinal(testData);

        // decryptInPlace
        MemorySegment segA = arena.allocate(encrypted.length);
        for (int i = 0; i < encrypted.length; i++) {
            segA.set(ValueLayout.JAVA_BYTE, i, encrypted[i]);
        }
        MemorySegmentDecryptor.decryptInPlace(arena, segA.address(), encrypted.length, TEST_KEY, TEST_IV, fileOffset);

        // decryptToDestination
        MemorySegment srcSeg = arena.allocate(encrypted.length);
        MemorySegment dstSeg = arena.allocate(encrypted.length);
        for (int i = 0; i < encrypted.length; i++) {
            srcSeg.set(ValueLayout.JAVA_BYTE, i, encrypted[i]);
        }
        MemorySegmentDecryptor.decryptToDestination(srcSeg.address(), dstSeg.address(), encrypted.length, TEST_KEY, TEST_IV, fileOffset);

        byte[] resultA = new byte[testData.length];
        byte[] resultB = new byte[testData.length];
        for (int i = 0; i < testData.length; i++) {
            resultA[i] = segA.get(ValueLayout.JAVA_BYTE, i);
            resultB[i] = dstSeg.get(ValueLayout.JAVA_BYTE, i);
        }
        assertArrayEquals(resultA, resultB);
        assertArrayEquals(testData, resultB);
    }

    public void testDecryptToDestinationPreservesSource() throws Exception {
        byte[] testData = new byte[4096];
        new SecureRandom().nextBytes(testData);

        Cipher cipher = AesCipherFactory.CIPHER_POOL.get();
        SecretKeySpec keySpec = new SecretKeySpec(TEST_KEY, "AES");
        byte[] offsetIV = AesCipherFactory.computeOffsetIVForAesGcmEncrypted(TEST_IV, 0);
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(offsetIV));
        byte[] encrypted = cipher.doFinal(testData);

        MemorySegment srcSeg = arena.allocate(encrypted.length);
        MemorySegment dstSeg = arena.allocate(encrypted.length);
        for (int i = 0; i < encrypted.length; i++) {
            srcSeg.set(ValueLayout.JAVA_BYTE, i, encrypted[i]);
        }

        MemorySegmentDecryptor.decryptToDestination(srcSeg.address(), dstSeg.address(), encrypted.length, TEST_KEY, TEST_IV, 0);

        // Source should still contain ciphertext
        byte[] srcAfter = new byte[encrypted.length];
        for (int i = 0; i < encrypted.length; i++) {
            srcAfter[i] = srcSeg.get(ValueLayout.JAVA_BYTE, i);
        }
        assertArrayEquals("Source buffer should be unmodified", encrypted, srcAfter);
    }

    /**
     * Encrypts data frame-by-frame using per-frame IVs, matching the write path.
     * Each frame gets its own IV derived from directoryKey + messageId + frameNumber.
     */
    private byte[] encryptFrameBased(
        byte[] plaintext,
        byte[] fileKey,
        byte[] directoryKey,
        byte[] messageId,
        long frameSize,
        long fileOffset,
        String filePath,
        EncryptionMetadataCache cache
    ) throws Exception {
        byte[] result = new byte[plaintext.length];
        long remaining = plaintext.length;
        long currentOffset = fileOffset;
        int bufferOffset = 0;

        while (remaining > 0) {
            long frameNumber = currentOffset / frameSize;
            long frameStart = frameNumber * frameSize;
            long frameEnd = frameStart + frameSize;
            long offsetWithinFrame = currentOffset - frameStart;
            int bytesInFrame = (int) Math.min(remaining, frameEnd - currentOffset);

            byte[] frameIV = AesCipherFactory.computeFrameIV(directoryKey, messageId, frameNumber, offsetWithinFrame, filePath, cache);
            byte[] offsetIV = AesCipherFactory.computeOffsetIVForAesGcmEncrypted(frameIV, offsetWithinFrame);

            Cipher cipher = AesCipherFactory.CIPHER_POOL.get();
            cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(fileKey, "AES"), new IvParameterSpec(offsetIV));

            int skip = (int) (offsetWithinFrame & ((1 << AesCipherFactory.AES_BLOCK_SIZE_BYTES_IN_POWER) - 1));
            if (skip > 0) {
                cipher.update(new byte[skip]);
            }

            byte[] encrypted = cipher.update(plaintext, bufferOffset, bytesInFrame);
            System.arraycopy(encrypted, 0, result, bufferOffset, bytesInFrame);

            currentOffset += bytesInFrame;
            bufferOffset += bytesInFrame;
            remaining -= bytesInFrame;
        }
        return result;
    }

    public void testDecryptToDestinationFrameBasedMultiFrame() throws Exception {
        // Use a small frame size (64 bytes) to force multi-frame decryption
        long frameSize = 64;
        int dataSize = 200; // spans 4 frames: [0-63], [64-127], [128-191], [192-199]
        byte[] directoryKey = new byte[32];
        byte[] messageId = new byte[16];
        Arrays.fill(directoryKey, (byte) 0xDD);
        Arrays.fill(messageId, (byte) 0xEE);
        String filePath = "/test/multiframe";
        EncryptionMetadataCache cache = new EncryptionMetadataCache();
        byte[] fileKey = TEST_KEY;

        byte[] plaintext = new byte[dataSize];
        new SecureRandom().nextBytes(plaintext);

        byte[] encrypted = encryptFrameBased(plaintext, fileKey, directoryKey, messageId, frameSize, 0, filePath, cache);

        // Decrypt using decryptToDestinationFrameBased
        MemorySegment srcSeg = arena.allocate(encrypted.length);
        MemorySegment dstSeg = arena.allocate(encrypted.length);
        MemorySegment.copy(encrypted, 0, srcSeg, ValueLayout.JAVA_BYTE, 0, encrypted.length);

        MemorySegmentDecryptor
            .decryptToDestinationFrameBased(
                srcSeg.address(),
                dstSeg.address(),
                dataSize,
                fileKey,
                directoryKey,
                messageId,
                frameSize,
                0,
                filePath,
                cache
            );

        byte[] decrypted = new byte[dataSize];
        for (int i = 0; i < dataSize; i++) {
            decrypted[i] = dstSeg.get(ValueLayout.JAVA_BYTE, i);
        }
        assertArrayEquals("Multi-frame decryptToDestinationFrameBased failed", plaintext, decrypted);
    }

    public void testDecryptToDestinationFrameBasedMultiFrameWithOffset() throws Exception {
        // Start at an offset that's mid-frame to exercise the slow path from the first byte
        long frameSize = 64;
        long fileOffset = 50; // starts in frame 0, 14 bytes from frame end
        int dataSize = 100;   // spans frames 0 and 1
        byte[] directoryKey = new byte[32];
        byte[] messageId = new byte[16];
        Arrays.fill(directoryKey, (byte) 0xAA);
        Arrays.fill(messageId, (byte) 0xBB);
        String filePath = "/test/multiframe_offset";
        EncryptionMetadataCache cache = new EncryptionMetadataCache();
        byte[] fileKey = TEST_KEY;

        byte[] plaintext = new byte[dataSize];
        new SecureRandom().nextBytes(plaintext);

        byte[] encrypted = encryptFrameBased(plaintext, fileKey, directoryKey, messageId, frameSize, fileOffset, filePath, cache);

        MemorySegment srcSeg = arena.allocate(encrypted.length);
        MemorySegment dstSeg = arena.allocate(encrypted.length);
        MemorySegment.copy(encrypted, 0, srcSeg, ValueLayout.JAVA_BYTE, 0, encrypted.length);

        MemorySegmentDecryptor
            .decryptToDestinationFrameBased(
                srcSeg.address(),
                dstSeg.address(),
                dataSize,
                fileKey,
                directoryKey,
                messageId,
                frameSize,
                fileOffset,
                filePath,
                cache
            );

        byte[] decrypted = new byte[dataSize];
        for (int i = 0; i < dataSize; i++) {
            decrypted[i] = dstSeg.get(ValueLayout.JAVA_BYTE, i);
        }
        assertArrayEquals("Multi-frame decryptToDestinationFrameBased with offset failed", plaintext, decrypted);
    }

    public void testDecryptInPlaceFrameBasedMultiFrame() throws Exception {
        long frameSize = 64;
        int dataSize = 200;
        byte[] directoryKey = new byte[32];
        byte[] messageId = new byte[16];
        Arrays.fill(directoryKey, (byte) 0xCC);
        Arrays.fill(messageId, (byte) 0xFF);
        String filePath = "/test/inplace_multiframe";
        EncryptionMetadataCache cache = new EncryptionMetadataCache();
        byte[] fileKey = TEST_KEY;

        byte[] plaintext = new byte[dataSize];
        new SecureRandom().nextBytes(plaintext);

        byte[] encrypted = encryptFrameBased(plaintext, fileKey, directoryKey, messageId, frameSize, 0, filePath, cache);

        MemorySegment seg = arena.allocate(encrypted.length);
        MemorySegment.copy(encrypted, 0, seg, ValueLayout.JAVA_BYTE, 0, encrypted.length);

        MemorySegmentDecryptor
            .decryptInPlaceFrameBased(seg.address(), dataSize, fileKey, directoryKey, messageId, frameSize, 0, filePath, cache);

        byte[] decrypted = new byte[dataSize];
        for (int i = 0; i < dataSize; i++) {
            decrypted[i] = seg.get(ValueLayout.JAVA_BYTE, i);
        }
        assertArrayEquals("Multi-frame decryptInPlaceFrameBased failed", plaintext, decrypted);
    }

    public void testDecryptToChunkedDestinationsFrameBasedMultiFrame() throws Exception {
        // Test the chunked destinations variant across frame boundaries
        long frameSize = 64;
        int chunkSize = 32;
        int chunkCount = 7; // 7 * 32 = 224 bytes, spans 4 frames
        int totalSize = chunkSize * chunkCount;
        byte[] directoryKey = new byte[32];
        byte[] messageId = new byte[16];
        Arrays.fill(directoryKey, (byte) 0x11);
        Arrays.fill(messageId, (byte) 0x22);
        String filePath = "/test/chunked_multiframe";
        EncryptionMetadataCache cache = new EncryptionMetadataCache();
        byte[] fileKey = TEST_KEY;

        byte[] plaintext = new byte[totalSize];
        new SecureRandom().nextBytes(plaintext);

        byte[] encrypted = encryptFrameBased(plaintext, fileKey, directoryKey, messageId, frameSize, 0, filePath, cache);

        MemorySegment srcSeg = arena.allocate(encrypted.length);
        MemorySegment.copy(encrypted, 0, srcSeg, ValueLayout.JAVA_BYTE, 0, encrypted.length);

        long[] dstAddrs = new long[chunkCount];
        int[] chunkSizes = new int[chunkCount];
        MemorySegment[] dstSegs = new MemorySegment[chunkCount];
        for (int i = 0; i < chunkCount; i++) {
            dstSegs[i] = arena.allocate(chunkSize);
            dstAddrs[i] = dstSegs[i].address();
            chunkSizes[i] = chunkSize;
        }

        MemorySegmentDecryptor
            .decryptToChunkedDestinationsFrameBased(
                srcSeg.address(),
                dstAddrs,
                chunkSizes,
                chunkCount,
                fileKey,
                directoryKey,
                messageId,
                frameSize,
                0,
                filePath,
                cache
            );

        // Reassemble and verify
        byte[] decrypted = new byte[totalSize];
        for (int i = 0; i < chunkCount; i++) {
            for (int j = 0; j < chunkSize; j++) {
                decrypted[i * chunkSize + j] = dstSegs[i].get(ValueLayout.JAVA_BYTE, j);
            }
        }
        assertArrayEquals("Multi-frame decryptToChunkedDestinationsFrameBased failed", plaintext, decrypted);
    }

    public void testChunkedDecryption() throws Exception {
        // Test that chunked processing works correctly
        int dataSize = 20000; // Larger than default chunk size
        byte[] testData = new byte[dataSize];
        new SecureRandom().nextBytes(testData);

        // Encrypt
        Cipher cipher = AesCipherFactory.CIPHER_POOL.get();
        SecretKeySpec keySpec = new SecretKeySpec(TEST_KEY, "AES");
        byte[] offsetIV = AesCipherFactory.computeOffsetIVForAesGcmEncrypted(TEST_IV, 0);
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(offsetIV));
        byte[] encrypted = cipher.doFinal(testData);

        // Allocate segment
        MemorySegment segment = arena.allocate(encrypted.length);
        for (int i = 0; i < encrypted.length; i++) {
            segment.set(ValueLayout.JAVA_BYTE, i, encrypted[i]);
        }

        // Decrypt using chunked processing
        MemorySegmentDecryptor.decryptSegment(segment, 0, TEST_KEY, TEST_IV, encrypted.length);

        // Verify
        byte[] decrypted = new byte[testData.length];
        for (int i = 0; i < decrypted.length; i++) {
            decrypted[i] = segment.get(ValueLayout.JAVA_BYTE, i);
        }

        assertArrayEquals(testData, decrypted);
    }
}

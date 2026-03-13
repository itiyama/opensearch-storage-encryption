/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.security.Provider;
import java.security.Security;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.crypto.spec.SecretKeySpec;

import org.apache.lucene.store.LockFactory;
import org.opensearch.common.Randomness;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.block_loader.CryptoDirectIOBlockLoader;
import org.opensearch.index.store.bufferpoolfs.BufferPoolDirectory;
import org.opensearch.index.store.bufferpoolfs.WriteCacheMode;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.hybrid.HybridCryptoDirectory;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.metrics.CryptoMetricsService;
import org.opensearch.index.store.niofs.CryptoNIOFSDirectory;
import org.opensearch.index.store.pool.MemorySegmentPool;
import org.opensearch.index.store.pool.Pool;
import org.opensearch.index.store.read_ahead.Worker;
import org.opensearch.index.store.read_ahead.impl.QueuingWorker;
import org.opensearch.telemetry.metrics.MetricsRegistry;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Shared factory for constructing crypto directory instances in tests.
 * Provides real (not mocked) encryption infrastructure with random keys.
 */
public final class CryptoTestDirectoryFactory {

    private static final Provider PROVIDER = Security.getProvider("SunJCE");
    public static final Set<String> NIO_EXTENSIONS = Set.of("si", "cfe", "fnm", "fdx", "fdm");

    /** No-op worker for tests — avoids threads that trigger Lucene's ThreadLeakControl. */
    public static final Worker NOOP_WORKER = new NoOpWorker();

    private CryptoTestDirectoryFactory() {}

    /**
     * Resolves WriteCacheMode from the {@code tests.cacheMode} system property
     * (set by Gradle). Defaults to WRITE_THROUGH when unset.
     */
    public static WriteCacheMode resolveWriteCacheMode() {
        String value = System.getProperty("tests.cacheMode");
        if (value == null) {
            return WriteCacheMode.WRITE_THROUGH;
        }
        try {
            return WriteCacheMode.valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
            return WriteCacheMode.WRITE_THROUGH;
        }
    }

    /** Ensures CryptoMetricsService is initialized for tests. Safe to call multiple times. */
    public static void initMetrics() {
        try {
            CryptoMetricsService.initialize(mock(MetricsRegistry.class));
        } catch (Exception e) {
            // already initialized
        }
    }

    /** Creates a random 256-bit AES KeyResolver using the test's random source. */
    public static KeyResolver createKeyResolver() {
        byte[] rawKey = new byte[32];
        Randomness.get().nextBytes(rawKey);
        KeyResolver resolver = mock(KeyResolver.class);
        when(resolver.getDataKey()).thenReturn(new SecretKeySpec(rawKey, "AES"));
        return resolver;
    }

    public static CryptoNIOFSDirectory createCryptoNIOFSDirectory(Path path, LockFactory lockFactory) throws IOException {
        initMetrics();
        return new CryptoNIOFSDirectory(lockFactory, path, PROVIDER, createKeyResolver(), new EncryptionMetadataCache());
    }

    public static BufferPoolDirectory createBufferPoolDirectory(Path path, LockFactory lockFactory) throws IOException {
        initMetrics();
        KeyResolver keyResolver = createKeyResolver();
        EncryptionMetadataCache metadataCache = new EncryptionMetadataCache();
        Pool<RefCountedMemorySegment> pool = new MemorySegmentPool(131072, 8192);
        CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment> blockCache =
            createBlockCache(pool, keyResolver, metadataCache, 1000);
        CryptoDirectIOBlockLoader blockLoader = new CryptoDirectIOBlockLoader(pool, keyResolver, metadataCache);

        ExecutorService executor = Executors.newFixedThreadPool(4);
        Worker worker = new QueuingWorker(100, executor);

        return new BufferPoolDirectory(path, lockFactory, PROVIDER, keyResolver, pool, blockCache, blockLoader, worker, metadataCache, resolveWriteCacheMode());
    }

    public static HybridCryptoDirectory createHybridCryptoDirectory(Path path, LockFactory lockFactory) throws IOException {
        initMetrics();
        KeyResolver keyResolver = createKeyResolver();
        EncryptionMetadataCache metadataCache = new EncryptionMetadataCache();
        Pool<RefCountedMemorySegment> pool = new MemorySegmentPool(131072, 8192);
        CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment> blockCache =
            createBlockCache(pool, keyResolver, metadataCache, 1000);
        CryptoDirectIOBlockLoader blockLoader = new CryptoDirectIOBlockLoader(pool, keyResolver, metadataCache);

        ExecutorService executor = Executors.newFixedThreadPool(4);
        Worker worker = new QueuingWorker(100, executor);

        BufferPoolDirectory bufferPoolDir = new BufferPoolDirectory(
            path, lockFactory, PROVIDER, keyResolver, pool, blockCache, blockLoader, worker, metadataCache, resolveWriteCacheMode()
        );

        return new HybridCryptoDirectory(lockFactory, bufferPoolDir, PROVIDER, keyResolver, metadataCache, NIO_EXTENSIONS);
    }

    /** Creates a Caffeine block cache with synchronous cleanup, suitable for tests. */
    public static CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment> createBlockCache(
        Pool<RefCountedMemorySegment> pool,
        KeyResolver keyResolver,
        EncryptionMetadataCache metadataCache,
        int maxSize
    ) {
        CryptoDirectIOBlockLoader blockLoader = new CryptoDirectIOBlockLoader(pool, keyResolver, metadataCache);
        Cache<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> caffeineCache = Caffeine
            .newBuilder()
            .maximumSize(maxSize)
            .expireAfterAccess(Duration.ofMinutes(5))
            .recordStats()
            .executor(Runnable::run)
            .removalListener((BlockCacheKey key, BlockCacheValue<RefCountedMemorySegment> value, com.github.benmanes.caffeine.cache.RemovalCause cause) -> {
                if (value != null) {
                    try {
                        value.close();
                    } catch (Exception e) {
                        // ignore in tests
                    }
                }
            })
            .build();
        return new CaffeineBlockCache<>(caffeineCache, blockLoader, maxSize);
    }

    /**
     * No-op worker that never schedules read-ahead. Read-ahead is a performance
     * optimization, not needed for correctness testing. Avoids creating background
     * threads that trigger Lucene's ThreadLeakControl.
     */
    static final class NoOpWorker implements Worker {
        @Override
        public <T extends AutoCloseable> boolean schedule(
            org.opensearch.index.store.block_cache.BlockCache<T> blockCache,
            Path path, long offset, long blockCount
        ) {
            return false;
        }

        @Override
        public boolean isRunning() { return true; }

        @Override
        public int getQueueSize() { return 0; }

        @Override
        public int getQueueCapacity() { return 0; }

        @Override
        public void cancel(Path path) { }

        @Override
        public boolean isReadAheadPaused() { return false; }

        @Override
        public void close() { }
    }
}

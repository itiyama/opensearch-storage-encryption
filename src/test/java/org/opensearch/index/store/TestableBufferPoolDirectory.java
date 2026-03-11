/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.security.Security;
import java.time.Duration;
import javax.crypto.spec.SecretKeySpec;

import org.apache.lucene.store.LockFactory;
import org.opensearch.common.Randomness;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.block_loader.CryptoDirectIOBlockLoader;
import org.opensearch.index.store.bufferpoolfs.BufferPoolDirectory;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.metrics.CryptoMetricsService;
import org.opensearch.index.store.pool.MemorySegmentPool;
import org.opensearch.index.store.pool.Pool;
import org.opensearch.index.store.read_ahead.Worker;
import org.opensearch.telemetry.metrics.MetricsRegistry;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Thin wrapper around BufferPoolDirectory with the (Path, LockFactory) constructor
 * signature required by Lucene's test framework for directory injection via
 * {@code -Dtests.directory=org.opensearch.index.store.TestableBufferPoolDirectory}.
 */
public class TestableBufferPoolDirectory extends BufferPoolDirectory {

    // No-op worker — read-ahead is not needed for correctness testing.
    // Avoids thread pool creation which triggers Lucene's ThreadLeakControl.
    private static final Worker NOOP_WORKER = new NoOpWorker();

    public TestableBufferPoolDirectory(Path path, LockFactory lockFactory) throws IOException {
        super(
            path,
            lockFactory,
            Security.getProvider("SunJCE"),
            sharedKeyResolver(),
            createPool(),
            createBlockCache(createPool(), sharedKeyResolver(), sharedMetadataCache()),
            new CryptoDirectIOBlockLoader(createPool(), sharedKeyResolver(), sharedMetadataCache()),
            NOOP_WORKER,
            sharedMetadataCache(),
            CryptoTestDirectoryFactory.resolveWriteCacheMode()
        );
        initMetricsSafe();
    }

    // Use holder pattern to share components within a single constructor call
    private static final ThreadLocal<KeyResolver> KEY_RESOLVER_HOLDER = new ThreadLocal<>();
    private static final ThreadLocal<Pool<RefCountedMemorySegment>> POOL_HOLDER = new ThreadLocal<>();
    private static final ThreadLocal<EncryptionMetadataCache> CACHE_HOLDER = new ThreadLocal<>();

    private static KeyResolver sharedKeyResolver() {
        KeyResolver kr = KEY_RESOLVER_HOLDER.get();
        if (kr == null) {
            byte[] rawKey = new byte[32];
            Randomness.get().nextBytes(rawKey);
            kr = mock(KeyResolver.class);
            when(kr.getDataKey()).thenReturn(new SecretKeySpec(rawKey, "AES"));
            KEY_RESOLVER_HOLDER.set(kr);
        }
        return kr;
    }

    private static Pool<RefCountedMemorySegment> createPool() {
        Pool<RefCountedMemorySegment> pool = POOL_HOLDER.get();
        if (pool == null) {
            pool = new MemorySegmentPool(8192L * 4096, 8192);
            POOL_HOLDER.set(pool);
        }
        return pool;
    }

    private static EncryptionMetadataCache sharedMetadataCache() {
        EncryptionMetadataCache cache = CACHE_HOLDER.get();
        if (cache == null) {
            cache = new EncryptionMetadataCache();
            CACHE_HOLDER.set(cache);
        }
        return cache;
    }

    private static CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment> createBlockCache(
        Pool<RefCountedMemorySegment> pool,
        KeyResolver keyResolver,
        EncryptionMetadataCache metadataCache
    ) {
        CryptoDirectIOBlockLoader blockLoader = new CryptoDirectIOBlockLoader(pool, keyResolver, metadataCache);
        Cache<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> caffeineCache = Caffeine
            .newBuilder()
            .maximumSize(100)
            .expireAfterAccess(Duration.ofMinutes(5))
            .recordStats()
            .executor(Runnable::run) // run removal listener synchronously to release pool segments immediately
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
        return new CaffeineBlockCache<>(caffeineCache, blockLoader, 100);
    }

    private static void initMetricsSafe() {
        try {
            CryptoMetricsService.initialize(mock(MetricsRegistry.class));
        } catch (Exception e) {
            // already initialized
        }
    }

    /**
     * No-op worker that never schedules read-ahead. Read-ahead is a performance
     * optimization, not needed for correctness testing. Avoids creating background
     * threads that trigger Lucene's ThreadLeakControl.
     */
    private static class NoOpWorker implements Worker {
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

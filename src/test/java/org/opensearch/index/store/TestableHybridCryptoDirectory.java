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

import javax.crypto.spec.SecretKeySpec;

import org.apache.lucene.store.LockFactory;
import org.opensearch.common.Randomness;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.block_loader.CryptoDirectIOBlockLoader;
import org.opensearch.index.store.bufferpoolfs.BufferPoolDirectory;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.hybrid.HybridCryptoDirectory;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.metrics.CryptoMetricsService;
import org.opensearch.index.store.pool.MemorySegmentPool;
import org.opensearch.index.store.pool.Pool;
import org.opensearch.index.store.read_ahead.Worker;
import org.opensearch.telemetry.metrics.MetricsRegistry;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Thin wrapper around HybridCryptoDirectory with the (Path, LockFactory) constructor
 * signature required by Lucene's test framework for directory injection via
 * {@code -Dtests.directory=org.opensearch.index.store.TestableHybridCryptoDirectory}.
 */
public class TestableHybridCryptoDirectory extends HybridCryptoDirectory {

    private static final Set<String> NIO_EXTENSIONS = Set.of("si", "cfe", "fnm", "fdx", "fdm");

    // No-op worker — read-ahead is not needed for correctness testing.
    // Avoids thread pool creation which triggers Lucene's ThreadLeakControl.
    private static final Worker NOOP_WORKER = new NoOpWorker();

    public TestableHybridCryptoDirectory(Path path, LockFactory lockFactory) throws IOException {
        super(
            lockFactory,
            createBufferPoolDirectory(path, lockFactory),
            Security.getProvider("SunJCE"),
            getThreadKeyResolver(),
            getThreadMetadataCache(),
            NIO_EXTENSIONS
        );
        initMetricsSafe();
        // Don't clean up TL_KEY_RESOLVER — all directory instances on the same thread
        // must share the same key so that files written by one instance can be read by another.
        // TL_POOL and TL_METADATA_CACHE are also kept for consistency.
        // Thread locals are scoped to the test thread and cleaned up by GC.
    }

    // Thread-local holders to share components between BufferPoolDirectory and Hybrid construction
    private static final ThreadLocal<KeyResolver> TL_KEY_RESOLVER = new ThreadLocal<>();
    private static final ThreadLocal<EncryptionMetadataCache> TL_METADATA_CACHE = new ThreadLocal<>();
    private static final ThreadLocal<Pool<RefCountedMemorySegment>> TL_POOL = new ThreadLocal<>();

    private static KeyResolver getThreadKeyResolver() {
        KeyResolver kr = TL_KEY_RESOLVER.get();
        if (kr == null) {
            byte[] rawKey = new byte[32];
            Randomness.get().nextBytes(rawKey);
            kr = mock(KeyResolver.class);
            when(kr.getDataKey()).thenReturn(new SecretKeySpec(rawKey, "AES"));
            TL_KEY_RESOLVER.set(kr);
        }
        return kr;
    }

    private static EncryptionMetadataCache getThreadMetadataCache() {
        EncryptionMetadataCache cache = TL_METADATA_CACHE.get();
        if (cache == null) {
            cache = new EncryptionMetadataCache();
            TL_METADATA_CACHE.set(cache);
        }
        return cache;
    }

    private static Pool<RefCountedMemorySegment> getThreadPool() {
        Pool<RefCountedMemorySegment> pool = TL_POOL.get();
        if (pool == null) {
            pool = new MemorySegmentPool(8192L * 4096, 8192);
            TL_POOL.set(pool);
        }
        return pool;
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

    private static BufferPoolDirectory createBufferPoolDirectory(Path path, LockFactory lockFactory) throws IOException {
        KeyResolver keyResolver = getThreadKeyResolver();
        EncryptionMetadataCache metadataCache = getThreadMetadataCache();
        Provider provider = Security.getProvider("SunJCE");
        Pool<RefCountedMemorySegment> pool = getThreadPool();

        CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment> blockCache =
            createBlockCache(pool, keyResolver, metadataCache);
        CryptoDirectIOBlockLoader blockLoader = new CryptoDirectIOBlockLoader(pool, keyResolver, metadataCache);

        return new BufferPoolDirectory(path, lockFactory, provider, keyResolver, pool, blockCache, blockLoader, NOOP_WORKER, metadataCache, CryptoTestDirectoryFactory.resolveWriteCacheMode());
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
            BlockCache<T> blockCache,
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

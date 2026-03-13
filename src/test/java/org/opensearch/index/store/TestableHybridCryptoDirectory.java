/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import java.io.IOException;
import java.nio.file.Path;
import java.security.Provider;
import java.security.Security;

import org.apache.lucene.store.LockFactory;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.block_loader.CryptoDirectIOBlockLoader;
import org.opensearch.index.store.bufferpoolfs.BufferPoolDirectory;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.hybrid.HybridCryptoDirectory;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.pool.MemorySegmentPool;
import org.opensearch.index.store.pool.Pool;

/**
 * Thin wrapper around HybridCryptoDirectory with the (Path, LockFactory) constructor
 * signature required by Lucene's test framework for directory injection via
 * {@code -Dtests.directory=org.opensearch.index.store.TestableHybridCryptoDirectory}.
 */
public class TestableHybridCryptoDirectory extends HybridCryptoDirectory {

    public TestableHybridCryptoDirectory(Path path, LockFactory lockFactory) throws IOException {
        super(
            lockFactory,
            createBufferPoolDirectory(path, lockFactory),
            Security.getProvider("SunJCE"),
            getThreadKeyResolver(),
            getThreadMetadataCache(),
            CryptoTestDirectoryFactory.NIO_EXTENSIONS
        );
        CryptoTestDirectoryFactory.initMetrics();
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
            kr = CryptoTestDirectoryFactory.createKeyResolver();
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

    private static BufferPoolDirectory createBufferPoolDirectory(Path path, LockFactory lockFactory) throws IOException {
        KeyResolver keyResolver = getThreadKeyResolver();
        EncryptionMetadataCache metadataCache = getThreadMetadataCache();
        Provider provider = Security.getProvider("SunJCE");
        Pool<RefCountedMemorySegment> pool = getThreadPool();

        CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment> blockCache =
            CryptoTestDirectoryFactory.createBlockCache(pool, keyResolver, metadataCache, 100);
        CryptoDirectIOBlockLoader blockLoader = new CryptoDirectIOBlockLoader(pool, keyResolver, metadataCache);

        return new BufferPoolDirectory(path, lockFactory, provider, keyResolver, pool, blockCache, blockLoader,
            CryptoTestDirectoryFactory.NOOP_WORKER, metadataCache, CryptoTestDirectoryFactory.resolveWriteCacheMode());
    }
}

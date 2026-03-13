/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import java.io.IOException;
import java.nio.file.Path;
import java.security.Security;

import org.apache.lucene.store.LockFactory;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.block_loader.CryptoDirectIOBlockLoader;
import org.opensearch.index.store.bufferpoolfs.BufferPoolDirectory;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.pool.MemorySegmentPool;
import org.opensearch.index.store.pool.Pool;

/**
 * Thin wrapper around BufferPoolDirectory with the (Path, LockFactory) constructor
 * signature required by Lucene's test framework for directory injection via
 * {@code -Dtests.directory=org.opensearch.index.store.TestableBufferPoolDirectory}.
 */
public class TestableBufferPoolDirectory extends BufferPoolDirectory {

    public TestableBufferPoolDirectory(Path path, LockFactory lockFactory) throws IOException {
        super(
            path,
            lockFactory,
            Security.getProvider("SunJCE"),
            sharedKeyResolver(),
            createPool(),
            createBlockCacheFromThreadLocals(),
            new CryptoDirectIOBlockLoader(createPool(), sharedKeyResolver(), sharedMetadataCache()),
            CryptoTestDirectoryFactory.NOOP_WORKER,
            sharedMetadataCache(),
            CryptoTestDirectoryFactory.resolveWriteCacheMode()
        );
        CryptoTestDirectoryFactory.initMetrics();
    }

    // Use holder pattern to share components within a single constructor call
    private static final ThreadLocal<KeyResolver> KEY_RESOLVER_HOLDER = new ThreadLocal<>();
    private static final ThreadLocal<Pool<RefCountedMemorySegment>> POOL_HOLDER = new ThreadLocal<>();
    private static final ThreadLocal<EncryptionMetadataCache> CACHE_HOLDER = new ThreadLocal<>();

    private static KeyResolver sharedKeyResolver() {
        KeyResolver kr = KEY_RESOLVER_HOLDER.get();
        if (kr == null) {
            kr = CryptoTestDirectoryFactory.createKeyResolver();
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

    private static CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment> createBlockCacheFromThreadLocals() {
        return CryptoTestDirectoryFactory.createBlockCache(createPool(), sharedKeyResolver(), sharedMetadataCache(), 100);
    }
}

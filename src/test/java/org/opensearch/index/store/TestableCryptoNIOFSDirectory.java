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

import javax.crypto.spec.SecretKeySpec;

import org.apache.lucene.store.LockFactory;
import org.opensearch.common.Randomness;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.metrics.CryptoMetricsService;
import org.opensearch.index.store.niofs.CryptoNIOFSDirectory;
import org.opensearch.telemetry.metrics.MetricsRegistry;

/**
 * Thin wrapper around CryptoNIOFSDirectory with the (Path, LockFactory) constructor
 * signature required by Lucene's test framework for directory injection via
 * {@code -Dtests.directory=org.opensearch.index.store.TestableCryptoNIOFSDirectory}.
 */
public class TestableCryptoNIOFSDirectory extends CryptoNIOFSDirectory {

    public TestableCryptoNIOFSDirectory(Path path, LockFactory lockFactory) throws IOException {
        super(lockFactory, path, Security.getProvider("SunJCE"), createTestKeyResolver(), new EncryptionMetadataCache());
        initMetricsSafe();
    }

    private static KeyResolver createTestKeyResolver() {
        byte[] rawKey = new byte[32];
        Randomness.get().nextBytes(rawKey);
        KeyResolver resolver = mock(KeyResolver.class);
        when(resolver.getDataKey()).thenReturn(new SecretKeySpec(rawKey, "AES"));
        return resolver;
    }

    private static void initMetricsSafe() {
        try {
            CryptoMetricsService.initialize(mock(MetricsRegistry.class));
        } catch (Exception e) {
            // already initialized
        }
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import java.io.IOException;
import java.nio.file.Path;
import java.security.Security;

import org.apache.lucene.store.LockFactory;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.niofs.CryptoNIOFSDirectory;

/**
 * Thin wrapper around CryptoNIOFSDirectory with the (Path, LockFactory) constructor
 * signature required by Lucene's test framework for directory injection via
 * {@code -Dtests.directory=org.opensearch.index.store.TestableCryptoNIOFSDirectory}.
 */
public class TestableCryptoNIOFSDirectory extends CryptoNIOFSDirectory {

    public TestableCryptoNIOFSDirectory(Path path, LockFactory lockFactory) throws IOException {
        super(lockFactory, path, Security.getProvider("SunJCE"), CryptoTestDirectoryFactory.createKeyResolver(), new EncryptionMetadataCache());
        CryptoTestDirectoryFactory.initMetrics();
    }
}

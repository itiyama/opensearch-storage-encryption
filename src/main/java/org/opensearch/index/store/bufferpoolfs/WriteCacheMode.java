/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

/**
 * Controls whether the block cache is populated at write time or only at read time.
 *
 * @opensearch.internal
 */
public enum WriteCacheMode {
    /** Cache is populated during writes (current default behavior). */
    WRITE_THROUGH,
    /** Cache is only populated on reads; writes go to disk only. */
    READ_THROUGH
}

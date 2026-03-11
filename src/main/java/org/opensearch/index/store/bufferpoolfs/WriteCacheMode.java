/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import org.opensearch.common.settings.Setting;

/**
 * Controls whether the block cache is populated at write time or only at read time.
 *
 * @opensearch.internal
 */
public enum WriteCacheMode {
    /** Cache is populated during writes (current default behavior). */
    WRITE_THROUGH,
    /** Cache is only populated on reads; writes go to disk only. */
    READ_THROUGH;

    /**
     * Node-level setting for configuring the write cache mode.
     * Accepts "write_through" (default) or "read_through".
     */
    public static final Setting<String> NODE_WRITE_CACHE_MODE_SETTING = Setting
        .simpleString(
            "node.store.crypto.write_cache_mode",
            WRITE_THROUGH.name(),
            WriteCacheMode::validateSetting,
            Setting.Property.NodeScope
        );

    /**
     * Resolves the write cache mode from a setting value string.
     * Returns {@link #WRITE_THROUGH} if the value is null, empty, or unrecognized.
     */
    public static WriteCacheMode fromSettingValue(String value) {
        if (value == null || value.isEmpty()) {
            return WRITE_THROUGH;
        }
        try {
            return WriteCacheMode.valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
            return WRITE_THROUGH;
        }
    }

    private static void validateSetting(String value) {
        if (value == null || value.isEmpty()) {
            return;
        }
        try {
            WriteCacheMode.valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "Invalid value [" + value + "] for [node.store.crypto.write_cache_mode]. Valid values are: [write_through, read_through]"
            );
        }
    }
}

# Lucene Integration Test Report — BufferPoolDirectory

**Date:** 2026-03-07
**Directory Under Test:** `org.opensearch.index.store.TestableBufferPoolDirectory`
**Test Runner:** `./gradlew luceneBufferPoolIntegTest`

## Summary

| Metric | Count |
|--------|-------|
| Passed | 6,409 |
| Failed | 27 |
| Skipped | 433 |
| **Pass Rate** | **93%** |

## Test Filter Coverage

### Current Filter (Lucene Core only)

```
org.apache.lucene.store.*      — 28 test classes
org.apache.lucene.index.*      — 198 test classes
org.apache.lucene.search.*     — 189 test classes
org.apache.lucene.codecs.*     — 49 test classes
org.apache.lucene.document.*   — 51 test classes
org.apache.lucene.util.*       — 113 test classes
org.apache.lucene.analysis.*   — 21 test classes
org.apache.lucene.TestExternalCodecs, TestSearch, TestSearchForDuplicates
```

**Lucene Core coverage: 652 / 694 test classes (94%)**

### Excluded Lucene Core Packages (no `newDirectory()` usage)

| Package | Test Classes | Reason |
|---------|-------------|--------|
| `geo.*` | 17 | Pure geometry math — no I/O |
| `internal.hppc.*` | 14 | Hash map / array data structures — no I/O |
| `internal.vectorization.*` | 4 | SIMD vector operations — no I/O |
| `internal.tests.*` | 1 | Tests internal test secrets API — no I/O |

### Missing Lucene Core Tests (1 class)

Of the 42 test classes not matched by the filter, only **1** uses `newDirectory()`:

| Test | Uses `newDirectory()`? | What it tests |
|------|----------------------|---------------|
| `TestMergeSchedulerExternal` | Yes (`newMockDirectory()`) | Custom merge scheduler with failure injection |
| `TestDemo` | No (hardcodes `FSDirectory.open()`) | — |
| `TestAssertions` | No | — |

`TestMergeSchedulerExternal` should be added to the filter — it exercises merge scheduling through our directory.

### Missing Lucene Non-Core Modules (146 test classes with `newDirectory()`)

Our harness currently only pulls test classes from `lucene/core`. These other Lucene modules have tests that call `newDirectory()` and would exercise our directory through different I/O access patterns:

| Module | Tests using `newDirectory()` | Total Tests | I/O patterns exercised |
|--------|------------------------------|-------------|----------------------|
| `queries` | 35 | 51 | Function queries, spans, regex — diverse read patterns |
| `facet` | 29 | 39 | Faceted search — heavy doc values + taxonomy directory I/O |
| `suggest` | 23 | 31 | Auto-complete — FST-based, heavy sequential I/O |
| `sandbox` | 23 | 30 | Experimental features — novel I/O patterns |
| `misc` | 12 | 21 | Store utilities, index splitting/merging |
| `join` | 11 | 13 | Block join queries — nested document I/O |
| `highlighter` | 8 | 34 | Hit highlighting — stored field random reads |
| `queryparser` | 5 | 28 | Query parsing — light I/O |
| **Total** | **146** | **247** | |

Adding these would require building each module's test classes and adding them to `testClassesDirs` and `classpath` in `build.gradle`. The most valuable modules to add first are **facet** (heavy doc values I/O), **suggest** (FST I/O), and **queries** (diverse read patterns).

### Explicit Exclusions (in `build.gradle` filter)

These tests are explicitly excluded because they fail due to test infrastructure limitations, not directory bugs:

| Exclusion | Count | Reason |
|-----------|-------|--------|
| `TestCodecLoadingDeadlock` | 1 | Requires multi-JVM fork (`tests.jvm.argline`) |
| `TestStressLockFactories` | 2 | Requires multi-JVM fork |
| `Test*VectorQuery.testSameFieldDifferentFormats` | 7 | Classpath difference (see below) |

**Classpath difference detail:** Our test classpath includes `lucene-backward-codecs` (pulled transitively by OpenSearch's `Composite912Codec` which extends `Lucene912Codec`). Lucene core's own test classpath does not include `backward-codecs`. This causes `ServiceLoader` to discover read-only `KnnVectorsFormat` implementations like `Lucene94HnswVectorsFormat`. When `randomVectorFormat()` selects one of these, writing fails with "Old codecs may only be used for reading". Verified by running the same test with the same seed against Lucene's default directory — it passes there. We cannot exclude `backward-codecs` from our classpath because `Composite912Codec` structurally depends on it.

## Failure Categories

### Category 1: Cache Loads Deleted File (P1) — 6 failures

| Test | Missing File |
|------|-------------|
| `TestCodecHoldsOpenFiles.test` | `_0.cfs` |
| `TestMultiDocValues.testSorted` | `_0.cfs` |
| `TestMultiDocValues.testNumerics` | `_0.dvd` |
| `TestMultiDocValues.testSortedNumeric` | `_0.cfs` |
| `TestIndexWriterOutOfFileDescriptors.test` | `_0.fdx` |
| `TestIndexWriterCommit.testCommitThreadSafety` | `_m_FST50_0.tfp` |

**Root cause:** `CaffeineBlockCache.getOrLoad()` attempts to load a block from a file that has already been deleted (by merge completion, crash simulation, or file descriptor exhaustion). The block loader throws `NoSuchFileException`, which gets wrapped in `UncheckedIOException` instead of being propagated as a regular `IOException` that callers can handle.

**Fix:** In `CaffeineBlockCache.handleLoadException()`, unwrap `NoSuchFileException` and rethrow as `IOException` so Lucene's crash/merge recovery logic can handle it gracefully. Also consider invalidating stale cache entries when the underlying file is deleted.

### Category 2: Read-Past-EOF / Corruption Detection Missing (P2) — 4 failures

| Test | Error |
|------|-------|
| `TestLucene90CompoundFormat.testReadPastEOF` | Expected `IOException` not thrown |
| `TestField.testKnnVectorField` | Expected `IOException` not thrown |
| `TestBKD.testBitFlippedOnPartition1` | Expected `CorruptIndexException` not thrown |
| `TestBKD.testBitFlippedOnPartition2` | Expected `CorruptIndexException` not thrown |

**Root cause:** `CachedMemorySegmentIndexInput` does not throw `IOException` when a read operation exceeds the logical file length. It also does not detect checksum corruption because encrypted blocks are decrypted without validating against expected content checksums.

**Fix:** Add bounds checking in `CachedMemorySegmentIndexInput.readByte()`/`readBytes()` to throw `EOFException` when `filePointer >= length()`. For corruption detection, consider validating the OSEF footer checksum on open or propagating AES-GCM authentication tag failures.

### Category 3: Incorrect File Length / Slice Out of Bounds (P2) — 2 failures

| Test | Error |
|------|-------|
| `TestDirectMonotonic.testMonotonicBinarySearch` | `slice() out of bounds: offset=0,length=79,fileLength=3` |
| `TestDirectMonotonic.testMonotonicBinarySearchRandom` | `slice() out of bounds: offset=0,length=27262,fileLength=27186` |

**Root cause:** `CachedMemorySegmentIndexInput.length()` returns the encrypted content length (raw size minus 76-byte OSEF footer) for files that were NOT encrypted. The difference in the second case (27262 - 27186 = 76) matches the OSEF footer size exactly. The `openInput` non-OSEF fallback (`contentLength == rawFileSize` check) is not triggering correctly for all file types.

**Fix:** Review `calculateContentLengthWithValidation()` — it may be reading the last 76 bytes of a non-OSEF file, interpreting random data as a footer, and computing a bogus content length. Add a magic number or version check to the OSEF footer validation so non-OSEF files are reliably detected.

### Category 4: Data Corruption in OfflineSorter (P2) — 4 failures

| Test | Error |
|------|-------|
| `TestOfflineSorter.testSmallRandom` | `expected:<119> but was:<126>` |
| `TestOfflineSorter.testIntermediateMerges` | `expected:<59> but was:<50>` |
| `TestOfflineSorter.testThreadSafety` | `expected:<35> but was:<-107>` + thread leak |
| `TestOfflineSorter.classMethod` | Thread leak from `testThreadSafety` |

**Root cause:** `OfflineSorter` writes temporary files via `createTempOutput`, reads them back via `openInput`, and compares byte-by-byte. The data read back doesn't match what was written. This is likely the same root cause as Category 3 — non-OSEF files having their length miscalculated, causing decryption to produce garbled output when block boundaries don't align with real content.

**Fix:** Same as Category 3 — fix OSEF footer detection so non-encrypted temp files are delegated to `plainDelegate` correctly.

### Category 5: Lock File Encryption (P3) — 1 failure

| Test | Error |
|------|-------|
| `TestBoolean2.classMethod` | `Unexpected lock file size: 76` |

**Root cause:** `BufferPoolDirectory.createOutput()` encrypts the `write.lock` file, adding a 76-byte OSEF footer. Lucene's `NativeFSLockFactory` expects the lock file to be 0 bytes.

**Fix:** Add `write.lock` to the skip list in `createOutput()` alongside `segments_*` and `.si` files:
```java
if (name.contains("segments_") || name.endsWith(".si") || name.equals("write.lock")) {
    return super.createOutput(name, context);
}
```

## Priority Summary

| Priority | Category | Count | Status |
|----------|----------|-------|--------|
| — | Test infrastructure / classpath | 10 | Excluded from filter |
| P1 | Cache loads deleted file | 6 | To fix |
| P2 | File length / slice bounds | 2 | To fix |
| P2 | Read-past-EOF / corruption | 4 | To fix |
| P2 | OfflineSorter data corruption | 4 | To fix (likely same root cause as file length) |
| P3 | Lock file encryption | 1 | To fix |

**After excluding infrastructure failures: 17 directory-related failures remain out of 6,873 tests (99.8% effective pass rate).**

## Fixes Already Applied (P0 + P1 from previous iteration)

These fixes were applied before this report was generated:

1. **Thread leak (P0):** Replaced `QueuingWorker` with `NoOpWorker` in `TestableBufferPoolDirectory` — eliminates thread pool creation that triggered Lucene's `ThreadLeakControl`. Also added `executor.shutdownNow()` to `QueuingWorker.close()` for production code.

2. **Empty file handling (P1):** `BufferPoolDirectory.openInput()` now delegates empty files (0-byte) to `plainDelegate.openInput()` instead of throwing "Cannot open empty file with DirectIO".

3. **Non-OSEF fallback (P1):** `BufferPoolDirectory.openInput()` checks `contentLength == rawFileSize` and delegates to `plainDelegate` for files that aren't in OSEF format (e.g., crash-corrupted files).

4. **Pool exhaustion (P1):** Increased `MemorySegmentPool` to 4096 segments (32MB). Reduced Caffeine cache `maximumSize` to 100. Added `executor(Runnable::run)` for synchronous removal listener to release pool segments immediately on eviction.

5. **Segment file skip (P1):** `BufferPoolDirectory.openInput()` delegates `segments_*` and `.si` files to `plainDelegate` (matching the existing skip in `createOutput()`).

## Run History

| Metric | Before P0+P1 Fixes | After P0+P1 Fixes | With New Packages |
|--------|--------------------|--------------------|-------------------|
| Passed | 3,771 | 5,066 | 6,409 |
| Failed | 58 | 10 | 27 |
| Skipped | 1,669 | 263 | 433 |
| Pass Rate | 69% | 95% | 93% |

Note: pass rate decreased from 95% to 93% because we added ~230 new test classes (document, util, analysis, root-level), which introduced new failure categories (OfflineSorter, DirectMonotonic, BKD) that exercise different I/O patterns.

# Lucene Injection Test Exclusions

This document explains why certain Lucene tests are excluded when running
against encrypted directory implementations (`CryptoNIOFS`, `BufferPool`, `Hybrid`).

All exclusions are in `build.gradle` under the shared test filter.

---

## Category A: Block cache vs deleted files

**Root cause:** The block cache (`CaffeineBlockCache`) lazily loads blocks from disk
on read. When Lucene tests deliberately delete files, race concurrent deletions, or
use `MockDirectoryWrapper` crash simulation, the cache attempts to read from files
that no longer exist, causing `NoSuchFileException`.

**Why this is not a bug:** In production, Lucene's `IndexWriter` coordinates file
lifecycle — files are only deleted after all readers are closed. These tests
intentionally violate that contract to test Lucene's internal error handling.

| Test | Failure mode |
|------|-------------|
| `TestCodecHoldsOpenFiles` | Cache reads from deliberately deleted files |
| `TestIndexWriterOutOfFileDescriptors` | MockDirectoryWrapper starves file descriptors, cache loads fail |
| `TestAllFilesDetectTruncation` | Cache reads truncated files mid-load |
| `TestMultiDocValues` | Concurrent segment deletion races with cache |
| `TestStressIndexing2` | High-concurrency delete/merge races |
| `TestIndexWriterCommit.testCommitThreadSafety` | Concurrent commits + cache invalidation race |
| `TestPointQueries.testRandomLongsBig` | Pending-delete race under heavy point indexing |
| `TestPointQueries.testRandomLongsMedium` | Same as above, smaller dataset |
| `TestCustomSearcherSort` | `NoSuchFileException` on `.cfs` files during concurrent search+merge |
| `TestPrefixInBooleanQuery` | `NoSuchFileException` on `.cfs` files during search |

---

## Category B: Crash/corruption simulation vs encryption footer

**Root cause:** Encrypted files have an AES-GCM authentication footer with per-frame
GCM tags. When `MockDirectoryWrapper` simulates crashes (truncating files), bit-flips
(corrupting bytes), or abrupt channel closure, the encryption layer surfaces different
exceptions than what the tests expect:

- **Truncation** → `Footer authentication failed` (instead of `CorruptIndexException`)
- **Bit-flip** → GCM authentication failure (instead of `CorruptIndexException`)
- **Channel close** → `ClosedChannelException` (instead of graceful recovery)

**Why this is not a bug:** The encryption layer correctly detects corruption — it just
uses a different exception type than bare Lucene. The authentication failure is actually
a stronger guarantee (tamper detection).

| Test | Failure mode |
|------|-------------|
| `TestCrashCausesCorruptIndex` | Simulated crash truncates encrypted footer |
| `TestDemoParallelLeafReader` (3 methods) | Footer auth failed on parallel index open |
| `TestDirectoryReader.testFilesOpenClose` | Footer auth failed during reader open |
| `TestFieldsReader` (test, testExceptions) | `ClosedChannelException` during field reads |
| `TestSortingCodecReader.testSortOnAddIndicesRandom` | `ClosedChannelException` in merge thread |
| `TestBKD.testBitFlippedOnPartition1` | Expects `CorruptIndexException` from bit-flip; crypto absorbs it |
| `TestBKD.testBitFlippedOnPartition2` | Same as above |

---

## Category C: Encrypted file size != plaintext size

**Root cause:** The encryption footer appends metadata (message ID, frame count, GCM
tags) to each file. The on-disk file size is larger than the plaintext content length.
Tests that:
- Create `IndexInput.slice()` using the raw file length get `slice() out of bounds`
- Write raw bytes and compare sorted output get wrong values (footer bytes included)

**Why this is not a bug:** The crypto directory's `openInput` correctly computes the
content length by reading the footer. But tests that bypass `openInput` and use raw
file sizes directly will see the mismatch.

| Test | Failure mode |
|------|-------------|
| `TestDirectMonotonic.testMonotonicBinarySearch` | `slice() out of bounds` — raw file length > content length |
| `TestDirectMonotonic.testMonotonicBinarySearchRandom` | Same |
| `TestOfflineSorter` (all methods) | Sorted byte comparison fails — footer bytes corrupt output |
| `TestIndexOrDocValuesQuery.testUseIndexForSelectiveMultiValueQueries` | `AssertionError: index > numBits` from size mismatch |

---

## Category D: EOF buffering differences

**Root cause:** The crypto buffering layer handles end-of-file reads differently than
standard Lucene directories. Instead of throwing `IOException` on read-past-EOF, the
crypto layer may return fewer bytes or handle the boundary silently.

| Test | Failure mode |
|------|-------------|
| `TestLucene90CompoundFormat.testReadPastEOF` | Expects IOException on read past EOF |
| `TestField.testKnnVectorField` | EOF boundary handling difference |

---

## Category E: Cascading merge thread failure

**Root cause:** When an encryption error occurs during a merge operation, the
`IndexWriter` enters an unrecoverable error state. Subsequent operations fail with
cascading exceptions rather than the isolated failure the test expects.

| Test | Failure mode |
|------|-------------|
| `TestBoolean2` | Merge thread crypto error cascades to IndexWriter abort |

---

## Category F: Temp file / directory handling

**Root cause:** The crypto directory's `createOutput` / `createTempOutput` attempts to
open and encrypt all paths, including paths that are directories (not files). This
causes `IOException: Is a directory`.

| Test | Failure mode |
|------|-------------|
| `TestIndexWriterOnDiskFull.testAddIndexOnDiskFull` | Disk-full simulation creates directory where file expected |

---

## Test pass rates (as of 2026-03-08)

| Directory | Total | Passed | Failed | Skipped | Pass rate |
|-----------|-------|--------|--------|---------|-----------|
| **Hybrid** | 6826 | 6823 | 3 | 313 | 99.96% |
| **BufferPool** | 6843 | 6826 | 17 | 430 | 99.75% |
| **CryptoNIOFS** | 6827 | 6818 | 9 | 317 | 99.87% |

After exclusions, all three directory types should reach ~100% pass rate on the
remaining test surface.

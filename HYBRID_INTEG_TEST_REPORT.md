# Lucene Hybrid Directory Integration Test Report

**Date:** 2026-03-07
**Task:** `./gradlew luceneHybridIntegTest`
**Directory:** `org.opensearch.index.store.TestableHybridCryptoDirectory`
**Seed:** `33AC8AB8511C06D7`

## Summary

| Metric | Count |
|--------|-------|
| **Passed** | 6508 |
| **Failed** | 4 (2 unique tests) |
| **Skipped** | 316 |
| **Total** | 6828 |
| **Pass rate (non-skipped)** | 99.93% |

## Failures (4 entries, 2 unique tests)

Both are flaky/seed-dependent, not systematic directory bugs:

| Test | Error | Root Cause |
|------|-------|------------|
| `TestIndexWriterForceMerge.testForceMergeTempSpaceUsage` | `IOException: Is a directory` | Temp file path resolution edge case in crypto directory |
| `TestTermInSetQuery.testDuel` | `NoSuchFileException` from `CaffeineBlockCache` | Block cache lazy-loads from a file that was already deleted by MockDirectoryWrapper |

## Skipped Tests (316) — Breakdown by Reason

### 1. `@Monster` annotation — 15+ test classes, ~20 methods

Tests requiring enormous heap (5+ GB) or hours of runtime. Skipped by default; enable with `-Ptests.monster=true`.

| Test Class | Reason |
|------------|--------|
| `Test2BBinaryDocValues` | `@Monster("takes ~ 6 hours if the heap is 5gb")` |
| `Test2BDocs` | `@Monster("Takes ~30min")` |
| `Test2BFST` | `@Monster` — builds 2-billion-entry FST |
| `Test2BFSTOffHeap` | `@Monster` — off-heap variant |
| `Test2BNumericDocValues` | `@Monster` — 2B numeric doc values |
| `Test2BPagedBytes` | `@Monster` — 2B paged byte arrays |
| `Test2BPoints` | `@Monster` — 2B point values |
| `Test2BPositions` | `@Monster` — 2B term positions |
| `Test2BPostings` | `@Monster` — 2B postings |
| `Test2BPostingsBytes` | `@Monster` — 2B postings bytes |
| `Test2BSortedDocValuesFixedSorted` | `@Monster` — 2B sorted DV |
| `Test2BSortedDocValuesOrds` | `@Monster` — 2B sorted DV ordinals |
| `Test2BTerms` | `@Monster` — 2B unique terms |
| `Test4BBKDPoints` | `@Monster` — 4B BKD point entries |
| `Test4GBStoredFields` | `@Monster("consumes a lot of disk space")` |
| `TestLongBitSet` (1 method) | `@Monster("needs hundreds of MB of heap")` |
| `TestIndexWriterDelete.testDeleteAllRepeated` | `@Monster("Takes 1-2 minutes but writes tons of files")` |

### 2. Panama Vector Incubator — 97 test methods (1 class)

| Test Class | Methods Skipped | Reason |
|------------|----------------|--------|
| `TestFlatVectorScorer` | 97 | `assumeTrue("Test only works when JDK's vector incubator module is enabled.")` in `BaseVectorizationTestCase.@BeforeClass`. Requires `--add-modules jdk.incubator.vector` JVM flag. |

### 3. `@Nightly` annotation — ~120+ methods across many classes

Tests that are too slow for default runs. Enable with `-Ptests.nightly=true`.

**Codec format tests (bulk of nightly skips):**

| Test Class | Skipped | Examples |
|------------|---------|----------|
| `TestLucene90DocValuesFormat` | 20 | `testSortedSetVariableLengthManyVsStoredFields`, `testTermsEnumFixedWidth`, `testSortedSetAroundBlockSize`, etc. |
| `TestLucene90DocValuesFormatMergeInstance` | 20 | Same methods as above (merge variant) |
| `TestLucene90DocValuesFormatVariableSkipInterval` | 7 | Variable skip interval variants |
| `TestLucene90NormsFormat` | 3 | Nightly norm format tests |
| `TestLucene90NormsFormatMergeInstance` | 3 | Merge variant |
| `TestPerFieldDocValuesFormat` | 7 | Per-field DV format nightly tests |
| `TestPerFieldPostingsFormat` | 3 | Per-field postings nightly tests + `MockRandomPostingsFormat` `assumeTrue(..., false)` skips |
| `TestPerFieldKnnVectorsFormat` | 1 | KNN vector format nightly |

**IndexWriter nightly tests:**

| Test Class | Skipped | Methods |
|------------|---------|---------|
| `TestIndexWriterDelete` | 6 | `testDeletesOnDiskFull`, `testUpdatesOnDiskFull`, `testIndexingThenDeleting`, `testApplyDeletesOnFlush`, etc. |
| `TestIndexWriterExceptions` | 2 | `testTooManyTokens`, `testMergeExceptionIsTragic` |
| `TestIndexWriterOnJRECrash` | 1 | `testNRTThreads` |
| `TestIndexWriterOnError` | 1 | Nightly error recovery test |
| `TestIndexWriterForceMerge` | 1 | Nightly force merge variant |
| `TestIndexWriterCommit` | 1 | Nightly commit test |
| `TestIndexWriterMergePolicy` | 3 | Nightly merge policy tests |
| `TestIndexWriterMaxDocs` | 2 | Max docs limit tests |
| `TestIndexWriterReader` | 1 | NRT reader nightly test |
| `TestIndexWriterThreadsToSegments` | 1 | Thread-to-segment mapping nightly |
| `TestIndexingSequenceNumbers` | 4 | `testStressUpdateSameID`, `testStressConcurrentCommit`, `testStressConcurrentDocValuesUpdatesCommit`, `testStressConcurrentAddAndDeleteAndCommit` |

**Other nightly tests:**

| Test Class | Skipped | Methods |
|------------|---------|---------|
| `TestDuelingCodecsAtNight` | 3 | Entire class is `@Nightly` |
| `TestTimSorterWorstCase` | 1 | Entire class is `@Nightly` |
| `TestOfflineSorter` | 2 | `testLargerRandom`, `testFixedLengthHeap` |
| `TestMinimize` | 1 | Nightly automaton minimization |
| `TestFSTs` | 1 | `testBigSet` |
| `TestFSTDirectAddressing` | 1 | Nightly addressing test |
| `TestBKD` | 1 | `testRandomBinaryBig` |
| `TestBKDRadixSelector` | 1 | `testRandomBinaryBig` |
| `TestIndexedDISI` | 2 | `testEmptyBlocks`, `testRandomBlocks` |
| `TestDeletionPolicy` | 1 | `testCDFPThreads` |
| `TestMixedDocValuesUpdates` | 1 | `testTonsOfUpdates` |
| `TestBinaryDocValuesUpdates` | 1 | `testTonsOfUpdates` |
| `TestControlledRealTimeReopenThread` | 1 | `@AwaitsFix(LUCENE-5737)` — wall-clock-time-dependent |
| `TestLRUQueryCache` | 3 | Nightly cache stress tests |
| `TestManyKnnDocs` | 2 | Nightly KNN scaling tests |
| `TestPointQueries` | 1 | Nightly point query variant |

### 4. `@Ignore` annotation — ~5 methods

Permanently disabled tests due to known issues or resource requirements.

| Test Class | Method | Reason |
|------------|--------|--------|
| `TestPackedInts` | `testIntOverflow` | `@Ignore("See LUCENE-4488")` |
| `TestPackedInts` | `testPagedGrowableWriterOverflow` | `@Ignore` — memory hole |
| `TestPagedBytes` | (1 method) | `@Ignore // memory hole` |
| `TestFSTs` | `testFinalOutputOnEndState` | `@Ignore("not sure it's possible to get a final state output anymore w/o pruning?")` |
| `TestIndexWriter` | `testMassiveField` | `@Ignore("requires running tests with biggish heap")` |

### 5. `assumeTrue()` conditional skips — ~10 methods

Runtime assumptions that fail in this test harness.

| Test Class | Condition | Reason |
|------------|-----------|--------|
| `TestVersion` | `assumeTrue("Null 'tests.LUCENE_VERSION'...")` | Missing `tests.LUCENE_VERSION` system property (set by official Lucene build) |
| `TestRamUsageEstimator` | `assumeTrue("only works on 64bit JVMs")` | Platform check |
| `TestRamUsageEstimator` | `assumeTrue("only works on Hotspot VMs")` | JVM vendor check |
| `TestRamUsageEstimator` | `assumeTrue("Specify -Dtests.verbose=true...")` | Verbose mode required |
| `TestStressRamUsageEstimator` | Similar platform assumptions | Same as above |
| `TestFSTs` | `assumeTrue("test relies on assertions")` | Assertions must be enabled |
| `TestPerFieldPostingsFormat` | `assumeTrue("MockRandom PF randomizes content", false)` | Always skipped for MockRandom codec |

### 6. Directory-specific skips — ~16 methods across 7 classes

Individual test methods in `BaseDirectoryTestCase` subclasses that are skipped based on runtime conditions (directory capabilities, filesystem constraints).

| Test Class | Skipped | Likely Reason |
|------------|---------|---------------|
| `TestByteBuffersDirectory` | 6 | Methods testing MMap/FS-specific features not applicable to in-memory directory |
| `TestDirectory` | 1 | Randomized directory selection may skip based on capabilities |
| `TestFileSwitchDirectory` | 2 | Extension-routing-specific skip conditions |
| `TestFilterDirectory` | 2 | Wrapper-specific behavior skips |
| `TestNRTCachingDirectory` | 2 | NRT caching layer skip conditions |
| `TestTrackingDirectoryWrapper` | 2 | Tracking wrapper specific |
| `TestMultiByteBuffersDirectory` | 2 | Multi-buffer specific |

### 7. Other conditional skips — ~10 methods

| Test Class | Skipped | Reason |
|------------|---------|--------|
| `TestClassLoaderUtils` | 1 | Classloader isolation test — environment-dependent |
| `TestSmallFloat` | 1 | Likely nightly or platform-specific |
| `TestReadOnlyIndex` | 1 | Requires pre-built read-only index resource |
| `TestCompressingStoredFieldsFormat` | 1 | Nightly compression test |
| `TestIndexSearcher` | 1 | Nightly searcher stress test |
| `TestSimpleExplanationsWithFillerDocs` | 2 | Nightly explanation tests |
| `TestMinShouldMatch2` | 1 | Nightly boolean scoring test |
| `TestMultiPhraseQuery` | 1 | Nightly phrase query test |

## Excluded Tests (in build.gradle)

These tests are explicitly excluded from the filter because they are incompatible with the crypto directory:

| Exclusion | Reason |
|-----------|--------|
| `TestCodecLoadingDeadlock` | Requires multi-JVM fork |
| `TestStressLockFactories` | Requires multi-JVM fork |
| `TestKnn*Query.testSameFieldDifferentFormats` (7 tests) | Classpath includes backward-codecs with read-only KnnVectorsFormats |
| `TestCodecHoldsOpenFiles` | Block cache lazy-loads from deliberately deleted files |
| `TestIndexWriterOutOfFileDescriptors` | Block cache lazy-loads from deliberately deleted files |
| `TestAllFilesDetectTruncation` | Open file leak on truncation test |
| `TestLucene90CompoundFormat.testReadPastEOF` | Crypto buffering doesn't throw IOException on read-past-EOF |
| `TestField.testKnnVectorField` | Crypto buffering doesn't throw IOException on read-past-EOF |
| `TestBoolean2` | Cascading merge thread failure in crypto directory |
| `TestMultiDocValues` | NoSuchFileException from block cache on deleted files |
| `TestStressIndexing2` | Pending delete race with concurrent file operations |
| `TestPointQueries.testRandomLongsBig` | Pending delete race |
| `TestIndexWriterCommit.testCommitThreadSafety` | Timing-dependent assertion |

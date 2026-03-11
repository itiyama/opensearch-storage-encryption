# Developer Guide

This guide helps developers get started with building and testing the OpenSearch Storage Encryption plugin.

## Prerequisites

1. **JDK 21** or higher (Note: Code uses preview features that are patched at build time)

2. Gradle (included via wrapper)

3. Linux with kernel 5.1+ (recommended for Direct I/O with io_uring support)

## Environment Setup

Create an environment file for your AWS credentials and KMS configuration:

```bash
# Create environment file for sensitive information
cat > .env << 'EOF'
# AWS Credentials for KMS
AWS_ACCESS_KEY_ID="your_access_key_here"
AWS_SECRET_ACCESS_KEY="your_secret_key_here"
AWS_SESSION_TOKEN="your_session_token_here"
# AWS KMS configuration
KMS_REGION="<your_kms_region>"
KMS_KEY_ARN="<your_kms_key_arn>"
EOF

# Edit the file with your actual values
# Then source the environment file
source .env
```

## Building the Plugin Alone

If you only need to build the plugin without setting up a full OpenSearch environment:

```bash
# Clone the Storage Encryption plugin
git clone https://github.com/opensearch-project/opensearch-storage-encryption.git
cd opensearch-storage-encryption

# Build the plugin
./gradlew clean assemble

# Run all checks including tests
./gradlew check
```

## Development Setup with OpenSearch

For a complete development environment:

```bash
# Set up required variables
OPENSEARCH_VERSION="3.3.0-SNAPSHOT"
BASE_DIR="$(pwd)"
OPENSEARCH_DIR="${BASE_DIR}/OpenSearch"
STORAGE_ENCRYPTION_DIR="${BASE_DIR}/opensearch-storage-encryption"
OPENSEARCH_DIST_DIR="${OPENSEARCH_DIR}/build/distribution/local/opensearch-${OPENSEARCH_VERSION}"
JVM_HEAP_SIZE="4g"
JVM_DIRECT_MEM_SIZE="4g"
DEBUG_PORT="5005"

# Create and navigate to your workspace directory
mkdir -p "${BASE_DIR}" && cd "${BASE_DIR}"

# Clone OpenSearch
git clone https://github.com/opensearch-project/OpenSearch.git "${OPENSEARCH_DIR}"

# Clone Storage Encryption plugin
git clone https://github.com/opensearch-project/opensearch-storage-encryption.git "${STORAGE_ENCRYPTION_DIR}"

# Build Storage Encryption plugin
cd "${STORAGE_ENCRYPTION_DIR}"
./gradlew clean assemble

# Build Crypto KMS plugin (required dependency)
cd "${OPENSEARCH_DIR}"
./gradlew :plugins:crypto-kms:assemble

# Build local distribution
./gradlew localDistro
```

## Installing and Configuring Plugins

```bash
# Navigate to the OpenSearch distribution directory
cd "${OPENSEARCH_DIST_DIR}/bin"

# Install Storage Encryption plugin
./opensearch-plugin install file:${STORAGE_ENCRYPTION_DIR}/build/distributions/storage-encryption.zip

# Install Crypto KMS plugin
./opensearch-plugin install file:${OPENSEARCH_DIR}/plugins/crypto-kms/build/distributions/crypto-kms-${OPENSEARCH_VERSION}.zip

# Create keystore and add credentials from environment variables
./opensearch-keystore create
echo "${AWS_SESSION_TOKEN}" | ./opensearch-keystore add -x kms.session_token
echo "${AWS_ACCESS_KEY_ID}" | ./opensearch-keystore add -x kms.access_key
echo "${AWS_SECRET_ACCESS_KEY}" | ./opensearch-keystore add -x kms.secret_key

# Append KMS configuration to opensearch.yml
cat >> "${OPENSEARCH_DIST_DIR}/config/opensearch.yml" << EOF
# KMS Configuration
kms.region: ${KMS_REGION}
kms.key_arn: ${KMS_KEY_ARN}
EOF

# Update JVM settings
JVM_OPTIONS_FILE="${OPENSEARCH_DIST_DIR}/config/jvm.options"

# Update heap size (
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "Running macOS version..."
    sed -i '' "s/-Xms1g/-Xms${JVM_HEAP_SIZE}/g" "${JVM_OPTIONS_FILE}"
    sed -i '' "s/-Xmx1g/-Xmx${JVM_HEAP_SIZE}/g" "${JVM_OPTIONS_FILE}"
else
    echo "Running Linux version..."
    sed -i "s/-Xms1g/-Xms${JVM_HEAP_SIZE}/g" "${JVM_OPTIONS_FILE}"
    sed -i "s/-Xmx1g/-Xmx${JVM_HEAP_SIZE}/g" "${JVM_OPTIONS_FILE}"
fi


add_jvm_option() {
    local option="$1"
    if ! grep -q "^${option}$" "${JVM_OPTIONS_FILE}"; then
        echo "${option}" >> "${JVM_OPTIONS_FILE}"
    fi
}

# Add required JVM options
add_jvm_option "-XX:MaxDirectMemorySize=${JVM_DIRECT_MEM_SIZE}"
```

## Running and Testing OpenSearch

```bash
# Start OpenSearch
./opensearch
```

## Running Tests

### Unit Tests

```bash
cd "${STORAGE_ENCRYPTION_DIR}"
./gradlew test
```

### Integration Tests

```bash
./gradlew integrationTest
```

### YAML Rest Tests

```bash
./gradlew yamlRestTest
```

### Lucene Directory Injection Tests

These tests run Lucene's own test suite against the encrypted directory implementations
(`CryptoNIOFS`, `BufferPool`, `Hybrid`) to validate compatibility. Apache Lucene does not
publish test classes as Maven artifacts, so a local Lucene source tree must be compiled first.

**Prerequisites:**

```bash
# 1. Clone or locate a Lucene source tree (default: sibling ../lucene directory)
# 2. Compile Lucene's core test classes
cd ../lucene
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
./gradlew -p lucene/core compileTestJava
```

**Running the tests:**

```bash
cd opensearch-storage-encryption
export JAVA_HOME=$(/usr/libexec/java_home -v 21)

# Run against a specific directory type
./gradlew luceneCryptoNIOFSIntegTest
./gradlew luceneBufferPoolIntegTest
./gradlew luceneHybridIntegTest

# Run all three
./gradlew allLuceneIntegTests
```

**Custom Lucene location** (if your Lucene tree is not at `../lucene`):

```bash
./gradlew luceneHybridIntegTest -PluceneDir=/path/to/lucene
```

**Write cache mode** — by default, a random `WriteCacheMode` (`WRITE_THROUGH` or `READ_THROUGH`)
is selected per build invocation. To force a specific mode:

```bash
./gradlew luceneHybridIntegTest -Ptests.cacheMode=READ_THROUGH
./gradlew test -Ptests.cacheMode=WRITE_THROUGH
```

## Debugging

To debug the plugin:

```bash
# Verify environment variables
echo "JVM_OPTIONS_FILE=${JVM_OPTIONS_FILE}"
echo "DEBUG_PORT=${DEBUG_PORT}"

# Add debug options to JVM configuration
if ! grep -F '-Xdebug' "${JVM_OPTIONS_FILE}" > /dev/null; then
    echo '-Xdebug' >> "${JVM_OPTIONS_FILE}"
fi

# Add debug port configuration
DEBUG_STRING="-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=*:${DEBUG_PORT}"
if ! grep -F "${DEBUG_STRING}" "${JVM_OPTIONS_FILE}" > /dev/null; then
    echo "${DEBUG_STRING}" >> "${JVM_OPTIONS_FILE}"
fi

# Verify the changes
echo "Updated JVM debug options:"
grep -F 'Xdebug\|Xrunjdwp' "${JVM_OPTIONS_FILE}"

# Connect your IDE debugger to the specified debug port
```

## Architecture Deep Dive

### Plugin Registration

The `CryptoDirectoryPlugin` registers `"cryptofs"` as a custom store type:

```java
@Override
public Map<String, DirectoryFactory> getDirectoryFactories() {
    return Collections.singletonMap("cryptofs", new CryptoDirectoryFactory());
}
```

When an index is created with `index.store.type: "cryptofs"`, the `CryptoDirectoryFactory` is invoked to create the encrypted directory.

### Directory Selection Logic

The `CryptoDirectoryFactory.newFSDirectory()` method selects the appropriate encrypted directory implementation based on the node's default store type:

- **HYBRIDFS** → `HybridCryptoDirectory` (wraps `CryptoDirectIODirectory` + handles NIO extensions)
- **MMAPFS** → `CryptoDirectIODirectory` (MMAP not natively supported)
- **NIOFS/SIMPLEFS** → `CryptoNIOFSDirectory`


## Testing

### Running Specific Tests

```bash
# Run a specific test class
./gradlew test --tests "CipherEncryptionDecryptionTests"

# Run tests matching a pattern
./gradlew test --tests "*Cache*"

# Run integration tests for a specific class
./gradlew internalClusterTest --tests "ShardMigrationIntegTests"

# Run a specific YAML test
./gradlew yamlRestTest --tests "*20_encrypted_index_crud*"
```

### Test Coverage

Generate test coverage report:

```bash
./gradlew test jacocoTestReport

# Report location: build/reports/jacoco/test/html/index.html
```

## Code Style

This project uses the OpenSearch code style with Spotless for formatting.

### Apply Formatting

```bash
# Check formatting
./gradlew spotlessCheck

# Apply formatting
./gradlew spotlessApply
```

### Code Style Rules

- **License Headers**: All files must have Apache 2.0 license header
- **Import Order**: java, javax, org, com
- **Eclipse Formatter**: Uses `.eclipseformat.xml` configuration
- **Line Length**: Maximum 120 characters (where practical)

## Contributing

1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/my-feature`)
3. **Make** your changes
4. **Run** tests and formatting:
   ```bash
   ./gradlew spotlessApply
   ./gradlew check
   ```
5. **Commit** with clear messages
6. **Push** to your fork
7. **Submit** a pull request


## Performance Considerations

Sample OpenSearch Benchmark http_logs workload in test mode:

**Workload**:

```
 opensearch-benchmark execute-test \
  --kill-running-processes \
  --pipeline=benchmark-only \
  --workload=big5 \
  --test-mode \
  --target-hosts=http://localhost:9200 \
  --workload-params '{
    "number_of_shards": "5",
    "number_of_replicas": "0",
    "index_settings": {
    "index.store.type": "cryptofs",
    "index.store.crypto.key_provider":"aws-kms",
    "index.store.crypto.kms.key_arn":"<kms_key>"
    }
  }'
```

### Profiling

For performance analysis:

```bash
# Enable JFR recording
-XX:StartFlightRecording=filename=recording.jfr,duration=60s

# Analyze with profiler.amazon.com
# Upload recording.jfr to internal profiling tools
```

## Troubleshooting

### Common Issues

#### Plugin Installation Fails
- **Cause**: Version mismatch between OpenSearch and plugin
- **Solution**: Ensure compatible versions between OpenSearch and Plugin version (Example: both being on `3.3.0-SNAPSHOT`)

#### KMS Integration Issues
- **Cause**: Invalid AWS credentials or permissions
- **Solution**: 
  - Verify credentials in keystore
  - Check KMS key policy allows encrypt/decrypt operations
  - Test with AWS CLI: `aws kms encrypt --key-id <arn> --plaintext "test"`

#### Memory Issues
- **Symptoms**: `OutOfMemoryError` or slow performance
- **Solutions**:
  - Increase heap: `-Xmx4g`
  - Increase direct memory: `-XX:MaxDirectMemorySize=4g`
  - Reduce pool size: `node.store.crypto.pool_size_percentage: 0.2`
  - Reduce cache ratio: `node.store.crypto.cache_to_pool_ratio: 0.5`

#### Build Failures
- **Cause**: Missing dependencies or JDK version mismatch
- **Solutions**:
  - Clean build: `./gradlew clean`
  - Verify JDK: `java -version` (must be 21+)
  - Check Gradle: `./gradlew --version`

#### Test Failures
- **Integration Tests**: Ensure crypto-kms plugin is built
- **AWS Tests**: Check AWS credentials and permissions
- **Timing Issues**: Increase timeouts in CI environments

### Debug Logging

Enable detailed logging in `log4j2.properties`:

```properties
logger.crypto.name = org.opensearch.index.store
logger.crypto.level = debug

logger.key.name = org.opensearch.index.store.key
logger.key.level = trace
```

## IntelliJ IDEA Setup for Lucene Injection Tests

Running and debugging the Lucene injection tests from IDEA requires extra setup because the
test classes live in an external Lucene source tree (Lucene does not publish test-jars to Maven).

### 1. Compile Lucene Test Classes and Import

First, compile Lucene's core test classes:

```bash
cd ../lucene
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
./gradlew -p lucene/core compileTestJava
```

Then open `opensearch-storage-encryption` in IDEA as a Gradle project. The Lucene test classes
are already linked as a dependency via `build.gradle` (the `luceneCoreTestClasses` and
`luceneCoreTestResources` entries). After Gradle sync, IDEA will resolve them automatically.

If your Lucene tree is not at the default `../lucene` location, add this to your
`gradle.properties` (project-level or `~/.gradle/gradle.properties`):

```properties
luceneDir=/path/to/lucene
```

### 2. Create a JUnit Run Configuration

To run a specific Lucene test (e.g. `TestIndexWriter.testDocCount`) against an encrypted directory:

1. **Run → Edit Configurations → + → JUnit**
2. Configure the following:

| Field | Value |
|-------|-------|
| **Name** | `Lucene Hybrid Injection - TestIndexWriter` |
| **Module** | `opensearch-storage-encryption.test` |
| **Test kind** | Class (or Method) |
| **Class** | `org.apache.lucene.index.TestIndexWriter` |
| **VM options** | see below |
| **Working directory** | `$MODULE_DIR$` |

**VM options** (paste as a single block):

```
--enable-native-access=ALL-UNNAMED
-Dtests.directory=org.opensearch.index.store.TestableHybridCryptoDirectory
-Dtests.security.manager=false
-Dtests.cacheMode=WRITE_THROUGH
-ea
```

Replace the `tests.directory` value for other directory types:
- `org.opensearch.index.store.TestableBufferPoolDirectory`
- `org.opensearch.index.store.TestableCryptoNIOFSDirectory`

### 4. Debugging

With the run configuration above, you can:

- **Set breakpoints** in both the encryption plugin code (e.g. `BufferPoolDirectory.openInput`)
  and Lucene test code (e.g. `TestIndexWriter`)
- **Run → Debug** the configuration to hit breakpoints in either codebase
- **Step through** the full write → encrypt → cache → read → decrypt flow

### 5. Reproducing a Specific Failure

Lucene's randomized testing framework uses seeds. To reproduce a specific failure, add
the seed to VM options:

```
-Dtests.seed=DEADBEEF
```

The seed is printed in the Gradle test output and in the IDEA test runner output on every run.

### Getting Help

- **Issues**: [GitHub Issues](https://github.com/opensearch-project/opensearch-storage-encryption/issues)
- **Discussions**: [OpenSearch Forums](https://forum.opensearch.org/)
- **Slack**: OpenSearch community Slack workspace

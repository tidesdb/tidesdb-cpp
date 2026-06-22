/**
 * Copyright (C) TidesDB
 *
 * Original Author: Alex Gaetano Padula
 *
 * Licensed under the Mozilla Public License, v. 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.mozilla.org/en-US/MPL/2.0/
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef TIDESDB_CPP_HPP
#define TIDESDB_CPP_HPP

#include <cstddef>
#include <cstdint>
#include <ctime>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

extern "C"
{
#include <tidesdb/db.h>

    /**
     * Create an S3-compatible object store connector (AWS S3, MinIO, etc.).
     * The underlying libtidesdb must have been built with TIDESDB_WITH_S3=ON;
     * otherwise this symbol is unresolved at link time.
     */
    tidesdb_objstore_t* tidesdb_objstore_s3_create(const char* endpoint, const char* bucket,
                                                   const char* prefix, const char* access_key,
                                                   const char* secret_key, const char* region,
                                                   int use_ssl, int use_path_style);
}

namespace tidesdb
{

/**
 * @brief Compression algorithms supported by TidesDB
 */
enum class CompressionAlgorithm
{
    None = TDB_COMPRESS_NONE,
#ifndef __sun
    Snappy = TDB_COMPRESS_SNAPPY,
#endif
    LZ4 = TDB_COMPRESS_LZ4,
    Zstd = TDB_COMPRESS_ZSTD,
    LZ4Fast = TDB_COMPRESS_LZ4_FAST
};

/**
 * @brief Sync modes for durability control
 */
enum class SyncMode
{
    None = TDB_SYNC_NONE,
    Full = TDB_SYNC_FULL,
    Interval = TDB_SYNC_INTERVAL
};

/**
 * @brief Log levels for database logging
 */
enum class LogLevel
{
    Debug = TDB_LOG_DEBUG,
    Info = TDB_LOG_INFO,
    Warn = TDB_LOG_WARN,
    Error = TDB_LOG_ERROR,
    Fatal = TDB_LOG_FATAL,
    None = TDB_LOG_NONE
};

/**
 * @brief Transaction isolation levels
 */
enum class IsolationLevel
{
    ReadUncommitted = TDB_ISOLATION_READ_UNCOMMITTED,
    ReadCommitted = TDB_ISOLATION_READ_COMMITTED,
    RepeatableRead = TDB_ISOLATION_REPEATABLE_READ,
    Snapshot = TDB_ISOLATION_SNAPSHOT,
    Serializable = TDB_ISOLATION_SERIALIZABLE
};

/**
 * @brief Error codes from TidesDB operations
 */
enum class ErrorCode
{
    Success = TDB_SUCCESS,
    Memory = TDB_ERR_MEMORY,
    InvalidArgs = TDB_ERR_INVALID_ARGS,
    NotFound = TDB_ERR_NOT_FOUND,
    IO = TDB_ERR_IO,
    Corruption = TDB_ERR_CORRUPTION,
    Exists = TDB_ERR_EXISTS,
    Conflict = TDB_ERR_CONFLICT,
    TooLarge = TDB_ERR_TOO_LARGE,
    MemoryLimit = TDB_ERR_MEMORY_LIMIT,
    InvalidDB = TDB_ERR_INVALID_DB,
    Unknown = TDB_ERR_UNKNOWN,
    Locked = TDB_ERR_LOCKED,
    Readonly = TDB_ERR_READONLY,
    Busy = TDB_ERR_BUSY,
    Precondition = TDB_ERR_PRECONDITION
};

/**
 * @brief Exception class for TidesDB errors
 */
class Exception : public std::runtime_error
{
   public:
    explicit Exception(ErrorCode code, const std::string& message)
        : std::runtime_error(message), code_(code)
    {
    }

    [[nodiscard]] ErrorCode code() const noexcept
    {
        return code_;
    }

    [[nodiscard]] static std::string errorMessage(int code)
    {
        switch (code)
        {
            case TDB_SUCCESS:
                return "success";
            case TDB_ERR_MEMORY:
                return "memory allocation failed";
            case TDB_ERR_INVALID_ARGS:
                return "invalid arguments";
            case TDB_ERR_NOT_FOUND:
                return "not found";
            case TDB_ERR_IO:
                return "I/O error";
            case TDB_ERR_CORRUPTION:
                return "data corruption";
            case TDB_ERR_EXISTS:
                return "already exists";
            case TDB_ERR_CONFLICT:
                return "transaction conflict";
            case TDB_ERR_TOO_LARGE:
                return "key or value too large";
            case TDB_ERR_MEMORY_LIMIT:
                return "memory limit exceeded";
            case TDB_ERR_INVALID_DB:
                return "invalid database handle";
            case TDB_ERR_LOCKED:
                return "database is locked";
            case TDB_ERR_READONLY:
                return "database is read-only";
            case TDB_ERR_BUSY:
                return "database is busy";
            case TDB_ERR_PRECONDITION:
                return "precondition failed";
            default:
                return "unknown error";
        }
    }

   private:
    ErrorCode code_;
};

/**
 * @brief Initialize TidesDB with optional custom memory allocation functions
 *
 * Calling this is optional: the first @ref TidesDB open lazily initializes the
 * library with the system allocator. Call init() explicitly *before* opening any
 * database only when you need to install custom allocators (e.g. embedding inside
 * Redis or another host that owns memory). Pass nullptr for any function to keep
 * the corresponding system allocator. Must be called at most once before a
 * matching @ref finalize.
 *
 * @param mallocFn Custom malloc (or nullptr for system malloc)
 * @param callocFn Custom calloc (or nullptr for system calloc)
 * @param reallocFn Custom realloc (or nullptr for system realloc)
 * @param freeFn Custom free (or nullptr for system free)
 * @return true if this call performed initialization, false if TidesDB was
 *         already initialized (the allocators are left unchanged)
 */
bool init(tidesdb_malloc_fn mallocFn = nullptr, tidesdb_calloc_fn callocFn = nullptr,
          tidesdb_realloc_fn reallocFn = nullptr, tidesdb_free_fn freeFn = nullptr);

/**
 * @brief Finalize TidesDB and reset the allocator
 *
 * Call after all TidesDB operations are complete (all databases closed). After
 * this returns, @ref init may be called again with different allocators.
 */
void finalize();

/**
 * @brief Raise this process's open-file ceiling toward @p desired descriptors
 *
 * Lets a database keep more SSTables open. Must be called BEFORE opening the
 * database -- the engine sizes its open-SSTable cap to fit the ceiling at open
 * time. This is an explicit, opt-in operator action; TidesDB never raises the
 * limit itself. A failed or partial raise is non-fatal.
 *
 * @param desired Target descriptor count; <= 0 just reports the current ceiling
 * @return The open-file ceiling in effect after the attempt
 */
long raiseOpenFileLimit(long desired);

/**
 * @brief Built-in comparator function pointers
 *
 * These match the comparators TidesDB registers by name. Use the matching name
 * string in @ref ColumnFamilyConfig::comparatorName to select one for a column
 * family, or pass the function pointer directly to
 * @ref TidesDB::registerComparator when wrapping one under a different name.
 */
namespace comparators
{
/** Binary byte-by-byte comparison (registered name "memcmp", the default). */
inline const tidesdb_comparator_fn memcmp = tidesdb_comparator_memcmp;
/** Null-terminated string comparison (registered name "lexicographic"). */
inline const tidesdb_comparator_fn lexicographic = tidesdb_comparator_lexicographic;
/** Unsigned 64-bit integer comparison (registered name "uint64"). */
inline const tidesdb_comparator_fn uint64 = tidesdb_comparator_uint64;
/** Signed 64-bit integer comparison (registered name "int64"). */
inline const tidesdb_comparator_fn int64 = tidesdb_comparator_int64;
/** Reverse binary comparison (registered name "reverse"). */
inline const tidesdb_comparator_fn reverseMemcmp = tidesdb_comparator_reverse_memcmp;
/** Case-insensitive ASCII comparison (registered name "case_insensitive"). */
inline const tidesdb_comparator_fn caseInsensitive = tidesdb_comparator_case_insensitive;
}  // namespace comparators

/**
 * @brief Object store connector factories
 *
 * A connector returned by these factories is handed to @ref Config::objectStore.
 * Ownership transfers to the database on a successful open -- the database frees
 * the connector when it closes, so callers must not destroy it themselves. If
 * @ref TidesDB construction throws, the connector is leaked by the C library, so
 * only build a connector immediately before opening.
 */
namespace objstore
{
/**
 * @brief Create a filesystem-backed object store connector
 *
 * Stores objects as files under @p rootDir mirroring the key path structure.
 * Always available. Intended for testing and local replication.
 * @param rootDir Directory to store objects in
 * @return Connector handle, or nullptr on error
 */
inline tidesdb_objstore_t* filesystem(const std::string& rootDir)
{
    return tidesdb_objstore_fs_create(rootDir.c_str());
}

/**
 * @brief Create an S3-compatible connector (AWS S3, MinIO, etc.)
 *
 * The underlying libtidesdb must have been built with TIDESDB_WITH_S3=ON;
 * otherwise this symbol is unresolved at link time. Empty @p prefix or
 * @p region are forwarded as nullptr.
 *
 * @param endpoint S3 endpoint (e.g. "s3.amazonaws.com" or "minio.local:9000")
 * @param bucket Bucket name
 * @param prefix Key prefix (e.g. "production/db1/"), or empty for none
 * @param accessKey AWS access key ID
 * @param secretKey AWS secret access key
 * @param region AWS region (e.g. "us-east-1"), or empty for MinIO
 * @param useSsl true for HTTPS, false for HTTP
 * @param usePathStyle true for path-style URLs (MinIO), false for virtual-hosted (AWS)
 * @return Connector handle, or nullptr on error
 */
inline tidesdb_objstore_t* s3(const std::string& endpoint, const std::string& bucket,
                              const std::string& prefix, const std::string& accessKey,
                              const std::string& secretKey, const std::string& region, bool useSsl,
                              bool usePathStyle)
{
    return tidesdb_objstore_s3_create(endpoint.c_str(), bucket.c_str(),
                                      prefix.empty() ? nullptr : prefix.c_str(), accessKey.c_str(),
                                      secretKey.c_str(), region.empty() ? nullptr : region.c_str(),
                                      useSsl ? 1 : 0, usePathStyle ? 1 : 0);
}

/**
 * @brief Full S3 connector configuration including TLS and multipart tuning
 *
 * The all-default values are secure (TLS verification on, no custom CA) and use
 * the library's built-in multipart sizes.
 */
struct S3Config
{
    std::string endpoint;                // S3 endpoint (required)
    std::string bucket;                  // Bucket name (required)
    std::string prefix;                  // Key prefix, or empty for none
    std::string accessKey;               // AWS access key ID (required)
    std::string secretKey;               // AWS secret access key (required)
    std::string region;                  // AWS region, or empty for the default
    bool useSsl = true;                  // true for HTTPS, false for HTTP
    bool usePathStyle = false;           // true for path-style URLs (MinIO)
    std::string tlsCaPath;               // Custom CA bundle path, or empty for the system bundle
    bool tlsInsecureSkipVerify = false;  // true disables TLS verification (test only, insecure)
    std::size_t multipartThreshold = 0;  // Multipart upload threshold in bytes (0 = default)
    std::size_t multipartPartSize = 0;   // Multipart chunk size in bytes (0 = default)
};

/**
 * @brief Create an S3-compatible connector from a full configuration struct
 *
 * Exposes TLS and multipart settings the positional @ref s3 overload cannot.
 * Requires a libtidesdb built with TIDESDB_WITH_S3=ON.
 * @param config Connector configuration (fields are copied; need not outlive the call)
 * @return Connector handle, or nullptr on error
 */
inline tidesdb_objstore_t* s3(const S3Config& config)
{
    tidesdb_objstore_s3_config_t c{};
    c.endpoint = config.endpoint.c_str();
    c.bucket = config.bucket.c_str();
    c.prefix = config.prefix.empty() ? nullptr : config.prefix.c_str();
    c.access_key = config.accessKey.c_str();
    c.secret_key = config.secretKey.c_str();
    c.region = config.region.empty() ? nullptr : config.region.c_str();
    c.use_ssl = config.useSsl ? 1 : 0;
    c.use_path_style = config.usePathStyle ? 1 : 0;
    c.tls_ca_path = config.tlsCaPath.empty() ? nullptr : config.tlsCaPath.c_str();
    c.tls_insecure_skip_verify = config.tlsInsecureSkipVerify ? 1 : 0;
    c.multipart_threshold = config.multipartThreshold;
    c.multipart_part_size = config.multipartPartSize;
    return tidesdb_objstore_s3_create_config(&c);
}
}  // namespace objstore

// Forward declarations
class TidesDB;
class Transaction;
class Iterator;

/**
 * @brief Configuration for a column family
 */
struct ColumnFamilyConfig
{
    std::size_t writeBufferSize = 64 * 1024 * 1024;
    std::size_t levelSizeRatio = 10;
    int minLevels = 5;
    int dividingLevelOffset = 2;
    std::size_t klogValueThreshold = 512;
    CompressionAlgorithm compressionAlgorithm = CompressionAlgorithm::LZ4;
    bool enableBloomFilter = true;
    double bloomFPR = 0.01;
    bool enableBlockIndexes = true;
    int indexSampleRatio = 1;
    int blockIndexPrefixLen = 16;
    SyncMode syncMode = SyncMode::Interval;
    std::uint64_t syncIntervalUs = 128000;
    std::string comparatorName;
    std::string comparatorCtxStr;  // Context string persisted alongside comparatorName
    int skipListMaxLevel = 12;
    float skipListProbability = 0.25f;
    IsolationLevel defaultIsolationLevel = IsolationLevel::ReadCommitted;
    std::uint64_t minDiskSpace = 100 * 1024 * 1024;
    int l1FileCountTrigger = 4;
    int l0QueueStallThreshold = 20;
    double tombstoneDensityTrigger = 0.0;  // Per-SSTable tombstone density above which
                                           // compaction priority escalates (0.0 = disabled)
    std::uint64_t tombstoneDensityMinEntries =
        1024;               // SSTables with fewer entries are ignored by the density trigger
    bool useBtree = false;  // Use B+tree format for klog (default: false = block-based)
    tidesdb_commit_hook_fn commitHookFn = nullptr;  // Optional commit hook callback (runtime-only)
    void* commitHookCtx = nullptr;     // Optional user context for commit hook (runtime-only)
    int objectLazyCompaction = 0;      // 1 = compact less aggressively in object store mode
    int objectPrefetchCompaction = 1;  // 1 = download all inputs before merge

    /**
     * @brief Get default column family configuration from TidesDB
     */
    static ColumnFamilyConfig defaultConfig();

    /**
     * @brief Load column family configuration from an INI file
     * @param iniFile Path to the INI file
     * @param sectionName Section name in the INI file
     * @return Loaded configuration
     */
    static ColumnFamilyConfig loadFromIni(const std::string& iniFile,
                                          const std::string& sectionName);

    /**
     * @brief Save column family configuration to an INI file
     * @param iniFile Path to the INI file
     * @param sectionName Section name in the INI file
     * @param config Configuration to save
     */
    static void saveToIni(const std::string& iniFile, const std::string& sectionName,
                          const ColumnFamilyConfig& config);
};

/**
 * @brief Object store configuration for controlling cache, upload, and replication behavior
 */
struct ObjectStoreConfig
{
    std::string localCachePath;  // Local directory for cached SSTable files (empty = use db_path)
    std::size_t localCacheMaxBytes = 0;  // Max local cache size in bytes (0 = unlimited)
    bool cacheOnRead = true;             // Cache downloaded files locally
    bool cacheOnWrite = true;            // Keep local copy after upload
    int maxConcurrentUploads = 4;        // Parallel upload threads
    int maxConcurrentDownloads = 8;      // Parallel download threads
    std::size_t multipartThreshold =
        64 * 1024 * 1024;                             // Use multipart upload above this size (64MB)
    std::size_t multipartPartSize = 8 * 1024 * 1024;  // Chunk size for multipart uploads (8MB)
    bool syncManifestToObject = true;                 // Upload MANIFEST after each compaction
    bool replicateWal = true;                         // Upload closed WAL segments for replication
    bool walUploadSync = false;  // false = background WAL upload, true = block flush until uploaded
    std::size_t walSyncThresholdBytes =
        1048576;                   // Sync active WAL when it grows by this many bytes (0 = off)
    bool walSyncOnCommit = false;  // Upload WAL after every txn commit for RPO=0
    bool replicaMode = false;      // Enable read-only replica mode
    std::uint64_t replicaSyncIntervalUs = 5000000;  // MANIFEST poll interval in microseconds (5s)
    bool replicaReplayWal = true;  // Replay WAL from object store for near-real-time reads

    /**
     * @brief Get default object store configuration from TidesDB
     */
    static ObjectStoreConfig defaultConfig();
};

/**
 * @brief Database configuration
 */
struct Config
{
    std::string dbPath;
    int numFlushThreads = 2;
    int numCompactionThreads = 2;
    LogLevel logLevel = LogLevel::Info;
    std::size_t blockCacheSize = 64 * 1024 * 1024;
    std::size_t maxOpenSSTables = 256;
    bool logToFile = false;
    std::size_t logTruncationAt = 24 * 1024 * 1024;
    std::size_t maxMemoryUsage = 0;  // Global memory limit in bytes (0 = auto)
    bool unifiedMemtable = false;    // Enable unified memtable mode (default: per-CF memtables)
    std::size_t unifiedMemtableWriteBufferSize =
        0;                                         // Unified memtable write buffer size (0 = auto)
    int unifiedMemtableSkipListMaxLevel = 0;       // Skip list max level (0 = default 12)
    float unifiedMemtableSkipListProbability = 0;  // Skip list probability (0 = default 0.25)
    SyncMode unifiedMemtableSyncMode = SyncMode::None;  // Sync mode for unified WAL
    std::uint64_t unifiedMemtableSyncIntervalUs = 0;    // Sync interval for unified WAL
    int maxConcurrentFlushes =
        0;  // Global cap on in-flight memtable flushes across all CFs (0 = library default)
    bool finishCompactionsOnClose =
        false;  // false = cancel in-flight compactions at their next checkpoint for a fast
                // shutdown (no data loss); true = let them run to completion before close returns
    tidesdb_objstore_t* objectStore =
        nullptr;  // Pluggable object store connector (nullptr = local only)
    std::optional<ObjectStoreConfig>
        objectStoreConfig;  // Object store behavior config (nullopt = defaults)
};

/**
 * @brief Statistics for a column family
 */
struct Stats
{
    int numLevels = 0;
    std::size_t memtableSize = 0;
    std::vector<std::size_t> levelSizes;
    std::vector<int> levelNumSSTables;
    std::optional<ColumnFamilyConfig> config;
    std::uint64_t totalKeys = 0;
    std::uint64_t totalDataSize = 0;
    double avgKeySize = 0.0;
    double avgValueSize = 0.0;
    std::vector<std::uint64_t> levelKeyCounts;
    double readAmp = 0.0;
    double hitRate = 0.0;
    bool useBtree = false;              // Whether column family uses B+tree format
    std::uint64_t btreeTotalNodes = 0;  // Total B+tree nodes across all SSTables
    std::uint32_t btreeMaxHeight = 0;   // Maximum tree height across all SSTables
    double btreeAvgHeight = 0.0;        // Average tree height across all SSTables
    std::uint64_t totalTombstones = 0;  // Sum of tombstone counts across all SSTables
    double tombstoneRatio = 0.0;        // total_tombstones / total_keys (0.0 if total_keys == 0)
    std::vector<std::uint64_t>
        levelTombstoneCounts;    // Per-level tombstone counts (parallels levelKeyCounts)
    double maxSstDensity = 0.0;  // Worst per-SSTable tombstone density observed (0.0 to 1.0)
    int maxSstDensityLevel = 0;  // 1-based level where maxSstDensity was observed (0 if none)
    // Write-amplification counters (lifetime since open, on-disk framed bytes). Divide the
    // write totals by userBytesWritten for this CF's write amplification. walBytesWritten is
    // zero in unified memtable mode (the shared WAL volume is reported db-wide in
    // DbStats::uwalBytesWritten). The *Count fields count output SSTables.
    std::uint64_t walBytesWritten = 0;         // Framed bytes appended to this CF's WAL
    std::uint64_t flushBytesWritten = 0;       // On-disk bytes this CF's flushes wrote to L0
    std::uint64_t compactionBytesWritten = 0;  // On-disk bytes this CF's compactions wrote
    std::uint64_t compactionBytesRead = 0;     // On-disk bytes this CF's compactions read as input
    std::uint64_t userBytesWritten = 0;        // Logical key+value bytes committed (WA denominator)
    std::uint64_t flushCount = 0;              // Flushed SSTables produced by this CF
    std::uint64_t compactionCount = 0;         // Compaction output SSTables produced by this CF
};

/**
 * @brief Database-level statistics
 */
struct DbStats
{
    int numColumnFamilies = 0;
    std::uint64_t totalMemory = 0;
    std::uint64_t availableMemory = 0;
    std::size_t resolvedMemoryLimit = 0;
    int memoryPressureLevel = 0;
    int flushPendingCount = 0;
    std::int64_t totalMemtableBytes = 0;
    int totalImmutableCount = 0;
    int totalSstableCount = 0;
    std::uint64_t totalDataSizeBytes = 0;
    int numOpenSstables = 0;
    std::uint64_t globalSeq = 0;
    std::int64_t txnMemoryBytes = 0;
    std::size_t compactionQueueSize = 0;
    std::size_t flushQueueSize = 0;
    bool unifiedMemtableEnabled = false;
    std::int64_t unifiedMemtableBytes = 0;
    int unifiedImmutableCount = 0;
    bool unifiedIsFlushing = false;
    std::uint32_t unifiedNextCfIndex = 0;
    std::uint64_t unifiedWalGeneration = 0;
    bool objectStoreEnabled = false;
    std::string objectStoreConnector;
    std::size_t localCacheBytesUsed = 0;
    std::size_t localCacheBytesMax = 0;
    int localCacheNumFiles = 0;
    std::uint64_t lastUploadedGeneration = 0;
    std::size_t uploadQueueDepth = 0;
    std::uint64_t totalUploads = 0;
    std::uint64_t totalUploadFailures = 0;
    bool replicaMode = false;
    // Single-writer fencing (object-store mode). primaryEpoch is the lease epoch this primary
    // currently holds (0 when not a primary / no lease); seenEpoch is the highest lease epoch a
    // replica has observed. A promotion that took bumps primaryEpoch; a fenced primary sees
    // replicaMode flip back to true.
    std::uint64_t primaryEpoch = 0;
    std::uint64_t seenEpoch = 0;
    // Write-amplification counters (lifetime since open, on-disk framed bytes). uwalBytesWritten
    // is the shared unified WAL volume (zero when unified mode is off); the remaining fields are
    // summed across all column families. db-wide WA = (uwal + wal + flush + compaction) / user
    // bytes. The *Count fields count output SSTables, not logical runs.
    std::uint64_t uwalBytesWritten = 0;        // Framed bytes appended to the shared unified WAL
    std::uint64_t walBytesWritten = 0;         // Per-CF WAL bytes summed across all CFs
    std::uint64_t flushBytesWritten = 0;       // Flush output bytes summed across all CFs
    std::uint64_t compactionBytesWritten = 0;  // Compaction output bytes summed across all CFs
    std::uint64_t compactionBytesRead = 0;     // Compaction input bytes summed across all CFs
    std::uint64_t userBytesWritten = 0;        // Logical committed bytes summed across all CFs
    std::uint64_t flushCount = 0;              // Flushed SSTables summed across all CFs
    std::uint64_t compactionCount = 0;         // Compaction output SSTables summed across all CFs
};

/**
 * @brief Block cache statistics
 */
struct CacheStats
{
    bool enabled = false;
    std::size_t totalEntries = 0;
    std::size_t totalBytes = 0;
    std::uint64_t hits = 0;
    std::uint64_t misses = 0;
    double hitRate = 0.0;
    std::size_t numPartitions = 0;
};

/**
 * @brief RAII wrapper for a column family
 */
class ColumnFamily
{
   public:
    ColumnFamily(const ColumnFamily&) = delete;
    ColumnFamily& operator=(const ColumnFamily&) = delete;
    ColumnFamily(ColumnFamily&& other) noexcept;
    ColumnFamily& operator=(ColumnFamily&& other) noexcept;
    ~ColumnFamily() = default;

    /**
     * @brief Get statistics for this column family
     */
    [[nodiscard]] Stats getStats() const;

    /**
     * @brief Manually trigger compaction
     */
    void compact();

    /**
     * @brief Synchronously compact only SSTables overlapping [startKey, endKey)
     *
     * Blocks the calling thread until the merge commits or fails. Pass an empty
     * optional (std::nullopt) for either endpoint to leave that side unbounded.
     * Both endpoints unbounded is rejected with ErrorCode::InvalidArgs - use
     * compact() for a full column-family compaction.
     *
     * @param startKey Start of range (inclusive); std::nullopt = unbounded
     * @param endKey   End of range (exclusive); std::nullopt = unbounded
     */
    void compactRange(const std::optional<std::vector<std::uint8_t>>& startKey,
                      const std::optional<std::vector<std::uint8_t>>& endKey);

    /**
     * @brief compactRange overload taking string_view bounds
     *
     * Pass empty optional for an unbounded endpoint. Empty (but present)
     * string_views are forwarded as zero-length keys, matching the C API.
     */
    void compactRange(const std::optional<std::string_view>& startKey,
                      const std::optional<std::string_view>& endKey);

    /**
     * @brief Manually trigger memtable flush
     */
    void flushMemtable();

    /**
     * @brief Synchronously flush memtable and run aggressive compaction to completion
     *
     * Blocks until all flush and compaction I/O is complete. Use before backup,
     * after bulk deletes, or during maintenance windows where a guaranteed clean
     * on-disk state is required.
     */
    void purge();

    /**
     * @brief Force an immediate fsync of the active write-ahead log
     *
     * Useful for explicit durability control when using SyncMode::None or
     * SyncMode::Interval. Thread-safe.
     */
    void syncWal();

    /**
     * @brief Check if a flush operation is in progress
     * @return true if flushing, false otherwise
     */
    [[nodiscard]] bool isFlushing() const;

    /**
     * @brief Check if a compaction operation is in progress
     * @return true if compacting, false otherwise
     */
    [[nodiscard]] bool isCompacting() const;

    /**
     * @brief Estimate the computational cost of iterating between two keys
     * @param keyA First key (bound of range)
     * @param keyB Second key (bound of range)
     * @return Estimated traversal cost (higher = more expensive, relative scalar)
     */
    [[nodiscard]] double rangeCost(std::string_view keyA, std::string_view keyB) const;

    /**
     * @brief Estimate the computational cost of iterating between two keys (byte vector overload)
     * @param keyA First key (bound of range)
     * @param keyB Second key (bound of range)
     * @return Estimated traversal cost (higher = more expensive, relative scalar)
     */
    [[nodiscard]] double rangeCost(const std::vector<std::uint8_t>& keyA,
                                   const std::vector<std::uint8_t>& keyB) const;

    /**
     * @brief Set or replace the commit hook for this column family
     * @param fn Commit hook callback
     * @param ctx User-provided context passed to the callback
     */
    void setCommitHook(tidesdb_commit_hook_fn fn, void* ctx);

    /**
     * @brief Clear (disable) the commit hook for this column family
     */
    void clearCommitHook();

    /**
     * @brief Update runtime-safe configuration settings
     * @param config New configuration (only runtime-safe fields are applied)
     * @param persistToDisk If true, save changes to config.ini
     */
    void updateRuntimeConfig(const ColumnFamilyConfig& config, bool persistToDisk = true);

    /**
     * @brief Get the underlying C handle (for internal use)
     */
    [[nodiscard]] tidesdb_column_family_t* handle() const noexcept
    {
        return cf_;
    }

   private:
    friend class TidesDB;
    explicit ColumnFamily(tidesdb_column_family_t* cf) : cf_(cf)
    {
    }

    tidesdb_column_family_t* cf_ = nullptr;
};

/**
 * @brief RAII wrapper for an iterator
 */
class Iterator
{
   public:
    Iterator(const Iterator&) = delete;
    Iterator& operator=(const Iterator&) = delete;
    Iterator(Iterator&& other) noexcept;
    Iterator& operator=(Iterator&& other) noexcept;
    ~Iterator();

    /**
     * @brief Seek to the first key
     */
    void seekToFirst();

    /**
     * @brief Seek to the last key
     */
    void seekToLast();

    /**
     * @brief Seek to the first key >= target
     */
    void seek(std::string_view key);

    /**
     * @brief Seek to the last key <= target
     */
    void seekForPrev(std::string_view key);

    /**
     * @brief Check if iterator is positioned at a valid entry
     */
    [[nodiscard]] bool valid() const;

    /**
     * @brief Move to the next entry
     */
    void next();

    /**
     * @brief Move to the previous entry
     */
    void prev();

    /**
     * @brief Get the current key
     */
    [[nodiscard]] std::vector<std::uint8_t> key() const;

    /**
     * @brief Get the current value
     */
    [[nodiscard]] std::vector<std::uint8_t> value() const;

    /**
     * @brief Get the current key and value in a single call
     * @return Pair of key and value byte vectors
     */
    [[nodiscard]] std::pair<std::vector<std::uint8_t>, std::vector<std::uint8_t>> keyValue() const;

   private:
    friend class Transaction;
    explicit Iterator(tidesdb_iter_t* iter) : iter_(iter)
    {
    }

    tidesdb_iter_t* iter_ = nullptr;
};

/**
 * @brief RAII wrapper for a transaction
 */
class Transaction
{
   public:
    Transaction(const Transaction&) = delete;
    Transaction& operator=(const Transaction&) = delete;
    Transaction(Transaction&& other) noexcept;
    Transaction& operator=(Transaction&& other) noexcept;
    ~Transaction();

    /**
     * @brief Put a key-value pair
     * @param cf Column family
     * @param key Key bytes
     * @param value Value bytes
     * @param ttl Unix timestamp for expiration, or -1 for no expiration
     */
    void put(ColumnFamily& cf, std::string_view key, std::string_view value, std::time_t ttl = -1);

    /**
     * @brief Put a key-value pair (byte vector overload)
     */
    void put(ColumnFamily& cf, const std::vector<std::uint8_t>& key,
             const std::vector<std::uint8_t>& value, std::time_t ttl = -1);

    /**
     * @brief Get a value by key
     * @return Value bytes, or empty vector if not found
     */
    [[nodiscard]] std::vector<std::uint8_t> get(ColumnFamily& cf, std::string_view key);

    /**
     * @brief Get a value by key (byte vector overload)
     */
    [[nodiscard]] std::vector<std::uint8_t> get(ColumnFamily& cf,
                                                const std::vector<std::uint8_t>& key);

    /**
     * @brief Delete a key
     */
    void del(ColumnFamily& cf, std::string_view key);

    /**
     * @brief Delete a key (byte vector overload)
     */
    void del(ColumnFamily& cf, const std::vector<std::uint8_t>& key);

    /**
     * @brief Single-delete a key
     *
     * Emits a single-delete tombstone. When a single-delete meets exactly one
     * prior put for the same key during compaction, both records are dropped.
     * Use only when the caller guarantees at most one put precedes the delete;
     * otherwise prefer del(). See tidesdb_txn_single_delete (TDB v9.1.0).
     */
    void singleDelete(ColumnFamily& cf, std::string_view key);

    /**
     * @brief Single-delete a key (byte vector overload)
     */
    void singleDelete(ColumnFamily& cf, const std::vector<std::uint8_t>& key);

    /**
     * @brief Commit the transaction
     */
    void commit();

    /**
     * @brief Rollback the transaction
     */
    void rollback();

    /**
     * @brief Create a savepoint
     */
    void savepoint(const std::string& name);

    /**
     * @brief Rollback to a savepoint
     */
    void rollbackToSavepoint(const std::string& name);

    /**
     * @brief Release a savepoint without rolling back
     */
    void releaseSavepoint(const std::string& name);

    /**
     * @brief Create a new iterator for a column family
     */
    [[nodiscard]] Iterator newIterator(ColumnFamily& cf);

    /**
     * @brief Reset a committed or aborted transaction for reuse
     * @param isolation New isolation level for the reset transaction
     */
    void reset(IsolationLevel isolation);

   private:
    friend class TidesDB;
    explicit Transaction(tidesdb_txn_t* txn) : txn_(txn)
    {
    }

    tidesdb_txn_t* txn_ = nullptr;
};

/**
 * @brief Main TidesDB database class
 *
 * This class provides a C++17 RAII wrapper around the TidesDB C API.
 * It manages the database lifecycle and provides methods for all database operations.
 */
class TidesDB
{
   public:
    /**
     * @brief Open a TidesDB database
     * @param config Database configuration
     * @throws Exception on failure
     */
    explicit TidesDB(const Config& config);

    TidesDB(const TidesDB&) = delete;
    TidesDB& operator=(const TidesDB&) = delete;
    TidesDB(TidesDB&& other) noexcept;
    TidesDB& operator=(TidesDB&& other) noexcept;

    /**
     * @brief Close the database
     */
    ~TidesDB();

    /**
     * @brief Create a new column family
     * @param name Column family name
     * @param config Column family configuration
     */
    void createColumnFamily(const std::string& name, const ColumnFamilyConfig& config);

    /**
     * @brief Drop a column family by name
     * @param name Column family name
     */
    void dropColumnFamily(const std::string& name);

    /**
     * @brief Delete a column family by handle (faster when you already hold a handle)
     * @param cf Column family handle
     */
    void deleteColumnFamily(ColumnFamily& cf);

    /**
     * @brief Get a column family by name
     * @param name Column family name
     * @return Column family handle
     */
    [[nodiscard]] ColumnFamily getColumnFamily(const std::string& name);

    /**
     * @brief List all column families
     * @return Vector of column family names
     */
    [[nodiscard]] std::vector<std::string> listColumnFamilies();

    /**
     * @brief Begin a new transaction with default isolation level
     * @return Transaction handle
     */
    [[nodiscard]] Transaction beginTransaction();

    /**
     * @brief Begin a new transaction with specified isolation level
     * @param isolation Isolation level
     * @return Transaction handle
     */
    [[nodiscard]] Transaction beginTransaction(IsolationLevel isolation);

    /**
     * @brief Get database-level statistics
     * @return Aggregate statistics across the entire database instance
     */
    [[nodiscard]] DbStats getDbStats();

    /**
     * @brief Get block cache statistics
     * @return Cache statistics
     */
    [[nodiscard]] CacheStats getCacheStats();

    /**
     * @brief Register a custom comparator
     * @param name Comparator name
     * @param fn Comparator function
     * @param ctxStr Context string (optional)
     * @param ctx Context pointer (optional)
     */
    void registerComparator(const std::string& name, tidesdb_comparator_fn fn = nullptr,
                            const std::string& ctxStr = "", void* ctx = nullptr);

    /**
     * @brief Get a registered comparator
     * @param name Comparator name
     * @param fn Output comparator function
     * @param ctx Output context pointer
     */
    void getComparator(const std::string& name, tidesdb_comparator_fn* fn, void** ctx);

    /**
     * @brief Rename a column family
     * @param oldName Current name of the column family
     * @param newName New name for the column family
     */
    void renameColumnFamily(const std::string& oldName, const std::string& newName);

    /**
     * @brief Clone a column family
     * @param srcName Source column family name
     * @param dstName Destination column family name
     */
    void cloneColumnFamily(const std::string& srcName, const std::string& dstName);

    /**
     * @brief Create a backup of the database
     * @param dir Backup directory (must be empty or non-existent)
     */
    void backup(const std::string& dir);

    /**
     * @brief Create a lightweight checkpoint of the database using hard links
     * @param dir Checkpoint directory (must be empty or non-existent)
     */
    void checkpoint(const std::string& dir);

    /**
     * @brief Synchronously flush and aggressively compact all column families
     *
     * Calls purge() on each column family and then drains the global flush and
     * compaction queues. Blocks until all work is complete. Throws on the first
     * column family that fails.
     */
    void purge();

    /**
     * @brief Switch a read-only replica to primary mode
     */
    void promoteToPrimary();

    /**
     * @brief Cancel background compaction db-wide for a fast shutdown
     *
     * In-flight merges bail out safely and queued compaction is skipped; flushes
     * are unaffected so durability is preserved. Blocks (bounded) until compaction
     * is idle. The cancellation is sticky for the session and reset on the next
     * open -- intended to be called right before closing the database.
     */
    void cancelBackgroundWork();

    /**
     * @brief Get default database configuration
     * @return Default Config struct
     */
    static Config defaultConfig();

   private:
    tidesdb_t* db_ = nullptr;
};

}  // namespace tidesdb

#endif  // TIDESDB_CPP_HPP

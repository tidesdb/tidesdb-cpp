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
#include <vector>

extern "C"
{
#include <tidesdb/db.h>
}

namespace tidesdb
{

/**
 * @brief Compression algorithms supported by TidesDB
 */
enum class CompressionAlgorithm
{
    None = NO_COMPRESSION,
    LZ4 = LZ4_COMPRESSION,
    Zstd = ZSTD_COMPRESSION,
    LZ4Fast = LZ4_FAST_COMRESSION
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
    Locked = TDB_ERR_LOCKED
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
            default:
                return "unknown error";
        }
    }

   private:
    ErrorCode code_;
};

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
    int skipListMaxLevel = 12;
    float skipListProbability = 0.25f;
    IsolationLevel defaultIsolationLevel = IsolationLevel::ReadCommitted;
    std::uint64_t minDiskSpace = 100 * 1024 * 1024;
    int l1FileCountTrigger = 4;
    int l0QueueStallThreshold = 20;

    /**
     * @brief Get default column family configuration from TidesDB
     */
    static ColumnFamilyConfig defaultConfig();
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
     * @brief Manually trigger memtable flush
     */
    void flushMemtable();

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
     * @brief Drop a column family
     * @param name Column family name
     */
    void dropColumnFamily(const std::string& name);

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
     * @brief Get block cache statistics
     * @return Cache statistics
     */
    [[nodiscard]] CacheStats getCacheStats();

    /**
     * @brief Register a custom comparator
     * @param name Comparator name
     * @param ctxStr Context string (optional)
     */
    void registerComparator(const std::string& name, const std::string& ctxStr = "");

   private:
    tidesdb_t* db_ = nullptr;
};

}  // namespace tidesdb

#endif  // TIDESDB_CPP_HPP

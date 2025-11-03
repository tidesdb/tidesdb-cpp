/**
 *
 * Copyright (C) TidesDB
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
#ifndef TIDESDB_HPP
#define TIDESDB_HPP
#include <atomic>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

using atomic_size_t = std::atomic<size_t>;
using atomic_uint64_t = std::atomic<uint64_t>;
using atomic_uint_fast64_t = std::atomic<uint_fast64_t>;

#define _Atomic(T) std::atomic<T>
#define _TIDESDB_ATOMIC_TYPES_DEFINED
extern "C"
{
#include <tidesdb/tidesdb.h>
}

#undef _Atomic
#undef _TIDESDB_ATOMIC_TYPES_DEFINED

namespace tidesdb
{

// Forward declarations
class Transaction;
class Iterator;
class ColumnFamily;

/**
 * @brief TidesDB exception class
 */
class Exception : public std::runtime_error
{
   public:
    explicit Exception(const std::string& message, int error_code = TDB_ERROR)
        : std::runtime_error(message), error_code_(error_code)
    {
    }

    int error_code() const noexcept
    {
        return error_code_;
    }

    static Exception from_code(int code, const std::string& context = "")
    {
        std::string message;

        switch (code)
        {
            case TDB_ERR_MEMORY:
                message = "memory allocation failed";
                break;
            case TDB_ERR_INVALID_ARGS:
                message = "invalid arguments";
                break;
            case TDB_ERR_IO:
                message = "I/O error";
                break;
            case TDB_ERR_NOT_FOUND:
                message = "not found";
                break;
            case TDB_ERR_EXISTS:
                message = "already exists";
                break;
            case TDB_ERR_CORRUPT:
                message = "data corruption";
                break;
            case TDB_ERR_LOCK:
                message = "lock acquisition failed";
                break;
            case TDB_ERR_TXN_COMMITTED:
                message = "transaction already committed";
                break;
            case TDB_ERR_TXN_ABORTED:
                message = "transaction aborted";
                break;
            case TDB_ERR_READONLY:
                message = "read-only transaction";
                break;
            case TDB_ERR_FULL:
                message = "database full";
                break;
            case TDB_ERR_INVALID_NAME:
                message = "invalid name";
                break;
            case TDB_ERR_COMPARATOR_NOT_FOUND:
                message = "comparator not found";
                break;
            case TDB_ERR_MAX_COMPARATORS:
                message = "max comparators reached";
                break;
            case TDB_ERR_INVALID_CF:
                message = "invalid column family";
                break;
            case TDB_ERR_THREAD:
                message = "thread operation failed";
                break;
            case TDB_ERR_CHECKSUM:
                message = "checksum verification failed";
                break;
            case TDB_ERR_KEY_DELETED:
                message = "key deleted";
                break;
            case TDB_ERR_KEY_EXPIRED:
                message = "key expired";
                break;
            default:
                message = "unknown error";
                break;
        }

        if (!context.empty())
        {
            message = context + ": " + message + " (code: " + std::to_string(code) + ")";
        }
        else
        {
            message = message + " (code: " + std::to_string(code) + ")";
        }

        return Exception(message, code);
    }

   private:
    int error_code_;
};

/**
 * @brief Compression algorithm enumeration
 */
enum class CompressionAlgo
{
    SNAPPY = COMPRESS_SNAPPY,
    LZ4 = COMPRESS_LZ4,
    ZSTD = COMPRESS_ZSTD
};

/**
 * @brief Sync mode enumeration
 */
enum class SyncMode
{
    NONE = TDB_SYNC_NONE,
    BACKGROUND = TDB_SYNC_BACKGROUND,
    FULL = TDB_SYNC_FULL
};

/**
 * @brief Column family configuration
 */
struct ColumnFamilyConfig
{
    size_t memtable_flush_size = 67108864;  // 64MB
    int max_sstables_before_compaction = 512;
    int compaction_threads = 4;
    int max_level = 12;
    float probability = 0.25f;
    bool compressed = true;
    CompressionAlgo compress_algo = CompressionAlgo::SNAPPY;
    double bloom_filter_fp_rate = 0.01;
    bool enable_background_compaction = true;
    int background_compaction_interval = 1000000;  // 1 second in microseconds
    bool use_sbha = true;
    SyncMode sync_mode = SyncMode::BACKGROUND;
    int sync_interval = 1000;
    std::optional<std::string> comparator_name;

    /**
     * @brief Get default column family configuration
     */
    static ColumnFamilyConfig default_config()
    {
        auto c_config = tidesdb_default_column_family_config();
        ColumnFamilyConfig config;
        config.memtable_flush_size = c_config.memtable_flush_size;
        config.max_sstables_before_compaction = c_config.max_sstables_before_compaction;
        config.compaction_threads = c_config.compaction_threads;
        config.max_level = c_config.max_level;
        config.probability = c_config.probability;
        config.compressed = c_config.compressed != 0;
        config.compress_algo = static_cast<CompressionAlgo>(c_config.compress_algo);
        config.bloom_filter_fp_rate = c_config.bloom_filter_fp_rate;
        config.enable_background_compaction = c_config.enable_background_compaction != 0;
        config.background_compaction_interval = c_config.background_compaction_interval;
        config.use_sbha = c_config.use_sbha != 0;
        config.sync_mode = static_cast<SyncMode>(c_config.sync_mode);
        config.sync_interval = c_config.sync_interval;
        return config;
    }

    /**
     * @brief Convert to C structure
     */
    tidesdb_column_family_config_t to_c_struct() const
    {
        tidesdb_column_family_config_t c_config;
        c_config.memtable_flush_size = memtable_flush_size;
        c_config.max_sstables_before_compaction = max_sstables_before_compaction;
        c_config.compaction_threads = compaction_threads;
        c_config.max_level = max_level;
        c_config.probability = probability;
        c_config.compressed = compressed ? 1 : 0;
        c_config.compress_algo = static_cast<compress_type>(compress_algo);
        c_config.bloom_filter_fp_rate = bloom_filter_fp_rate;
        c_config.enable_background_compaction = enable_background_compaction ? 1 : 0;
        c_config.background_compaction_interval = background_compaction_interval;
        c_config.use_sbha = use_sbha ? 1 : 0;
        c_config.sync_mode = static_cast<tidesdb_sync_mode_t>(sync_mode);
        c_config.sync_interval = sync_interval;
        c_config.comparator_name = comparator_name ? comparator_name->c_str() : nullptr;
        return c_config;
    }
};

/**
 * @brief Column family statistics
 */
struct ColumnFamilyStats
{
    std::string name;
    std::string comparator_name;
    int num_sstables;
    size_t total_sstable_size;
    size_t memtable_size;
    int memtable_entries;
    ColumnFamilyConfig config;
};

/**
 * @brief Iterator for traversing key-value pairs
 */
class Iterator
{
   public:
    explicit Iterator(tidesdb_iter_t* iter) : iter_(iter)
    {
    }

    ~Iterator()
    {
        if (iter_)
        {
            tidesdb_iter_free(iter_);
        }
    }

    // Non-copyable
    Iterator(const Iterator&) = delete;
    Iterator& operator=(const Iterator&) = delete;

    // Movable
    Iterator(Iterator&& other) noexcept : iter_(other.iter_)
    {
        other.iter_ = nullptr;
    }

    Iterator& operator=(Iterator&& other) noexcept
    {
        if (this != &other)
        {
            if (iter_)
            {
                tidesdb_iter_free(iter_);
            }
            iter_ = other.iter_;
            other.iter_ = nullptr;
        }
        return *this;
    }

    /**
     * @brief Seek to first entry
     */
    void seek_to_first()
    {
        check_valid();
        int result = tidesdb_iter_seek_to_first(iter_);
        if (result != TDB_SUCCESS)
        {
            throw Exception::from_code(result, "failed to seek to first");
        }
    }

    /**
     * @brief Seek to last entry
     */
    void seek_to_last()
    {
        check_valid();
        int result = tidesdb_iter_seek_to_last(iter_);
        if (result != TDB_SUCCESS)
        {
            throw Exception::from_code(result, "failed to seek to last");
        }
    }

    /**
     * @brief Check if iterator is at a valid position
     */
    bool valid() const
    {
        return iter_ && tidesdb_iter_valid(iter_);
    }

    /**
     * @brief Move to next entry
     * @note Returns normally even if iterator becomes invalid (end of iteration)
     */
    void next()
    {
        check_valid();
        tidesdb_iter_next(iter_);
    }

    /**
     * @brief Move to previous entry
     * @note Returns normally even if iterator becomes invalid (beginning of iteration)
     */
    void prev()
    {
        check_valid();
        tidesdb_iter_prev(iter_);
    }

    /**
     * @brief Get current key
     * @note The returned vector is a copy. The iterator retains ownership of the internal key data.
     */
    std::vector<uint8_t> key() const
    {
        check_valid();

        uint8_t* key_ptr = nullptr;
        size_t key_size = 0;

        int result = tidesdb_iter_key(iter_, &key_ptr, &key_size);
        if (result != TDB_SUCCESS)
        {
            throw Exception::from_code(result, "failed to get key");
        }

        // Note: key_ptr points to internal iterator memory, do NOT free it
        return std::vector<uint8_t>(key_ptr, key_ptr + key_size);
    }

    /**
     * @brief Get current value
     * @note The returned vector is a copy. The iterator retains ownership of the internal value
     * data.
     */
    std::vector<uint8_t> value() const
    {
        check_valid();

        uint8_t* value_ptr = nullptr;
        size_t value_size = 0;

        int result = tidesdb_iter_value(iter_, &value_ptr, &value_size);
        if (result != TDB_SUCCESS)
        {
            throw Exception::from_code(result, "failed to get value");
        }

        // Note: value_ptr points to internal iterator memory, do NOT free it
        return std::vector<uint8_t>(value_ptr, value_ptr + value_size);
    }

    /**
     * @brief Get current key as string
     */
    std::string key_string() const
    {
        auto key_data = key();
        return std::string(key_data.begin(), key_data.end());
    }

    /**
     * @brief Get current value as string
     */
    std::string value_string() const
    {
        auto value_data = value();
        return std::string(value_data.begin(), value_data.end());
    }

   private:
    tidesdb_iter_t* iter_;

    void check_valid() const
    {
        if (!iter_)
        {
            throw Exception("Iterator is closed");
        }
    }
};

/**
 * @brief Transaction for operations
 */
class Transaction
{
   public:
    explicit Transaction(tidesdb_txn_t* txn) : txn_(txn), committed_(false)
    {
    }

    ~Transaction()
    {
        if (txn_)
        {
            tidesdb_txn_free(txn_);
        }
    }

    // Non-copyable
    Transaction(const Transaction&) = delete;
    Transaction& operator=(const Transaction&) = delete;

    // Movable
    Transaction(Transaction&& other) noexcept : txn_(other.txn_), committed_(other.committed_)
    {
        other.txn_ = nullptr;
    }

    Transaction& operator=(Transaction&& other) noexcept
    {
        if (this != &other)
        {
            if (txn_)
            {
                tidesdb_txn_free(txn_);
            }
            txn_ = other.txn_;
            committed_ = other.committed_;
            other.txn_ = nullptr;
        }
        return *this;
    }

    /**
     * @brief Put a key-value pair
     */
    void put(const std::string& column_family, const std::vector<uint8_t>& key,
             const std::vector<uint8_t>& value, time_t ttl = -1)
    {
        check_valid();

        int result = tidesdb_txn_put(txn_, column_family.c_str(), key.data(), key.size(),
                                     value.data(), value.size(), ttl);

        if (result != TDB_SUCCESS)
        {
            throw Exception::from_code(result, "failed to put key-value pair");
        }
    }

    /**
     * @brief Put a key-value pair (string overload)
     */
    void put(const std::string& column_family, const std::string& key, const std::string& value,
             time_t ttl = -1)
    {
        std::vector<uint8_t> key_data(key.begin(), key.end());
        std::vector<uint8_t> value_data(value.begin(), value.end());
        put(column_family, key_data, value_data, ttl);
    }

    /**
     * @brief Get a value
     */
    std::vector<uint8_t> get(const std::string& column_family,
                             const std::vector<uint8_t>& key) const
    {
        check_valid();

        uint8_t* value_ptr = nullptr;
        size_t value_size = 0;

        int result = tidesdb_txn_get(txn_, column_family.c_str(), key.data(), key.size(),
                                     &value_ptr, &value_size);

        if (result != TDB_SUCCESS)
        {
            throw Exception::from_code(result, "failed to get value");
        }

        std::vector<uint8_t> value_data(value_ptr, value_ptr + value_size);
        free(value_ptr);
        return value_data;
    }

    /**
     * @brief Get a value (string overload)
     */
    std::string get(const std::string& column_family, const std::string& key) const
    {
        std::vector<uint8_t> key_data(key.begin(), key.end());
        auto value_data = get(column_family, key_data);
        return std::string(value_data.begin(), value_data.end());
    }

    /**
     * @brief Delete a key-value pair
     */
    void remove(const std::string& column_family, const std::vector<uint8_t>& key)
    {
        check_valid();

        int result = tidesdb_txn_delete(txn_, column_family.c_str(), key.data(), key.size());

        if (result != TDB_SUCCESS)
        {
            throw Exception::from_code(result, "failed to delete key");
        }
    }

    /**
     * @brief Delete a key-value pair (string overload)
     */
    void remove(const std::string& column_family, const std::string& key)
    {
        std::vector<uint8_t> key_data(key.begin(), key.end());
        remove(column_family, key_data);
    }

    /**
     * @brief Commit the transaction
     */
    void commit()
    {
        check_valid();

        if (committed_)
        {
            throw Exception("Transaction already committed");
        }

        int result = tidesdb_txn_commit(txn_);
        if (result != TDB_SUCCESS)
        {
            throw Exception::from_code(result, "failed to commit transaction");
        }

        committed_ = true;
    }

    /**
     * @brief Rollback the transaction
     */
    void rollback()
    {
        check_valid();

        if (committed_)
        {
            throw Exception("Transaction already committed");
        }

        int result = tidesdb_txn_rollback(txn_);
        if (result != TDB_SUCCESS)
        {
            throw Exception::from_code(result, "failed to rollback transaction");
        }
    }

    /**
     * @brief Create a new iterator
     */
    std::unique_ptr<Iterator> new_iterator(const std::string& column_family)
    {
        check_valid();

        tidesdb_iter_t* iter = nullptr;
        int result = tidesdb_iter_new(txn_, column_family.c_str(), &iter);

        if (result != TDB_SUCCESS)
        {
            throw Exception::from_code(result, "failed to create iterator");
        }

        return std::make_unique<Iterator>(iter);
    }

   private:
    tidesdb_txn_t* txn_;
    bool committed_;

    void check_valid() const
    {
        if (!txn_)
        {
            throw Exception("Transaction is closed");
        }
    }
};

/**
 * @brief Column family handle
 */
class ColumnFamily
{
   public:
    explicit ColumnFamily(tidesdb_column_family_t* cf, const std::string& name)
        : cf_(cf), name_(name)
    {
    }

    /**
     * @brief Get column family name
     */
    const std::string& name() const
    {
        return name_;
    }

    /**
     * @brief Manually trigger compaction
     */
    void compact()
    {
        if (!cf_)
        {
            throw Exception("Column family is invalid");
        }

        int result = tidesdb_compact(cf_);
        if (result != TDB_SUCCESS)
        {
            throw Exception::from_code(result, "failed to compact column family");
        }
    }

   private:
    tidesdb_column_family_t* cf_;
    std::string name_;
};

/**
 * @brief Main TidesDB database class
 */
class DB
{
   public:
    /**
     * @brief Open a database
     */
    explicit DB(const std::string& db_path, bool enable_debug_logging = false,
                int max_open_file_handles = TDB_DEFAULT_MAX_OPEN_FILE_HANDLES)
    {
        tidesdb_config_t config;
        // db_path is a char array, need to copy string into it
        std::strncpy(config.db_path, db_path.c_str(), sizeof(config.db_path) - 1);
        config.db_path[sizeof(config.db_path) - 1] = '\0';  // Ensure null termination
        config.enable_debug_logging = enable_debug_logging ? 1 : 0;
        config.max_open_file_handles = max_open_file_handles;

        int result = tidesdb_open(&config, &db_);
        if (result != TDB_SUCCESS)
        {
            throw Exception::from_code(result, "failed to open database");
        }
    }

    ~DB()
    {
        if (db_)
        {
            tidesdb_close(db_);
        }
    }

    // Non-copyable
    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;

    // Movable
    DB(DB&& other) noexcept : db_(other.db_)
    {
        other.db_ = nullptr;
    }

    DB& operator=(DB&& other) noexcept
    {
        if (this != &other)
        {
            if (db_)
            {
                tidesdb_close(db_);
            }
            db_ = other.db_;
            other.db_ = nullptr;
        }
        return *this;
    }

    /**
     * @brief Create a column family
     */
    void create_column_family(const std::string& name, const ColumnFamilyConfig& config =
                                                           ColumnFamilyConfig::default_config())
    {
        check_valid();

        auto c_config = config.to_c_struct();
        int result = tidesdb_create_column_family(db_, name.c_str(), &c_config);

        if (result != TDB_SUCCESS)
        {
            throw Exception::from_code(result, "failed to create column family");
        }
    }

    /**
     * @brief Drop a column family
     */
    void drop_column_family(const std::string& name)
    {
        check_valid();

        int result = tidesdb_drop_column_family(db_, name.c_str());
        if (result != TDB_SUCCESS)
        {
            throw Exception::from_code(result, "failed to drop column family");
        }
    }

    /**
     * @brief Get a column family handle
     */
    ColumnFamily get_column_family(const std::string& name)
    {
        check_valid();

        tidesdb_column_family_t* cf = tidesdb_get_column_family(db_, name.c_str());
        if (!cf)
        {
            throw Exception("Column family not found: " + name, TDB_ERR_NOT_FOUND);
        }

        return ColumnFamily(cf, name);
    }

    /**
     * @brief List all column families
     */
    std::vector<std::string> list_column_families()
    {
        check_valid();

        char** names = nullptr;
        int count = 0;

        int result = tidesdb_list_column_families(db_, &names, &count);
        if (result != TDB_SUCCESS)
        {
            throw Exception::from_code(result, "failed to list column families");
        }

        std::vector<std::string> cf_names;
        for (int i = 0; i < count; ++i)
        {
            cf_names.emplace_back(names[i]);
            free(names[i]);
        }
        free(names);

        return cf_names;
    }

    /**
     * @brief Get column family statistics
     */
    ColumnFamilyStats get_column_family_stats(const std::string& name)
    {
        check_valid();

        tidesdb_column_family_stat_t* c_stats = nullptr;
        int result = tidesdb_get_column_family_stats(db_, name.c_str(), &c_stats);

        if (result != TDB_SUCCESS)
        {
            throw Exception::from_code(result, "failed to get column family stats");
        }

        ColumnFamilyStats stats;
        stats.name = c_stats->name;
        stats.comparator_name = c_stats->comparator_name;
        stats.num_sstables = c_stats->num_sstables;
        stats.total_sstable_size = c_stats->total_sstable_size;
        stats.memtable_size = c_stats->memtable_size;
        stats.memtable_entries = c_stats->memtable_entries;

        stats.config.memtable_flush_size = c_stats->config.memtable_flush_size;
        stats.config.max_sstables_before_compaction =
            c_stats->config.max_sstables_before_compaction;
        stats.config.compaction_threads = c_stats->config.compaction_threads;
        stats.config.max_level = c_stats->config.max_level;
        stats.config.probability = c_stats->config.probability;
        stats.config.compressed = c_stats->config.compressed != 0;
        stats.config.compress_algo = static_cast<CompressionAlgo>(c_stats->config.compress_algo);
        stats.config.bloom_filter_fp_rate = c_stats->config.bloom_filter_fp_rate;
        stats.config.enable_background_compaction =
            c_stats->config.enable_background_compaction != 0;
        stats.config.use_sbha = c_stats->config.use_sbha != 0;
        stats.config.sync_mode = static_cast<SyncMode>(c_stats->config.sync_mode);
        stats.config.sync_interval = c_stats->config.sync_interval;

        free(c_stats);
        return stats;
    }

    /**
     * @brief Begin a write transaction
     */
    std::unique_ptr<Transaction> begin_transaction()
    {
        check_valid();

        tidesdb_txn_t* txn = nullptr;
        int result = tidesdb_txn_begin(db_, &txn);

        if (result != TDB_SUCCESS)
        {
            throw Exception::from_code(result, "failed to begin transaction");
        }

        return std::make_unique<Transaction>(txn);
    }

    /**
     * @brief Begin a read-only transaction
     */
    std::unique_ptr<Transaction> begin_read_transaction()
    {
        check_valid();

        tidesdb_txn_t* txn = nullptr;
        int result = tidesdb_txn_begin_read(db_, &txn);

        if (result != TDB_SUCCESS)
        {
            throw Exception::from_code(result, "failed to begin read transaction");
        }

        return std::make_unique<Transaction>(txn);
    }

   private:
    tidesdb_t* db_ = nullptr;

    void check_valid() const
    {
        if (!db_)
        {
            throw Exception("Database is closed");
        }
    }
};

}  // namespace tidesdb

#endif  // TIDESDB_HPP
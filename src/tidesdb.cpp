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
#include "tidesdb/tidesdb.hpp"

#include <cstring>
#include <utility>

namespace tidesdb
{

namespace
{

void checkResult(int result, const std::string& context)
{
    if (result != TDB_SUCCESS)
    {
        throw Exception(static_cast<ErrorCode>(result),
                        context + ": " + Exception::errorMessage(result) +
                            " (code: " + std::to_string(result) + ")");
    }
}

}  // anonymous namespace

//-----------------------------------------------------------------------------
// ColumnFamilyConfig
//-----------------------------------------------------------------------------

ColumnFamilyConfig ColumnFamilyConfig::defaultConfig()
{
    tidesdb_column_family_config_t cConfig = tidesdb_default_column_family_config();

    ColumnFamilyConfig config;
    config.writeBufferSize = cConfig.write_buffer_size;
    config.levelSizeRatio = cConfig.level_size_ratio;
    config.minLevels = cConfig.min_levels;
    config.dividingLevelOffset = cConfig.dividing_level_offset;
    config.klogValueThreshold = cConfig.klog_value_threshold;
    config.compressionAlgorithm =
        static_cast<CompressionAlgorithm>(static_cast<int>(cConfig.compression_algorithm));
    config.enableBloomFilter = cConfig.enable_bloom_filter != 0;
    config.bloomFPR = cConfig.bloom_fpr;
    config.enableBlockIndexes = cConfig.enable_block_indexes != 0;
    config.indexSampleRatio = cConfig.index_sample_ratio;
    config.blockIndexPrefixLen = cConfig.block_index_prefix_len;
    config.syncMode = static_cast<SyncMode>(cConfig.sync_mode);
    config.syncIntervalUs = cConfig.sync_interval_us;
    config.comparatorName = cConfig.comparator_name;
    config.skipListMaxLevel = cConfig.skip_list_max_level;
    config.skipListProbability = cConfig.skip_list_probability;
    config.defaultIsolationLevel = static_cast<IsolationLevel>(cConfig.default_isolation_level);
    config.minDiskSpace = cConfig.min_disk_space;
    config.l1FileCountTrigger = cConfig.l1_file_count_trigger;
    config.l0QueueStallThreshold = cConfig.l0_queue_stall_threshold;
    config.useBtree = cConfig.use_btree != 0;

    return config;
}

ColumnFamilyConfig ColumnFamilyConfig::loadFromIni(const std::string& iniFile,
                                                   const std::string& sectionName)
{
    tidesdb_column_family_config_t cConfig;
    int result = tidesdb_cf_config_load_from_ini(iniFile.c_str(), sectionName.c_str(), &cConfig);
    checkResult(result, "failed to load config from INI");

    ColumnFamilyConfig config;
    config.writeBufferSize = cConfig.write_buffer_size;
    config.levelSizeRatio = cConfig.level_size_ratio;
    config.minLevels = cConfig.min_levels;
    config.dividingLevelOffset = cConfig.dividing_level_offset;
    config.klogValueThreshold = cConfig.klog_value_threshold;
    config.compressionAlgorithm =
        static_cast<CompressionAlgorithm>(static_cast<int>(cConfig.compression_algorithm));
    config.enableBloomFilter = cConfig.enable_bloom_filter != 0;
    config.bloomFPR = cConfig.bloom_fpr;
    config.enableBlockIndexes = cConfig.enable_block_indexes != 0;
    config.indexSampleRatio = cConfig.index_sample_ratio;
    config.blockIndexPrefixLen = cConfig.block_index_prefix_len;
    config.syncMode = static_cast<SyncMode>(cConfig.sync_mode);
    config.syncIntervalUs = cConfig.sync_interval_us;
    config.comparatorName = cConfig.comparator_name;
    config.skipListMaxLevel = cConfig.skip_list_max_level;
    config.skipListProbability = cConfig.skip_list_probability;
    config.defaultIsolationLevel = static_cast<IsolationLevel>(cConfig.default_isolation_level);
    config.minDiskSpace = cConfig.min_disk_space;
    config.l1FileCountTrigger = cConfig.l1_file_count_trigger;
    config.l0QueueStallThreshold = cConfig.l0_queue_stall_threshold;
    config.useBtree = cConfig.use_btree != 0;

    return config;
}

void ColumnFamilyConfig::saveToIni(const std::string& iniFile, const std::string& sectionName,
                                   const ColumnFamilyConfig& config)
{
    tidesdb_column_family_config_t cConfig;
    cConfig.write_buffer_size = config.writeBufferSize;
    cConfig.level_size_ratio = config.levelSizeRatio;
    cConfig.min_levels = config.minLevels;
    cConfig.dividing_level_offset = config.dividingLevelOffset;
    cConfig.klog_value_threshold = config.klogValueThreshold;
    cConfig.compression_algorithm =
        static_cast<::compression_algorithm>(config.compressionAlgorithm);
    cConfig.enable_bloom_filter = config.enableBloomFilter ? 1 : 0;
    cConfig.bloom_fpr = config.bloomFPR;
    cConfig.enable_block_indexes = config.enableBlockIndexes ? 1 : 0;
    cConfig.index_sample_ratio = config.indexSampleRatio;
    cConfig.block_index_prefix_len = config.blockIndexPrefixLen;
    cConfig.sync_mode = static_cast<int>(config.syncMode);
    cConfig.sync_interval_us = config.syncIntervalUs;
    cConfig.skip_list_max_level = config.skipListMaxLevel;
    cConfig.skip_list_probability = config.skipListProbability;
    cConfig.default_isolation_level =
        static_cast<tidesdb_isolation_level_t>(config.defaultIsolationLevel);
    cConfig.min_disk_space = config.minDiskSpace;
    cConfig.l1_file_count_trigger = config.l1FileCountTrigger;
    cConfig.l0_queue_stall_threshold = config.l0QueueStallThreshold;
    cConfig.use_btree = config.useBtree ? 1 : 0;

    std::memset(cConfig.comparator_name, 0, TDB_MAX_COMPARATOR_NAME);
    if (!config.comparatorName.empty())
    {
        std::strncpy(cConfig.comparator_name, config.comparatorName.c_str(),
                     TDB_MAX_COMPARATOR_NAME - 1);
    }
    std::memset(cConfig.comparator_ctx_str, 0, TDB_MAX_COMPARATOR_CTX);
    cConfig.comparator_fn_cached = nullptr;
    cConfig.comparator_ctx_cached = nullptr;
    cConfig.commit_hook_fn = nullptr;
    cConfig.commit_hook_ctx = nullptr;

    int result = tidesdb_cf_config_save_to_ini(iniFile.c_str(), sectionName.c_str(), &cConfig);
    checkResult(result, "failed to save config to INI");
}

//-----------------------------------------------------------------------------
// ColumnFamily
//-----------------------------------------------------------------------------

ColumnFamily::ColumnFamily(ColumnFamily&& other) noexcept : cf_(other.cf_)
{
    other.cf_ = nullptr;
}

ColumnFamily& ColumnFamily::operator=(ColumnFamily&& other) noexcept
{
    if (this != &other)
    {
        cf_ = other.cf_;
        other.cf_ = nullptr;
    }
    return *this;
}

Stats ColumnFamily::getStats() const
{
    tidesdb_stats_t* cStats = nullptr;
    int result = tidesdb_get_stats(cf_, &cStats);
    checkResult(result, "failed to get stats");

    Stats stats;
    stats.numLevels = cStats->num_levels;
    stats.memtableSize = cStats->memtable_size;

    if (cStats->num_levels > 0 && cStats->level_sizes != nullptr)
    {
        stats.levelSizes.resize(cStats->num_levels);
        for (int i = 0; i < cStats->num_levels; ++i)
        {
            stats.levelSizes[i] = cStats->level_sizes[i];
        }
    }

    if (cStats->num_levels > 0 && cStats->level_num_sstables != nullptr)
    {
        stats.levelNumSSTables.resize(cStats->num_levels);
        for (int i = 0; i < cStats->num_levels; ++i)
        {
            stats.levelNumSSTables[i] = cStats->level_num_sstables[i];
        }
    }

    // New stats fields
    stats.totalKeys = cStats->total_keys;
    stats.totalDataSize = cStats->total_data_size;
    stats.avgKeySize = cStats->avg_key_size;
    stats.avgValueSize = cStats->avg_value_size;
    stats.readAmp = cStats->read_amp;
    stats.hitRate = cStats->hit_rate;

    // B+tree stats
    stats.useBtree = cStats->use_btree != 0;
    stats.btreeTotalNodes = cStats->btree_total_nodes;
    stats.btreeMaxHeight = cStats->btree_max_height;
    stats.btreeAvgHeight = cStats->btree_avg_height;

    if (cStats->num_levels > 0 && cStats->level_key_counts != nullptr)
    {
        stats.levelKeyCounts.resize(cStats->num_levels);
        for (int i = 0; i < cStats->num_levels; ++i)
        {
            stats.levelKeyCounts[i] = cStats->level_key_counts[i];
        }
    }

    if (cStats->config != nullptr)
    {
        ColumnFamilyConfig cfConfig;
        cfConfig.writeBufferSize = cStats->config->write_buffer_size;
        cfConfig.levelSizeRatio = cStats->config->level_size_ratio;
        cfConfig.minLevels = cStats->config->min_levels;
        cfConfig.dividingLevelOffset = cStats->config->dividing_level_offset;
        cfConfig.klogValueThreshold = cStats->config->klog_value_threshold;
        cfConfig.compressionAlgorithm = static_cast<CompressionAlgorithm>(
            static_cast<int>(cStats->config->compression_algorithm));
        cfConfig.enableBloomFilter = cStats->config->enable_bloom_filter != 0;
        cfConfig.bloomFPR = cStats->config->bloom_fpr;
        cfConfig.enableBlockIndexes = cStats->config->enable_block_indexes != 0;
        cfConfig.indexSampleRatio = cStats->config->index_sample_ratio;
        cfConfig.blockIndexPrefixLen = cStats->config->block_index_prefix_len;
        cfConfig.syncMode = static_cast<SyncMode>(cStats->config->sync_mode);
        cfConfig.syncIntervalUs = cStats->config->sync_interval_us;
        cfConfig.comparatorName = cStats->config->comparator_name;
        cfConfig.skipListMaxLevel = cStats->config->skip_list_max_level;
        cfConfig.skipListProbability = cStats->config->skip_list_probability;
        cfConfig.defaultIsolationLevel =
            static_cast<IsolationLevel>(cStats->config->default_isolation_level);
        cfConfig.minDiskSpace = cStats->config->min_disk_space;
        cfConfig.l1FileCountTrigger = cStats->config->l1_file_count_trigger;
        cfConfig.l0QueueStallThreshold = cStats->config->l0_queue_stall_threshold;
        cfConfig.useBtree = cStats->config->use_btree != 0;
        stats.config = cfConfig;
    }

    tidesdb_free_stats(cStats);
    return stats;
}

void ColumnFamily::compact()
{
    int result = tidesdb_compact(cf_);
    checkResult(result, "failed to compact column family");
}

void ColumnFamily::flushMemtable()
{
    int result = tidesdb_flush_memtable(cf_);
    checkResult(result, "failed to flush memtable");
}

bool ColumnFamily::isFlushing() const
{
    return tidesdb_is_flushing(cf_) != 0;
}

bool ColumnFamily::isCompacting() const
{
    return tidesdb_is_compacting(cf_) != 0;
}

double ColumnFamily::rangeCost(std::string_view keyA, std::string_view keyB) const
{
    double cost = 0.0;
    int result =
        tidesdb_range_cost(cf_, reinterpret_cast<const uint8_t*>(keyA.data()), keyA.size(),
                           reinterpret_cast<const uint8_t*>(keyB.data()), keyB.size(), &cost);
    checkResult(result, "failed to estimate range cost");
    return cost;
}

double ColumnFamily::rangeCost(const std::vector<std::uint8_t>& keyA,
                               const std::vector<std::uint8_t>& keyB) const
{
    double cost = 0.0;
    int result = tidesdb_range_cost(cf_, keyA.data(), keyA.size(), keyB.data(), keyB.size(), &cost);
    checkResult(result, "failed to estimate range cost");
    return cost;
}

void ColumnFamily::setCommitHook(tidesdb_commit_hook_fn fn, void* ctx)
{
    int result = tidesdb_cf_set_commit_hook(cf_, fn, ctx);
    checkResult(result, "failed to set commit hook");
}

void ColumnFamily::clearCommitHook()
{
    int result = tidesdb_cf_set_commit_hook(cf_, nullptr, nullptr);
    checkResult(result, "failed to clear commit hook");
}

void ColumnFamily::updateRuntimeConfig(const ColumnFamilyConfig& config, bool persistToDisk)
{
    tidesdb_column_family_config_t cConfig;
    cConfig.write_buffer_size = config.writeBufferSize;
    cConfig.level_size_ratio = config.levelSizeRatio;
    cConfig.min_levels = config.minLevels;
    cConfig.dividing_level_offset = config.dividingLevelOffset;
    cConfig.klog_value_threshold = config.klogValueThreshold;
    cConfig.compression_algorithm =
        static_cast<::compression_algorithm>(config.compressionAlgorithm);
    cConfig.enable_bloom_filter = config.enableBloomFilter ? 1 : 0;
    cConfig.bloom_fpr = config.bloomFPR;
    cConfig.enable_block_indexes = config.enableBlockIndexes ? 1 : 0;
    cConfig.index_sample_ratio = config.indexSampleRatio;
    cConfig.block_index_prefix_len = config.blockIndexPrefixLen;
    cConfig.sync_mode = static_cast<int>(config.syncMode);
    cConfig.sync_interval_us = config.syncIntervalUs;
    cConfig.skip_list_max_level = config.skipListMaxLevel;
    cConfig.skip_list_probability = config.skipListProbability;
    cConfig.default_isolation_level =
        static_cast<tidesdb_isolation_level_t>(config.defaultIsolationLevel);
    cConfig.min_disk_space = config.minDiskSpace;
    cConfig.l1_file_count_trigger = config.l1FileCountTrigger;
    cConfig.l0_queue_stall_threshold = config.l0QueueStallThreshold;
    cConfig.use_btree = config.useBtree ? 1 : 0;

    std::memset(cConfig.comparator_name, 0, TDB_MAX_COMPARATOR_NAME);
    if (!config.comparatorName.empty())
    {
        std::strncpy(cConfig.comparator_name, config.comparatorName.c_str(),
                     TDB_MAX_COMPARATOR_NAME - 1);
    }
    std::memset(cConfig.comparator_ctx_str, 0, TDB_MAX_COMPARATOR_CTX);
    cConfig.comparator_fn_cached = nullptr;
    cConfig.comparator_ctx_cached = nullptr;
    cConfig.commit_hook_fn = nullptr;
    cConfig.commit_hook_ctx = nullptr;

    int result = tidesdb_cf_update_runtime_config(cf_, &cConfig, persistToDisk ? 1 : 0);
    checkResult(result, "failed to update runtime config");
}

//-----------------------------------------------------------------------------
// Iterator
//-----------------------------------------------------------------------------

Iterator::Iterator(Iterator&& other) noexcept : iter_(other.iter_)
{
    other.iter_ = nullptr;
}

Iterator& Iterator::operator=(Iterator&& other) noexcept
{
    if (this != &other)
    {
        if (iter_ != nullptr)
        {
            tidesdb_iter_free(iter_);
        }
        iter_ = other.iter_;
        other.iter_ = nullptr;
    }
    return *this;
}

Iterator::~Iterator()
{
    if (iter_ != nullptr)
    {
        tidesdb_iter_free(iter_);
        iter_ = nullptr;
    }
}

void Iterator::seekToFirst()
{
    int result = tidesdb_iter_seek_to_first(iter_);
    checkResult(result, "failed to seek to first");
}

void Iterator::seekToLast()
{
    int result = tidesdb_iter_seek_to_last(iter_);
    checkResult(result, "failed to seek to last");
}

void Iterator::seek(std::string_view key)
{
    int result = tidesdb_iter_seek(iter_, reinterpret_cast<const uint8_t*>(key.data()), key.size());
    checkResult(result, "failed to seek");
}

void Iterator::seekForPrev(std::string_view key)
{
    int result =
        tidesdb_iter_seek_for_prev(iter_, reinterpret_cast<const uint8_t*>(key.data()), key.size());
    checkResult(result, "failed to seek for prev");
}

bool Iterator::valid() const
{
    return tidesdb_iter_valid(iter_) != 0;
}

void Iterator::next()
{
    int result = tidesdb_iter_next(iter_);
    // TDB_ERR_NOT_FOUND is expected at end of iteration, not an error
    if (result != TDB_SUCCESS && result != TDB_ERR_NOT_FOUND)
    {
        checkResult(result, "failed to move to next");
    }
}

void Iterator::prev()
{
    int result = tidesdb_iter_prev(iter_);
    // TDB_ERR_NOT_FOUND is expected at end of iteration, not an error
    if (result != TDB_SUCCESS && result != TDB_ERR_NOT_FOUND)
    {
        checkResult(result, "failed to move to prev");
    }
}

std::vector<std::uint8_t> Iterator::key() const
{
    uint8_t* keyData = nullptr;
    size_t keySize = 0;

    int result = tidesdb_iter_key(iter_, &keyData, &keySize);
    checkResult(result, "failed to get key");

    std::vector<std::uint8_t> keyVec(keyData, keyData + keySize);
    return keyVec;
}

std::vector<std::uint8_t> Iterator::value() const
{
    uint8_t* valueData = nullptr;
    size_t valueSize = 0;

    int result = tidesdb_iter_value(iter_, &valueData, &valueSize);
    checkResult(result, "failed to get value");

    std::vector<std::uint8_t> valueVec(valueData, valueData + valueSize);
    return valueVec;
}

//-----------------------------------------------------------------------------
// Transaction
//-----------------------------------------------------------------------------

Transaction::Transaction(Transaction&& other) noexcept : txn_(other.txn_)
{
    other.txn_ = nullptr;
}

Transaction& Transaction::operator=(Transaction&& other) noexcept
{
    if (this != &other)
    {
        if (txn_ != nullptr)
        {
            tidesdb_txn_free(txn_);
        }
        txn_ = other.txn_;
        other.txn_ = nullptr;
    }
    return *this;
}

Transaction::~Transaction()
{
    if (txn_ != nullptr)
    {
        tidesdb_txn_free(txn_);
        txn_ = nullptr;
    }
}

void Transaction::put(ColumnFamily& cf, std::string_view key, std::string_view value,
                      std::time_t ttl)
{
    int result =
        tidesdb_txn_put(txn_, cf.handle(), reinterpret_cast<const uint8_t*>(key.data()), key.size(),
                        reinterpret_cast<const uint8_t*>(value.data()), value.size(), ttl);
    checkResult(result, "failed to put key-value pair");
}

void Transaction::put(ColumnFamily& cf, const std::vector<std::uint8_t>& key,
                      const std::vector<std::uint8_t>& value, std::time_t ttl)
{
    int result =
        tidesdb_txn_put(txn_, cf.handle(), key.data(), key.size(), value.data(), value.size(), ttl);
    checkResult(result, "failed to put key-value pair");
}

std::vector<std::uint8_t> Transaction::get(ColumnFamily& cf, std::string_view key)
{
    uint8_t* valueData = nullptr;
    size_t valueSize = 0;

    int result = tidesdb_txn_get(txn_, cf.handle(), reinterpret_cast<const uint8_t*>(key.data()),
                                 key.size(), &valueData, &valueSize);
    checkResult(result, "failed to get value");

    std::vector<std::uint8_t> valueVec(valueData, valueData + valueSize);
    std::free(valueData);
    return valueVec;
}

std::vector<std::uint8_t> Transaction::get(ColumnFamily& cf, const std::vector<std::uint8_t>& key)
{
    uint8_t* valueData = nullptr;
    size_t valueSize = 0;

    int result = tidesdb_txn_get(txn_, cf.handle(), key.data(), key.size(), &valueData, &valueSize);
    checkResult(result, "failed to get value");

    std::vector<std::uint8_t> valueVec(valueData, valueData + valueSize);
    std::free(valueData);
    return valueVec;
}

void Transaction::del(ColumnFamily& cf, std::string_view key)
{
    int result = tidesdb_txn_delete(txn_, cf.handle(), reinterpret_cast<const uint8_t*>(key.data()),
                                    key.size());
    checkResult(result, "failed to delete key");
}

void Transaction::del(ColumnFamily& cf, const std::vector<std::uint8_t>& key)
{
    int result = tidesdb_txn_delete(txn_, cf.handle(), key.data(), key.size());
    checkResult(result, "failed to delete key");
}

void Transaction::commit()
{
    int result = tidesdb_txn_commit(txn_);
    checkResult(result, "failed to commit transaction");
}

void Transaction::rollback()
{
    int result = tidesdb_txn_rollback(txn_);
    checkResult(result, "failed to rollback transaction");
}

void Transaction::savepoint(const std::string& name)
{
    int result = tidesdb_txn_savepoint(txn_, name.c_str());
    checkResult(result, "failed to create savepoint");
}

void Transaction::rollbackToSavepoint(const std::string& name)
{
    int result = tidesdb_txn_rollback_to_savepoint(txn_, name.c_str());
    checkResult(result, "failed to rollback to savepoint");
}

void Transaction::releaseSavepoint(const std::string& name)
{
    int result = tidesdb_txn_release_savepoint(txn_, name.c_str());
    checkResult(result, "failed to release savepoint");
}

Iterator Transaction::newIterator(ColumnFamily& cf)
{
    tidesdb_iter_t* iter = nullptr;
    int result = tidesdb_iter_new(txn_, cf.handle(), &iter);
    checkResult(result, "failed to create iterator");
    return Iterator(iter);
}

void Transaction::reset(IsolationLevel isolation)
{
    int result = tidesdb_txn_reset(txn_, static_cast<tidesdb_isolation_level_t>(isolation));
    checkResult(result, "failed to reset transaction");
}

//-----------------------------------------------------------------------------
// TidesDB
//-----------------------------------------------------------------------------

TidesDB::TidesDB(const Config& config)
{
    tidesdb_config_t cConfig;
    cConfig.db_path = const_cast<char*>(config.dbPath.c_str());
    cConfig.num_flush_threads = config.numFlushThreads;
    cConfig.num_compaction_threads = config.numCompactionThreads;
    cConfig.log_level = static_cast<tidesdb_log_level_t>(config.logLevel);
    cConfig.block_cache_size = config.blockCacheSize;
    cConfig.max_open_sstables = config.maxOpenSSTables;
    cConfig.log_to_file = config.logToFile ? 1 : 0;
    cConfig.log_truncation_at = config.logTruncationAt;

    int result = tidesdb_open(&cConfig, &db_);
    checkResult(result, "failed to open database");
}

TidesDB::TidesDB(TidesDB&& other) noexcept : db_(other.db_)
{
    other.db_ = nullptr;
}

TidesDB& TidesDB::operator=(TidesDB&& other) noexcept
{
    if (this != &other)
    {
        if (db_ != nullptr)
        {
            tidesdb_close(db_);
        }
        db_ = other.db_;
        other.db_ = nullptr;
    }
    return *this;
}

TidesDB::~TidesDB()
{
    if (db_ != nullptr)
    {
        tidesdb_close(db_);
        db_ = nullptr;
    }
}

void TidesDB::createColumnFamily(const std::string& name, const ColumnFamilyConfig& config)
{
    tidesdb_column_family_config_t cConfig;
    cConfig.write_buffer_size = config.writeBufferSize;
    cConfig.level_size_ratio = config.levelSizeRatio;
    cConfig.min_levels = config.minLevels;
    cConfig.dividing_level_offset = config.dividingLevelOffset;
    cConfig.klog_value_threshold = config.klogValueThreshold;
    cConfig.compression_algorithm =
        static_cast<::compression_algorithm>(config.compressionAlgorithm);
    cConfig.enable_bloom_filter = config.enableBloomFilter ? 1 : 0;
    cConfig.bloom_fpr = config.bloomFPR;
    cConfig.enable_block_indexes = config.enableBlockIndexes ? 1 : 0;
    cConfig.index_sample_ratio = config.indexSampleRatio;
    cConfig.block_index_prefix_len = config.blockIndexPrefixLen;
    cConfig.sync_mode = static_cast<int>(config.syncMode);
    cConfig.sync_interval_us = config.syncIntervalUs;
    cConfig.skip_list_max_level = config.skipListMaxLevel;
    cConfig.skip_list_probability = config.skipListProbability;
    cConfig.default_isolation_level =
        static_cast<tidesdb_isolation_level_t>(config.defaultIsolationLevel);
    cConfig.min_disk_space = config.minDiskSpace;
    cConfig.l1_file_count_trigger = config.l1FileCountTrigger;
    cConfig.l0_queue_stall_threshold = config.l0QueueStallThreshold;
    cConfig.use_btree = config.useBtree ? 1 : 0;

    std::memset(cConfig.comparator_name, 0, TDB_MAX_COMPARATOR_NAME);
    if (!config.comparatorName.empty())
    {
        std::strncpy(cConfig.comparator_name, config.comparatorName.c_str(),
                     TDB_MAX_COMPARATOR_NAME - 1);
    }
    std::memset(cConfig.comparator_ctx_str, 0, TDB_MAX_COMPARATOR_CTX);
    cConfig.comparator_fn_cached = nullptr;
    cConfig.comparator_ctx_cached = nullptr;
    cConfig.commit_hook_fn = config.commitHookFn;
    cConfig.commit_hook_ctx = config.commitHookCtx;

    int result = tidesdb_create_column_family(db_, name.c_str(), &cConfig);
    checkResult(result, "failed to create column family");
}

void TidesDB::dropColumnFamily(const std::string& name)
{
    int result = tidesdb_drop_column_family(db_, name.c_str());
    checkResult(result, "failed to drop column family");
}

ColumnFamily TidesDB::getColumnFamily(const std::string& name)
{
    tidesdb_column_family_t* cf = tidesdb_get_column_family(db_, name.c_str());
    if (cf == nullptr)
    {
        throw Exception(ErrorCode::NotFound, "column family not found: " + name);
    }
    return ColumnFamily(cf);
}

std::vector<std::string> TidesDB::listColumnFamilies()
{
    char** names = nullptr;
    int count = 0;

    int result = tidesdb_list_column_families(db_, &names, &count);
    checkResult(result, "failed to list column families");

    std::vector<std::string> cfNames;
    cfNames.reserve(count);

    for (int i = 0; i < count; ++i)
    {
        cfNames.emplace_back(names[i]);
        std::free(names[i]);
    }
    std::free(names);

    return cfNames;
}

Transaction TidesDB::beginTransaction()
{
    tidesdb_txn_t* txn = nullptr;
    int result = tidesdb_txn_begin(db_, &txn);
    checkResult(result, "failed to begin transaction");
    return Transaction(txn);
}

Transaction TidesDB::beginTransaction(IsolationLevel isolation)
{
    tidesdb_txn_t* txn = nullptr;
    int result = tidesdb_txn_begin_with_isolation(
        db_, static_cast<tidesdb_isolation_level_t>(isolation), &txn);
    checkResult(result, "failed to begin transaction with isolation");
    return Transaction(txn);
}

CacheStats TidesDB::getCacheStats()
{
    tidesdb_cache_stats_t cStats;
    int result = tidesdb_get_cache_stats(db_, &cStats);
    checkResult(result, "failed to get cache stats");

    CacheStats stats;
    stats.enabled = cStats.enabled != 0;
    stats.totalEntries = cStats.total_entries;
    stats.totalBytes = cStats.total_bytes;
    stats.hits = cStats.hits;
    stats.misses = cStats.misses;
    stats.hitRate = cStats.hit_rate;
    stats.numPartitions = cStats.num_partitions;

    return stats;
}

void TidesDB::registerComparator(const std::string& name, tidesdb_comparator_fn fn,
                                 const std::string& ctxStr, void* ctx)
{
    const char* ctxStrPtr = ctxStr.empty() ? nullptr : ctxStr.c_str();
    int result = tidesdb_register_comparator(db_, name.c_str(), fn, ctxStrPtr, ctx);
    checkResult(result, "failed to register comparator");
}

void TidesDB::getComparator(const std::string& name, tidesdb_comparator_fn* fn, void** ctx)
{
    int result = tidesdb_get_comparator(db_, name.c_str(), fn, ctx);
    checkResult(result, "failed to get comparator");
}

void TidesDB::renameColumnFamily(const std::string& oldName, const std::string& newName)
{
    int result = tidesdb_rename_column_family(db_, oldName.c_str(), newName.c_str());
    checkResult(result, "failed to rename column family");
}

void TidesDB::cloneColumnFamily(const std::string& srcName, const std::string& dstName)
{
    int result = tidesdb_clone_column_family(db_, srcName.c_str(), dstName.c_str());
    checkResult(result, "failed to clone column family");
}

void TidesDB::backup(const std::string& dir)
{
    int result = tidesdb_backup(db_, const_cast<char*>(dir.c_str()));
    checkResult(result, "failed to create backup");
}

void TidesDB::checkpoint(const std::string& dir)
{
    int result = tidesdb_checkpoint(db_, dir.c_str());
    checkResult(result, "failed to create checkpoint");
}

Config TidesDB::defaultConfig()
{
    tidesdb_config_t cConfig = tidesdb_default_config();

    Config config;
    config.dbPath = cConfig.db_path ? cConfig.db_path : "";
    config.numFlushThreads = cConfig.num_flush_threads;
    config.numCompactionThreads = cConfig.num_compaction_threads;
    config.logLevel = static_cast<LogLevel>(cConfig.log_level);
    config.blockCacheSize = cConfig.block_cache_size;
    config.maxOpenSSTables = cConfig.max_open_sstables;
    config.logToFile = cConfig.log_to_file != 0;
    config.logTruncationAt = cConfig.log_truncation_at;

    return config;
}

}  // namespace tidesdb

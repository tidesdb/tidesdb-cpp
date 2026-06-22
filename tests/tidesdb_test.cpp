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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

class TidesDBTest : public ::testing::Test
{
   protected:
    void SetUp() override
    {
        fs::remove_all(testDbPath_);
    }

    void TearDown() override
    {
        fs::remove_all(testDbPath_);
    }

    tidesdb::Config getConfig() const
    {
        tidesdb::Config config;
        config.dbPath = testDbPath_;
        config.numFlushThreads = 2;
        config.numCompactionThreads = 2;
        config.logLevel = tidesdb::LogLevel::Info;
        config.blockCacheSize = 64 * 1024 * 1024;
        config.maxOpenSSTables = 256;
        return config;
    }

    const std::string testDbPath_ = "testdb_cpp";
};

TEST_F(TidesDBTest, OpenClose)
{
    tidesdb::TidesDB db(getConfig());
}

TEST_F(TidesDBTest, CreateDropColumnFamily)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    cfConfig.writeBufferSize = 64 * 1024 * 1024;
    cfConfig.compressionAlgorithm = tidesdb::CompressionAlgorithm::LZ4;

    db.createColumnFamily("test_cf", cfConfig);
    db.dropColumnFamily("test_cf");
}

TEST_F(TidesDBTest, ListColumnFamilies)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();

    std::vector<std::string> cfNames = {"cf1", "cf2", "cf3"};
    for (const auto& name : cfNames)
    {
        db.createColumnFamily(name, cfConfig);
    }

    auto list = db.listColumnFamilies();
    ASSERT_EQ(list.size(), cfNames.size());

    for (const auto& expectedName : cfNames)
    {
        bool found = false;
        for (const auto& name : list)
        {
            if (name == expectedName)
            {
                found = true;
                break;
            }
        }
        ASSERT_TRUE(found) << "Expected column family " << expectedName << " not found";
    }
}

TEST_F(TidesDBTest, TransactionPutGetDelete)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    {
        auto txn = db.beginTransaction();
        txn.put(cf, "key", "value", -1);
        txn.commit();
    }

    {
        auto txn = db.beginTransaction();
        auto value = txn.get(cf, "key");
        std::string valueStr(value.begin(), value.end());
        ASSERT_EQ(valueStr, "value");
    }

    {
        auto txn = db.beginTransaction();
        txn.del(cf, "key");
        txn.commit();
    }

    {
        auto txn = db.beginTransaction();
        EXPECT_THROW(txn.get(cf, "key"), tidesdb::Exception);
    }
}

TEST_F(TidesDBTest, TransactionSingleDelete)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    {
        auto txn = db.beginTransaction();
        txn.put(cf, "sd_key", "sd_value", -1);
        txn.commit();
    }

    {
        auto txn = db.beginTransaction();
        txn.singleDelete(cf, "sd_key");
        txn.commit();
    }

    {
        auto txn = db.beginTransaction();
        EXPECT_THROW(txn.get(cf, "sd_key"), tidesdb::Exception);
    }

    // byte vector overload
    std::vector<std::uint8_t> key{'b', 'y', 't', 'e', 'k', 'e', 'y'};
    std::vector<std::uint8_t> value{'v', 'a', 'l'};

    {
        auto txn = db.beginTransaction();
        txn.put(cf, key, value, -1);
        txn.commit();
    }

    {
        auto txn = db.beginTransaction();
        txn.singleDelete(cf, key);
        txn.commit();
    }

    {
        auto txn = db.beginTransaction();
        EXPECT_THROW(txn.get(cf, key), tidesdb::Exception);
    }
}

TEST_F(TidesDBTest, TransactionWithTTL)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    auto ttl = std::time(nullptr) + 2;

    {
        auto txn = db.beginTransaction();
        txn.put(cf, "temp_key", "temp_value", ttl);
        txn.commit();
    }

    {
        auto txn = db.beginTransaction();
        auto value = txn.get(cf, "temp_key");
        std::string valueStr(value.begin(), value.end());
        ASSERT_EQ(valueStr, "temp_value");
    }

    std::this_thread::sleep_for(std::chrono::seconds(3));

    {
        auto txn = db.beginTransaction();
        EXPECT_THROW(txn.get(cf, "temp_key"), tidesdb::Exception);
    }
}

TEST_F(TidesDBTest, MultiOperationTransaction)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    {
        auto txn = db.beginTransaction();
        txn.put(cf, "key1", "value1", -1);
        txn.put(cf, "key2", "value2", -1);
        txn.put(cf, "key3", "value3", -1);
        txn.commit();
    }

    {
        auto txn = db.beginTransaction();
        for (int i = 1; i <= 3; ++i)
        {
            std::string key = "key" + std::to_string(i);
            std::string expectedValue = "value" + std::to_string(i);

            auto value = txn.get(cf, key);
            std::string valueStr(value.begin(), value.end());
            ASSERT_EQ(valueStr, expectedValue);
        }
    }
}

TEST_F(TidesDBTest, TransactionRollback)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    {
        auto txn = db.beginTransaction();
        txn.put(cf, "rollback_key", "rollback_value", -1);
        txn.rollback();
    }

    {
        auto txn = db.beginTransaction();
        EXPECT_THROW(txn.get(cf, "rollback_key"), tidesdb::Exception);
    }
}

TEST_F(TidesDBTest, Savepoints)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    {
        auto txn = db.beginTransaction();
        txn.put(cf, "key1", "value1", -1);
        txn.savepoint("sp1");
        txn.put(cf, "key2", "value2", -1);
        txn.rollbackToSavepoint("sp1");
        txn.put(cf, "key3", "value3", -1);
        txn.commit();
    }

    {
        auto txn = db.beginTransaction();

        auto value1 = txn.get(cf, "key1");
        std::string value1Str(value1.begin(), value1.end());
        ASSERT_EQ(value1Str, "value1");

        EXPECT_THROW(txn.get(cf, "key2"), tidesdb::Exception);

        auto value3 = txn.get(cf, "key3");
        std::string value3Str(value3.begin(), value3.end());
        ASSERT_EQ(value3Str, "value3");
    }
}

TEST_F(TidesDBTest, IsolationLevels)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    std::vector<std::pair<std::string, tidesdb::IsolationLevel>> isolationLevels = {
        {"read_uncommitted", tidesdb::IsolationLevel::ReadUncommitted},
        {"read_committed", tidesdb::IsolationLevel::ReadCommitted},
        {"repeatable_read", tidesdb::IsolationLevel::RepeatableRead},
        {"snapshot", tidesdb::IsolationLevel::Snapshot},
        {"serializable", tidesdb::IsolationLevel::Serializable}};

    for (const auto& [name, level] : isolationLevels)
    {
        auto txn = db.beginTransaction(level);
        std::string key = "key_" + name;
        std::string value = "value_" + name;
        txn.put(cf, key, value, -1);
        txn.commit();
    }
}

TEST_F(TidesDBTest, IteratorForward)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    std::map<std::string, std::string> testData = {{"key1", "value1"},
                                                   {"key2", "value2"},
                                                   {"key3", "value3"},
                                                   {"key4", "value4"},
                                                   {"key5", "value5"}};

    {
        auto txn = db.beginTransaction();
        for (const auto& [k, v] : testData)
        {
            txn.put(cf, k, v, -1);
        }
        txn.commit();
    }

    {
        auto txn = db.beginTransaction();
        auto iter = txn.newIterator(cf);
        iter.seekToFirst();

        int count = 0;
        while (iter.valid())
        {
            auto key = iter.key();
            auto value = iter.value();
            std::string keyStr(key.begin(), key.end());
            std::string valueStr(value.begin(), value.end());

            ASSERT_TRUE(testData.find(keyStr) != testData.end());
            ASSERT_EQ(testData[keyStr], valueStr);

            count++;
            iter.next();
        }

        ASSERT_EQ(count, static_cast<int>(testData.size()));
    }
}

TEST_F(TidesDBTest, IteratorBackward)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    std::map<std::string, std::string> testData = {
        {"key1", "value1"}, {"key2", "value2"}, {"key3", "value3"}};

    {
        auto txn = db.beginTransaction();
        for (const auto& [k, v] : testData)
        {
            txn.put(cf, k, v, -1);
        }
        txn.commit();
    }

    {
        auto txn = db.beginTransaction();
        auto iter = txn.newIterator(cf);
        iter.seekToLast();

        int count = 0;
        while (iter.valid())
        {
            auto key = iter.key();
            auto value = iter.value();
            std::string keyStr(key.begin(), key.end());
            std::string valueStr(value.begin(), value.end());

            ASSERT_TRUE(testData.find(keyStr) != testData.end());
            ASSERT_EQ(testData[keyStr], valueStr);

            count++;
            iter.prev();
        }

        ASSERT_GE(count, 2);
    }
}

TEST_F(TidesDBTest, IteratorSeek)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    {
        auto txn = db.beginTransaction();
        for (int i = 1; i <= 10; ++i)
        {
            char key[16], value[16];
            std::snprintf(key, sizeof(key), "key%02d", i);
            std::snprintf(value, sizeof(value), "value%02d", i);
            txn.put(cf, key, value, -1);
        }
        txn.commit();
    }

    {
        auto txn = db.beginTransaction();
        auto iter = txn.newIterator(cf);
        iter.seek("key05");

        if (iter.valid())
        {
            auto key = iter.key();
            std::string keyStr(key.begin(), key.end());
            ASSERT_GE(keyStr, "key05");
        }
    }
}

TEST_F(TidesDBTest, GetStats)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    cfConfig.writeBufferSize = 2 * 1024 * 1024;
    cfConfig.compressionAlgorithm = tidesdb::CompressionAlgorithm::LZ4;
    cfConfig.enableBloomFilter = true;
    cfConfig.bloomFPR = 0.01;

    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    {
        auto txn = db.beginTransaction();
        for (int i = 0; i < 10; ++i)
        {
            std::string key = "key" + std::to_string(i);
            std::string value = "value" + std::to_string(i);
            txn.put(cf, key, value, -1);
        }
        txn.commit();
    }

    auto stats = cf.getStats();
    ASSERT_GE(stats.numLevels, 0);
}

TEST_F(TidesDBTest, CacheStats)
{
    tidesdb::TidesDB db(getConfig());

    auto cacheStats = db.getCacheStats();
    ASSERT_TRUE(cacheStats.enabled);
}

TEST_F(TidesDBTest, Compaction)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    cfConfig.writeBufferSize = 1024;

    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    for (int batch = 0; batch < 5; ++batch)
    {
        auto txn = db.beginTransaction();
        for (int i = 0; i < 20; ++i)
        {
            std::string key = "key" + std::to_string(batch) + "_" + std::to_string(i);
            std::vector<std::uint8_t> value(512, static_cast<std::uint8_t>(i % 256));
            txn.put(cf, std::vector<std::uint8_t>(key.begin(), key.end()), value, -1);
        }
        txn.commit();
    }

    cf.compact();
}

TEST_F(TidesDBTest, FlushMemtable)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    {
        auto txn = db.beginTransaction();
        for (int i = 0; i < 100; ++i)
        {
            std::string key = "key" + std::to_string(i);
            std::string value = "value" + std::to_string(i);
            txn.put(cf, key, value, -1);
        }
        txn.commit();
    }

    cf.flushMemtable();
}

TEST_F(TidesDBTest, PurgeColumnFamily)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    {
        auto txn = db.beginTransaction();
        for (int i = 0; i < 100; ++i)
        {
            std::string key = "key" + std::to_string(i);
            std::string value = "value" + std::to_string(i);
            txn.put(cf, key, value, -1);
        }
        txn.commit();
    }

    ASSERT_NO_THROW(cf.purge());
}

TEST_F(TidesDBTest, PurgeDatabase)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("cf1", cfConfig);
    db.createColumnFamily("cf2", cfConfig);

    auto cf1 = db.getColumnFamily("cf1");
    auto cf2 = db.getColumnFamily("cf2");

    {
        auto txn = db.beginTransaction();
        for (int i = 0; i < 50; ++i)
        {
            txn.put(cf1, "k1_" + std::to_string(i), "v1_" + std::to_string(i), -1);
            txn.put(cf2, "k2_" + std::to_string(i), "v2_" + std::to_string(i), -1);
        }
        txn.commit();
    }

    ASSERT_NO_THROW(db.purge());
}

TEST_F(TidesDBTest, NonExistentColumnFamily)
{
    tidesdb::TidesDB db(getConfig());

    EXPECT_THROW(db.getColumnFamily("nonexistent_cf"), tidesdb::Exception);
    EXPECT_THROW(db.dropColumnFamily("nonexistent_cf"), tidesdb::Exception);
}

TEST_F(TidesDBTest, SyncModes)
{
    tidesdb::TidesDB db(getConfig());

    std::vector<std::tuple<std::string, tidesdb::SyncMode, std::uint64_t>> syncModes = {
        {"none", tidesdb::SyncMode::None, 0},
        {"interval", tidesdb::SyncMode::Interval, 128000},
        {"full", tidesdb::SyncMode::Full, 0}};

    for (const auto& [name, mode, intervalUs] : syncModes)
    {
        std::string cfName = "cf_" + name;

        auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
        cfConfig.syncMode = mode;
        cfConfig.syncIntervalUs = intervalUs;

        db.createColumnFamily(cfName, cfConfig);

        auto cf = db.getColumnFamily(cfName);
        auto stats = cf.getStats();

        if (stats.config.has_value())
        {
            ASSERT_EQ(stats.config->syncMode, mode);
        }
    }
}

TEST_F(TidesDBTest, CompressionAlgorithms)
{
    tidesdb::TidesDB db(getConfig());

    std::vector<std::pair<std::string, tidesdb::CompressionAlgorithm>> compressionAlgos = {
        {"none", tidesdb::CompressionAlgorithm::None},
        {"lz4", tidesdb::CompressionAlgorithm::LZ4},
        {"lz4_fast", tidesdb::CompressionAlgorithm::LZ4Fast},
        {"zstd", tidesdb::CompressionAlgorithm::Zstd}};

    for (const auto& [name, algo] : compressionAlgos)
    {
        std::string cfName = "cf_" + name;

        auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
        cfConfig.compressionAlgorithm = algo;

        db.createColumnFamily(cfName, cfConfig);

        auto cf = db.getColumnFamily(cfName);
        auto stats = cf.getStats();

        if (stats.config.has_value())
        {
            ASSERT_EQ(stats.config->compressionAlgorithm, algo);
        }
    }
}

TEST_F(TidesDBTest, ByteVectorOperations)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    std::vector<std::uint8_t> key = {0x01, 0x02, 0x03, 0x04};
    std::vector<std::uint8_t> value = {0x10, 0x20, 0x30, 0x40, 0x50};

    {
        auto txn = db.beginTransaction();
        txn.put(cf, key, value, -1);
        txn.commit();
    }

    {
        auto txn = db.beginTransaction();
        auto gotValue = txn.get(cf, key);
        ASSERT_EQ(gotValue, value);
    }

    {
        auto txn = db.beginTransaction();
        txn.del(cf, key);
        txn.commit();
    }

    {
        auto txn = db.beginTransaction();
        EXPECT_THROW(txn.get(cf, key), tidesdb::Exception);
    }
}

TEST_F(TidesDBTest, RenameColumnFamily)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("old_cf", cfConfig);

    auto cf = db.getColumnFamily("old_cf");
    {
        auto txn = db.beginTransaction();
        txn.put(cf, "key", "value", -1);
        txn.commit();
    }

    db.renameColumnFamily("old_cf", "new_cf");

    EXPECT_THROW(db.getColumnFamily("old_cf"), tidesdb::Exception);

    auto newCf = db.getColumnFamily("new_cf");
    {
        auto txn = db.beginTransaction();
        auto value = txn.get(newCf, "key");
        std::string valueStr(value.begin(), value.end());
        ASSERT_EQ(valueStr, "value");
    }
}

TEST_F(TidesDBTest, IsFlushingAndCompacting)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    bool isFlushing = cf.isFlushing();
    bool isCompacting = cf.isCompacting();

    ASSERT_FALSE(isFlushing);
    ASSERT_FALSE(isCompacting);
}

TEST_F(TidesDBTest, Backup)
{
    std::string backupPath = testDbPath_ + "_backup";
    fs::remove_all(backupPath);

    {
        tidesdb::TidesDB db(getConfig());

        auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
        db.createColumnFamily("test_cf", cfConfig);

        auto cf = db.getColumnFamily("test_cf");
        {
            auto txn = db.beginTransaction();
            txn.put(cf, "backup_key", "backup_value", -1);
            txn.commit();
        }

        db.backup(backupPath);
    }

    ASSERT_TRUE(fs::exists(backupPath));

    {
        tidesdb::Config config;
        config.dbPath = backupPath;
        config.numFlushThreads = 2;
        config.numCompactionThreads = 2;
        config.logLevel = tidesdb::LogLevel::Info;
        config.blockCacheSize = 64 * 1024 * 1024;
        config.maxOpenSSTables = 256;

        tidesdb::TidesDB backupDb(config);
        auto cf = backupDb.getColumnFamily("test_cf");
        auto txn = backupDb.beginTransaction();
        auto value = txn.get(cf, "backup_key");
        std::string valueStr(value.begin(), value.end());
        ASSERT_EQ(valueStr, "backup_value");
    }

    fs::remove_all(backupPath);
}

TEST_F(TidesDBTest, Checkpoint)
{
    std::string checkpointPath = testDbPath_ + "_checkpoint";
    fs::remove_all(checkpointPath);

    {
        tidesdb::TidesDB db(getConfig());

        auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
        db.createColumnFamily("test_cf", cfConfig);

        auto cf = db.getColumnFamily("test_cf");
        {
            auto txn = db.beginTransaction();
            txn.put(cf, "checkpoint_key", "checkpoint_value", -1);
            txn.commit();
        }

        db.checkpoint(checkpointPath);
    }

    ASSERT_TRUE(fs::exists(checkpointPath));

    {
        tidesdb::Config config;
        config.dbPath = checkpointPath;
        config.numFlushThreads = 2;
        config.numCompactionThreads = 2;
        config.logLevel = tidesdb::LogLevel::Info;
        config.blockCacheSize = 64 * 1024 * 1024;
        config.maxOpenSSTables = 256;

        tidesdb::TidesDB checkpointDb(config);
        auto cf = checkpointDb.getColumnFamily("test_cf");
        auto txn = checkpointDb.beginTransaction();
        auto value = txn.get(cf, "checkpoint_key");
        std::string valueStr(value.begin(), value.end());
        ASSERT_EQ(valueStr, "checkpoint_value");
    }

    fs::remove_all(checkpointPath);
}

TEST_F(TidesDBTest, ExtendedStats)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    cfConfig.writeBufferSize = 2 * 1024 * 1024;
    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    {
        auto txn = db.beginTransaction();
        for (int i = 0; i < 100; ++i)
        {
            std::string key = "key" + std::to_string(i);
            std::string value = "value" + std::to_string(i);
            txn.put(cf, key, value, -1);
        }
        txn.commit();
    }

    auto stats = cf.getStats();

    ASSERT_GE(stats.numLevels, 0);
    ASSERT_GE(stats.totalKeys, 0u);
    ASSERT_GE(stats.totalDataSize, 0u);
    ASSERT_GE(stats.avgKeySize, 0.0);
    ASSERT_GE(stats.avgValueSize, 0.0);
    ASSERT_GE(stats.readAmp, 0.0);
    ASSERT_GE(stats.hitRate, 0.0);
}

TEST_F(TidesDBTest, UpdateRuntimeConfig)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    cfConfig.writeBufferSize = 64 * 1024 * 1024;
    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    auto newConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    newConfig.writeBufferSize = 128 * 1024 * 1024;
    newConfig.skipListMaxLevel = 16;
    newConfig.bloomFPR = 0.001;

    cf.updateRuntimeConfig(newConfig, false);

    auto stats = cf.getStats();
    if (stats.config.has_value())
    {
        ASSERT_EQ(stats.config->writeBufferSize, 128 * 1024 * 1024u);
    }
}

TEST_F(TidesDBTest, DefaultConfig)
{
    auto defaultConfig = tidesdb::TidesDB::defaultConfig();

    ASSERT_GE(defaultConfig.numFlushThreads, 0);
    ASSERT_GE(defaultConfig.numCompactionThreads, 0);
    ASSERT_GE(defaultConfig.blockCacheSize, 0u);
    ASSERT_GE(defaultConfig.maxOpenSSTables, 0u);
}

TEST_F(TidesDBTest, UseBtreeConfig)
{
    tidesdb::TidesDB db(getConfig());

    // Test with useBtree = false (default, block-based format)
    {
        auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
        ASSERT_FALSE(cfConfig.useBtree);  // Default should be false
        db.createColumnFamily("block_cf", cfConfig);

        auto cf = db.getColumnFamily("block_cf");
        auto stats = cf.getStats();
        ASSERT_FALSE(stats.useBtree);
        if (stats.config.has_value())
        {
            ASSERT_FALSE(stats.config->useBtree);
        }
    }

    // Test with useBtree = true (B+tree format)
    {
        auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
        cfConfig.useBtree = true;
        db.createColumnFamily("btree_cf", cfConfig);

        auto cf = db.getColumnFamily("btree_cf");
        auto stats = cf.getStats();
        ASSERT_TRUE(stats.useBtree);
        if (stats.config.has_value())
        {
            ASSERT_TRUE(stats.config->useBtree);
        }
    }
}

TEST_F(TidesDBTest, BtreeStats)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    cfConfig.useBtree = true;
    cfConfig.writeBufferSize = 1024;  // Small buffer to trigger flush
    db.createColumnFamily("btree_cf", cfConfig);

    auto cf = db.getColumnFamily("btree_cf");

    // Insert data to populate B+tree structures
    {
        auto txn = db.beginTransaction();
        for (int i = 0; i < 50; ++i)
        {
            std::string key = "key" + std::to_string(i);
            std::string value = "value" + std::to_string(i);
            txn.put(cf, key, value, -1);
        }
        txn.commit();
    }

    // Force flush to create SSTables with B+tree format
    cf.flushMemtable();

    auto stats = cf.getStats();

    // Verify B+tree stats fields exist and are valid
    ASSERT_TRUE(stats.useBtree);
    ASSERT_GE(stats.btreeTotalNodes, 0u);
    ASSERT_GE(stats.btreeMaxHeight, 0u);
    ASSERT_GE(stats.btreeAvgHeight, 0.0);
}

TEST_F(TidesDBTest, CloneColumnFamily)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("source_cf", cfConfig);

    auto cf = db.getColumnFamily("source_cf");

    // Write data to source
    {
        auto txn = db.beginTransaction();
        txn.put(cf, "key1", "value1", -1);
        txn.put(cf, "key2", "value2", -1);
        txn.put(cf, "key3", "value3", -1);
        txn.commit();
    }

    // Clone the column family
    db.cloneColumnFamily("source_cf", "cloned_cf");

    // Verify cloned CF exists and has the same data
    auto clonedCf = db.getColumnFamily("cloned_cf");
    {
        auto txn = db.beginTransaction();
        for (int i = 1; i <= 3; ++i)
        {
            std::string key = "key" + std::to_string(i);
            std::string expectedValue = "value" + std::to_string(i);

            auto value = txn.get(clonedCf, key);
            std::string valueStr(value.begin(), value.end());
            ASSERT_EQ(valueStr, expectedValue);
        }
    }

    // Verify source CF still exists and is independent
    auto sourceCf = db.getColumnFamily("source_cf");
    {
        auto txn = db.beginTransaction();
        auto value = txn.get(sourceCf, "key1");
        std::string valueStr(value.begin(), value.end());
        ASSERT_EQ(valueStr, "value1");
    }

    // Verify independence: write to clone doesn't affect source
    {
        auto txn = db.beginTransaction();
        txn.put(clonedCf, "clone_only_key", "clone_only_value", -1);
        txn.commit();
    }

    {
        auto txn = db.beginTransaction();
        EXPECT_THROW(txn.get(sourceCf, "clone_only_key"), tidesdb::Exception);
    }
}

TEST_F(TidesDBTest, CloneColumnFamilyErrors)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("existing_cf", cfConfig);

    // Clone non-existent source
    EXPECT_THROW(db.cloneColumnFamily("nonexistent_cf", "new_cf"), tidesdb::Exception);

    // Clone to existing destination
    EXPECT_THROW(db.cloneColumnFamily("existing_cf", "existing_cf"), tidesdb::Exception);
}

TEST_F(TidesDBTest, RangeCost)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    cfConfig.writeBufferSize = 1024;  // Small buffer to trigger flush
    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    // Insert data across a key range
    {
        auto txn = db.beginTransaction();
        for (int i = 0; i < 100; ++i)
        {
            char key[32], value[64];
            std::snprintf(key, sizeof(key), "user:%04d", i);
            std::snprintf(value, sizeof(value), "data_%04d", i);
            txn.put(cf, key, value, -1);
        }
        txn.commit();
    }

    // Flush to create SSTables so range cost has data to estimate
    cf.flushMemtable();

    // Estimate cost for two ranges
    double costA = cf.rangeCost("user:0000", "user:0049");
    double costB = cf.rangeCost("user:0000", "user:0099");

    // Both costs should be non-negative
    ASSERT_GE(costA, 0.0);
    ASSERT_GE(costB, 0.0);

    // Larger range should have >= cost than smaller range
    ASSERT_GE(costB, costA);
}

TEST_F(TidesDBTest, RangeCostByteVector)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    // Insert some data
    {
        auto txn = db.beginTransaction();
        for (int i = 0; i < 10; ++i)
        {
            std::string key = "key" + std::to_string(i);
            std::string value = "value" + std::to_string(i);
            txn.put(cf, key, value, -1);
        }
        txn.commit();
    }

    std::vector<std::uint8_t> keyA = {'k', 'e', 'y', '0'};
    std::vector<std::uint8_t> keyB = {'k', 'e', 'y', '9'};

    double cost = cf.rangeCost(keyA, keyB);
    ASSERT_GE(cost, 0.0);
}

TEST_F(TidesDBTest, RangeCostKeyOrderIrrelevant)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    // Insert some data
    {
        auto txn = db.beginTransaction();
        for (int i = 0; i < 10; ++i)
        {
            std::string key = "key" + std::to_string(i);
            std::string value = "value" + std::to_string(i);
            txn.put(cf, key, value, -1);
        }
        txn.commit();
    }

    // Key order should not matter per the C API docs
    double costAB = cf.rangeCost("key0", "key9");
    double costBA = cf.rangeCost("key9", "key0");

    ASSERT_DOUBLE_EQ(costAB, costBA);
}

TEST_F(TidesDBTest, TransactionReset)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    // Begin, use, commit, then reset and reuse
    auto txn = db.beginTransaction();

    // First batch of work
    txn.put(cf, "key1", "value1", -1);
    txn.commit();

    // Reset instead of free + begin
    txn.reset(tidesdb::IsolationLevel::ReadCommitted);

    // Second batch of work using the same transaction
    txn.put(cf, "key2", "value2", -1);
    txn.commit();

    // Verify both batches were committed
    {
        auto readTxn = db.beginTransaction();
        auto value1 = readTxn.get(cf, "key1");
        std::string value1Str(value1.begin(), value1.end());
        ASSERT_EQ(value1Str, "value1");

        auto value2 = readTxn.get(cf, "key2");
        std::string value2Str(value2.begin(), value2.end());
        ASSERT_EQ(value2Str, "value2");
    }
}

TEST_F(TidesDBTest, TransactionResetWithDifferentIsolation)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    // Start with ReadCommitted
    auto txn = db.beginTransaction(tidesdb::IsolationLevel::ReadCommitted);
    txn.put(cf, "key1", "value1", -1);
    txn.commit();

    // Reset to RepeatableRead
    txn.reset(tidesdb::IsolationLevel::RepeatableRead);
    txn.put(cf, "key2", "value2", -1);
    txn.commit();

    // Reset to Snapshot
    txn.reset(tidesdb::IsolationLevel::Snapshot);
    txn.put(cf, "key3", "value3", -1);
    txn.commit();

    // Verify all writes succeeded
    {
        auto readTxn = db.beginTransaction();
        for (int i = 1; i <= 3; ++i)
        {
            std::string key = "key" + std::to_string(i);
            std::string expectedValue = "value" + std::to_string(i);

            auto value = readTxn.get(cf, key);
            std::string valueStr(value.begin(), value.end());
            ASSERT_EQ(valueStr, expectedValue);
        }
    }
}

TEST_F(TidesDBTest, TransactionResetAfterRollback)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    auto txn = db.beginTransaction();

    // Put and rollback
    txn.put(cf, "rolled_back_key", "rolled_back_value", -1);
    txn.rollback();

    // Reset after rollback
    txn.reset(tidesdb::IsolationLevel::ReadCommitted);

    // New work after reset
    txn.put(cf, "new_key", "new_value", -1);
    txn.commit();

    // Verify rolled back key doesn't exist, new key does
    {
        auto readTxn = db.beginTransaction();
        EXPECT_THROW(readTxn.get(cf, "rolled_back_key"), tidesdb::Exception);
    }

    {
        auto readTxn = db.beginTransaction();
        auto value = readTxn.get(cf, "new_key");
        std::string valueStr(value.begin(), value.end());
        ASSERT_EQ(valueStr, "new_value");
    }
}

TEST_F(TidesDBTest, SyncWal)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    cfConfig.syncMode = tidesdb::SyncMode::None;
    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    // Write some data
    {
        auto txn = db.beginTransaction();
        for (int i = 0; i < 10; ++i)
        {
            std::string key = "key" + std::to_string(i);
            std::string value = "value" + std::to_string(i);
            txn.put(cf, key, value, -1);
        }
        txn.commit();
    }

    // Force WAL sync -- should not throw
    ASSERT_NO_THROW(cf.syncWal());
}

TEST_F(TidesDBTest, SyncWalWithIntervalMode)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    cfConfig.syncMode = tidesdb::SyncMode::Interval;
    cfConfig.syncIntervalUs = 1000000;  // 1 second
    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    // Write data
    {
        auto txn = db.beginTransaction();
        txn.put(cf, "key1", "value1", -1);
        txn.commit();
    }

    // Explicit sync before the interval fires
    ASSERT_NO_THROW(cf.syncWal());
}

TEST_F(TidesDBTest, GetDbStats)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("cf1", cfConfig);
    db.createColumnFamily("cf2", cfConfig);

    auto cf1 = db.getColumnFamily("cf1");
    auto cf2 = db.getColumnFamily("cf2");

    // Write some data
    {
        auto txn = db.beginTransaction();
        for (int i = 0; i < 20; ++i)
        {
            txn.put(cf1, "k1_" + std::to_string(i), "v1_" + std::to_string(i), -1);
            txn.put(cf2, "k2_" + std::to_string(i), "v2_" + std::to_string(i), -1);
        }
        txn.commit();
    }

    auto dbStats = db.getDbStats();

    ASSERT_EQ(dbStats.numColumnFamilies, 2);
    ASSERT_GT(dbStats.totalMemory, 0u);
    ASSERT_GE(dbStats.resolvedMemoryLimit, 0u);
    ASSERT_GE(dbStats.memoryPressureLevel, 0);
    ASSERT_GE(dbStats.globalSeq, 0u);
    ASSERT_GE(dbStats.flushQueueSize, 0u);
    ASSERT_GE(dbStats.compactionQueueSize, 0u);
}

TEST_F(TidesDBTest, GetDbStatsEmpty)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("empty_cf", cfConfig);

    auto dbStats = db.getDbStats();

    ASSERT_EQ(dbStats.numColumnFamilies, 1);
    ASSERT_GT(dbStats.totalMemory, 0u);
}

// Commit hook test helpers
struct HookTestCtx
{
    std::atomic<int> callCount{0};
    std::atomic<int> totalOps{0};
    std::atomic<uint64_t> lastCommitSeq{0};
    std::mutex mu;
    std::vector<std::string> capturedKeys;
};

static int testCommitHook(const tidesdb_commit_op_t* ops, int num_ops, uint64_t commit_seq,
                          void* ctx)
{
    auto* hctx = static_cast<HookTestCtx*>(ctx);
    hctx->callCount.fetch_add(1);
    hctx->totalOps.fetch_add(num_ops);
    hctx->lastCommitSeq.store(commit_seq);

    std::lock_guard<std::mutex> lock(hctx->mu);
    for (int i = 0; i < num_ops; ++i)
    {
        hctx->capturedKeys.emplace_back(reinterpret_cast<const char*>(ops[i].key), ops[i].key_size);
    }
    return 0;
}

TEST_F(TidesDBTest, CommitHookBasic)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    HookTestCtx hookCtx;
    cf.setCommitHook(testCommitHook, &hookCtx);

    // Commit a transaction -- hook should fire
    {
        auto txn = db.beginTransaction();
        txn.put(cf, "key1", "value1", -1);
        txn.put(cf, "key2", "value2", -1);
        txn.commit();
    }

    ASSERT_GE(hookCtx.callCount.load(), 1);
    ASSERT_GE(hookCtx.totalOps.load(), 2);
    ASSERT_GT(hookCtx.lastCommitSeq.load(), 0u);

    // Verify captured keys
    {
        std::lock_guard<std::mutex> lock(hookCtx.mu);
        bool foundKey1 = false, foundKey2 = false;
        for (const auto& k : hookCtx.capturedKeys)
        {
            if (k == "key1") foundKey1 = true;
            if (k == "key2") foundKey2 = true;
        }
        ASSERT_TRUE(foundKey1);
        ASSERT_TRUE(foundKey2);
    }
}

TEST_F(TidesDBTest, CommitHookClear)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    HookTestCtx hookCtx;
    cf.setCommitHook(testCommitHook, &hookCtx);

    // First commit -- hook fires
    {
        auto txn = db.beginTransaction();
        txn.put(cf, "key1", "value1", -1);
        txn.commit();
    }

    int countAfterFirst = hookCtx.callCount.load();
    ASSERT_GE(countAfterFirst, 1);

    // Clear the hook
    cf.clearCommitHook();

    // Second commit -- hook should NOT fire
    {
        auto txn = db.beginTransaction();
        txn.put(cf, "key2", "value2", -1);
        txn.commit();
    }

    ASSERT_EQ(hookCtx.callCount.load(), countAfterFirst);
}

TEST_F(TidesDBTest, CommitHookDeleteOps)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    // Insert data first
    {
        auto txn = db.beginTransaction();
        txn.put(cf, "del_key", "del_value", -1);
        txn.commit();
    }

    HookTestCtx hookCtx;
    cf.setCommitHook(testCommitHook, &hookCtx);

    // Delete the key -- hook should capture the delete
    {
        auto txn = db.beginTransaction();
        txn.del(cf, "del_key");
        txn.commit();
    }

    ASSERT_GE(hookCtx.callCount.load(), 1);
    ASSERT_GE(hookCtx.totalOps.load(), 1);
}

TEST_F(TidesDBTest, CommitHookMonotonicSeq)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    // Custom hook that records all commit sequences
    struct SeqCtx
    {
        std::mutex mu;
        std::vector<uint64_t> seqs;
    } seqCtx;

    auto seqHook = [](const tidesdb_commit_op_t*, int, uint64_t commit_seq, void* ctx) -> int
    {
        auto* sctx = static_cast<SeqCtx*>(ctx);
        std::lock_guard<std::mutex> lock(sctx->mu);
        sctx->seqs.push_back(commit_seq);
        return 0;
    };

    cf.setCommitHook(seqHook, &seqCtx);

    // Multiple commits
    for (int i = 0; i < 5; ++i)
    {
        auto txn = db.beginTransaction();
        txn.put(cf, "key" + std::to_string(i), "value" + std::to_string(i), -1);
        txn.commit();
    }

    // Verify monotonically increasing sequence numbers
    std::lock_guard<std::mutex> lock(seqCtx.mu);
    ASSERT_GE(seqCtx.seqs.size(), 5u);
    for (size_t i = 1; i < seqCtx.seqs.size(); ++i)
    {
        ASSERT_GT(seqCtx.seqs[i], seqCtx.seqs[i - 1]);
    }
}

TEST_F(TidesDBTest, CommitHookViaConfig)
{
    tidesdb::TidesDB db(getConfig());

    HookTestCtx hookCtx;

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    cfConfig.commitHookFn = testCommitHook;
    cfConfig.commitHookCtx = &hookCtx;
    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    // Commit -- hook should already be active from config
    {
        auto txn = db.beginTransaction();
        txn.put(cf, "key1", "value1", -1);
        txn.commit();
    }

    ASSERT_GE(hookCtx.callCount.load(), 1);
    ASSERT_GE(hookCtx.totalOps.load(), 1);
}

TEST_F(TidesDBTest, DeleteColumnFamily)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("to_delete_cf", cfConfig);

    auto cf = db.getColumnFamily("to_delete_cf");

    // Write some data
    {
        auto txn = db.beginTransaction();
        txn.put(cf, "key1", "value1", -1);
        txn.commit();
    }

    // Delete by handle
    db.deleteColumnFamily(cf);

    // Should no longer exist
    EXPECT_THROW(db.getColumnFamily("to_delete_cf"), tidesdb::Exception);
}

TEST_F(TidesDBTest, IteratorKeyValue)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    {
        auto txn = db.beginTransaction();
        txn.put(cf, "key1", "value1", -1);
        txn.put(cf, "key2", "value2", -1);
        txn.put(cf, "key3", "value3", -1);
        txn.commit();
    }

    {
        auto txn = db.beginTransaction();
        auto iter = txn.newIterator(cf);
        iter.seekToFirst();

        int count = 0;
        while (iter.valid())
        {
            auto [key, value] = iter.keyValue();
            std::string keyStr(key.begin(), key.end());
            std::string valueStr(value.begin(), value.end());

            ASSERT_FALSE(keyStr.empty());
            ASSERT_FALSE(valueStr.empty());

            count++;
            iter.next();
        }

        ASSERT_EQ(count, 3);
    }
}

TEST_F(TidesDBTest, UnifiedMemtableConfig)
{
    tidesdb::Config config;
    config.dbPath = testDbPath_;
    config.numFlushThreads = 2;
    config.numCompactionThreads = 2;
    config.logLevel = tidesdb::LogLevel::Info;
    config.blockCacheSize = 64 * 1024 * 1024;
    config.maxOpenSSTables = 256;
    config.unifiedMemtable = true;
    config.unifiedMemtableWriteBufferSize = 32 * 1024 * 1024;
    config.unifiedMemtableSkipListMaxLevel = 16;
    config.unifiedMemtableSkipListProbability = 0.25f;
    config.unifiedMemtableSyncMode = tidesdb::SyncMode::None;
    config.unifiedMemtableSyncIntervalUs = 0;

    tidesdb::TidesDB db(config);

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("test_cf", cfConfig);

    auto cf = db.getColumnFamily("test_cf");

    // Write and read data with unified memtable mode
    {
        auto txn = db.beginTransaction();
        txn.put(cf, "ukey", "uvalue", -1);
        txn.commit();
    }

    {
        auto txn = db.beginTransaction();
        auto value = txn.get(cf, "ukey");
        std::string valueStr(value.begin(), value.end());
        ASSERT_EQ(valueStr, "uvalue");
    }
}

TEST_F(TidesDBTest, DbStatsUnifiedFields)
{
    tidesdb::Config config;
    config.dbPath = testDbPath_;
    config.numFlushThreads = 2;
    config.numCompactionThreads = 2;
    config.logLevel = tidesdb::LogLevel::Info;
    config.blockCacheSize = 64 * 1024 * 1024;
    config.maxOpenSSTables = 256;
    config.unifiedMemtable = true;

    tidesdb::TidesDB db(config);

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("test_cf", cfConfig);

    auto dbStats = db.getDbStats();

    ASSERT_TRUE(dbStats.unifiedMemtableEnabled);
    ASSERT_FALSE(dbStats.objectStoreEnabled);
    ASSERT_FALSE(dbStats.replicaMode);

    // Single-writer fencing epochs are reported and zero when not in object-store mode
    ASSERT_EQ(dbStats.primaryEpoch, 0u);
    ASSERT_EQ(dbStats.seenEpoch, 0u);
}

TEST_F(TidesDBTest, ErrorCodeReadonly)
{
    // Verify the Readonly error code maps correctly
    ASSERT_EQ(static_cast<int>(tidesdb::ErrorCode::Readonly), TDB_ERR_READONLY);
    ASSERT_EQ(static_cast<int>(tidesdb::ErrorCode::Readonly), -13);

    // Verify error message
    std::string msg = tidesdb::Exception::errorMessage(TDB_ERR_READONLY);
    ASSERT_EQ(msg, "database is read-only");
}

TEST_F(TidesDBTest, ErrorCodePrecondition)
{
    // Verify the Precondition error code maps correctly
    ASSERT_EQ(static_cast<int>(tidesdb::ErrorCode::Precondition), TDB_ERR_PRECONDITION);
    ASSERT_EQ(static_cast<int>(tidesdb::ErrorCode::Precondition), -15);

    // Verify error message
    std::string msg = tidesdb::Exception::errorMessage(TDB_ERR_PRECONDITION);
    ASSERT_EQ(msg, "precondition failed");
}

TEST_F(TidesDBTest, FinishCompactionsOnClose)
{
    // Default should leave the fast-shutdown behavior (cancel in-flight compactions)
    auto defaultConfig = tidesdb::TidesDB::defaultConfig();
    ASSERT_FALSE(defaultConfig.finishCompactionsOnClose);

    // Opening with the flag enabled must succeed; close runs in-flight compactions to completion
    tidesdb::Config config = getConfig();
    config.finishCompactionsOnClose = true;

    tidesdb::TidesDB db(config);

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("test_cf", cfConfig);
    auto cf = db.getColumnFamily("test_cf");

    {
        auto txn = db.beginTransaction();
        for (int i = 0; i < 50; ++i)
        {
            txn.put(cf, "key_" + std::to_string(i), "value_" + std::to_string(i), -1);
        }
        txn.commit();
    }

    auto value = [&]
    {
        auto txn = db.beginTransaction();
        return txn.get(cf, "key_0");
    }();
    ASSERT_FALSE(value.empty());
}

TEST_F(TidesDBTest, DefaultConfigUnifiedFields)
{
    auto defaultConfig = tidesdb::TidesDB::defaultConfig();

    // Default should have unified memtable disabled
    ASSERT_FALSE(defaultConfig.unifiedMemtable);
    ASSERT_EQ(defaultConfig.unifiedMemtableWriteBufferSize, 0u);
    ASSERT_EQ(defaultConfig.unifiedMemtableSkipListMaxLevel, 0);
    ASSERT_EQ(defaultConfig.unifiedMemtableSkipListProbability, 0.0f);
    ASSERT_EQ(defaultConfig.unifiedMemtableSyncMode, tidesdb::SyncMode::None);
    ASSERT_EQ(defaultConfig.unifiedMemtableSyncIntervalUs, 0u);
}

TEST_F(TidesDBTest, ColumnFamilyConfigObjectStoreFields)
{
    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();

    ASSERT_GE(cfConfig.objectLazyCompaction, 0);
    ASSERT_GE(cfConfig.objectPrefetchCompaction, 0);
}

TEST_F(TidesDBTest, TombstoneCfConfigRoundTrip)
{
    tidesdb::TidesDB db(getConfig());

    auto defaults = tidesdb::ColumnFamilyConfig::defaultConfig();
    ASSERT_GT(defaults.tombstoneDensityMinEntries, 0u)
        << "default tombstone_density_min_entries should be sourced from C library";

    auto cfConfig = defaults;
    cfConfig.tombstoneDensityTrigger = 0.5;
    cfConfig.tombstoneDensityMinEntries = 256;

    db.createColumnFamily("ts_cf", cfConfig);

    auto cf = db.getColumnFamily("ts_cf");
    auto stats = cf.getStats();

    ASSERT_TRUE(stats.config.has_value());
    ASSERT_DOUBLE_EQ(stats.config->tombstoneDensityTrigger, 0.5);
    ASSERT_EQ(stats.config->tombstoneDensityMinEntries, 256u);
}

TEST_F(TidesDBTest, TombstoneStatsAfterDeletes)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    // Use a large write buffer so the puts and the deletes each flush to a single
    // L0 SSTable. With a tiny buffer the keys spread across many L0 files, crossing
    // l1_file_count_trigger and letting background compaction purge the delete
    // tombstones before they can be observed here.
    cfConfig.writeBufferSize = 64 * 1024 * 1024;
    db.createColumnFamily("ts_cf", cfConfig);

    auto cf = db.getColumnFamily("ts_cf");

    constexpr int kNumKeys = 200;

    {
        auto txn = db.beginTransaction();
        for (int i = 0; i < kNumKeys; ++i)
        {
            std::string key = "key" + std::to_string(i);
            std::string value = "value" + std::to_string(i);
            txn.put(cf, key, value, -1);
        }
        txn.commit();
    }
    cf.flushMemtable();

    {
        auto txn = db.beginTransaction();
        for (int i = 0; i < kNumKeys / 2; ++i)
        {
            std::string key = "key" + std::to_string(i);
            txn.del(cf, key);
        }
        txn.commit();
    }
    cf.flushMemtable();

    // Brief wait for the flush to land
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    auto stats = cf.getStats();

    ASSERT_GT(stats.totalTombstones, 0u);
    ASSERT_GE(stats.tombstoneRatio, 0.0);
    ASSERT_LE(stats.tombstoneRatio, 1.0);
    ASSERT_GE(stats.maxSstDensity, 0.0);
    ASSERT_LE(stats.maxSstDensity, 1.0);
    ASSERT_EQ(stats.levelTombstoneCounts.size(), static_cast<std::size_t>(stats.numLevels));
    if (stats.maxSstDensityLevel != 0)
    {
        ASSERT_GE(stats.maxSstDensityLevel, 1);
        ASSERT_LE(stats.maxSstDensityLevel, stats.numLevels);
    }
}

TEST_F(TidesDBTest, CompactRange)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    cfConfig.writeBufferSize = 1024;  // force multiple SSTables
    db.createColumnFamily("cr_cf", cfConfig);

    auto cf = db.getColumnFamily("cr_cf");

    // Several batches of writes + flushes to spread across SSTables/levels
    for (int batch = 0; batch < 4; ++batch)
    {
        auto txn = db.beginTransaction();
        for (int i = 0; i < 100; ++i)
        {
            std::string key = "k" + std::to_string(batch) + "_" + std::to_string(i);
            std::string value = "v" + std::to_string(batch) + "_" + std::to_string(i);
            txn.put(cf, key, value, -1);
        }
        txn.commit();
        cf.flushMemtable();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Narrow range compaction should succeed
    std::vector<std::uint8_t> startKey{'k', '1', '_', '0'};
    std::vector<std::uint8_t> endKey{'k', '2', '_', '0'};
    ASSERT_NO_THROW(cf.compactRange(startKey, endKey));

    // Both endpoints unbounded must be rejected
    try
    {
        cf.compactRange(std::optional<std::vector<std::uint8_t>>{},
                        std::optional<std::vector<std::uint8_t>>{});
        FAIL() << "expected InvalidArgs exception for both-null range";
    }
    catch (const tidesdb::Exception& e)
    {
        ASSERT_EQ(e.code(), tidesdb::ErrorCode::InvalidArgs);
    }

    // Keys outside the compacted range must still read back unchanged
    {
        auto txn = db.beginTransaction();
        auto value = txn.get(cf, "k0_0");
        std::string valueStr(value.begin(), value.end());
        ASSERT_EQ(valueStr, "v0_0");

        auto value3 = txn.get(cf, "k3_50");
        std::string value3Str(value3.begin(), value3.end());
        ASSERT_EQ(value3Str, "v3_50");
    }
}

TEST_F(TidesDBTest, CompactRangeStringViewOverload)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    cfConfig.writeBufferSize = 1024;
    db.createColumnFamily("cr_sv_cf", cfConfig);

    auto cf = db.getColumnFamily("cr_sv_cf");

    {
        auto txn = db.beginTransaction();
        for (int i = 0; i < 50; ++i)
        {
            std::string key = "k_" + std::to_string(i);
            std::string value = "v_" + std::to_string(i);
            txn.put(cf, key, value, -1);
        }
        txn.commit();
    }
    cf.flushMemtable();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    using OptSV = std::optional<std::string_view>;
    ASSERT_NO_THROW(
        cf.compactRange(OptSV{std::string_view{"k_1"}}, OptSV{std::string_view{"k_3"}}));

    // Unbounded start (nullopt), bounded end -- should succeed
    ASSERT_NO_THROW(cf.compactRange(OptSV{}, OptSV{std::string_view{"k_5"}}));
}

TEST_F(TidesDBTest, MaxConcurrentFlushesConfig)
{
    tidesdb::Config config = getConfig();
    config.maxConcurrentFlushes = 1;

    tidesdb::TidesDB db(config);

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    db.createColumnFamily("mcf_cf", cfConfig);

    auto cf = db.getColumnFamily("mcf_cf");

    {
        auto txn = db.beginTransaction();
        txn.put(cf, "key", "value", -1);
        txn.commit();
    }
    ASSERT_NO_THROW(cf.flushMemtable());
}

TEST_F(TidesDBTest, MaxConcurrentFlushesDefault)
{
    auto defaultConfig = tidesdb::TidesDB::defaultConfig();

    // 0 is the documented sentinel returned by tidesdb_default_config(): at open time
    // the engine pins max_concurrent_flushes to the resolved num_flush_threads. Assert
    // the sentinel is carried through faithfully rather than altered by the wrapper.
    ASSERT_EQ(defaultConfig.maxConcurrentFlushes, 0)
        << "default max_concurrent_flushes should match the C library sentinel (0 = auto)";

    // The sentinel must be accepted by open and resolved internally without error.
    auto cfg = getConfig();
    cfg.maxConcurrentFlushes = defaultConfig.maxConcurrentFlushes;
    ASSERT_NO_THROW({ tidesdb::TidesDB db(cfg); });
}

TEST_F(TidesDBTest, ErrorCodeBusy)
{
    // Verify the Busy error code maps correctly
    ASSERT_EQ(static_cast<int>(tidesdb::ErrorCode::Busy), TDB_ERR_BUSY);
    ASSERT_EQ(static_cast<int>(tidesdb::ErrorCode::Busy), -14);

    std::string msg = tidesdb::Exception::errorMessage(TDB_ERR_BUSY);
    ASSERT_EQ(msg, "database is busy");
}

// Counting allocator state shared with non-capturing lambdas (statics, not captures)
static std::atomic<int> g_allocMallocCalls{0};
static std::atomic<int> g_allocFreeCalls{0};

TEST_F(TidesDBTest, InitFinalizeCustomAllocator)
{
    // Reset any lazy initialization performed by earlier tests so init() is deterministic.
    tidesdb::finalize();

    g_allocMallocCalls = 0;
    g_allocFreeCalls = 0;

    auto countingMalloc = [](std::size_t size) -> void*
    {
        g_allocMallocCalls.fetch_add(1);
        return std::malloc(size);
    };
    auto countingCalloc = [](std::size_t n, std::size_t size) -> void*
    { return std::calloc(n, size); };
    auto countingRealloc = [](void* p, std::size_t size) -> void* { return std::realloc(p, size); };
    auto countingFree = [](void* p)
    {
        if (p) g_allocFreeCalls.fetch_add(1);
        std::free(p);
    };

    // Non-capturing lambdas convert to plain C function pointers.
    ASSERT_TRUE(tidesdb::init(countingMalloc, countingCalloc, countingRealloc, countingFree));
    // A second init is a no-op while already initialized.
    ASSERT_FALSE(tidesdb::init());

    {
        tidesdb::TidesDB db(getConfig());
        auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
        db.createColumnFamily("alloc_cf", cfConfig);
        auto cf = db.getColumnFamily("alloc_cf");
        auto txn = db.beginTransaction();
        txn.put(cf, "k", "v", -1);
        txn.commit();
    }

    EXPECT_GT(g_allocMallocCalls.load(), 0);
    EXPECT_GT(g_allocFreeCalls.load(), 0);

    // Reset and re-establish a clean default-allocator init for subsequent tests.
    tidesdb::finalize();
    ASSERT_TRUE(tidesdb::init());
}

TEST_F(TidesDBTest, RaiseOpenFileLimit)
{
    // desired <= 0 just reports the current ceiling
    long current = tidesdb::raiseOpenFileLimit(0);
    ASSERT_GT(current, 0);

    // Requesting the current ceiling should report a non-negative ceiling back
    long raised = tidesdb::raiseOpenFileLimit(current);
    ASSERT_GE(raised, 0);
}

TEST_F(TidesDBTest, CancelBackgroundWork)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    cfConfig.writeBufferSize = 1024;  // small buffer to spawn flushes/compaction
    db.createColumnFamily("cbw_cf", cfConfig);

    auto cf = db.getColumnFamily("cbw_cf");

    for (int batch = 0; batch < 3; ++batch)
    {
        auto txn = db.beginTransaction();
        for (int i = 0; i < 50; ++i)
        {
            txn.put(cf, "k" + std::to_string(batch) + "_" + std::to_string(i), "value", -1);
        }
        txn.commit();
        cf.flushMemtable();
    }

    // Sticky cancellation of background compaction -- should return cleanly
    ASSERT_NO_THROW(db.cancelBackgroundWork());
}

TEST_F(TidesDBTest, BuiltInComparators)
{
    tidesdb::TidesDB db(getConfig());

    // The "lexicographic" comparator is strcmp-based and treats keys as NUL-terminated C
    // strings (it ignores the key sizes), so keys handed to it MUST carry a trailing '\0';
    // otherwise strcmp reads past the stored key into adjacent memory, producing
    // non-deterministic ordering and flaky lookups. Build NUL-terminated key bytes here -- the
    // trailing '\0' is harmless for the size-bounded comparators (memcmp, reverse,
    // case_insensitive), which just treat it as an ordinary final byte present in both the
    // stored key and the lookup key.
    auto nulTerminatedKey = [](const std::string& s)
    {
        std::vector<std::uint8_t> bytes(s.begin(), s.end());
        bytes.push_back('\0');
        return bytes;
    };

    for (const std::string& name : {std::string("memcmp"), std::string("lexicographic"),
                                    std::string("reverse"), std::string("case_insensitive")})
    {
        auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
        cfConfig.comparatorName = name;
        db.createColumnFamily("cmp_" + name, cfConfig);

        auto cf = db.getColumnFamily("cmp_" + name);
        const auto alphaKey = nulTerminatedKey("alpha");
        const auto betaKey = nulTerminatedKey("beta");
        const std::vector<std::uint8_t> oneValue{'1'};
        const std::vector<std::uint8_t> twoValue{'2'};
        {
            auto txn = db.beginTransaction();
            txn.put(cf, alphaKey, oneValue, -1);
            txn.put(cf, betaKey, twoValue, -1);
            txn.commit();
        }

        auto txn = db.beginTransaction();
        auto value = txn.get(cf, alphaKey);
        ASSERT_EQ(std::string(value.begin(), value.end()), "1") << "comparator: " << name;

        auto stats = cf.getStats();
        if (stats.config.has_value())
        {
            ASSERT_EQ(stats.config->comparatorName, name);
        }
    }
}

TEST_F(TidesDBTest, ReverseComparatorOrdering)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    cfConfig.comparatorName = "reverse";
    db.createColumnFamily("rev_cf", cfConfig);

    auto cf = db.getColumnFamily("rev_cf");
    {
        auto txn = db.beginTransaction();
        txn.put(cf, "a", "1", -1);
        txn.put(cf, "b", "2", -1);
        txn.put(cf, "c", "3", -1);
        txn.commit();
    }

    // Reverse comparator: the first key in iteration order is the largest
    auto txn = db.beginTransaction();
    auto iter = txn.newIterator(cf);
    iter.seekToFirst();
    ASSERT_TRUE(iter.valid());
    auto key = iter.key();
    ASSERT_EQ(std::string(key.begin(), key.end()), "c");
}

TEST_F(TidesDBTest, ComparatorFunctionPointers)
{
    tidesdb::TidesDB db(getConfig());

    // The exposed built-in function pointers are non-null
    ASSERT_NE(tidesdb::comparators::memcmp, nullptr);
    ASSERT_NE(tidesdb::comparators::uint64, nullptr);
    ASSERT_NE(tidesdb::comparators::reverseMemcmp, nullptr);

    // Register a built-in function pointer under a custom name and read it back
    db.registerComparator("my_reverse", tidesdb::comparators::reverseMemcmp);

    tidesdb_comparator_fn fn = nullptr;
    void* ctx = nullptr;
    db.getComparator("my_reverse", &fn, &ctx);
    ASSERT_EQ(fn, tidesdb::comparators::reverseMemcmp);
}

TEST_F(TidesDBTest, ComparatorCtxStrIniRoundTrip)
{
    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    cfConfig.comparatorName = "uint64";
    cfConfig.comparatorCtxStr = "endian=little";

    std::string iniPath = testDbPath_ + "_cfg.ini";
    fs::remove(iniPath);

    tidesdb::ColumnFamilyConfig::saveToIni(iniPath, "cf_section", cfConfig);
    auto loaded = tidesdb::ColumnFamilyConfig::loadFromIni(iniPath, "cf_section");

    ASSERT_EQ(loaded.comparatorName, "uint64");
    ASSERT_EQ(loaded.comparatorCtxStr, "endian=little");

    fs::remove(iniPath);
}

TEST_F(TidesDBTest, WriteAmplificationStats)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    cfConfig.writeBufferSize = 1024;  // small buffer to force a flush
    db.createColumnFamily("wa_cf", cfConfig);

    auto cf = db.getColumnFamily("wa_cf");
    {
        auto txn = db.beginTransaction();
        for (int i = 0; i < 200; ++i)
        {
            txn.put(cf, "key" + std::to_string(i), std::string(64, 'x'), -1);
        }
        txn.commit();
    }
    cf.flushMemtable();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    auto stats = cf.getStats();

    ASSERT_GT(stats.userBytesWritten, 0u);
    ASSERT_GT(stats.flushBytesWritten, 0u);
    ASSERT_GE(stats.flushCount, 1u);
    ASSERT_GE(stats.compactionBytesWritten, 0u);
    ASSERT_GE(stats.compactionBytesRead, 0u);
    ASSERT_GE(stats.walBytesWritten, 0u);
}

TEST_F(TidesDBTest, DbStatsWriteAmplification)
{
    tidesdb::TidesDB db(getConfig());

    auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
    cfConfig.writeBufferSize = 1024;
    db.createColumnFamily("dbwa_cf", cfConfig);

    auto cf = db.getColumnFamily("dbwa_cf");
    {
        auto txn = db.beginTransaction();
        for (int i = 0; i < 200; ++i)
        {
            txn.put(cf, "key" + std::to_string(i), std::string(64, 'y'), -1);
        }
        txn.commit();
    }
    cf.flushMemtable();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    auto stats = db.getDbStats();

    ASSERT_GT(stats.userBytesWritten, 0u);
    ASSERT_GT(stats.flushBytesWritten, 0u);
    // Per-CF WAL volume is reported (unified mode is off, so uwal stays zero)
    ASSERT_GE(stats.walBytesWritten, 0u);
    ASSERT_EQ(stats.uwalBytesWritten, 0u);
}

TEST_F(TidesDBTest, FilesystemObjectStore)
{
    std::string objDir = testDbPath_ + "_objstore";
    fs::remove_all(objDir);

    {
        tidesdb::Config config = getConfig();
        config.objectStore = tidesdb::objstore::filesystem(objDir);
        ASSERT_NE(config.objectStore, nullptr);
        config.objectStoreConfig = tidesdb::ObjectStoreConfig::defaultConfig();

        // Object store mode requires (and auto-enables) unified memtable mode
        tidesdb::TidesDB db(config);

        auto cfConfig = tidesdb::ColumnFamilyConfig::defaultConfig();
        db.createColumnFamily("os_cf", cfConfig);

        auto cf = db.getColumnFamily("os_cf");
        {
            auto txn = db.beginTransaction();
            txn.put(cf, "okey", "ovalue", -1);
            txn.commit();
        }
        cf.flushMemtable();
        std::this_thread::sleep_for(std::chrono::milliseconds(300));

        auto dbStats = db.getDbStats();
        ASSERT_TRUE(dbStats.objectStoreEnabled);

        {
            auto txn = db.beginTransaction();
            auto value = txn.get(cf, "okey");
            ASSERT_EQ(std::string(value.begin(), value.end()), "ovalue");
        }
    }

    fs::remove_all(objDir);
}

TEST_F(TidesDBTest, S3ConfigStructDefaults)
{
    // Construct the S3 config struct (does not link the S3 connector symbol).
    tidesdb::objstore::S3Config cfg;
    cfg.endpoint = "s3.amazonaws.com";
    cfg.bucket = "my-bucket";
    cfg.accessKey = "ak";
    cfg.secretKey = "sk";
    cfg.region = "us-east-1";

    ASSERT_TRUE(cfg.useSsl);
    ASSERT_FALSE(cfg.usePathStyle);
    ASSERT_FALSE(cfg.tlsInsecureSkipVerify);
    ASSERT_EQ(cfg.multipartThreshold, 0u);
    ASSERT_EQ(cfg.multipartPartSize, 0u);
}

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

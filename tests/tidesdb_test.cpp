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

#include <chrono>
#include <cstdlib>
#include <filesystem>
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

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

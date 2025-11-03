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
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <map>
#include <thread>

#include "../tidesdb.hpp"

namespace fs = std::filesystem;

// Test fixture for database tests
class TidesDBTest : public ::testing::Test
{
   protected:
    std::string db_path;

    void SetUp() override
    {
        db_path = "./test_db_" +
                  std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
    }

    void TearDown() override
    {
        cleanup_db(db_path);
    }

    void cleanup_db(const std::string& path)
    {
        if (fs::exists(path))
        {
            fs::remove_all(path);
        }
    }
};

// Open and close database
TEST_F(TidesDBTest, OpenClose)
{
    {
        tidesdb::DB db(db_path);
        // Database automatically closed on destruction
    }
}

// Create and drop column family
TEST_F(TidesDBTest, CreateDropColumnFamily)
{
    tidesdb::DB db(db_path);

    // Create column family
    db.create_column_family("test_cf");

    // Verify it exists
    auto cf_list = db.list_column_families();
    EXPECT_NE(std::find(cf_list.begin(), cf_list.end(), "test_cf"), cf_list.end());

    // Drop column family
    db.drop_column_family("test_cf");

    // Verify it's gone
    cf_list = db.list_column_families();
    EXPECT_EQ(std::find(cf_list.begin(), cf_list.end(), "test_cf"), cf_list.end());
}

// Create column family with custom config
TEST_F(TidesDBTest, CreateColumnFamilyWithConfig)
{
    tidesdb::DB db(db_path);

    tidesdb::ColumnFamilyConfig config;
    config.memtable_flush_size = 128 * 1024 * 1024;  // 128MB
    config.max_sstables_before_compaction = 512;
    config.compaction_threads = 4;
    config.compressed = true;
    config.compress_algo = tidesdb::CompressionAlgo::LZ4;
    config.bloom_filter_fp_rate = 0.01;
    config.enable_background_compaction = true;
    config.sync_mode = tidesdb::SyncMode::BACKGROUND;
    config.sync_interval = 1000;

    db.create_column_family("custom_cf", config);

    // Verify configuration
    auto stats = db.get_column_family_stats("custom_cf");
    EXPECT_EQ(stats.config.memtable_flush_size, 128 * 1024 * 1024);
    EXPECT_EQ(stats.config.compress_algo, tidesdb::CompressionAlgo::LZ4);
    EXPECT_TRUE(stats.config.compressed);

    db.drop_column_family("custom_cf");
}

// List column families
TEST_F(TidesDBTest, ListColumnFamilies)
{
    tidesdb::DB db(db_path);

    // Create multiple column families
    std::vector<std::string> cf_names = {"cf1", "cf2", "cf3"};
    for (const auto& name : cf_names)
    {
        db.create_column_family(name);
    }

    // List them
    auto cf_list = db.list_column_families();
    EXPECT_EQ(cf_list.size(), cf_names.size());

    for (const auto& name : cf_names)
    {
        EXPECT_NE(std::find(cf_list.begin(), cf_list.end(), name), cf_list.end());
    }

    // Clean up
    for (const auto& name : cf_names)
    {
        db.drop_column_family(name);
    }
}

// Transaction put, get, delete
TEST_F(TidesDBTest, TransactionPutGetDelete)
{
    tidesdb::DB db(db_path);
    db.create_column_family("test_cf");

    // Put data
    {
        auto txn = db.begin_transaction();
        txn->put("test_cf", "key1", "value1");
        txn->put("test_cf", "key2", "value2");
        txn->commit();
    }

    // Get data
    {
        auto txn = db.begin_read_transaction();
        auto value1 = txn->get("test_cf", "key1");
        EXPECT_EQ(value1, "value1");

        auto value2 = txn->get("test_cf", "key2");
        EXPECT_EQ(value2, "value2");
    }

    // Delete data
    {
        auto txn = db.begin_transaction();
        txn->remove("test_cf", "key1");
        txn->commit();
    }

    // Verify deletion
    {
        auto txn = db.begin_read_transaction();
        EXPECT_THROW({ txn->get("test_cf", "key1"); }, tidesdb::Exception);
    }

    db.drop_column_family("test_cf");
}

// Transaction with TTL
TEST_F(TidesDBTest, TransactionWithTTL)
{
    tidesdb::DB db(db_path);
    db.create_column_family("test_cf");

    // Put with TTL (2 seconds from now)
    auto ttl = std::time(nullptr) + 2;
    {
        auto txn = db.begin_transaction();
        txn->put("test_cf", "temp_key", "temp_value", ttl);
        txn->commit();
    }

    // Verify it exists
    {
        auto txn = db.begin_read_transaction();
        auto value = txn->get("test_cf", "temp_key");
        EXPECT_EQ(value, "temp_value");
    }

    // Wait for expiration
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // Verify it's expired
    {
        auto txn = db.begin_read_transaction();
        EXPECT_THROW({ txn->get("test_cf", "temp_key"); }, tidesdb::Exception);
    }

    db.drop_column_family("test_cf");
}

// Multi-operation transaction
TEST_F(TidesDBTest, MultiOperationTransaction)
{
    tidesdb::DB db(db_path);
    db.create_column_family("test_cf");

    // Multiple operations
    {
        auto txn = db.begin_transaction();
        for (int i = 1; i <= 10; ++i)
        {
            std::string key = "key" + std::to_string(i);
            std::string value = "value" + std::to_string(i);
            txn->put("test_cf", key, value);
        }
        txn->commit();
    }

    // Verify all were written
    {
        auto txn = db.begin_read_transaction();
        for (int i = 1; i <= 10; ++i)
        {
            std::string key = "key" + std::to_string(i);
            std::string expected = "value" + std::to_string(i);
            auto value = txn->get("test_cf", key);
            EXPECT_EQ(value, expected);
        }
    }

    db.drop_column_family("test_cf");
}

// Transaction rollback
TEST_F(TidesDBTest, TransactionRollback)
{
    tidesdb::DB db(db_path);
    db.create_column_family("test_cf");

    // Put and rollback
    {
        auto txn = db.begin_transaction();
        txn->put("test_cf", "rollback_key", "rollback_value");
        txn->rollback();
    }

    // Verify data wasn't written
    {
        auto txn = db.begin_read_transaction();
        EXPECT_THROW({ txn->get("test_cf", "rollback_key"); }, tidesdb::Exception);
    }

    db.drop_column_family("test_cf");
}

// Forward iteration
TEST_F(TidesDBTest, ForwardIteration)
{
    tidesdb::DB db(db_path);
    db.create_column_family("test_cf");

    // Insert test data
    std::map<std::string, std::string> test_data = {{"key1", "value1"},
                                                    {"key2", "value2"},
                                                    {"key3", "value3"},
                                                    {"key4", "value4"},
                                                    {"key5", "value5"}};

    {
        auto txn = db.begin_transaction();
        for (const auto& [key, value] : test_data)
        {
            txn->put("test_cf", key, value);
        }
        txn->commit();
    }

    // Iterate forward
    {
        auto txn = db.begin_read_transaction();
        auto iter = txn->new_iterator("test_cf");
        iter->seek_to_first();

        int count = 0;
        while (iter->valid())
        {
            auto key = iter->key_string();
            auto value = iter->value_string();

            EXPECT_NE(test_data.find(key), test_data.end());
            EXPECT_EQ(test_data[key], value);

            count++;
            iter->next();
        }

        EXPECT_EQ(count, static_cast<int>(test_data.size()));
    }

    db.drop_column_family("test_cf");
}

// Backward iteration
TEST_F(TidesDBTest, BackwardIteration)
{
    tidesdb::DB db(db_path);
    db.create_column_family("test_cf");

    // Insert test data
    std::map<std::string, std::string> test_data = {
        {"key1", "value1"}, {"key2", "value2"}, {"key3", "value3"}};

    {
        auto txn = db.begin_transaction();
        for (const auto& [key, value] : test_data)
        {
            txn->put("test_cf", key, value);
        }
        txn->commit();
    }

    // Iterate backward
    {
        auto txn = db.begin_read_transaction();
        auto iter = txn->new_iterator("test_cf");
        iter->seek_to_last();

        int count = 0;
        while (iter->valid())
        {
            auto key = iter->key_string();
            auto value = iter->value_string();

            EXPECT_NE(test_data.find(key), test_data.end());
            EXPECT_EQ(test_data[key], value);

            count++;
            iter->prev();
        }

        EXPECT_EQ(count, static_cast<int>(test_data.size()));
    }

    db.drop_column_family("test_cf");
}

// Get column family stats
TEST_F(TidesDBTest, GetColumnFamilyStats)
{
    tidesdb::DB db(db_path);

    tidesdb::ColumnFamilyConfig config;
    config.memtable_flush_size = 2 * 1024 * 1024;  // 2MB
    config.max_level = 12;
    config.compressed = true;
    config.compress_algo = tidesdb::CompressionAlgo::SNAPPY;
    config.bloom_filter_fp_rate = 0.01;

    db.create_column_family("test_cf", config);

    // Add some data
    {
        auto txn = db.begin_transaction();
        for (int i = 0; i < 10; ++i)
        {
            std::string key = "key" + std::to_string(i);
            std::string value = "value" + std::to_string(i);
            txn->put("test_cf", key, value);
        }
        txn->commit();
    }

    // Get statistics
    auto stats = db.get_column_family_stats("test_cf");

    EXPECT_EQ(stats.name, "test_cf");
    EXPECT_EQ(stats.config.memtable_flush_size, 2 * 1024 * 1024);
    EXPECT_EQ(stats.config.max_level, 12);
    EXPECT_TRUE(stats.config.compressed);
    EXPECT_EQ(stats.config.compress_algo, tidesdb::CompressionAlgo::SNAPPY);
    EXPECT_GT(stats.memtable_entries, 0);

    db.drop_column_family("test_cf");
}

// Compaction
TEST_F(TidesDBTest, Compaction)
{
    tidesdb::DB db(db_path);

    // Create CF with small flush threshold
    tidesdb::ColumnFamilyConfig config;
    config.memtable_flush_size = 1024;  // 1KB
    config.enable_background_compaction = false;
    config.compaction_threads = 2;

    db.create_column_family("test_cf", config);

    // Add data to create SSTables
    for (int batch = 0; batch < 5; ++batch)
    {
        auto txn = db.begin_transaction();
        for (int i = 0; i < 20; ++i)
        {
            std::string key = "key" + std::to_string(batch) + "_" + std::to_string(i);
            std::string value(512, 'x');  // 512 bytes
            txn->put("test_cf", key, value);
        }
        txn->commit();
    }

    // Get column family
    auto cf = db.get_column_family("test_cf");

    // Get stats before compaction
    auto stats_before = db.get_column_family_stats("test_cf");

    // Perform compaction if we have enough SSTables
    if (stats_before.num_sstables >= 2)
    {
        cf.compact();

        auto stats_after = db.get_column_family_stats("test_cf");
        // Stats should be available after compaction
        EXPECT_EQ(stats_after.name, "test_cf");
    }

    db.drop_column_family("test_cf");
}

// Sync modes
TEST_F(TidesDBTest, SyncModes)
{
    tidesdb::DB db(db_path);

    struct SyncModeTest
    {
        tidesdb::SyncMode mode;
        std::string name;
    };

    std::vector<SyncModeTest> sync_modes = {{tidesdb::SyncMode::NONE, "none"},
                                            {tidesdb::SyncMode::BACKGROUND, "background"},
                                            {tidesdb::SyncMode::FULL, "full"}};

    for (const auto& sm : sync_modes)
    {
        std::string cf_name = "cf_" + sm.name;

        tidesdb::ColumnFamilyConfig config;
        config.sync_mode = sm.mode;
        config.sync_interval = (sm.mode == tidesdb::SyncMode::BACKGROUND) ? 1000 : 0;

        db.create_column_family(cf_name, config);

        // Verify sync mode
        auto stats = db.get_column_family_stats(cf_name);
        EXPECT_EQ(stats.config.sync_mode, sm.mode);

        db.drop_column_family(cf_name);
    }
}

// Compression algorithms
TEST_F(TidesDBTest, CompressionAlgorithms)
{
    tidesdb::DB db(db_path);

    struct AlgoTest
    {
        tidesdb::CompressionAlgo algo;
        std::string name;
        bool compressed;
    };

    std::vector<AlgoTest> algorithms = {{tidesdb::CompressionAlgo::SNAPPY, "snappy", true},
                                        {tidesdb::CompressionAlgo::LZ4, "lz4", true},
                                        {tidesdb::CompressionAlgo::ZSTD, "zstd", true}};

    // Test with compression enabled
    for (const auto& alg : algorithms)
    {
        std::string cf_name = "cf_" + alg.name;

        tidesdb::ColumnFamilyConfig config;
        config.compressed = alg.compressed;
        config.compress_algo = alg.algo;

        db.create_column_family(cf_name, config);

        // Verify compression
        auto stats = db.get_column_family_stats(cf_name);
        EXPECT_TRUE(stats.config.compressed);
        EXPECT_EQ(stats.config.compress_algo, alg.algo);

        db.drop_column_family(cf_name);
    }

    // Test with compression disabled
    {
        tidesdb::ColumnFamilyConfig config;
        config.compressed = false;
        db.create_column_family("cf_no_compression", config);

        auto stats = db.get_column_family_stats("cf_no_compression");
        EXPECT_FALSE(stats.config.compressed);

        db.drop_column_family("cf_no_compression");
    }
}

// Error handling
TEST_F(TidesDBTest, ErrorHandling)
{
    tidesdb::DB db(db_path);

    // Try to get stats for non-existent CF
    EXPECT_THROW({ db.get_column_family_stats("nonexistent_cf"); }, tidesdb::Exception);

    // Try to drop non-existent CF
    EXPECT_THROW({ db.drop_column_family("nonexistent_cf"); }, tidesdb::Exception);
}

// Binary data
TEST_F(TidesDBTest, BinaryData)
{
    tidesdb::DB db(db_path);
    db.create_column_family("test_cf");

    // Store binary data
    std::vector<uint8_t> binary_key = {0x00, 0x01, 0x02, 0xFF};
    std::vector<uint8_t> binary_value = {0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF};

    {
        auto txn = db.begin_transaction();
        txn->put("test_cf", binary_key, binary_value);
        txn->commit();
    }

    // Retrieve binary data
    {
        auto txn = db.begin_read_transaction();
        auto value = txn->get("test_cf", binary_key);
        EXPECT_EQ(value, binary_value);
    }

    db.drop_column_family("test_cf");
}

// Move semantics
TEST_F(TidesDBTest, MoveSemantics)
{
    // Test DB move
    tidesdb::DB db1(db_path);
    db1.create_column_family("test_cf");

    tidesdb::DB db2 = std::move(db1);

    // db2 should work, db1 should be moved-from
    auto cf_list = db2.list_column_families();
    EXPECT_FALSE(cf_list.empty());

    db2.drop_column_family("test_cf");
}

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
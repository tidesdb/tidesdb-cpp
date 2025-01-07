/*
 *
 * Copyright (C) TidesDB
 *
 * Original Author: Evgeny Kornev
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

#include "../tidesdb.hpp"

std::string column_name = "my_db";
const int flush_threshold = (1024 * 1024) * 128;
const tidesdb_compression_algo_t compression_algo = TDB_COMPRESS_SNAPPY;
const float probability = TDB_USING_HT_PROBABILITY;
const int max_level = TDB_USING_HT_MAX_LEVEL;
const bool bloom_filter = true;
const bool compressed = true;
const tidesdb_memtable_ds_t memtable_ds = TDB_MEMTABLE_HASH_TABLE;

TEST(TidesDB, Open_and_Close)
{
    TidesDB::DB db;
    EXPECT_EQ(db.Open("tmp"), 0);
    EXPECT_EQ(db.Close(), 0);
}
TEST(TidesDB, Create_and_Drop_Column_Family)
{
    TidesDB::DB db;
    EXPECT_EQ(db.Open("tmp"), 0);
    EXPECT_EQ(db.CreateColumnFamily(column_name, flush_threshold, max_level, probability,
                                    bloom_filter, compression_algo, compressed, memtable_ds),
              0);
    EXPECT_EQ(db.DropColumnFamily(column_name), 0);
    EXPECT_EQ(db.Close(), 0);
}

TEST(TidesDB, Create_and_Column_Family_and_Put)
{
    TidesDB::DB db;
    EXPECT_EQ(db.Open("tmp"), 0);
    EXPECT_EQ(db.CreateColumnFamily(column_name, flush_threshold, max_level, probability,
                                    bloom_filter, compression_algo, compressed, memtable_ds),
              0);
    const std::vector<uint8_t> key = {'k', 'e', 'y'};
    const std::vector<uint8_t> value = {'v', 'a', 'l', 'u', 'e'};
    EXPECT_EQ(db.Put(column_name, &key, &value, std::chrono::seconds(-1)), 0);
    EXPECT_EQ(db.DropColumnFamily(column_name), 0);
    EXPECT_EQ(db.Close(), 0);
}

TEST(TidesDB, Put_and_Get)
{
    TidesDB::DB db;
    std::vector<uint8_t> got_value;
    EXPECT_EQ(db.Open("tmp"), 0);
    EXPECT_EQ(db.CreateColumnFamily(column_name, flush_threshold, max_level, probability,
                                    bloom_filter, compression_algo, compressed, memtable_ds),
              0);

    const std::vector<uint8_t> key = {'k', 'e', 'y'};
    const std::vector<uint8_t> value = {'v', 'a', 'l', 'u', 'e'};
    EXPECT_EQ(db.Put(column_name, &key, &value, std::chrono::seconds(-1)), 0);
    EXPECT_EQ(db.Get(column_name, &key, &got_value), 0);
    EXPECT_EQ(std::string(got_value.begin(), got_value.end()),
              std::string(value.begin(), value.end()));
    EXPECT_EQ(db.DropColumnFamily(column_name), 0);
    EXPECT_EQ(db.Close(), 0);
}

TEST(TidesDB, Put_and_Delete)
{
    TidesDB::DB db;
    std::vector<uint8_t> got_value;
    EXPECT_EQ(db.Open("tmp"), 0);
    EXPECT_EQ(db.CreateColumnFamily(column_name, flush_threshold, max_level, probability,
                                    bloom_filter, compression_algo, compressed, memtable_ds),
              0);
    const std::vector<uint8_t> key = {'k', 'e', 'y'};
    const std::vector<uint8_t> value = {'v', 'a', 'l', 'u', 'e'};
    EXPECT_EQ(db.Put(column_name, &key, &value, std::chrono::seconds(-1)), 0);
    EXPECT_EQ(db.Delete(column_name, &key), 0);
    EXPECT_NE(db.Get(column_name, &key, &got_value), 0);
    EXPECT_EQ(db.DropColumnFamily(column_name), 0);
    EXPECT_EQ(db.Close(), 0);
}

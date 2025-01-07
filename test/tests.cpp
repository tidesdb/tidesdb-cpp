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

const char* column_name = "my_db";
const char* key = "key";
const char* value = "value";
const int flush_threshold = (1024 * 1024) * 128;
const tidesdb_compression_algo_t compression_algo = TDB_COMPRESS_SNAPPY;
const float probability = TDB_USING_HT_PROBABILITY;
const int max_level = TDB_USING_HT_MAX_LEVEL;
const bool bloom_filter = true;
const bool compressed = true;
const tidesdb_memtable_ds_t memtable_ds = TDB_MEMTABLE_HASH_TABLE;

TEST(TidesDB, Open_and_Close)
{
    TidesDB db;
    EXPECT_EQ(db.open("tmp"), 0);
    EXPECT_EQ(db.close(), 0);
}
TEST(TidesDB, Create_and_Drop_Column_Family)
{
    TidesDB db;
    EXPECT_EQ(db.open("tmp"), 0);
    EXPECT_EQ(db.create_column_family(column_name, flush_threshold, max_level, probability,
                                      bloom_filter, compression_algo, compressed, memtable_ds),
              0);
    EXPECT_EQ(db.drop_column_family(column_name), 0);
    EXPECT_EQ(db.close(), 0);
}

TEST(TidesDB, Create_and_Column_Family_and_Put)
{
    TidesDB db;
    EXPECT_EQ(db.open("tmp"), 0);
    EXPECT_EQ(db.create_column_family(column_name, flush_threshold, max_level, probability,
                                      bloom_filter, compression_algo, compressed, memtable_ds),
              0);
    EXPECT_EQ(db.put(column_name, (const uint8_t*)key, (const uint8_t*)value, -1), 0);
    EXPECT_EQ(db.drop_column_family(column_name), 0);
    EXPECT_EQ(db.close(), 0);
}

TEST(TidesDB, Put_and_Get)
{
    TidesDB db;
    uint8_t* got_value = nullptr;
    size_t got_value_size = 0;
    EXPECT_EQ(db.open("tmp"), 0);
    EXPECT_EQ(db.create_column_family(column_name, flush_threshold, max_level, probability,
                                      bloom_filter, compression_algo, compressed, memtable_ds),
              0);
    EXPECT_EQ(db.put(column_name, (const uint8_t*)key, (const uint8_t*)value, -1), 0);
    EXPECT_EQ(db.get(column_name, (const uint8_t*)key, &got_value, got_value_size), 0);
    EXPECT_STREQ(value, (const char*)got_value);
    EXPECT_EQ(db.drop_column_family(column_name), 0);
    EXPECT_EQ(db.close(), 0);
}

TEST(TidesDB, Put_and_Delete)
{
    TidesDB db;
    uint8_t* got_value = nullptr;
    size_t got_value_size = 0;
    EXPECT_EQ(db.open("tmp"), 0);
    EXPECT_EQ(db.create_column_family(column_name, flush_threshold, max_level, probability,
                                      bloom_filter, compression_algo, compressed, memtable_ds),
              0);
    EXPECT_EQ(db.put(column_name, (const uint8_t*)key, (const uint8_t*)value, -1), 0);
    EXPECT_EQ(db.delete_key(column_name, (const uint8_t*)key), 0);
    EXPECT_NE(db.get(column_name, (const uint8_t*)key, &got_value, got_value_size), 0);
    EXPECT_EQ(db.drop_column_family(column_name), 0);
    EXPECT_EQ(db.close(), 0);
}

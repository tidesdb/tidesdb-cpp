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

#pragma once
#include <tidesdb/tidesdb.h>

#include <chrono>
#include <iostream>
#include <utility>
#include <vector>

/*
 * TidesDB Namespace
 * contains database classes
 */
namespace TidesDB
{

/*
 * SSTableStat Struct
 * represents statistics about an SSTable.
 */
struct SSTableStat
{
    std::string path;
    size_t size;
    size_t num_blocks;
};

/*
 * ColumnFamilyConfig Struct
 * represents configuration for a column family.
 */
struct ColumnFamilyConfig
{
    std::string name;
    int32_t flush_threshold;
    int32_t max_level;
    float probability;
    bool compressed;
    tidesdb_compression_algo_t compress_algo;
    bool bloom_filter;
};

/*
 * ColumnFamilyStat Class
 * represents statistics about a column family.
 */
class ColumnFamilyStat
{
   public:
    std::string name;
    int num_sstables;
    size_t memtable_size;
    size_t memtable_entries_count;
    bool incremental_merging;
    ColumnFamilyConfig config;
    std::vector<SSTableStat> sstable_stats;
};

/*
 * DB Class
 * represents TidesDB database.
 */
class DB
{
    tidesdb_t *tdb;

   public:
    /*
     * Open
     * Opens an existing database or creates a new one.
     */
    int Open(const std::string &dir_name);

    /*
     * Close
     * Closes the database.
     */
    [[nodiscard]] int Close() const;

    /*
     * CreateColumnFamily
     * Creates a new column family.
     */
    [[nodiscard]] int CreateColumnFamily(const std::string &name, int flush_threshold,
                                         int max_level, float probability, bool compressed,
                                         tidesdb_compression_algo_t compress_algo,
                                         bool bloom_filter) const;

    /*
     * DropColumnFamily
     * Drops an existing column family.
     */
    [[nodiscard]] int DropColumnFamily(const std::string &name) const;

    /*
     * Put
     * Puts a key-value pair into a column family.
     */
    int Put(const std::string &column_family_name, const std::vector<uint8_t> *key,
            const std::vector<uint8_t> *value, std::chrono::seconds ttl) const;
    /*
     * Get
     * Gets a value by key from a column family.
     */
    int Get(const std::string &column_family_name, const std::vector<uint8_t> *key,
            std::vector<uint8_t> *value) const;

    /*
     * Range
     * Gets a range of key-value pairs from a column family.
     */
    int Range(const std::string &column_family_name, const std::vector<uint8_t> *start_key,
              const std::vector<uint8_t> *end_key,
              std::vector<std::pair<std::vector<uint8_t>, std::vector<uint8_t>>> *result) const;

    /*
     * ListColumnFamilies
     * Lists the column families in the database.
     */
    int ListColumnFamilies(std::vector<std::string> *families) const;

    /*
     * DeleteByRange
     * Deletes a range of key-value pairs from a column family.
     */
    int DeleteByRange(const std::string &column_family_name, const std::vector<uint8_t> *start_key,
                      const std::vector<uint8_t> *end_key) const;

    /*
     * GetColumnFamilyStat
     * Gets statistics about a column family.
     */
    int GetColumnFamilyStat(const std::string &column_family_name, ColumnFamilyStat *stat) const;

    /*
     * Delete
     * Deletes a key-value pair from a column family.
     */
    int Delete(const std::string &column_family_name, const std::vector<uint8_t> *key) const;

    /*
     * CompactSSTables
     * compacts column family sstables by pairing and merging.
     */
    [[nodiscard]] int CompactSSTables(const std::string &column_family_name, int max_threads) const;

    /*
     * StartIncrementalMerges
     * starts background incremental merges for a column family.
     */
    [[nodiscard]] int StartIncrementalMerges(const std::string &column_family_name,
                                             std::chrono::seconds seconds, int min_sstables) const;

    [[nodiscard]] tidesdb_t *GetTidesDB() const;

    /* constructor */
    DB();
};

/*
 * Txn Class
 * represents TidesDB column family transaction.
 */
class Txn
{
    tidesdb_txn_t *txn;
    tidesdb_t *tdb;

   public:
    /*
     * Txn
     * creates a new transaction for a database.
     */
    explicit Txn(const DB *db);
    ~Txn();

    /*
     * Begin
     * begins a transaction.
     */
    [[nodiscard]] int Begin();

    /*
     * Put
     * puts a key-value pair into a column family.
     */
    int Put(const std::vector<uint8_t> *key, const std::vector<uint8_t> *value,
            std::chrono::seconds ttl) const;

    /*
     * Get
     * gets a value by key from a column family.
     */
    int Get(const std::vector<uint8_t> *key, std::vector<uint8_t> *value) const;

    /*
     * Delete
     * deletes a key-value pair from a column family.
     */
    [[nodiscard]] int Delete(const std::vector<uint8_t> *key) const;

    /*
     * Commit
     * commits the transaction.
     */
    [[nodiscard]] int Commit() const;

    /*
     * Rollback
     * rolls back the transaction.
     */
    [[nodiscard]] int Rollback() const;
};

/*
 * Cursor Class
 * represents TidesDB column family cursor.
 */
class Cursor
{
    tidesdb_cursor_t *cursor;
    tidesdb_t *tdb;
    std::string column_family_name;

   public:
    /*
     * Cursor
     * creates a new cursor for a column family.
     */
    Cursor(const DB *db, std::string column_family_name);
    ~Cursor();

    /*
     * Init
     * initializes the cursor.
     */
    [[nodiscard]] int Init();

    /*
     * Next
     * goes to the next key-value pair in the column family.
     */
    [[nodiscard]] int Next() const;

    /*
     * Prev
     * goes to the previous key-value pair in the column family.
     */
    [[nodiscard]] int Prev() const;

    /*
     * Get
     * gets the current key-value pair in the column family cursor.
     */
    [[nodiscard]] int Get(std::vector<uint8_t> &key, std::vector<uint8_t> &value) const;
};

};  /* namespace TidesDB */

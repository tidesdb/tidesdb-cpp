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
#include "tidesdb.hpp"
#include <iostream>
#include <utility>

#define ERR_HANDLER()                           \
    if (err)                                    \
    {                                           \
        std::cout << err->message << std::endl; \
        int err_code = err->code;               \
        tidesdb_err_free(err);                  \
        return err_code;                        \
    }                                           \
    return 0;

namespace TidesDB
{

DB::DB(void)
{
    tdb = nullptr;
}

int DB::Open(const std::string &dir_name)
{
    tidesdb_err_t *err = tidesdb_open(dir_name.c_str(), &this->tdb);
    ERR_HANDLER()
}

int DB::Close() const
{
    tidesdb_err_t *err = tidesdb_close(this->tdb);
    ERR_HANDLER()
}

int DB::CreateColumnFamily(const std::string &column_family_name, int flush_threshold,
                           int max_level, float probability, bool compressed,
                           tidesdb_compression_algo_t compress_algo, bool bloom_filter,
                           tidesdb_memtable_ds_t memtable_ds) const
{
    tidesdb_err_t *err = tidesdb_create_column_family(
        this->tdb, column_family_name.c_str(), flush_threshold, max_level, probability, compressed,
        compress_algo, bloom_filter, memtable_ds);
    ERR_HANDLER()
}

int DB::DropColumnFamily(const std::string &column_family_name) const
{
    tidesdb_err_t *err = tidesdb_drop_column_family(this->tdb, column_family_name.c_str());
    ERR_HANDLER()
}

int DB::Put(const std::string &column_family_name, const std::vector<uint8_t> *key,
            const std::vector<uint8_t> *value, std::chrono::seconds ttl) const
{
    size_t key_size = key->size();
    size_t value_size = value->size();
    time_t ttl_time = ttl.count();
    tidesdb_err_t *err = tidesdb_put(this->tdb, column_family_name.c_str(), key->data(), key_size,
                                     value->data(), value_size, ttl_time);
    ERR_HANDLER()
}

int DB::Get(const std::string &column_family_name, const std::vector<uint8_t> *key,
            std::vector<uint8_t> *value) const
{
    size_t key_size = key->size();
    size_t value_size = value->size();
    uint8_t *value_data = value->data();
    tidesdb_err_t *err = tidesdb_get(this->tdb, column_family_name.c_str(), key->data(), key_size,
                                     &value_data, &value_size);
    if (err == nullptr)
    {
        value->resize(value_size);
    }
    ERR_HANDLER()
}

int DB::Delete(const std::string &column_family_name, const std::vector<uint8_t> *key) const
{
    size_t key_size = key->size();
    tidesdb_err_t *err =
        tidesdb_delete(this->tdb, column_family_name.c_str(), key->data(), key_size);
    ERR_HANDLER()
}

int DB::CompactSSTables(const std::string &column_family_name, int max_threads) const
{
    tidesdb_err_t *err =
        tidesdb_compact_sstables(this->tdb, column_family_name.c_str(), max_threads);
    ERR_HANDLER()
}

int DB::StartBackgroundPartialMerges(const std::string &column_family_name,
                                     std::chrono::seconds seconds, int min_sstables) const
{
    auto duration = seconds.count();
    if (duration > std::numeric_limits<int>::max() || duration < std::numeric_limits<int>::min())
    {
        return -1;
    }

    tidesdb_err_t *err = tidesdb_start_background_partial_merge(
        this->tdb, column_family_name.c_str(), static_cast<int>(duration), min_sstables);
    ERR_HANDLER()
}

Txn::Txn(DB *db)
{
    this->tdb = db->GetTidesDB();
    this->txn = nullptr;
}

Txn::~Txn()
{
    if (this->txn)
    {
        tidesdb_txn_free(this->txn);
    }
}

int Txn::Begin()
{
    tidesdb_err_t *err = tidesdb_txn_begin(this->tdb, &this->txn, nullptr);
    ERR_HANDLER()
}

int Txn::Put(const std::vector<uint8_t> *key, const std::vector<uint8_t> *value,
             std::chrono::seconds ttl) const
{
    time_t ttl_time = ttl.count();
    tidesdb_err_t *err = tidesdb_txn_put(this->txn, key->data(), size_t(key->size), value->data(),
                                         size_t(value->size), ttl_time);
    ERR_HANDLER()
}

int Txn::Delete(const std::vector<uint8_t> *key) const
{
    tidesdb_err_t *err = tidesdb_txn_delete(this->txn, key->data(), size_t(key->size));
    ERR_HANDLER()
}

int Txn::Commit() const
{
    tidesdb_err_t *err = tidesdb_txn_commit(this->txn);
    ERR_HANDLER()
}

int Txn::Rollback() const
{
    tidesdb_err_t *err = tidesdb_txn_rollback(this->txn);
    ERR_HANDLER()
}

Cursor::Cursor(DB *db, std::string column_family_name)
{
    this->tdb = db->GetTidesDB();
    this->cursor = nullptr;
    this->column_family_name = std::move(column_family_name);
}

int Cursor::Init()
{
    tidesdb_err_t *err = tidesdb_cursor_init(tdb, this->column_family_name.c_str(), &this->cursor);
    ERR_HANDLER()
}

Cursor::~Cursor()
{
    // free the cursor
    if (this->cursor)
    {
        tidesdb_cursor_free(this->cursor);
    }
}

int Cursor::Next() const
{
    tidesdb_err_t *err = tidesdb_cursor_next(this->cursor);
    ERR_HANDLER()
}

int Cursor::Prev() const
{
    tidesdb_err_t *err = tidesdb_cursor_prev(this->cursor);
    ERR_HANDLER()
}

int Cursor::Get(std::vector<uint8_t> &key, std::vector<uint8_t> &value)
{
    size_t key_size = key.size();
    size_t value_size = value.size();
    uint8_t *key_data = key.data();
    uint8_t *value_data = value.data();
    tidesdb_err_t *err =
        tidesdb_cursor_get(this->cursor, &key_data, &key_size, &value_data, &value_size);
    ERR_HANDLER()
}

}  // namespace TidesDB
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

namespace TidesDB
{

DB::DB(void)
{
    tdb = nullptr;
}

static void inline err_handler(tidesdb_err_t *err)
{
    if (err)
    {
        std::string error_message = err->message;
        const int error_code = err->code;
        tidesdb_err_free(err);
        throw std::runtime_error("Error " + std::to_string(error_code) + ": " + error_message);
    }
}

int DB::Open(const std::string &dir_name)
{
    tidesdb_err_t *err = tidesdb_open(dir_name.c_str(), &this->tdb);
    err_handler(err);
    return 0;
}

int DB::Close() const
{
    tidesdb_err_t *err = tidesdb_close(this->tdb);
    err_handler(err);
    return 0;
}

int DB::CreateColumnFamily(const std::string &column_family_name, int flush_threshold,
                           int max_level, float probability, bool compressed,
                           tidesdb_compression_algo_t compress_algo, bool bloom_filter,
                           tidesdb_memtable_ds_t memtable_ds) const
{
    tidesdb_err_t *err = tidesdb_create_column_family(
        this->tdb, column_family_name.c_str(), flush_threshold, max_level, probability, compressed,
        compress_algo, bloom_filter, memtable_ds);
    err_handler(err);
    return 0;
}

int DB::DropColumnFamily(const std::string &column_family_name) const
{
    tidesdb_err_t *err = tidesdb_drop_column_family(this->tdb, column_family_name.c_str());
    err_handler(err);
    return 0;
}

int DB::Put(const std::string &column_family_name, const std::vector<uint8_t> *key,
            const std::vector<uint8_t> *value, std::chrono::seconds ttl) const
{
    size_t key_size = key->size();
    size_t value_size = value->size();
    time_t ttl_time = ttl.count();
    tidesdb_err_t *err = tidesdb_put(this->tdb, column_family_name.c_str(), key->data(), key_size,
                                     value->data(), value_size, ttl_time);
    err_handler(err);
    return 0;
}

int DB::Get(const std::string &column_family_name, const std::vector<uint8_t> *key,
            std::vector<uint8_t> *value) const
{
    size_t key_size = key->size();
    unsigned char *value_data = nullptr;
    unsigned long value_size = 0;
    tidesdb_err_t *err = tidesdb_get(this->tdb, column_family_name.c_str(), key->data(), key_size,
                                     &value_data, &value_size);
    if (err == nullptr)
    {
        value->assign(value_data, value_data + value_size);
        free(value_data);
    }
    err_handler(err);
    return 0;
}

int DB::Delete(const std::string &column_family_name, const std::vector<uint8_t> *key) const
{
    size_t key_size = key->size();
    tidesdb_err_t *err =
        tidesdb_delete(this->tdb, column_family_name.c_str(), key->data(), key_size);
    err_handler(err);
    return 0;
}

int DB::CompactSSTables(const std::string &column_family_name, int max_threads) const
{
    tidesdb_err_t *err =
        tidesdb_compact_sstables(this->tdb, column_family_name.c_str(), max_threads);
    err_handler(err);
    return 0;
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
    err_handler(err);
    return 0;
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
    err_handler(err);
    return 0;
}

int Txn::Put(const std::vector<uint8_t> *key, const std::vector<uint8_t> *value,
             std::chrono::seconds ttl) const
{
    auto ttl_time = std::chrono::duration_cast<std::chrono::seconds>(ttl).count();
    tidesdb_err_t *err = tidesdb_txn_put(this->txn, key->data(), size_t(key->size()), value->data(),
                                         size_t(value->size()), ttl_time);
    err_handler(err);
    return 0;
}

int Txn::Delete(const std::vector<uint8_t> *key) const
{
    tidesdb_err_t *err = tidesdb_txn_delete(this->txn, key->data(), size_t(key->size()));
    err_handler(err);
    return 0;
}

int Txn::Commit() const
{
    tidesdb_err_t *err = tidesdb_txn_commit(this->txn);
    err_handler(err);
    return 0;
}

int Txn::Rollback() const
{
    tidesdb_err_t *err = tidesdb_txn_rollback(this->txn);
    if (err)
    {
        std::string error_message = err->message;
        int error_code = err->code;
        tidesdb_err_free(err);
        throw std::runtime_error("Error " + std::to_string(error_code) + ": " + error_message);
    }
    return 0;
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
    err_handler(err);
    return 0;
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
    err_handler(err);
    return 0;
}

int Cursor::Prev() const
{
    tidesdb_err_t *err = tidesdb_cursor_prev(this->cursor);
    err_handler(err);
    return 0;
}

int Cursor::Get(std::vector<uint8_t> &key, std::vector<uint8_t> &value)
{
    size_t key_size = key.size();
    size_t value_size = value.size();
    uint8_t *key_data = key.data();
    uint8_t *value_data = value.data();
    tidesdb_err_t *err =
        tidesdb_cursor_get(this->cursor, &key_data, &key_size, &value_data, &value_size);
    err_handler(err);
    return 0;
}

tidesdb_t *DB::GetTidesDB() const
{
    return this->tdb;
}

}  // namespace TidesDB
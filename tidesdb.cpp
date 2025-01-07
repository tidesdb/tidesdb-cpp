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

#include <tidesdb.h>

#include <iostream>

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

int DB::Open(const char *dir_name)
{
    tidesdb_err_t *err = tidesdb_open(dir_name, &this->tdb);
    ERR_HANDLER()
}

int DB::Close() const
{
    tidesdb_err_t *err = tidesdb_close(this->tdb);
    ERR_HANDLER()
}
int DB::CreateColumnFamily(const char *column_family_name, int flush_threshold, int max_level,
                           float probability, bool compressed,
                           tidesdb_compression_algo_t compress_algo, bool bloom_filter,
                           tidesdb_memtable_ds_t memtable_ds) const
{
    tidesdb_err_t *err = tidesdb_create_column_family(
        this->tdb, column_family_name, flush_threshold, max_level, probability, compressed,
        compress_algo, bloom_filter, memtable_ds);
    ERR_HANDLER()
}

int DB::DropColumnFamily(const char *column_family_name) const
{
    tidesdb_err_t *err = tidesdb_drop_column_family(this->tdb, column_family_name);
    ERR_HANDLER()
}

int DB::Put(const char *column_family_name, const uint8_t *key, const uint8_t *value,
            time_t ttl) const
{
    size_t key_size = strlen(reinterpret_cast<const char *>(key));
    size_t value_size = strlen(reinterpret_cast<const char *>(value));
    tidesdb_err_t *err =
        tidesdb_put(this->tdb, column_family_name, key, key_size, value, value_size, ttl);
    ERR_HANDLER()
}

int DB::Get(const char *column_family_name, const uint8_t *key, uint8_t **value,
            size_t &value_size) const
{
    size_t key_size = strlen(reinterpret_cast<const char *>(key));
    tidesdb_err_t *err =
        tidesdb_get(this->tdb, column_family_name, key, key_size, value, &value_size);
    if (err == nullptr)
    {
        uint8_t *buf = *value;
        buf[value_size] = 0;
    }
    ERR_HANDLER()
}

int DB::Delete(const char *column_family_name, const uint8_t *key) const
{
    size_t key_size = strlen(reinterpret_cast<const char *>(key));
    tidesdb_err_t *err = tidesdb_delete(this->tdb, column_family_name, key, key_size);
    ERR_HANDLER()
}

int DB::CompactSSTables(const char *column_family_name, int max_threads) const
{
    tidesdb_err_t *err = tidesdb_compact_sstables(this->tdb, column_family_name, max_threads);
    ERR_HANDLER()
}

int DB::StartBackgroundPartialMerges(const char *column_family_name, int seconds,
                                     int min_sstables) const
{
    tidesdb_err_t *err = tidesdb_start_background_partial_merge(this->tdb, column_family_name,
                                                                seconds, min_sstables);
    ERR_HANDLER()
}

Txn::Txn(tidesdb_t *tdb)
{
    this->tdb = tdb;
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

int Txn::Put(const uint8_t *key, size_t key_size, const uint8_t *value, size_t value_size,
             time_t ttl) const
{
    tidesdb_err_t *err = tidesdb_txn_put(this->txn, key, key_size, value, value_size, ttl);
    ERR_HANDLER()
}

int Txn::Delete(const uint8_t *key, size_t key_size) const
{
    tidesdb_err_t *err = tidesdb_txn_delete(this->txn, key, key_size);
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

Cursor::Cursor(tidesdb_t *tdb, const char *column_family_name)
{
    this->tdb = tdb;
    this->cursor = nullptr;
    this->column_family_name = strdup(column_family_name);
}

int Cursor::Init()
{
    tidesdb_err_t *err = tidesdb_cursor_init(tdb, this->column_family_name, &this->cursor);
    ERR_HANDLER()
}

Cursor::~Cursor()
{
    // free column_family_name
    free(this->column_family_name);

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

int Cursor::Get(uint8_t **key, size_t *key_size, uint8_t **value, size_t *value_size) const
{
    tidesdb_err_t *err = tidesdb_cursor_get(this->cursor, key, key_size, value, value_size);
    ERR_HANDLER()
}

}  // namespace TidesDB
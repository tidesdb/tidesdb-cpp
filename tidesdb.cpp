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

TidesDB::TidesDB(void)
{
    tdb = nullptr;
}

int TidesDB::open(const char *dir_name)
{
    tidesdb_err_t *err = tidesdb_open(dir_name, &this->tdb);
    ERR_HANDLER()
}

int TidesDB::close()
{
    tidesdb_err_t *err = tidesdb_close(this->tdb);
    ERR_HANDLER()
}
int TidesDB::create_column_family(const char *column_family_name, int flush_threshold,
                                  int max_level, float probability, bool compressed,
                                  tidesdb_compression_algo_t compress_algo, bool bloom_filter,
                                  tidesdb_memtable_ds_t memtable_ds)
{
    tidesdb_err_t *err = tidesdb_create_column_family(
        this->tdb, column_family_name, flush_threshold, max_level, probability, compressed,
        compress_algo, bloom_filter, memtable_ds);
    ERR_HANDLER()
}

int TidesDB::drop_column_family(const char *column_family_name)
{
    tidesdb_err_t *err = tidesdb_drop_column_family(this->tdb, column_family_name);
    ERR_HANDLER()
}

int TidesDB::put(const char *column_family_name, const uint8_t *key, const uint8_t *value,
                 time_t ttl)
{
    size_t key_size = strlen((const char *)key);
    size_t value_size = strlen((const char *)value);
    tidesdb_err_t *err =
        tidesdb_put(this->tdb, column_family_name, key, key_size, value, value_size, ttl);
    ERR_HANDLER()
}

int TidesDB::get(const char *column_family_name, const uint8_t *key, uint8_t **value,
                 size_t &value_size)
{
    size_t key_size = strlen((const char *)key);
    tidesdb_err_t *err =
        tidesdb_get(this->tdb, column_family_name, key, key_size, value, &value_size);
    if (err == nullptr)
    {
        uint8_t *buf = *value;
        buf[value_size] = 0;
    }
    ERR_HANDLER()
}

int TidesDB::delete_key(const char *column_family_name, const uint8_t *key)
{
    size_t key_size = strlen((const char *)key);
    tidesdb_err_t *err = tidesdb_delete(this->tdb, column_family_name, key, key_size);
    ERR_HANDLER()
}

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

#include <tidesdb.h>

#pragma once

class Tidesdb {
        tidesdb_t* tbh;
public:
        int open(const char* dir_name);
        int close();
	int create_column_family(const char *name, int flush_threshold,
                                            int max_level, float probability, bool compressed,
                                            tidesdb_compression_algo_t compress_algo,
                                            bool bloom_filter, tidesdb_memtable_ds_t memtable_ds);
        int drop_column_family(const char *name);
        int put(const char *column_family_name,
                const uint8_t *key,
                const uint8_t *value,
                time_t ttl);
        int get(const char *column_family_name,
                const uint8_t *key,
                uint8_t **value,
                size_t &value_size);
        int delete_key(const char *column_family_name,
                       const uint8_t *key);
        Tidesdb();
};


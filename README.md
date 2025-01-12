# tidesdb-cpp
Official C++ binding for TidesDB.

## Getting Started
You must make sure you have the TidesDB shared C library installed on your system. Be sure to also compile with TIDESDB_WITH_SANITIZER and TIDESDB_BUILD_TESTS OFF.

### Build and install
```
cmake -S . -B build
cmake --build build
cmake --install build
```

### Create a TidesDB instance
```cpp
#include <tidesdb.hpp>

int main() {
    TidesDB::DB db;
    db.Open("your_db_dir");

    // Perform db operations...

    db.Close();
    return 0;
}
```

### Creating and dropping column families
```cpp
db.CreateColumnFamily("my_column_family", (1024*1024)*64, TDB_DEFAULT_SKIP_LIST_MAX_LEVEL, TDB_DEFAULT_SKIP_LIST_PROBABILITY, true, TIDESDB_COMPRESSION_LZ4, true, TIDESDB_MEMTABLE_SKIPLIST);
db.DropColumnFamily("my_column_family");
```

### CRUD operations
#### No TTL
```cpp
std::vector<uint8_t> key = {1, 2, 3};
std::vector<uint8_t> value = {4, 5, 6};
db.Put("my_column_family", &key, &value, -1);
```

#### With TTL
```cpp
std::vector<uint8_t> key = {1, 2, 3};
std::vector<uint8_t> value = {4, 5, 6};
db.Put("my_column_family", &key, &value, std::chrono::seconds(3600));
```

#### Get
```cpp
std::vector<uint8_t> retrieved_value;
db.Get("my_column_family", &key, &retrieved_value);
```

#### Deleting data
```cpp
db.Delete("my_column_family", &key);
```

## Transactions
You can add operations to a transaction and commit them atomically.
```cpp
TidesDB::Txn txn(&db);
txn.Begin();
txn.Put(&key, &value, std::chrono::seconds(3600));
txn.Commit();
```

## Cursor
You can iterate over the keys in a column family.
```cpp
TidesDB::Cursor cursor(&db, "my_column_family");
cursor.Init();

std::vector<uint8_t> key, value;
cursor.Get(key, value);
cursor.Next(); // Go forwards
cursor.Prev(); // Go backwards
```

```cpp
TidesDB::Cursor cursor(&db, "my_column_family");
cursor.Init();

std::vector<uint8_t> key, value;
while (cursor.Get(key, value) == 0) {
    // Process key and value
    cursor.Next();
}
```


## Compactions
You can manually trigger a compaction.
```cpp
db.CompactSSTables("my_column_family", 4); // Use 4 threads for compaction
```

Or you can start partial background merge compactions.
```cpp
db.StartBackgroundPartialMerges("my_column_family", std::chrono::seconds(60), 5); // Merge every 60 seconds if there are at least 5 SSTables
```
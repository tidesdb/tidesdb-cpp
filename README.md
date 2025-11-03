# tidesdb-cpp

Official C++ bindings for TidesDB v1.

TidesDB is a fast and efficient key-value storage engine library written in C. The underlying data structure is based on a log-structured merge-tree (LSM-tree). This C++ binding provides a modern, type-safe interface to TidesDB with full support for all v1 features using C++17.

## Features

- **ACID Transactions** - Atomic, consistent, isolated, and durable transactions across column families
- **Optimized Concurrency** - Multiple concurrent readers, writers don't block readers
- **Column Families** - Isolated key-value stores with independent configuration
- **Bidirectional Iterators** - Iterate forward and backward over sorted key-value pairs
- **TTL Support** - Time-to-live for automatic key expiration
- **Compression** - Snappy, LZ4, or ZSTD compression support
- **Bloom Filters** - Reduce disk reads with configurable false positive rates
- **Background Compaction** - Automatic or manual SSTable compaction with parallel execution
- **Sync Modes** - Three durability levels: NONE, BACKGROUND, FULL
- **Custom Comparators** - Support for custom key comparison functions
- **Modern C++17** - RAII, move semantics, smart pointers, `std::optional`
- **Exception Safety** - Strong exception guarantees with RAII
- **Type Safety** - Compile-time type checking
- **Header-Only** - Single header file for easy integration

## Requirements

- **C++17** or later
- **CMake** 3.15+ (for building)
- **TidesDB v1** shared library installed

## Installation

### Install TidesDB C Library

```bash
# Clone TidesDB repository
git clone https://github.com/tidesdb/tidesdb.git
cd tidesdb

# Build and install (compile with sanitizer and tests OFF for bindings)
rm -rf build && cmake -S . -B build -DTIDESDB_WITH_SANITIZER=OFF -DTIDESDB_BUILD_TESTS=OFF
cmake --build build
sudo cmake --install build
```

**Dependencies:**
- Snappy
- LZ4
- Zstandard
- OpenSSL

**On Ubuntu/Debian:**
```bash
sudo apt install libzstd-dev liblz4-dev libsnappy-dev libssl-dev
```

**On macOS:**
```bash
brew install zstd lz4 snappy openssl
```

### Install C++ Bindings

#### Method 1: CMake Integration

```cmake
find_package(tidesdb REQUIRED)
target_link_libraries(your_target PRIVATE tidesdb::tidesdb)
```

#### Method 2: Header-Only

Simply copy `tidesdb.hpp` to your include path:

```bash
sudo cp tidesdb.hpp /usr/local/include/
```

Then include in your code:

```cpp
#include <tidesdb.hpp>
```

## Quick Start

```cpp
#include <tidesdb.hpp>
#include <iostream>

int main() {
    try {
        // Open database
        tidesdb::DB db("./mydb");
        
        // Create column family
        db.create_column_family("users");
        
        // Write data
        {
            auto txn = db.begin_transaction();
            txn->put("users", "user:1", "Alice");
            txn->commit();
        }
        
        // Read data
        {
            auto txn = db.begin_read_transaction();
            auto value = txn->get("users", "user:1");
            std::cout << "Value: " << value << std::endl;
        }
        
        // Clean up
        db.drop_column_family("users");
        
    } catch (const tidesdb::Exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
```

## Building

### Build with CMake

```bash
mkdir build && cd build
cmake ..
cmake --build .

# Run tests
ctest

# Run examples
./examples
```

### Build Options

```bash
cmake .. -DBUILD_EXAMPLES=ON  # Build examples (default: ON)
cmake .. -DBUILD_TESTS=ON     # Build tests (default: ON)
```

### Compiler Requirements

- GCC 7+ or Clang 5+ (C++17 support)
- MSVC 2017+ on Windows

## Usage

### Opening and Closing a Database

```cpp
#include <tidesdb.hpp>

// RAII - Database automatically closed on destruction
{
    // Constructor parameters:
    // 1. db_path: Path to database directory
    // 2. enable_debug_logging: Enable debug output (default: false)
    // 3. max_open_file_handles: Maximum open file handles (default: TDB_DEFAULT_MAX_OPEN_FILE_HANDLES)
    tidesdb::DB db("./mydb", false, 1024);
    
    // Use database...
    
}  // Database automatically closed
```

### Creating and Dropping Column Families

```cpp
// Create with default configuration
db.create_column_family("my_cf");

// Create with custom configuration
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

db.create_column_family("my_cf", config);

// Drop a column family
db.drop_column_family("my_cf");
```

### CRUD Operations

All operations are performed through transactions for ACID guarantees.

#### Writing Data

```cpp
// Simple write
{
    auto txn = db.begin_transaction();
    txn->put("my_cf", "key", "value");
    txn->commit();
}

// Multiple operations
{
    auto txn = db.begin_transaction();
    txn->put("my_cf", "key1", "value1");
    txn->put("my_cf", "key2", "value2");
    txn->put("my_cf", "key3", "value3");
    txn->commit();
}
```

#### Writing with TTL

```cpp
{
    auto txn = db.begin_transaction();
    
    // Expire in 10 seconds
    auto ttl = std::time(nullptr) + 10;
    txn->put("my_cf", "temp_key", "temp_value", ttl);
    
    txn->commit();
}

// TTL examples
auto ttl = -1;                         // No expiration
auto ttl = std::time(nullptr) + 300;   // Expire in 5 minutes
auto ttl = std::time(nullptr) + 3600;  // Expire in 1 hour
```

#### Reading Data

```cpp
{
    auto txn = db.begin_read_transaction();
    auto value = txn->get("my_cf", "key");
    std::cout << "Value: " << value << std::endl;
}
```

#### Deleting Data

```cpp
{
    auto txn = db.begin_transaction();
    txn->remove("my_cf", "key");
    txn->commit();
}
```

#### Transaction Rollback

```cpp
// Manual rollback
{
    auto txn = db.begin_transaction();
    txn->put("my_cf", "key", "value");
    txn->rollback();  // Changes not applied
}

// Automatic cleanup via RAII (but not automatic rollback)
{
    auto txn = db.begin_transaction();
    txn->put("my_cf", "key", "value");
    // Transaction freed on scope exit (not committed)
}
```

### Iterating Over Data

```cpp
// Forward iteration
{
    auto txn = db.begin_read_transaction();
    auto iter = txn->new_iterator("my_cf");
    iter->seek_to_first();
    
    while (iter->valid()) {
        auto key = iter->key_string();
        auto value = iter->value_string();
        std::cout << "Key: " << key << ", Value: " << value << std::endl;
        iter->next();
    }
}

// Backward iteration
{
    auto txn = db.begin_read_transaction();
    auto iter = txn->new_iterator("my_cf");
    iter->seek_to_last();
    
    while (iter->valid()) {
        auto key = iter->key_string();
        auto value = iter->value_string();
        std::cout << "Key: " << key << ", Value: " << value << std::endl;
        iter->prev();
    }
}

// Binary data
{
    auto txn = db.begin_read_transaction();
    auto iter = txn->new_iterator("my_cf");
    iter->seek_to_first();
    
    while (iter->valid()) {
        auto key = iter->key();    // Returns std::vector<uint8_t>
        auto value = iter->value(); // Returns std::vector<uint8_t>
        iter->next();
    }
}
```

### Column Family Statistics

```cpp
auto stats = db.get_column_family_stats("my_cf");

std::cout << "Name: " << stats.name << std::endl;
std::cout << "Comparator: " << stats.comparator_name << std::endl;
std::cout << "Number of SSTables: " << stats.num_sstables << std::endl;
std::cout << "Total SSTable Size: " << stats.total_sstable_size << " bytes" << std::endl;
std::cout << "Memtable Size: " << stats.memtable_size << " bytes" << std::endl;
std::cout << "Memtable Entries: " << stats.memtable_entries << std::endl;

// Access config
std::cout << "Memtable Flush Size: " << stats.config.memtable_flush_size << std::endl;
std::cout << "Compression: " << (stats.config.compressed ? "enabled" : "disabled") << std::endl;
```

### Listing Column Families

```cpp
auto cf_names = db.list_column_families();

for (const auto& name : cf_names) {
    std::cout << "Column Family: " << name << std::endl;
}
```

### Manual Compaction

```cpp
auto cf = db.get_column_family("my_cf");
cf.compact();
```

### Sync Modes

```cpp
// TDB_SYNC_NONE - Fastest, least durable
tidesdb::ColumnFamilyConfig config;
config.sync_mode = tidesdb::SyncMode::NONE;

// TDB_SYNC_BACKGROUND - Balanced
config.sync_mode = tidesdb::SyncMode::BACKGROUND;
config.sync_interval = 1000;  // Sync every 1 second

// TDB_SYNC_FULL - Most durable
config.sync_mode = tidesdb::SyncMode::FULL;

db.create_column_family("my_cf", config);
```

### Compression Algorithms

```cpp
tidesdb::ColumnFamilyConfig config;

// No compression
config.compressed = false;

// Snappy (fast, balanced)
config.compressed = true;
config.compress_algo = tidesdb::CompressionAlgo::SNAPPY;

// LZ4 (very fast, lower compression)
config.compressed = true;
config.compress_algo = tidesdb::CompressionAlgo::LZ4;

// Zstandard (slower, high compression)
config.compressed = true;
config.compress_algo = tidesdb::CompressionAlgo::ZSTD;

db.create_column_family("my_cf", config);
```

### Custom Comparators

```cpp
// Use a custom comparator (must be registered with TidesDB)
tidesdb::ColumnFamilyConfig config;
config.comparator_name = "reverse";  // Use registered comparator

db.create_column_family("my_cf", config);

// Or no custom comparator (use default lexicographic comparison)
config.comparator_name = std::nullopt;
```

**Note** Custom comparators must be registered at the C library level. Refer to the TidesDB documentation for details on registering custom comparators.

## Working with Binary Data

```cpp
// Store binary data
std::vector<uint8_t> binary_key = {0x00, 0x01, 0x02, 0xFF};
std::vector<uint8_t> binary_value = {0xDE, 0xAD, 0xBE, 0xEF};

{
    auto txn = db.begin_transaction();
    txn->put("my_cf", binary_key, binary_value);
    txn->commit();
}

// Retrieve binary data
{
    auto txn = db.begin_read_transaction();
    auto value = txn->get("my_cf", binary_key);
    // value is std::vector<uint8_t>
}
```

## Error Handling

```cpp
try {
    auto txn = db.begin_read_transaction();
    auto value = txn->get("my_cf", "nonexistent_key");
} catch (const tidesdb::Exception& e) {
    std::cerr << "Error: " << e.what() << std::endl;
    std::cerr << "Error code: " << e.error_code() << std::endl;
    
    if (e.error_code() == TDB_ERR_NOT_FOUND) {
        std::cerr << "Key not found" << std::endl;
    }
}
```

**Error Codes:**
- `TDB_SUCCESS` (0) - Operation successful
- `TDB_ERROR` (-1) - Generic error
- `TDB_ERR_MEMORY` (-2) - Memory allocation failed
- `TDB_ERR_INVALID_ARGS` (-3) - Invalid arguments
- `TDB_ERR_IO` (-4) - I/O error
- `TDB_ERR_NOT_FOUND` (-5) - Key not found
- `TDB_ERR_EXISTS` (-6) - Resource already exists
- `TDB_ERR_CORRUPT` (-7) - Data corruption
- `TDB_ERR_LOCK` (-8) - Lock acquisition failed
- `TDB_ERR_TXN_COMMITTED` (-9) - Transaction already committed
- `TDB_ERR_TXN_ABORTED` (-10) - Transaction aborted
- `TDB_ERR_READONLY` (-11) - Write on read-only transaction
- `TDB_ERR_FULL` (-12) - Database full
- `TDB_ERR_INVALID_NAME` (-13) - Invalid name
- `TDB_ERR_COMPARATOR_NOT_FOUND` (-14) - Comparator not found
- `TDB_ERR_MAX_COMPARATORS` (-15) - Max comparators reached
- `TDB_ERR_INVALID_CF` (-16) - Invalid column family
- `TDB_ERR_THREAD` (-17) - Thread operation failed
- `TDB_ERR_CHECKSUM` (-18) - Checksum verification failed
- `TDB_ERR_KEY_DELETED` (-19) - Key deleted
- `TDB_ERR_KEY_EXPIRED` (-20) - Key expired

## Complete Example

```cpp
#include <tidesdb.hpp>
#include <iostream>

int main() {
    try {
        // Open database
        tidesdb::DB db("./example_db");
        
        // Create column family with custom configuration
        tidesdb::ColumnFamilyConfig config;
        config.memtable_flush_size = 64 * 1024 * 1024;
        config.compressed = true;
        config.compress_algo = tidesdb::CompressionAlgo::LZ4;
        config.bloom_filter_fp_rate = 0.01;
        config.enable_background_compaction = true;
        config.sync_mode = tidesdb::SyncMode::BACKGROUND;
        config.sync_interval = 1000;
        
        db.create_column_family("users", config);
        
        // Write data
        {
            auto txn = db.begin_transaction();
            txn->put("users", "user:1", "Alice");
            txn->put("users", "user:2", "Bob");
            txn->put("users", "user:3", "Charlie");
            
            // Add temporary session with TTL
            auto ttl = std::time(nullptr) + 3600;  // Expire in 1 hour
            txn->put("users", "session:abc123", "session_data", ttl);
            
            txn->commit();
        }
        
        // Read data
        {
            auto txn = db.begin_read_transaction();
            auto value = txn->get("users", "user:1");
            std::cout << "User: " << value << std::endl;
        }
        
        // Iterate over all users
        std::cout << "\nAll users:" << std::endl;
        {
            auto txn = db.begin_read_transaction();
            auto iter = txn->new_iterator("users");
            iter->seek_to_first();
            
            while (iter->valid()) {
                auto key = iter->key_string();
                auto value = iter->value_string();
                if (key.find("user:") == 0) {
                    std::cout << "  " << key << " = " << value << std::endl;
                }
                iter->next();
            }
        }
        
        // Get statistics
        auto stats = db.get_column_family_stats("users");
        std::cout << "\nDatabase Statistics:" << std::endl;
        std::cout << "  Memtable Size: " << stats.memtable_size << " bytes" << std::endl;
        std::cout << "  Memtable Entries: " << stats.memtable_entries << std::endl;
        std::cout << "  Number of SSTables: " << stats.num_sstables << std::endl;
        
        // Clean up
        db.drop_column_family("users");
        
    } catch (const tidesdb::Exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
```

## Modern C++ Features

### RAII (Resource Acquisition Is Initialization)

```cpp
{
    tidesdb::DB db("./mydb");
    auto txn = db.begin_transaction();
    auto iter = txn->new_iterator("cf");
    
    // All resources automatically freed on scope exit
}
```

### Move Semantics

```cpp
// Database objects are movable
tidesdb::DB db1("./mydb");
tidesdb::DB db2 = std::move(db1);  // db1 is now invalid, db2 owns the database

// Transactions and iterators are also movable
auto txn = db2.begin_transaction();
auto txn2 = std::move(txn);  // txn is now invalid
```

### Smart Pointers

```cpp
// Transactions and iterators are returned as unique_ptr
std::unique_ptr<tidesdb::Transaction> txn = db.begin_transaction();
std::unique_ptr<tidesdb::Iterator> iter = txn->new_iterator("cf");

// Automatic cleanup when unique_ptr goes out of scope
```

### std::optional

```cpp
tidesdb::ColumnFamilyConfig config;
config.comparator_name = std::nullopt;  // No custom comparator
config.comparator_name = "reverse";     // Custom comparator
```

## API Reference

### Classes

- `tidesdb::DB` - Main database class
- `tidesdb::Transaction` - Transaction for atomic operations
- `tidesdb::Iterator` - Iterator for traversing key-value pairs
- `tidesdb::ColumnFamily` - Column family handle
- `tidesdb::Exception` - Exception class for errors

### Enums

- `tidesdb::CompressionAlgo` - Compression algorithms (SNAPPY, LZ4, ZSTD)
- `tidesdb::SyncMode` - Sync modes for durability (NONE, BACKGROUND, FULL)

### Structures

- `tidesdb::ColumnFamilyConfig` - Column family configuration
- `tidesdb::ColumnFamilyStats` - Column family statistics

## Performance Tips

1. **Use move semantics** to avoid unnecessary copies
2. **Batch operations** in transactions for better performance
3. **Use read transactions** for read-only operations
4. **Enable background compaction** for automatic maintenance
5. **Reserve capacity** for vectors when you know the size
6. **Use binary data** (`std::vector<uint8_t>`) to avoid string conversions
7. **Reuse transactions** when possible
8. **Profile your code** to identify bottlenecks
9. **Tune memtable_flush_size** based on your workload
10. **Choose appropriate compression** algorithm for your data

## Testing

```bash
# Build and run tests
mkdir build && cd build
cmake ..
cmake --build .
ctest

# Or run directly
./test_tidesdb
```

## Examples

```bash
# Build and run examples
mkdir build && cd build
cmake ..
cmake --build .
./examples
```

## Concurrency

TidesDB is designed for high concurrency:

- **Multiple readers can read concurrently** - No blocking between readers
- **Writers don't block readers** - Readers can access data during writes
- **Writers block other writers** - Only one writer per column family at a time
- **Read transactions** acquire read locks
- **Write transactions** acquire write locks on commit
- **Different column families** can be written concurrently

## License

Multiple licenses apply:

```
Mozilla Public License Version 2.0 (TidesDB)

-- AND --

BSD 3 Clause (Snappy)
BSD 2 (LZ4)
BSD 2 (xxHash - Yann Collet)
BSD (Zstandard)
Apache 2.0 (OpenSSL 3.0+) / OpenSSL License (OpenSSL 1.x)
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For issues, questions, or discussions
- GitHub Issues: https://github.com/tidesdb/tidesdb-cpp/issues
- Discord Community: https://discord.gg/tWEmjR66cy
- Main TidesDB Repository: https://github.com/tidesdb/tidesdb

## Links

- [TidesDB Main Repository](https://github.com/tidesdb/tidesdb)
- [TidesDB Documentation](https://github.com/tidesdb/tidesdb#readme)
- [Other Language Bindings](https://github.com/tidesdb/tidesdb#bindings)
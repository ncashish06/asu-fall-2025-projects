// General Libraries
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include "csv.hpp"

// RocksDB Libraries
#include <rocksdb/db.h>
#include <rocksdb/options.h>

// Namespaces
using namespace std;
using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::DBOptions;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Slice;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;

using ROCKSDB_NAMESPACE::Iterator; // needed for iterator implementation

// Function to create a kvs
DB *create_kvs(const string &csv_file_path, const string &db_path)
{

    // Open the csv file
    csv::CSVReader reader(csv_file_path);
    csv::CSVRow row;

    // Get the headers
    vector<string> header = reader.get_col_names();

    DB *db = nullptr;

    // TODO: Open RocksDB database with options
    Options options;
    options.create_if_missing = true;

    Status status = DB::Open(options, db_path, &db);
    if (!status.ok())
    {
        cerr << "Failed to open RocksDB at " << db_path << " : " << status.ToString() << endl;
        return nullptr;
    }

    // TODO: Load CSV data into database using WriteBatch
    WriteBatch batch;

    for (csv::CSVRow &csv_row : reader)
    {
        string id_value;
        try
        {
            id_value = csv_row["id"].get<string>();
        }
        catch (...)
        {
            continue;
        }

        size_t col_no = 0;
        for (csv::CSVField &field : csv_row)
        {
            const string &col_name = header[col_no];
            string composite_key = id_value + "_" + col_name;

            string value = field.get<string>();
            batch.Put(composite_key, value);
            col_no++;
        }
    }

    Status write_status = db->Write(WriteOptions(), &batch);
    if (!write_status.ok())
    {
        cerr << "Error writing batch to RocksDB: " << write_status.ToString() << endl;
    }

    return db;
}

// Function to perform a MultiGet operation
vector<string> multi_get(DB *db, const vector<string> &keys)
{
    vector<string> values;

    // TODO: Implement MultiGet operation
    if (db == nullptr || keys.empty())
    {
        return values;
    }

    vector<Slice> key_slices;
    key_slices.reserve(keys.size());
    for (const auto &k : keys)
    {
        key_slices.emplace_back(k);
    }

    vector<string> retrieved_values(keys.size());
    vector<Status> statuses = db->MultiGet(ReadOptions(), key_slices, &retrieved_values);

    for (size_t i = 0; i < keys.size(); ++i)
    {
        if (statuses[i].ok())
        {
            values.push_back(retrieved_values[i]);
        }
        else
        {
            values.push_back("");
        }
    }

    // Only return the display_name of the subreddit(s)
    return values;
}

// Function to iterate over a range of keys and return the corresponding values
vector<string> iterate_over_range(DB *db, const string &start_key, const string &end_key)
{
    vector<string> result;

    // TODO: Create iterator and iterate from start_key to end_key
    // TODO: Filter results to only include keys containing "_display_name"
    if (db == nullptr)
    {
        return result;
    }

    unique_ptr<Iterator> it(db->NewIterator(ReadOptions()));

    for (it->Seek(start_key); it->Valid(); it->Next())
    {
        string current_key = it->key().ToString();

        if (current_key > end_key)
        {
            break;
        }

        if (current_key < start_key)
        {
            continue;
        }

        if (current_key.find("_display_name") != string::npos)
        {
            result.push_back(it->value().ToString());
        }
    }

    // Only return the display_name of the subreddit(s)
    return result;
}

// Function to delete a particular comment from the kvs
Status delete_key(DB *db, const string &key)
{
    Status s;

    // TODO: Delete the key from the database
    if (db == nullptr)
    {
        return Status::InvalidArgument("DB pointer is null");
    }

    s = db->Delete(WriteOptions(), key);

    return s;
}

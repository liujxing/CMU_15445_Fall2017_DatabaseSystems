/*
 * extendible_hash.h : implementation of in-memory hash table using extendible
 * hashing
 *
 * Functionality: The buffer pool manager must maintain a page table to be able
 * to quickly map a PageId to its corresponding memory location; or alternately
 * report that the PageId does not match any currently-buffered page.
 */

#pragma once

#include <cstdlib>
#include <vector>
#include <string>
#include <mutex>
#include <list>

#include "hash/hash_table.h"

namespace cmudb {

template <typename K, typename V>
class ExtendibleHash : public HashTable<K, V> {

    struct Element {
        K key;
        V value;
        Element() = default;
        Element(const K & key, const V & value): key(key), value(value) {}
    };

    struct Bucket {
        int local_depth = 0;
        std::list<Element> elements;
    };

public:
    // constructor
    ExtendibleHash(size_t size);
    // helper function to generate hash addressing
    size_t HashKey(const K &key);
    // helper function to get global & local depth
    int GetGlobalDepth() const;
    int GetLocalDepth(int bucket_id) const;
    int GetNumBuckets() const;
    // lookup and modifier
    bool Find(const K &key, V &value) override;
    bool Remove(const K &key) override;
    void Insert(const K &key, const V &value) override;

private:
    // add your own member variables here
    const size_t size; // size of each bucket
    std::mutex mutex; // mutex for access control
    int global_depth; // global depth
    std::list<Bucket> buckets; // local depth and bucket elements
    std::vector<Bucket*> directory; // directory mapping hashcode to buckets
    const std::hash<K> hash_function; // hash function to convert key into hash code

    void HandleOverflowBucket(Bucket * bucket); // handle the overflow

};
} // namespace cmudb

#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"

namespace cmudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size): size(size){
    global_depth = 0;
    buckets.emplace_back();
    directory.emplace_back(& buckets.front());
}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) {
    const size_t hash = hash_function(key);
    const uint mask = (1u << (unsigned)global_depth) - 1;
    const size_t masked_hash = hash & mask;
    return masked_hash;
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
    return global_depth;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
    const Bucket * bucket = directory[bucket_id];
    return bucket->local_depth;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
    return int(buckets.size());
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {

    std::lock_guard<std::mutex> lock(mutex);

    const size_t bucket_id = HashKey(key);
    const Bucket * bucket = directory[bucket_id];
    const std::list<Element> & elements = bucket->elements;
    for (auto iterator = elements.begin(); iterator != elements.end(); iterator++) {
        if (iterator -> key == key) {
            value = iterator -> value;
            return true;
        }
    }
    return false;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {

    std::lock_guard<std::mutex> lock(mutex);

    const size_t bucket_id = HashKey(key);
    Bucket * bucket = directory[bucket_id];
    std::list<ExtendibleHash::Element> & elements = bucket->elements;
    for (auto iterator = elements.begin(); iterator != elements.end(); iterator++) {
        if (iterator -> key == key) {
            elements.erase(iterator);
            return true;
        }
    }
    return false;
}

template <typename K, typename V>
void ExtendibleHash<K, V>::HandleOverflowBucket(cmudb::ExtendibleHash<K, V>::Bucket *bucket) {
    std::list<Element> & elements = bucket->elements;
    if (elements.size() <= size) return;

    // adjust the directory when local_depth == global_depth
    if (bucket->local_depth == global_depth) {
        global_depth += 1;

        // double the size of directory and set the directory pointer to point to the same position as companion pointer
        const size_t directory_size = directory.size();
        directory.resize(directory_size*2);

        for (size_t i = 0; i < directory_size; i++) {
            directory[i + directory_size] = directory[i];
        }
    }

    // increase local depth of bucket
    bucket->local_depth += 1;

    // create companion bucket
    buckets.emplace_back();
    Bucket & companion_bucket = buckets.back();
    companion_bucket.local_depth = bucket->local_depth;

    // make the directory to point to bucket vs companion bucket
    for (size_t i = directory.size()/2; i < directory.size(); i++) {
        if (directory[i] == bucket) {
            directory[i] = & companion_bucket;
        }
    }

    // put the elements in bucket into bucket vs companion bucket
    for (auto iterator = elements.begin(); iterator != elements.end();) {
        const size_t bucket_id = HashKey(iterator->key);
        const Bucket* target_bucket = directory[bucket_id];
        if (target_bucket == bucket) {
            iterator++;
        } else {
            auto prev_iterator = iterator;
            iterator++;
            companion_bucket.elements.emplace_back(*prev_iterator);
            elements.erase(prev_iterator);
        }
    }

    // check again if either of bucket/companion_bucket is overflow
    HandleOverflowBucket(bucket);
    HandleOverflowBucket(&companion_bucket);

}


/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {

    std::lock_guard<std::mutex> lock(mutex);

    const size_t bucket_id = HashKey(key);
    Bucket * bucket = directory[bucket_id];
    std::list<Element> & elements = bucket->elements;

    // check if the key exists in the hash table
    for (auto iterator = elements.begin(); iterator != elements.end(); iterator++) {
        if (iterator -> key == key) {
            iterator->value = value;
            return;
        }
    }

    // insert the key into current bucket
    elements.emplace_back(key, value);

    // handle the overflow of buckets
    HandleOverflowBucket(bucket);


}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb

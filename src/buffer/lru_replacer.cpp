/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"


namespace cmudb {

template <typename T> LRUReplacer<T>::LRUReplacer() {
    head = new Node();
    tail = new Node();
    head->next = tail;
    head->prev = nullptr;
    tail->prev = head;
    tail->next = nullptr;
}

template <typename T> LRUReplacer<T>::~LRUReplacer() {
    Node* node = head;
    while (node != nullptr) {
        Node* next_node = node->next;
        delete node;
        node = next_node;
    }
}

/*
 * Insert value into LRU
 */
template <typename T> void LRUReplacer<T>::Insert(const T &value) {
    // create new node
    mutex.lock();
    Node* node;
    if (node_map.find(value) == node_map.end()) {
        node = new Node();
        node->value = value;
        node_map[value] = node;
    } else {
        node = node_map[value];
        Node* node_prev = node->prev;
        Node* node_next = node->next;
        node_prev->next = node_next;
        node_next->prev = node_prev;
    }

    // put node into list
    Node* tail_prev = tail->prev;
    tail_prev->next = node;
    node->next = tail;
    node->prev = tail_prev;
    tail->prev = node;
    mutex.unlock();
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> bool LRUReplacer<T>::Victim(T &value) {
    mutex.lock();
    if (node_map.empty()) {
        mutex.unlock();
        return false;
    }

    Node* node = head->next;
    value = node->value;
    node_map.erase(value);
    Node* node_next = node->next;
    head->next = node_next;
    node_next->prev = head;
    delete node;
    mutex.unlock();
    return true;

}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
    mutex.lock();
    // does not find the element
    if (node_map.find(value) == node_map.end()) {
        mutex.unlock();
        return false;
    }


    // find the element
    Node* node = node_map[value];
    node_map.erase(value);
    Node* node_prev = node->prev;
    Node* node_next = node->next;
    node_prev->next = node_next;
    node_next->prev = node_prev;
    delete node;
    mutex.unlock();
    return true;
}

template <typename T> size_t LRUReplacer<T>::Size() { return node_map.size(); }

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb

//
// Created by Chengjun Ying on 2021/5/6.
//

#include <vector>

#include "db/memtable.h"
#include "memory/arena.h"
#include "rocksdb/memtablerep.h"

namespace ROCKSDB_NAMESPACE {
namespace {

const size_t order = 8;

struct Node {
  std::vector<char *> key;
  std::vector<Node *> ptr;
  Node *prev = nullptr;
  Node *next = nullptr;
  bool IsLeafNode() const { return ptr.empty(); }
};

class BpTreeRep : public MemTableRep {
 public:
  explicit BpTreeRep(const KeyComparator &compare, Allocator *allocator)
      : MemTableRep(allocator), compare_(compare){};
  void Insert(KeyHandle handle) override;
  void Bulkload() override;
  bool Contains(const char *key) const override;
  void Get(const LookupKey &k, void *callback_args,
           bool (*callback_func)(void *, const char *)) override;
  void MarkReadOnly() override;
  size_t ApproximateMemoryUsage() override { return memory_usage_; }
  MemTableRep::Iterator *GetIterator(Arena *arena) override;

  class Iterator : public MemTableRep::Iterator {
   public:
    ~Iterator() override = default;
    Iterator(BpTreeRep* bpTreeRep) {
      SetBpTree(bpTreeRep);
    }
    bool Valid() const override { return cur_node != nullptr; }
    const char *key() const override {
      assert(Valid());
      return cur_node->key[cur_index];
    }
    void Next() override {
      assert(Valid());
      if (cur_index == cur_node->key.size() - 1) {
        cur_node = cur_node->next;
        cur_index = 0;
      } else {
        cur_index++;
      }
      assert(cur_index < cur_node->key.size());
    }
    void Prev() override {
      assert(Valid());
      if (cur_index == 0) {
        cur_node = cur_node->prev;
        cur_index = cur_node->key.size() - 1;
      } else {
        cur_index--;
      }
      assert(cur_index >= 0);
    }
    void Seek(const Slice &internal_key, const char *memtable_key) override {
      const char *encoded_key = (memtable_key != nullptr)
                                    ? memtable_key
                                    : EncodeKey(&tmp_, internal_key);
      bpTree_->FindGreaterOrEqual(cur_node, cur_index, bpTree_->root_,
                                  encoded_key);
    }
    void SeekForPrev(const Slice &internal_key,
                     const char *memtable_key) override {
      assert(false);
    }
    void SeekToFirst() override {
      cur_node = bpTree_->leaf_head_;
      cur_index = 0;
    }
    void SeekToLast() override {
      cur_node = bpTree_->leaf_tail_;
      cur_index = bpTree_->leaf_tail_->key.size() - 1;
    }
    void SetBpTree(BpTreeRep *bpTreeRep) {
      assert(bpTreeRep != nullptr);
      bpTree_ = bpTreeRep;
      cur_node = nullptr;
      cur_index = 0;
    }

   private:
    BpTreeRep *bpTree_;
    Node *cur_node;
    size_t cur_index;

   protected:
    std::string tmp_;  // For passing to EncodeKey
  };

 private:
  friend class Iterator;
  size_t memory_usage_ = 0;
  bool immutable_ = false;
  std::vector<char *> keys_;
  Node *leaf_head_ = nullptr;
  Node *leaf_tail_ = nullptr;
  Node *root_ = nullptr;
  mutable port::RWMutex rwlock_;
  const KeyComparator &compare_;

  void ConstructLeafNode();
  void ConstructBpTree(Node *start, Node *end);
  Status Find(Node *cur, const char *key) const;
  void FindGreaterOrEqual(Node *&ret, size_t &index, Node *cur,
                          const char *key) const;
};

void rocksdb::BpTreeRep::Insert(KeyHandle handle) {
  auto *key = static_cast<char *>(handle);
  assert(!immutable_);
  WriteLock l(&rwlock_);
  keys_.push_back(key);
}

void rocksdb::BpTreeRep::Bulkload() {
  ConstructLeafNode();
  ConstructBpTree(leaf_head_, leaf_tail_);
}

bool rocksdb::BpTreeRep::Contains(const char *key) const {
  return Status::OK() == Find(root_, key);
}

void rocksdb::BpTreeRep::Get(const LookupKey &k, void *callback_args,
                             bool (*callback_func)(void *, const char *)) {
  BpTreeRep::Iterator iter(this);
  Slice dummy_slice;
  for (iter.Seek(dummy_slice, k.memtable_key().data());
       iter.Valid() && callback_func(callback_args, iter.key()); iter.Next()) {
  }
}

void rocksdb::BpTreeRep::MarkReadOnly() {
  WriteLock l(&rwlock_);
  immutable_ = true;
}

MemTableRep::Iterator* rocksdb::BpTreeRep::GetIterator(Arena *arena) {
  void *mem =
      arena ? arena->AllocateAligned(sizeof(BpTreeRep::Iterator))
            : operator new(sizeof(BpTreeRep::Iterator));
  return new (mem) BpTreeRep::Iterator(this);
}

void rocksdb::BpTreeRep::ConstructLeafNode() {
  auto iter = keys_.begin();
  while (iter != keys_.end()) {
    size_t rep = 0;
    Node *node =
        reinterpret_cast<Node *>(allocator_->AllocateAligned(sizeof(Node)));
    for (; rep < order && iter != keys_.end(); rep++, iter++) {
      node->key.push_back(*iter);
    }
    if (leaf_head_ == nullptr) {
      leaf_head_ = leaf_tail_ = node;
    } else {
      leaf_tail_->next = node;
      node->prev = leaf_tail_;
      leaf_tail_ = node;
    }
  }
}

void rocksdb::BpTreeRep::ConstructBpTree(Node *start, Node *end) {
  // only one node, exit
  if (start == end) {
    root_ = start;
    return;
  }
  Node *p = start;
  Node *head = nullptr;
  Node *tail = nullptr;
  while (p != nullptr) {
    size_t rep = 0;
    Node *node =
        reinterpret_cast<Node *>(allocator_->AllocateAligned(sizeof(Node)));
    for (; rep < order && p != nullptr; rep++, p = p->next) {
      node->key.push_back(p->key[0]);
      node->ptr.push_back(p);
    }
    if (head == nullptr) {
      head = tail = node;
    } else {
      tail->next = node;
      node->prev = tail;
      tail = node;
    }
  }
  ConstructBpTree(head, tail);
}

Status rocksdb::BpTreeRep::Find(Node *cur, const char *key) const {
  if (compare_(key, cur->key[0]) < 0) {
    return Status::NotFound();
  }
  for (size_t i = 0; i < cur->key.size(); i++) {
    if (i == cur->key.size() - 1) {
      if (cur->IsLeafNode()) {
        // leaf node
        if (compare_(key, cur->key[i]) == 0) {
          return Status::OK();
        } else {
          return Status::NotFound();
        }
      } else {
        // none leaf
        return Find(cur->ptr[i], key);
      }
    } else if (compare_(key, cur->key[i]) >= 0 &&
               compare_(key, cur->key[i + 1]) < 0) {
      if (cur->IsLeafNode()) {
        // leaf node
        if (compare_(key, cur->key[i]) == 0) {
          return Status::OK();
        } else {
          return Status::NotFound();
        }
      } else {
        // none leaf
        return Find(cur->ptr[i], key);
      }
    }
  }
  return Status::NotFound();
}

void rocksdb::BpTreeRep::FindGreaterOrEqual(Node *&ret, size_t &index,
                                            Node *cur, const char *key) const {
  // leaf node
  if (cur->IsLeafNode()) {
    while (cur != nullptr) {
      for (size_t i = 0; i < cur->key.size(); i++) {
        if (compare_(cur->key[i], key) >= 0) {
          ret = cur;
          index = i;
          return;
        }
      }
      cur = cur->next;
    }
    index = 0;
    ret = nullptr;
    return;
  }
  // none leaf node
  for (size_t i = 0; i < cur->key.size(); i++) {
    if (i == cur->key.size() - 1 && compare_(key, cur->key[i]) >= 0) {
      FindGreaterOrEqual(ret, index, cur->ptr[i], key);
      return;
    }
    if (compare_(key, cur->key[i]) >= 0 && compare_(key, cur->key[i + 1]) < 0) {
      FindGreaterOrEqual(ret, index, cur->ptr[i], key);
      return;
    }
  }
  index = 0;
  ret = nullptr;
}

}  // namespace
}  // namespace ROCKSDB_NAMESPACE

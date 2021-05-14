//
// Created by Chengjun Ying on 2021/5/6.
//
#include <algorithm>

#include "db/memtable.h"
#include "memory/arena.h"
#include "memtable/stl_wrappers.h"
#include "rocksdb/memtablerep.h"

namespace ROCKSDB_NAMESPACE {
namespace {

using namespace stl_wrappers;
const uint32_t order = 64;
const uint32_t max_build_buffer_size = 32 * order;

struct Node {
  uint32_t key_nums = 0;
  char *key_buf = nullptr;
  Node *next = nullptr;
  static char *NextKey(char *cur_p) {
    uint32_t key_len = 0;
    const char *p_internal_key = GetVarint32Ptr(cur_p, cur_p + 5, &key_len);
    // assert(key_len >= 8);
    return const_cast<char *>(p_internal_key) + key_len;
  }
  char *GetKey(uint32_t index) const {
    // assert(key_buf != nullptr);
    // assert(index < key_nums);
    char *p = key_buf;
    for (uint32_t i = 0; i < index; i++) {
      p = NextKey(p);
    }
    return p;
  }
  virtual bool IsLeafNode() const = 0;
  virtual ~Node() { delete[] key_buf; }
};

struct LeafNode : Node {
  std::vector<char *> value_ptr;
  bool IsLeafNode() const override { return true; }
};

struct InternalNode : Node {
  std::vector<Node *> node_ptr;
  bool IsLeafNode() const override { return false; }
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
  ~BpTreeRep() override {
    for (auto &n : allocated) {
      delete n;
    }
  }

  class Iterator : public MemTableRep::Iterator {
   public:
    ~Iterator() override{};
    Iterator(BpTreeRep *bpTreeRep) { SetBpTree(bpTreeRep); }
    bool Valid() const override { return cur_node != nullptr; }
    const char *key() const override {
      // assert(Valid());
      // assert(cur_node->IsLeafNode());
      return dynamic_cast<LeafNode *>(cur_node)->value_ptr[cur_index];
    }
    void Next() override {
      // assert(Valid());
      if (cur_index == cur_node->key_nums - 1) {
        cur_node = cur_node->next;
        cur_index = 0;
      } else {
        cur_index++;
      }
      // assert(cur_node == nullptr || cur_index < cur_node->key_nums);
    }
    void Prev() override { assert(false); }
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
      cur_index = bpTree_->leaf_tail_->key_nums - 1;
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
    uint32_t cur_index;

   protected:
    std::string tmp_;  // For passing to EncodeKey
  };

 private:
  friend class Iterator;
  uint32_t memory_usage_ = 0;
  bool immutable_ = false;
  std::vector<char *> keys_;
  Node *leaf_head_ = nullptr;
  Node *leaf_tail_ = nullptr;
  Node *root_ = nullptr;
  mutable port::RWMutex rwlock_;
  const KeyComparator &compare_;
  std::vector<Node *> allocated;

  void ConstructLeafNode();
  void ConstructBpTree(Node *start, Node *end);
  Status Find(Node *cur, const char *key) const;
  void FindGreaterOrEqual(Node *&ret, uint32_t &index, Node *cur,
                          const char *key) const;
};

void rocksdb::BpTreeRep::Insert(KeyHandle handle) {
  auto *key = static_cast<char *>(handle);
  assert(!immutable_);
  WriteLock l(&rwlock_);
  keys_.push_back(key);
}

void rocksdb::BpTreeRep::Bulkload() {
  std::sort(keys_.begin(), keys_.end(), Compare(compare_));
  ConstructLeafNode();
  ConstructBpTree(leaf_head_, leaf_tail_);
}

bool rocksdb::BpTreeRep::Contains(const char *key) const {
  return Status::OK() == Find(root_, key);
}

void rocksdb::BpTreeRep::Get(const LookupKey &k, void *callback_args,
                             bool (*callback_func)(void *, const char *)) {
  // assert(immutable_);
  BpTreeRep::Iterator iter(this);
  Slice dummy_slice;
  // Note: Find internal key instead of memtable key
  for (iter.Seek(dummy_slice, k.memtable_key().data());
       iter.Valid() && callback_func(callback_args, iter.key()); iter.Next()) {
  }
}

void rocksdb::BpTreeRep::MarkReadOnly() {
  WriteLock l(&rwlock_);
  immutable_ = true;
}

MemTableRep::Iterator *rocksdb::BpTreeRep::GetIterator(Arena *arena) {
  void *mem = arena ? arena->AllocateAligned(sizeof(BpTreeRep::Iterator)) :
                    operator new(sizeof(BpTreeRep::Iterator));
  return new (mem) BpTreeRep::Iterator(this);
}

void rocksdb::BpTreeRep::ConstructLeafNode() {
  auto iter = keys_.begin();
  char build_buf[max_build_buffer_size];
  // build each leaf node
  while (iter != keys_.end()) {
    auto *node = new LeafNode();
    allocated.push_back(node);
    uint32_t key_nums = 0;
    auto p_buf = build_buf;
    // each puts nums_order keys
    for (; key_nums < order && iter != keys_.end(); key_nums++, iter++) {
      // push key handle(key & value) as value
      node->value_ptr.push_back(*iter);
      // fetch memtable_key from key handle
      uint32_t key_len = 0;
      const char *p_internal_key = GetVarint32Ptr(*iter, *iter + 5, &key_len);
      // assert(key_len >= 8);
      // assert(p_internal_key - *iter >= 1);
      // assert(p_internal_key - *iter <= 5);
      // now key_len represents memtable_key size
      uint32_t memtable_key_len = key_len + p_internal_key - *iter;
      // copy memtable_key to buffer
      memcpy(p_buf, *iter, memtable_key_len);
      p_buf = p_buf + memtable_key_len;
    }
    node->key_nums = key_nums;
    node->key_buf = new char[p_buf - build_buf];
    memcpy(node->key_buf, build_buf, p_buf - build_buf);
    if (leaf_head_ == nullptr) {
      leaf_head_ = leaf_tail_ = node;
    } else {
      leaf_tail_->next = node;
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
  Node *loop_p = start;
  Node *head = nullptr;
  Node *tail = nullptr;
  char build_buf[max_build_buffer_size];
  // build each internal node
  while (loop_p != nullptr) {
    auto *node = new InternalNode();
    allocated.push_back(node);
    uint32_t key_nums = 0;
    auto p_buf = build_buf;
    // each puts nums_order keys
    for (; key_nums < order && loop_p != nullptr;
         key_nums++, loop_p = loop_p->next) {
      node->node_ptr.push_back(loop_p);
      // fetch memtable_key from key handle
      uint32_t key_len = 0;
      const char *p_internal_key =
          GetVarint32Ptr(loop_p->key_buf, loop_p->key_buf + 5, &key_len);
      // assert(key_len >= 8);
      // assert(p_internal_key - loop_p->key_buf >= 1);
      // assert(p_internal_key - loop_p->key_buf <= 5);
      // now key_len represents memtable_key size
      uint32_t memtable_key_len = key_len + p_internal_key - loop_p->key_buf;
      // copy memtable_key to buffer
      memcpy(p_buf, loop_p->key_buf, memtable_key_len);
      p_buf = p_buf + memtable_key_len;
    }
    node->key_nums = key_nums;
    node->key_buf = new char[p_buf - build_buf];
    memcpy(node->key_buf, build_buf, p_buf - build_buf);
    if (head == nullptr) {
      head = tail = node;
    } else {
      tail->next = node;
      tail = node;
    }
  }
  ConstructBpTree(head, tail);
}

Status rocksdb::BpTreeRep::Find(Node *cur, const char *key) const {
  Node *ret = nullptr;
  uint32_t index = 0;
  FindGreaterOrEqual(ret, index, cur, key);
  bool flag = (ret != nullptr) && compare_(key, ret->GetKey(index)) == 0;
  return flag ? Status::OK() : Status::NotFound();
}

void rocksdb::BpTreeRep::FindGreaterOrEqual(Node *&ret, uint32_t &index,
                                            Node *cur, const char *key) const {
  // leaf node
  if (cur->IsLeafNode()) {
    while (cur != nullptr) {
      char *cur_key = cur->GetKey(0);
      // condition that the key is less than any node keys
      if (compare_(key, cur_key) <= 0) {
        ret = cur;
        index = 0;
        return;
      }
      for (uint32_t i = 0; i < cur->key_nums; i++) {
        if (compare_(cur_key, key) >= 0) {
          ret = cur;
          index = i;
          return;
        }
        if (LIKELY(i != cur->key_nums - 1)) {
          cur_key = Node::NextKey(cur_key);
        }
      }
      cur = cur->next;
    }
    index = 0;
    ret = nullptr;
    return;
  }
  // none leaf node
  char *cur_key = cur->GetKey(0);
  // condition that the key is less than any node keys
  if (compare_(key, cur_key) <= 0) {
    FindGreaterOrEqual(ret, index,
                       dynamic_cast<InternalNode *>(cur)->node_ptr[0], key);
    return;
  }
  for (uint32_t i = 0; i < cur->key_nums; i++) {
    if (i == cur->key_nums - 1) {
      // assert(cur->key_nums != 1 && compare_(key, cur_key) >= 0);
      FindGreaterOrEqual(ret, index,
                         dynamic_cast<InternalNode *>(cur)->node_ptr[i], key);
      return;
    }
    char *next_key = Node::NextKey(cur_key);
    if (compare_(key, cur_key) >= 0 && compare_(key, next_key) < 0) {
      FindGreaterOrEqual(ret, index,
                         dynamic_cast<InternalNode *>(cur)->node_ptr[i], key);
      return;
    }
    cur_key = next_key;
  }
  index = 0;
  ret = nullptr;
}

}  // namespace

MemTableRep *BpTreeRepFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator &compare, Allocator *allocator,
    const SliceTransform *, Logger * /*logger*/) {
  return new BpTreeRep(compare, allocator);
}

MemTableRepFactory *NewBpTreeRepFactory() { return new BpTreeRepFactory; }

}  // namespace ROCKSDB_NAMESPACE

//
// Created by Chengjun Ying on 2021/5/6.
//

#include "db/memtable.h"
#include "memory/arena.h"
#include "rocksdb/memtablerep.h"

namespace ROCKSDB_NAMESPACE {
namespace {

class BpTreeRep : public MemTableRep {
 private:
  size_t memory_usage = 0;
 public:
//  explicit BpTreeRep(const KeyComparator& compare, Allocator* allocator);
//
//  KeyHandle Allocate(const size_t len, char** buf) override;
//  void Insert(KeyHandle handle) override;
//  bool Contains(const char* key) const override;
//  void Get(const LookupKey& k, void* callback_args,
//           bool (*callback_func)(void* arg, const char* entry)) override;
  KeyHandle Allocate(const size_t len, char **buf) override {
    return MemTableRep::Allocate(len, buf);
  }
  void Insert(KeyHandle handle) override {}
  bool Contains(const char *key) const override { return false; }
  void Get(const LookupKey &k, void *callback_args,
           bool (*callback_func)(void *, const char *)) override {
    MemTableRep::Get(k, callback_args, callback_func);
  }
  size_t ApproximateMemoryUsage() override { return memory_usage; }
  Iterator* GetIterator(Arena* arena) override { return nullptr; }
};

}  // namespace
}  // namespace ROCKSDB_NAMESPACE

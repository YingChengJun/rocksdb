//
// Created by Chengjun Ying on 2021/4/25.
//
#include <cstdio>
#include "memtable/bp_tree_rep.h"

int main(int argc, char **argv) {
  printf("hello world!");
  auto *rep = new ROCKSDB_NAMESPACE::BpTreeRep();
  delete rep;
}
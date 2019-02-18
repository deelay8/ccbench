#pragma once

#include <iostream>
#include <set>
#include <vector>

#include "/home/tanabe/package/tbb/include/tbb/scalable_allocator.h"

#include "../../include/inline.hpp"

#include "common.hpp"
#include "procedure.hpp"
#include "tuple.hpp"

using namespace std;

enum class TransactionStatus : uint8_t {
  inFlight,
  committed,
  aborted,
};

class Transaction {
public:
  int thid;
  uint64_t commit_ts;
  uint64_t appro_commit_ts;

  TransactionStatus status;
  vector<SetElement> readSet;
  vector<SetElement> writeSet;
  vector<unsigned int> cll; // current lock list;
  //use for lockWriteSet() to record locks;

  Transaction(int thid) {
    readSet.reserve(MAX_OPE);
    writeSet.reserve(MAX_OPE);
    cll.reserve(MAX_OPE);

    this->thid = thid;
  }

  void tbegin();
  int tread(unsigned int key);
  void twrite(unsigned int key, unsigned int val);
  bool validationPhase();
  void abort();
  void writePhase();
  void lockWriteSet();
  void unlockCLL();
  SetElement *searchWriteSet(unsigned int key);
  SetElement *searchReadSet(unsigned int key);
  void dispWS();
};
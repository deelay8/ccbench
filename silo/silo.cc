#include <cctype>
#include <ctype.h>
#include <algorithm>
#include <string.h>
#include <sched.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/time.h>

#define GLOBAL_VALUE_DEFINE
#include "include/atomic_tool.hpp"
#include "include/common.hpp"
#include "include/result.hpp"
#include "include/transaction.hpp"

#include "../include/debug.hpp"
#include "../include/fileio.hpp"
#include "../include/random.hpp"
#include "../include/tsc.hpp"
#include "../include/zipf.hpp"

using namespace std;

extern void chkArg(const int argc, char *argv[]);
extern bool chkClkSpan(uint64_t &start, uint64_t &stop, uint64_t threshold);
extern bool chkEpochLoaded();
extern void displayDB();
extern void displayPRO();
extern void makeDB();
extern void makeProcedure(Procedure *pro, Xoroshiro128Plus &rnd);
extern void makeProcedure(Procedure *pro, Xoroshiro128Plus &rnd, FastZipf &zipf);
extern void setThreadAffinity(int myid);
extern void waitForReadyOfAllThread();

void threadEndProcess(int *myid);

static void *
epoch_worker(void *arg)
{
//1. 40msごとに global epoch を必要に応じてインクリメントする
//2. 十分条件
//  全ての worker が最新の epoch を読み込んでいる。
//
  const int *myid = (int *)arg;
  uint64_t EpochTimerStart, EpochTimerStop;
  Result rsobject;

  setThreadAffinity(*myid);
  waitForReadyOfAllThread();

  //----------
  rsobject.Bgn = rdtsc();
  EpochTimerStart = rdtsc();

  for (;;) {
    usleep(1);
    rsobject.End = rdtsc();
    if (chkClkSpan(rsobject.Bgn, rsobject.End, EXTIME * 1000 * 1000 * CLOCK_PER_US)) {
      rsobject.Finish.store(true, std::memory_order_release);
      return nullptr;
    }

    EpochTimerStop = rdtsc();
    //chkEpochLoaded は最新のグローバルエポックを
    //全てのワーカースレッドが読み込んだか確認する．
    if (chkClkSpan(EpochTimerStart, EpochTimerStop, EPOCH_TIME * CLOCK_PER_US * 1000) && chkEpochLoaded()) {
      atomicAddGE();
      EpochTimerStart = EpochTimerStop;
    }
  }
  //----------

  return nullptr;
}

static void *
worker(void *arg)
{
  const int *myid = (int *)arg;
  Xoroshiro128Plus rnd;
  rnd.init();
  Procedure pro[MAX_OPE];
  TxnExecutor trans(*myid);
  Result rsobject;
  FastZipf zipf(&rnd, ZIPF_SKEW, TUPLE_NUM);
  
  /*
  std::string logpath("/work/tanabe/ccbench/silo/log/log");
  std::string logpath("/dev/sdb1/silo/log");
  logpath += std::to_string(*myid);
  cout << logpath << endl;
  createEmptyFile(logpath);
  trans.logfile.open(logpath, O_TRUNC | O_WRONLY, 0644);
  trans.logfile.ftruncate(10^9);
  */

  setThreadAffinity(*myid);
  //printf("Thread #%d: on CPU %d\n", *myid, sched_getcpu());
  //printf("sysconf(_SC_NPROCESSORS_CONF) %d\n", sysconf(_SC_NPROCESSORS_CONF));
  waitForReadyOfAllThread();
  
  try {
    //start work(transaction)
    for (;;) {
      if (YCSB)
        makeProcedure(pro, rnd, zipf);
      else
        makeProcedure(pro, rnd);

      asm volatile ("" ::: "memory");
RETRY:
      trans.tbegin();
      if (rsobject.Finish.load(memory_order_acquire)) {
        rsobject.sumUpCommitCounts();
        rsobject.sumUpAbortCounts();
        return nullptr;
      }

      //Read phase
      for (unsigned int i = 0; i < MAX_OPE; ++i) {
        if (pro[i].ope == Ope::READ) {
            trans.tread(pro[i].key);
        }
        else if (pro[i].ope == Ope::WRITE) {
          if (RMW) {
            trans.tread(pro[i].key);
            trans.twrite(pro[i].key, pro[i].val);
          }
          else
            trans.twrite(pro[i].key, pro[i].val);
        }
        else
          ERR;
      }
      
      //Validation phase
      if (trans.validationPhase()) {
        ++rsobject.localCommitCounts;
        trans.writePhase();
      } else {
        ++rsobject.localAbortCounts;
        trans.abort();
        goto RETRY;
      }

    }
  } catch (bad_alloc) {
    ERR;
  }

  return NULL;
}

pthread_t
threadCreate(int id)
{
  pthread_t t;
  int *myid;

  try {
    myid = new int;
  } catch (bad_alloc) {
    ERR;
  }
  *myid = id;

  if (*myid == 0) {
    if (pthread_create(&t, NULL, epoch_worker, (void *)myid)) ERR;
  } else {
    if (pthread_create(&t, NULL, worker, (void *)myid)) ERR;
  }

  return t;
}

int 
main(int argc, char *argv[]) 
{
  Result rsobject;
  chkArg(argc, argv);
  makeDB();
  
  //displayDB();
  //displayPRO();

  pthread_t thread[THREAD_NUM];

  for (unsigned int i = 0; i < THREAD_NUM; ++i) {
    thread[i] = threadCreate(i);
  }

  for (unsigned int i = 0; i < THREAD_NUM; ++i) {
    pthread_join(thread[i], NULL);
  }

  //displayDB();

  rsobject.displayTPS();
  //rsobject.displayAbortRate();

  return 0;
}
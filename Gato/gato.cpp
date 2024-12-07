#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <optional>
#include <chrono>
#include <cassert>
#include <algorithm>
#include <unordered_map>
#include <mutex>

#define PAGE_SIZE 4096
#define THREAD_NUM 64        // 스레드 개수
#define TUPLE_NUM 1000000    // 레코드 수
#define MAX_OPE 10           // 트랜잭션당 최대 작업 수
#define EX_TIME 3            // 실행 시간 (초)
#define BATCH_SIZE 1000      // 한 번에 처리할 트랜잭션 수

uint64_t tx_counter = 0;

class Result {
public:
    uint64_t commit_cnt_ = 0;
};

std::vector<Result> AllResult(THREAD_NUM);

enum class Ope { READ, WRITE };

class Task {
public:
    Ope ope_;
    uint64_t key_;

    Task(Ope ope, uint64_t key) : ope_(ope), key_(key) {}
};

// 버전 관리 클래스
class Tuple {
public:
    struct Version {
        uint64_t begin_timestamp_;
        uint64_t end_timestamp_;
        uint64_t value_;
        bool placeholder_;
        Version* prev_pointer_;

        Version(uint64_t begin, uint64_t end, uint64_t value, bool placeholder, Version* prev)
            : begin_timestamp_(begin), end_timestamp_(end), value_(value), placeholder_(placeholder), prev_pointer_(prev) {}
    };

    Version* latest_version_;

    Tuple() {
        latest_version_ = new Version(0, UINT64_MAX, 0, false, nullptr);
    }

    void addPlaceholder(uint64_t timestamp) {
        auto new_version = new Version(timestamp, UINT64_MAX, 0, true, latest_version_);
        if (latest_version_) {
            latest_version_->end_timestamp_ = timestamp;
        }
        latest_version_ = new_version;
    }

    bool updatePlaceholder(uint64_t timestamp, uint64_t value) {
        Version* version = latest_version_;
        while (version) {
            if (version->begin_timestamp_ == timestamp && version->placeholder_) {
                version->value_ = value;
                version->placeholder_ = false;
                return true;
            }
            version = version->prev_pointer_;
        }
        return false;
    }

    std::optional<uint64_t> getVersion(uint64_t timestamp) {
        Version* version = latest_version_;
        while (version) {
            if (version->begin_timestamp_ <= timestamp && timestamp < version->end_timestamp_ && !version->placeholder_) {
                return version->value_;
            }
            version = version->prev_pointer_;
        }
        return std::nullopt;
    }
};

Tuple *Table;

enum class Status { UNPROCESSED, EXECUTING, COMMITTED };

class Transaction {
public:
    uint64_t timestamp_;
    Status status_;
    std::vector<Task> task_set_;
    std::vector<std::pair<uint64_t, uint64_t>> read_set_;
    std::vector<uint64_t> write_set_;

    Transaction(uint64_t timestamp)
        : timestamp_(timestamp), status_(Status::UNPROCESSED) {}

    void startExecution() {
        status_ = Status::EXECUTING;
    }

    void commit() {
        read_set_.clear();
        write_set_.clear();
        status_ = Status::COMMITTED;
    }
};

// 동적 파티셔닝 관련 변수
std::unordered_map<uint64_t, int> record_to_thread_map; // 레코드 -> 스레드 매핑
std::vector<std::atomic<int>> thread_load(THREAD_NUM);  // 스레드 부하 추적
std::mutex partition_mutex;                             // 매핑 테이블 보호

void initializeDynamicPartitioning() {
    for (uint64_t i = 0; i < TUPLE_NUM; ++i) {
        int thread_id = i % THREAD_NUM;
        record_to_thread_map[i] = thread_id;
        thread_load[thread_id]++;
    }
}

bool isOverloaded(int thread_id) {
    const int overload_threshold = BATCH_SIZE / THREAD_NUM; // 배치 크기 기준
    return thread_load[thread_id] > overload_threshold;
}

void redistributeLoad(int overloaded_thread) {
    std::lock_guard<std::mutex> lock(partition_mutex);

    for (auto &[key, thread_id] : record_to_thread_map) {
        if (thread_id == overloaded_thread) {
            for (int target_thread = 0; target_thread < THREAD_NUM; ++target_thread) {
                if (!isOverloaded(target_thread)) {
                    record_to_thread_map[key] = target_thread;
                    thread_load[overloaded_thread]--;
                    thread_load[target_thread]++;
                    break;
                }
            }
        }
    }
}

void makeDB() {
    posix_memalign((void **)&Table, PAGE_SIZE, TUPLE_NUM * sizeof(Tuple));
    for (int i = 0; i < TUPLE_NUM; i++) {
        Table[i] = Tuple();
    }
    initializeDynamicPartitioning();
}

void concurrencyControlPhase(Transaction &trans, int thread_id) {
    for (const auto &task : trans.task_set_) {
        int assigned_thread;
        {
            std::lock_guard<std::mutex> lock(partition_mutex);
            assigned_thread = record_to_thread_map[task.key_];
        }

        if (assigned_thread == thread_id) {
            if (task.ope_ == Ope::WRITE) {
                Table[task.key_].addPlaceholder(trans.timestamp_);
                trans.write_set_.emplace_back(task.key_);
            }
        }
    }

    if (isOverloaded(thread_id)) {
        redistributeLoad(thread_id);
    }
}

void executeTransaction(Transaction &trans) {
    trans.startExecution();

    for (const auto &task : trans.task_set_) {
        if (task.ope_ == Ope::READ) {
            auto value = Table[task.key_].getVersion(trans.timestamp_);
            if (value.has_value()) {
                trans.read_set_.emplace_back(task.key_, value.value());
            } else {
                std::cerr << "Error: Missing version during execution.\n";
                return;
            }
        }
    }

    for (const auto &key : trans.write_set_) {
        Table[key].updatePlaceholder(trans.timestamp_, 100);
    }

    trans.commit();
}

void processBatch(std::vector<Transaction> &batch, int thread_id) {
    for (auto &trans : batch) {
        if (trans.status_ == Status::UNPROCESSED) {
            concurrencyControlPhase(trans, thread_id);
        }
    }

    for (auto &trans : batch) {
        if (trans.status_ == Status::UNPROCESSED) {
            executeTransaction(trans);
        }
    }
}

void worker(int thread_id, int &ready, const bool &start, const bool &quit) {
    Result &myres = AllResult[thread_id];
    std::vector<Transaction> batch;

    __atomic_store_n(&ready, 1, __ATOMIC_SEQ_CST);

    while (!__atomic_load_n(&start, __ATOMIC_SEQ_CST)) {}

    while (!__atomic_load_n(&quit, __ATOMIC_SEQ_CST)) {
        batch.clear();

        for (size_t i = 0; i < BATCH_SIZE; ++i) {
            uint64_t tx_pos = __atomic_fetch_add(&tx_counter, 1, __ATOMIC_SEQ_CST);
            if (tx_pos >= TUPLE_NUM) return;

            Transaction trans(tx_pos);
            trans.task_set_ = {
                Task(Ope::READ, tx_pos % TUPLE_NUM),
                Task(Ope::WRITE, (tx_pos + 1) % TUPLE_NUM)
            };
            batch.emplace_back(trans);
        }

        processBatch(batch, thread_id);
        myres.commit_cnt_ += batch.size();
    }
}

int main() {
    makeDB();

    bool start = false;
    bool quit = false;

    std::vector<int> readys(THREAD_NUM, 0);
    std::vector<std::thread> workers;

    for (size_t i = 0; i < THREAD_NUM; ++i) {
        workers.emplace_back(worker, i, std::ref(readys[i]), std::ref(start), std::ref(quit));
    }

    while (std::any_of(readys.begin(), readys.end(), [](int r) { return r == 0; })) {}

    __atomic_store_n(&start, true, __ATOMIC_SEQ_CST);
    std::this_thread::sleep_for(std::chrono::seconds(EX_TIME));
    __atomic_store_n(&quit, true, __ATOMIC_SEQ_CST);

    for (auto &worker : workers) {
        worker.join();
    }

    uint64_t total_commits = 0;
    for (const auto &result : AllResult) {
        total_commits += result.commit_cnt_;
    }

    std::cout << "Throughput: " << total_commits / EX_TIME << " txn/sec" << std::endl;
    return 0;
}

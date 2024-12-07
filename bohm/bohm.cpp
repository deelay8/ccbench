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

#define PAGE_SIZE 4096        // Memory page size
#define THREAD_NUM 64         // Number of worker threads
#define TUPLE_NUM 1000000     // Number of database records
#define MAX_OPE 10            // Maximum number of operations per transaction
#define EX_TIME 3             // Execution time in seconds
#define BATCH_SIZE 1000       // Batch size for processing transactions

uint64_t tx_counter = 0;      // Global transaction counter

class Result {
public:
    uint64_t commit_cnt_ = 0; // Count of committed transactions
};

std::vector<Result> AllResult(THREAD_NUM);

enum class Ope { READ, WRITE };

class Task {
public:
    Ope ope_;      // Operation type (READ or WRITE)
    uint64_t key_; // Key associated with the operation

    Task(Ope ope, uint64_t key) : ope_(ope), key_(key) {}
};

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

    Transaction()
        : timestamp_(0), status_(Status::UNPROCESSED) {}
    
    void startExecution() {
        status_ = Status::EXECUTING;
    }

    void commit() {
        read_set_.clear();
        write_set_.clear();
        status_ = Status::COMMITTED;
    }
};

// Static partitioning: Each thread owns a set of records
std::vector<std::vector<uint64_t>> thread_partitions(THREAD_NUM);

void assignRecordsToThreads() {
    for (uint64_t i = 0; i < TUPLE_NUM; ++i) {
        int thread_id = i % THREAD_NUM;
        thread_partitions[thread_id].push_back(i);
    }
}

std::vector<Transaction> transactions(TUPLE_NUM);
std::vector<Transaction> ready_queue;
std::mutex partition_mutex;

// Initializes the database table
void makeDB() {
    posix_memalign((void **)&Table, PAGE_SIZE, TUPLE_NUM * sizeof(Tuple));
    for (int i = 0; i < TUPLE_NUM; i++) {
        Table[i] = Tuple();
    }
}

// Initializes all transactions
void initializeTransactions() {
    for (uint64_t i = 0; i < TUPLE_NUM; ++i) {
        transactions[i] = Transaction(i);
        transactions[i].task_set_ = {
            Task(Ope::READ, i % TUPLE_NUM),
            Task(Ope::WRITE, (i + 1) % TUPLE_NUM)
        };
    }
}

// CC phase worker function
void cc_worker(int thread_id, const bool &start, const bool &quit) {
    while (!__atomic_load_n(&start, __ATOMIC_SEQ_CST)) {}

    while (!__atomic_load_n(&quit, __ATOMIC_SEQ_CST)) {
        std::vector<Transaction> local_batch;

        // Collect transactions for this thread's batch
        for (size_t i = 0; i < BATCH_SIZE; ++i) {
            uint64_t tx_pos = __atomic_fetch_add(&tx_counter, 1, __ATOMIC_SEQ_CST);
            if (tx_pos >= TUPLE_NUM) return;

            local_batch.emplace_back(transactions[tx_pos]);
        }

        // Process each transaction in the CC phase
        for (auto &trans : local_batch) {
            for (const auto &task : trans.task_set_) {
                if (task.ope_ == Ope::WRITE) {
                    Table[task.key_].addPlaceholder(trans.timestamp_);
                    trans.write_set_.emplace_back(task.key_);
                }
            }

            // Push the transaction to the ready queue
            {
                std::lock_guard<std::mutex> lock(partition_mutex);
                ready_queue.push_back(trans);
            }
        }
    }
}

// Execution phase worker function
void execution_worker(int thread_id, const bool &start, const bool &quit) {
    while (!__atomic_load_n(&start, __ATOMIC_SEQ_CST)) {}

    while (!__atomic_load_n(&quit, __ATOMIC_SEQ_CST)) {
        std::vector<Transaction> local_batch;

        // Retrieve transactions from the ready queue
        {
            std::lock_guard<std::mutex> lock(partition_mutex);
            if (ready_queue.empty()) continue;
            local_batch.swap(ready_queue);
        }

        // Process each transaction in the execution phase
        for (auto &trans : local_batch) {
            if (trans.status_ == Status::UNPROCESSED) {
                trans.startExecution();

                // Execute READ operations
                for (const auto &task : trans.task_set_) {
                    if (task.ope_ == Ope::READ) {
                        auto value = Table[task.key_].getVersion(trans.timestamp_);
                        if (value.has_value()) {
                            trans.read_set_.emplace_back(task.key_, value.value());
                        } else {
                            std::cerr << "Error: Missing version during execution. Key: " << task.key_ << "\n";
                            return;
                        }
                    }
                }

                // Execute WRITE operations
                for (const auto &key : trans.write_set_) {
                    Table[key].updatePlaceholder(trans.timestamp_, 100);
                }

                trans.commit();
                AllResult[thread_id].commit_cnt_++;
            }
        }
    }
}

int main() {
    makeDB();
    initializeTransactions();
    assignRecordsToThreads(); // Static partitioning of records

    bool start = false;
    bool quit = false;

    std::vector<std::thread> cc_workers, execution_workers;

    // Launch CC workers
    for (size_t i = 0; i < THREAD_NUM / 2; ++i) {
        cc_workers.emplace_back(cc_worker, i, std::ref(start), std::ref(quit));
    }

    // Launch Execution workers
    for (size_t i = THREAD_NUM / 2; i < THREAD_NUM; ++i) {
        execution_workers.emplace_back(execution_worker, i, std::ref(start), std::ref(quit));
    }

    __atomic_store_n(&start, true, __ATOMIC_SEQ_CST);
    std::this_thread::sleep_for(std::chrono::seconds(EX_TIME));
    __atomic_store_n(&quit, true, __ATOMIC_SEQ_CST);

    for (auto &worker : cc_workers) worker.join();
    for (auto &worker : execution_workers) worker.join();

    uint64_t total_commits = 0;
    for (const auto &result : AllResult) {
        total_commits += result.commit_cnt_;
    }

    std::cout << "Throughput: " << total_commits / EX_TIME << " txn/sec" << std::endl;
    return 0;
}

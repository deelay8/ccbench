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
#include <condition_variable>
#include <queue>
#include <functional> 

#define PAGE_SIZE 4096
#define DEFAULT_THREAD_NUM 8       // Default number of threads for debugging
#define DEFAULT_TUPLE_NUM 1000     // Default number of records for debugging
#define MAX_OPE 10                 // Maximum operations per transaction
#define EX_TIME 3                  // Execution time in seconds
#define BATCH_SIZE 50              // Reduced batch size for debugging
#define MAX_RETRY 10              // Max retries for failed transactions

uint64_t tx_counter = 0;           // Global transaction counter

class Result {
public:
    uint64_t commit_cnt_ = 0; // Count of committed transactions
};

std::vector<Result> AllResult;

enum class Ope { READ, WRITE };

class Task {
public:
    Ope ope_;
    uint64_t key_;

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
            : begin_timestamp_(begin), end_timestamp_(end), value_(value),
              placeholder_(placeholder), prev_pointer_(prev) {}
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

Tuple* Table;

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
std::vector<std::vector<uint64_t>> thread_partitions;

// Assigns records only to CC phase threads
void assignRecordsToCCThreads(size_t cc_thread_num, size_t tuple_num) {
    thread_partitions.resize(cc_thread_num);
    for (uint64_t i = 0; i < tuple_num; ++i) {
        int thread_id = i % cc_thread_num;
        thread_partitions[thread_id].push_back(i);
    }
}

// Debugging function to display record distribution
void debugRecordDistribution(size_t cc_thread_num) {
    for (size_t thread_id = 0; thread_id < cc_thread_num; ++thread_id) {
        std::cout << "[DEBUG] CC Thread " << thread_id << " assigned records: ";
        for (const auto& record : thread_partitions[thread_id]) {
            std::cout << record << " ";
        }
        std::cout << std::endl;
    }
}

std::vector<Transaction> transactions;
std::priority_queue<Transaction, std::vector<Transaction>, std::function<bool(const Transaction&, const Transaction&)>> ready_queue(
    [](const Transaction& a, const Transaction& b) {
        return a.timestamp_ > b.timestamp_; // Prioritize lower timestamps
    }
);
std::mutex partition_mutex;
std::condition_variable ready_queue_cv;

// Initializes the database table
void makeDB(size_t tuple_num) {
    posix_memalign((void**)&Table, PAGE_SIZE, tuple_num * sizeof(Tuple));
    for (size_t i = 0; i < tuple_num; i++) {
        Table[i] = Tuple();
    }
}

// Initializes all transactions
void initializeTransactions(size_t tuple_num) {
    transactions.resize(tuple_num);
    for (uint64_t i = 0; i < tuple_num; ++i) {
        transactions[i] = Transaction(i);
        transactions[i].task_set_ = {
            Task(Ope::READ, i % tuple_num),
            Task(Ope::WRITE, (i + 1) % tuple_num)
        };
    }
}

// CC phase worker function
void cc_worker(int thread_id, const bool& start, const bool& quit) {
    while (!__atomic_load_n(&start, __ATOMIC_SEQ_CST)) {}

    while (!__atomic_load_n(&quit, __ATOMIC_SEQ_CST)) {
        std::vector<Transaction> local_batch;

        // Fetch a batch of transactions
        uint64_t start_pos = __atomic_fetch_add(&tx_counter, BATCH_SIZE, __ATOMIC_SEQ_CST);
        uint64_t end_pos = std::min(start_pos + BATCH_SIZE, (uint64_t)transactions.size());
        for (uint64_t i = start_pos; i < end_pos; ++i) {
            local_batch.emplace_back(transactions[i]);
        }

        // sort
        std::sort(local_batch.begin(), local_batch.end(), [](const Transaction& a, const Transaction& b) {
            return a.timestamp_ < b.timestamp_;
        });

        // Process each transaction in the CC phase
        for (auto& trans : local_batch) {
            for (const auto& task : trans.task_set_) {
                if (task.ope_ == Ope::WRITE &&
                    std::find(thread_partitions[thread_id].begin(),
                              thread_partitions[thread_id].end(),
                              task.key_) != thread_partitions[thread_id].end()) {
                    Table[task.key_].addPlaceholder(trans.timestamp_);
                    trans.write_set_.emplace_back(task.key_);
                }
            }

            // Push the transaction to the ready queue
            {
                std::lock_guard<std::mutex> lock(partition_mutex);
                ready_queue.push(trans);
            }
        }
        ready_queue_cv.notify_all(); // Notify Execution Workers
    }
}

void execution_worker(int thread_id, const bool& start, const bool& quit) {
    while (!__atomic_load_n(&start, __ATOMIC_SEQ_CST)) {}

    while (!__atomic_load_n(&quit, __ATOMIC_SEQ_CST)) {
        std::vector<Transaction> local_batch;

        {
            std::unique_lock<std::mutex> lock(partition_mutex);
            while (ready_queue.empty() && !quit) {
                ready_queue_cv.wait(lock);
            }
            // Retrieve the transaction with the lowest timestamp from the priority queue
            while (!ready_queue.empty()) {
                local_batch.push_back(ready_queue.top());
                ready_queue.pop();
            }
        }

        for (auto& trans : local_batch) {
            if (trans.status_ == Status::UNPROCESSED) {
                trans.startExecution();
                std::cout << "[DEBUG] Thread " << thread_id << ": Executing transaction " 
                          << trans.timestamp_ << std::endl;

                bool success = true;
                int retry_count = 0;

                do {
                    success = true;
                    for (const auto& task : trans.task_set_) {
                        switch (task.ope_) {
                        case Ope::READ: {
                            auto value = Table[task.key_].getVersion(trans.timestamp_);
                            if (!value.has_value()) {
                                success = false;
                                retry_count++;
                                std::cerr << "[DEBUG] Thread " << thread_id 
                                          << ": Missing version for key " << task.key_ 
                                          << " in transaction " << trans.timestamp_ 
                                          << ", retry count: " << retry_count << std::endl;
                                std::this_thread::sleep_for(std::chrono::microseconds(10));
                            } else {
                                std::cout << "[DEBUG] Thread " << thread_id 
                                          << ": READ value " << value.value() 
                                          << " for key " << task.key_ 
                                          << " in transaction " << trans.timestamp_ << std::endl;
                            }
                            break;
                        }
                        case Ope::WRITE: {
                            Table[task.key_].updatePlaceholder(trans.timestamp_, 100);
                            std::cout << "[DEBUG] Thread " << thread_id 
                                      << ": Updated WRITE placeholder for key " << task.key_ 
                                      << " in transaction " << trans.timestamp_ << std::endl;
                            break;
                        }
                        default:
                            std::cerr << "[ERROR] Thread " << thread_id 
                                      << ": Unknown operation type in transaction " 
                                      << trans.timestamp_ << std::endl;
                            success = false; 
                        }
                        if (!success) break;
                    }
                } while (!success && retry_count < MAX_RETRY);

                if (success) {
                    trans.commit();
                    AllResult[thread_id].commit_cnt_++;
                    std::cout << "[DEBUG] Thread " << thread_id 
                              << ": Transaction " << trans.timestamp_ 
                              << " committed successfully after " 
                              << retry_count << " retries" << std::endl;
                } else {
                    std::cerr << "[ERROR] Thread " << thread_id 
                              << ": Transaction " << trans.timestamp_ 
                              << " failed after max retries (" 
                              << retry_count << ")" << std::endl;
                }
            }
        }
    }
}


int main(int argc, char* argv[]) {
    size_t thread_num = DEFAULT_THREAD_NUM;
    size_t tuple_num = DEFAULT_TUPLE_NUM;

    if (argc > 1) thread_num = std::stoul(argv[1]);
    if (argc > 2) tuple_num = std::stoul(argv[2]);

    AllResult.resize(thread_num);

    size_t cc_thread_num = thread_num / 2;
    size_t exec_thread_num = thread_num - cc_thread_num;

    makeDB(tuple_num);
    initializeTransactions(tuple_num);
    assignRecordsToCCThreads(cc_thread_num, tuple_num);

    // Debug record distribution
    debugRecordDistribution(cc_thread_num);

    bool start = false;
    bool quit = false;

    std::vector<std::thread> cc_workers, execution_workers;

    // Launch CC workers
    for (size_t i = 0; i < cc_thread_num; ++i) {
        cc_workers.emplace_back(cc_worker, i, std::ref(start), std::ref(quit));
    }

    // Launch Execution workers
    for (size_t i = 0; i < exec_thread_num; ++i) {
        execution_workers.emplace_back(execution_worker, i + cc_thread_num, std::ref(start), std::ref(quit));
    }

    __atomic_store_n(&start, true, __ATOMIC_SEQ_CST);
    std::this_thread::sleep_for(std::chrono::seconds(EX_TIME));
    __atomic_store_n(&quit, true, __ATOMIC_SEQ_CST);

    for (auto& worker : cc_workers) worker.join();
    for (auto& worker : execution_workers) worker.join();

    uint64_t total_commits = 0;
    for (const auto& result : AllResult) {
        total_commits += result.commit_cnt_;
    }

    std::cout << "Throughput: " << total_commits / EX_TIME << " txn/sec" << std::endl;
    return 0;
}

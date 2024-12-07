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

// Result class to store the number of committed transactions for each thread
class Result {
public:
    uint64_t commit_cnt_ = 0; // Count of committed transactions
};

// Vector to store the results of all threads
std::vector<Result> AllResult(THREAD_NUM);

// Enumeration to represent the type of operations in a transaction
enum class Ope { READ, WRITE };

// Task class represents an individual operation in a transaction
class Task {
public:
    Ope ope_;      // Operation type (READ or WRITE)
    uint64_t key_; // Key associated with the operation

    Task(Ope ope, uint64_t key) : ope_(ope), key_(key) {}
};

// Tuple class represents a database record and its version history
class Tuple {
public:
    struct Version {
        uint64_t begin_timestamp_; // Start timestamp for the version
        uint64_t end_timestamp_;   // End timestamp for the version
        uint64_t value_;           // Value stored in this version
        bool placeholder_;         // Placeholder flag (used during CC phase)
        Version* prev_pointer_;    // Pointer to the previous version

        Version(uint64_t begin, uint64_t end, uint64_t value, bool placeholder, Version* prev)
            : begin_timestamp_(begin), end_timestamp_(end), value_(value), placeholder_(placeholder), prev_pointer_(prev) {}
    };

    Version* latest_version_; // Pointer to the latest version of the tuple

    Tuple() {
        latest_version_ = new Version(0, UINT64_MAX, 0, false, nullptr);
    }

    // Adds a placeholder version to the tuple
    void addPlaceholder(uint64_t timestamp) {
        auto new_version = new Version(timestamp, UINT64_MAX, 0, true, latest_version_);
        if (latest_version_) {
            latest_version_->end_timestamp_ = timestamp;
        }
        latest_version_ = new_version;
    }

    // Updates a placeholder version with the actual value
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

    // Retrieves the appropriate version for a given timestamp
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

Tuple *Table; // Pointer to the database table (array of tuples)

// Enumeration to represent the status of a transaction
enum class Status { UNPROCESSED, EXECUTING, COMMITTED };

// Transaction class represents a single database transaction
class Transaction {
public:
    uint64_t timestamp_; // Timestamp assigned to the transaction
    Status status_;      // Current status of the transaction
    std::vector<Task> task_set_;              // Set of tasks (operations) in the transaction
    std::vector<std::pair<uint64_t, uint64_t>> read_set_; // Set of read operations
    std::vector<uint64_t> write_set_;         // Set of write operations

    Transaction(uint64_t timestamp)
        : timestamp_(timestamp), status_(Status::UNPROCESSED) {}

    // Marks the transaction as executing
    void startExecution() {
        status_ = Status::EXECUTING;
    }

    // Commits the transaction and clears its read/write sets
    void commit() {
        read_set_.clear();
        write_set_.clear();
        status_ = Status::COMMITTED;
    }
};

// Vector to store all transactions
std::vector<Transaction> transactions(TUPLE_NUM);

// Mutex to synchronize access to shared resources
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
void cc_worker(int thread_id, const bool &start, const bool &quit, std::vector<Transaction> &ready_queue) {
    while (!__atomic_load_n(&start, __ATOMIC_SEQ_CST)) {}

    while (!__atomic_load_n(&quit, __ATOMIC_SEQ_CST)) {
        for (auto &trans : transactions) {
            if (trans.status_ == Status::UNPROCESSED) {
                for (const auto &task : trans.task_set_) {
                    if (task.ope_ == Ope::WRITE) {
                        Table[task.key_].addPlaceholder(trans.timestamp_);
                        trans.write_set_.emplace_back(task.key_);
                    }
                }

                // Add the transaction to the ready queue
                {
                    std::lock_guard<std::mutex> lock(partition_mutex);
                    ready_queue.push_back(trans);
                }
            }
        }
    }
}

// Execution phase worker function
void execution_worker(int thread_id, const bool &start, const bool &quit, std::vector<Transaction> &ready_queue) {
    while (!__atomic_load_n(&start, __ATOMIC_SEQ_CST)) {}

    while (!__atomic_load_n(&quit, __ATOMIC_SEQ_CST)) {
        std::vector<Transaction> local_batch;

        // Retrieve transactions from the ready queue
        {
            std::lock_guard<std::mutex> lock(partition_mutex);
            local_batch.swap(ready_queue);
        }

        // Process each transaction in the batch
        for (auto &trans : local_batch) {
            if (trans.status_ == Status::UNPROCESSED) {
                trans.status_ = Status::EXECUTING;

                // Execute READ operations
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

                // Execute WRITE operations
                for (const auto &key : trans.write_set_) {
                    Table[key].updatePlaceholder(trans.timestamp_, 100);
                }

                trans.commit();

                // Increment the committed transaction count for this thread
                AllResult[thread_id].commit_cnt_++;
            }
        }
    }
}

int main() {
    makeDB();
    initializeTransactions();

    bool start = false;
    bool quit = false;

    std::vector<std::thread> cc_workers, execution_workers;
    std::vector<Transaction> ready_queue;

    // Launch CC and execution workers
    for (size_t i = 0; i < THREAD_NUM / 2; ++i) {
        cc_workers.emplace_back(cc_worker, i, std::ref(start), std::ref(quit), std::ref(ready_queue));
        execution_workers.emplace_back(execution_worker, i, std::ref(start), std::ref(quit), std::ref(ready_queue));
    }

    // Start execution
    __atomic_store_n(&start, true, __ATOMIC_SEQ_CST);
    std::this_thread::sleep_for(std::chrono::seconds(EX_TIME));
    __atomic_store_n(&quit, true, __ATOMIC_SEQ_CST);

    // Wait for all threads to finish
    for (auto &worker : cc_workers) worker.join();
    for (auto &worker : execution_workers) worker.join();

    // Calculate throughput
    uint64_t total_commits = 0;
    for (const auto &result : AllResult) {
        total_commits += result.commit_cnt_;
    }

    std::cout << "Throughput: " << total_commits / EX_TIME << " txn/sec" << std::endl;
    return 0;
}

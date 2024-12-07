#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <optional>
#include <chrono>
#include <cassert>
#include <algorithm>

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
        uint64_t begin_timestamp_;  // 버전 유효 시작 시간
        uint64_t end_timestamp_;    // 버전 유효 종료 시간
        uint64_t value_;            // 값
        bool placeholder_;          // Placeholder 여부
        Version* prev_pointer_;     // 이전 버전에 대한 포인터

        Version(uint64_t begin, uint64_t end, uint64_t value, bool placeholder, Version* prev)
            : begin_timestamp_(begin), end_timestamp_(end), value_(value), placeholder_(placeholder), prev_pointer_(prev) {}
    };

    Version* latest_version_; // 가장 최신 버전에 대한 포인터

    Tuple() {
        latest_version_ = new Version(0, UINT64_MAX, 0, false, nullptr);
    }

    // Placeholder 추가 (쓰기 작업 시 호출)
    void addPlaceholder(uint64_t timestamp) {
        auto new_version = new Version(timestamp, UINT64_MAX, 0, true, latest_version_);

        // 이전 버전의 종료 타임스탬프 설정
        if (latest_version_) {
            latest_version_->end_timestamp_ = timestamp;
        }

        // 최신 버전을 업데이트
        latest_version_ = new_version;
    }

    // Placeholder를 실제 값으로 업데이트
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
        return false; // 해당 Placeholder를 찾지 못함
    }

    // 특정 타임스탬프에 유효한 버전 가져오기
    std::optional<uint64_t> getVersion(uint64_t timestamp) {
        Version* version = latest_version_;
        while (version) {
            if (version->begin_timestamp_ <= timestamp && timestamp < version->end_timestamp_ && !version->placeholder_) {
                return version->value_;
            }
            version = version->prev_pointer_;
        }
        return std::nullopt; // 유효한 버전이 없음
    }
};

Tuple *Table;

enum class Status { UNPROCESSED, EXECUTING, COMMITTED };

// 트랜잭션 정의
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

// 정적 파티셔닝
std::vector<std::vector<uint64_t>> thread_partitions(THREAD_NUM);

void assignRecordsToThreads() {
    for (uint64_t i = 0; i < TUPLE_NUM; ++i) {
        int thread_id = i % THREAD_NUM;
        thread_partitions[thread_id].push_back(i);
    }
}

// DB 초기화
void makeDB() {
    posix_memalign((void **)&Table, PAGE_SIZE, TUPLE_NUM * sizeof(Tuple));
    for (int i = 0; i < TUPLE_NUM; i++) {
        Table[i] = Tuple();
    }
}

// CC Phase
void concurrencyControlPhase(Transaction &trans, int thread_id) {
    for (const auto &task : trans.task_set_) {
        if (std::find(thread_partitions[thread_id].begin(), thread_partitions[thread_id].end(), task.key_) 
            != thread_partitions[thread_id].end()) {
            if (task.ope_ == Ope::WRITE) {
                Table[task.key_].addPlaceholder(trans.timestamp_);
                trans.write_set_.emplace_back(task.key_);
            }
        }
    }
}

// Execution Phase
void executeTransaction(Transaction &trans) {
    trans.startExecution();

    // Read 작업 수행
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

    // Write 작업 수행
    for (const auto &key : trans.write_set_) {
        Table[key].updatePlaceholder(trans.timestamp_, 100); // 예제 값 100
    }

    trans.commit();
}

// 배치 처리
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

// 스레드 작업
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

    assignRecordsToThreads();

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

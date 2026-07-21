#pragma once

// Lock-free single-producer/single-consumer ring buffer over POSIX shared
// memory (shm_open + mmap). No syscalls on the hot path: the producer and
// the consumer synchronize purely through two atomic indices.
//
// Design notes:
//  - head/tail are monotonically increasing 64-bit counters, never wrapped;
//    the position in the buffer is (index & mask). This removes the classic
//    "full vs empty" ambiguity and makes used/free space a plain subtraction.
//  - head and tail live on separate cache lines to avoid false sharing.
//  - Each side keeps a cached copy of the other side's index and re-reads
//    the shared atomic only when the cached value is not enough to proceed.
//    This keeps the hot path free of cache-line ping-pong.
//  - Records are always contiguous. If a record does not fit before the end
//    of the buffer, the producer writes kWrapMarker into the size field and
//    the record starts at offset 0. Since every record size is a multiple
//    of 8, there are always at least 4 bytes for the marker.
//  - The producer publishes a record with a release store to head; the
//    consumer acquires it and reads the payload in place (zero copy on the
//    consuming side). Symmetrically for tail.

#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <new>
#include <stdexcept>
#include <string>
#include <system_error>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "protocol.hpp"

namespace ipc {

class SpscRing {
    static constexpr uint32_t kMagic = 0x50414B52;  // "PAKR"
    static constexpr size_t kDataOffset = 4096;     // control block page

    struct ControlBlock {
        uint32_t magic;
        uint64_t capacity;
        std::atomic<uint32_t> ready;
        alignas(64) std::atomic<uint64_t> head;  // written by producer only
        alignas(64) std::atomic<uint64_t> tail;  // written by consumer only
    };
    static_assert(sizeof(ControlBlock) <= kDataOffset);

public:
    SpscRing(const SpscRing&) = delete;
    SpscRing& operator=(const SpscRing&) = delete;

    SpscRing(SpscRing&& other) noexcept { *this = std::move(other); }
    SpscRing& operator=(SpscRing&& other) noexcept {
        if (this != &other) {
            close();
            cb_ = other.cb_;
            data_ = other.data_;
            capacity_ = other.capacity_;
            mask_ = other.mask_;
            map_len_ = other.map_len_;
            name_ = std::move(other.name_);
            owner_ = other.owner_;
            cached_tail_ = other.cached_tail_;
            cached_head_ = other.cached_head_;
            pending_ = other.pending_;
            other.cb_ = nullptr;
            other.owner_ = false;
        }
        return *this;
    }

    ~SpscRing() { close(); }

    // Producer side: creates the segment, removing a stale one if a previous
    // run crashed. `capacity` must be a power of two.
    static SpscRing create(const std::string& name, size_t capacity) {
        if (capacity == 0 || (capacity & (capacity - 1)) != 0) {
            throw std::invalid_argument("ring capacity must be a power of two");
        }
        shm_unlink(name.c_str());
        int fd = shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
        if (fd < 0) {
            throw std::system_error(errno, std::generic_category(), "shm_open(create)");
        }
        const size_t total = kDataOffset + capacity;
        if (ftruncate(fd, static_cast<off_t>(total)) != 0) {
            int e = errno;
            ::close(fd);
            shm_unlink(name.c_str());
            throw std::system_error(e, std::generic_category(), "ftruncate");
        }
        void* mem = mmap(nullptr, total, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        ::close(fd);
        if (mem == MAP_FAILED) {
            shm_unlink(name.c_str());
            throw std::system_error(errno, std::generic_category(), "mmap");
        }

        SpscRing r;
        r.cb_ = new (mem) ControlBlock{};
        r.data_ = static_cast<uint8_t*>(mem) + kDataOffset;
        r.capacity_ = capacity;
        r.mask_ = capacity - 1;
        r.map_len_ = total;
        r.name_ = name;
        r.owner_ = true;

        // Pre-fault all pages so the benchmark does not measure page faults.
        std::memset(r.data_, 0, capacity);

        r.cb_->magic = kMagic;
        r.cb_->capacity = capacity;
        r.cb_->ready.store(1, std::memory_order_release);
        return r;
    }

    // Consumer side: waits until the producer has created and initialized
    // the segment. `stop` aborts the wait (e.g. on SIGINT).
    static SpscRing open(const std::string& name,
                         const volatile std::sig_atomic_t* stop = nullptr) {
        int fd = -1;
        size_t total = 0;
        for (;;) {
            if (stop && *stop) throw std::runtime_error("interrupted");
            fd = shm_open(name.c_str(), O_RDWR, 0);
            if (fd >= 0) {
                struct stat st{};
                if (fstat(fd, &st) == 0 &&
                    st.st_size >= static_cast<off_t>(kDataOffset)) {
                    total = static_cast<size_t>(st.st_size);
                    break;
                }
                ::close(fd);
            } else if (errno != ENOENT) {
                throw std::system_error(errno, std::generic_category(), "shm_open(open)");
            }
            sleep_ms(50);
        }
        void* mem = mmap(nullptr, total, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        ::close(fd);
        if (mem == MAP_FAILED) {
            throw std::system_error(errno, std::generic_category(), "mmap");
        }

        SpscRing r;
        r.cb_ = static_cast<ControlBlock*>(mem);
        r.map_len_ = total;
        r.name_ = name;
        while (r.cb_->ready.load(std::memory_order_acquire) != 1) {
            if (stop && *stop) throw std::runtime_error("interrupted");
            sleep_ms(1);
        }
        if (r.cb_->magic != kMagic ||
            total != kDataOffset + r.cb_->capacity) {
            throw std::runtime_error("shared memory segment is not a valid ring");
        }
        r.data_ = static_cast<uint8_t*>(mem) + kDataOffset;
        r.capacity_ = r.cb_->capacity;
        r.mask_ = r.capacity_ - 1;
        return r;
    }

    size_t capacity() const { return capacity_; }

    // The worst-case wrap wastes just under one record of space, so a record
    // may need up to ~2x its size of free room; capacity/2 is always safe.
    size_t max_record_size() const { return capacity_ / 2; }

    // ---- producer side ----------------------------------------------------

    // Copies header + payload into the ring. Returns false if there is not
    // enough free space (the caller decides how to wait).
    bool try_push(const RecordHeader& hdr, const void* payload) {
        const size_t rec = record_size(hdr.payload_size);
        uint64_t head = cb_->head.load(std::memory_order_relaxed);
        const size_t pos = head & mask_;
        const size_t to_end = capacity_ - pos;
        const size_t need = (rec <= to_end) ? rec : to_end + rec;

        if (capacity_ - (head - cached_tail_) < need) {
            cached_tail_ = cb_->tail.load(std::memory_order_acquire);
            if (capacity_ - (head - cached_tail_) < need) return false;
        }

        uint8_t* p;
        if (rec <= to_end) {
            p = data_ + pos;
        } else {
            std::memcpy(data_ + pos, &kWrapMarker, sizeof(kWrapMarker));
            head += to_end;
            p = data_;
        }
        std::memcpy(p, &hdr, sizeof(hdr));
        std::memcpy(p + sizeof(hdr), payload, hdr.payload_size);
        cb_->head.store(head + rec, std::memory_order_release);
        return true;
    }

    // ---- consumer side ----------------------------------------------------

    // Returns the next record, or nullptr if the ring is empty. The record
    // (header + payload) is valid in place until pop_end() is called.
    const RecordHeader* try_pop_begin() {
        for (;;) {
            const uint64_t tail = cb_->tail.load(std::memory_order_relaxed);
            if (cached_head_ == tail) {
                cached_head_ = cb_->head.load(std::memory_order_acquire);
                if (cached_head_ == tail) return nullptr;
            }
            const size_t pos = tail & mask_;
            uint32_t size_field;
            std::memcpy(&size_field, data_ + pos, sizeof(size_field));
            if (size_field == kWrapMarker) {
                cb_->tail.store(tail + (capacity_ - pos), std::memory_order_release);
                continue;
            }
            pending_ = record_size(size_field);
            return reinterpret_cast<const RecordHeader*>(data_ + pos);
        }
    }

    // Releases the record returned by the last try_pop_begin().
    void pop_end() {
        cb_->tail.store(cb_->tail.load(std::memory_order_relaxed) + pending_,
                        std::memory_order_release);
    }

private:
    SpscRing() = default;

    void close() {
        if (cb_) {
            munmap(cb_, map_len_);
            if (owner_) shm_unlink(name_.c_str());
            cb_ = nullptr;
        }
    }

    static void sleep_ms(long ms) {
        timespec ts{ms / 1000, (ms % 1000) * 1'000'000L};
        nanosleep(&ts, nullptr);
    }

    ControlBlock* cb_ = nullptr;
    uint8_t* data_ = nullptr;
    size_t capacity_ = 0;
    size_t mask_ = 0;
    size_t map_len_ = 0;
    std::string name_;
    bool owner_ = false;

    uint64_t cached_tail_ = 0;  // producer's cache of tail
    uint64_t cached_head_ = 0;  // consumer's cache of head
    size_t pending_ = 0;        // consumer: size of the record being read
};

}  // namespace ipc

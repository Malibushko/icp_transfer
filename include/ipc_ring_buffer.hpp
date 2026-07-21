#pragma once

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

class RingBuffer {
    static constexpr uint32_t kMagic = 0x50414B52;
    static constexpr size_t kDataOffset = 4096;

    struct ControlBlock {
        uint32_t magic;
        uint64_t capacity;
        std::atomic<uint32_t> ready;
        alignas(64) std::atomic<uint64_t> head;
        alignas(64) std::atomic<uint64_t> tail;
    };
    static_assert(sizeof(ControlBlock) <= kDataOffset);

public:
    RingBuffer(const RingBuffer&) = delete;
    RingBuffer& operator=(const RingBuffer&) = delete;

    RingBuffer(RingBuffer&& other) noexcept { *this = std::move(other); }
    RingBuffer& operator=(RingBuffer&& other) noexcept {
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

    ~RingBuffer() { close(); }

    static RingBuffer create(const std::string& name, size_t capacity) {
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

        RingBuffer r;
        r.cb_ = new (mem) ControlBlock{};
        r.data_ = static_cast<uint8_t*>(mem) + kDataOffset;
        r.capacity_ = capacity;
        r.mask_ = capacity - 1;
        r.map_len_ = total;
        r.name_ = name;
        r.owner_ = true;

        std::memset(r.data_, 0, capacity);

        r.cb_->magic = kMagic;
        r.cb_->capacity = capacity;
        r.cb_->ready.store(1, std::memory_order_release);
        return r;
    }

    static RingBuffer open(const std::string& name,
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

        RingBuffer r;
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

    size_t max_record_size() const { return capacity_ / 2; }

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

    void pop_end() {
        cb_->tail.store(cb_->tail.load(std::memory_order_relaxed) + pending_,
                        std::memory_order_release);
    }

private:
    RingBuffer() = default;

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

    uint64_t cached_tail_ = 0;
    uint64_t cached_head_ = 0;
    size_t pending_ = 0;
};

}

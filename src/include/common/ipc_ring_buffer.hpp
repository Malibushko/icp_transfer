#pragma once

#include <atomic>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <new>
#include <stdexcept>
#include <string>
#include <utility>

#include "clock.hpp"
#include "protocol.hpp"
#include "shared_memory.hpp"

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
    RingBuffer(RingBuffer&&) noexcept = default;
    RingBuffer& operator=(RingBuffer&&) noexcept = default;
    ~RingBuffer() = default;

    static RingBuffer create(const std::string& name, size_t capacity) {
        if (capacity == 0 || (capacity & (capacity - 1)) != 0) {
            throw std::invalid_argument("ring capacity must be a power of two");
        }
        RingBuffer r;
        r.region_ = MappedRegion::create(name, kDataOffset + capacity);
        r.cb_ = new (r.region_.data()) ControlBlock{};
        r.data_ = static_cast<uint8_t*>(r.region_.data()) + kDataOffset;
        r.capacity_ = capacity;
        r.mask_ = capacity - 1;

        std::memset(r.data_, 0, capacity);
        r.cb_->magic = kMagic;
        r.cb_->capacity = capacity;
        r.cb_->ready.store(1, std::memory_order_release);
        return r;
    }

    static RingBuffer open(const std::string& name,
                           const volatile std::sig_atomic_t* stop = nullptr) {
        RingBuffer r;
        r.region_ = MappedRegion::open(name, kDataOffset, stop);
        r.cb_ = static_cast<ControlBlock*>(r.region_.data());

        while (r.cb_->ready.load(std::memory_order_acquire) != 1) {
            if (stop && *stop) throw std::runtime_error("interrupted");
            sleep_ms(1);
        }
        if (r.cb_->magic != kMagic ||
            r.region_.size() != kDataOffset + r.cb_->capacity) {
            throw std::runtime_error("shared memory segment is not a valid ring");
        }
        r.data_ = static_cast<uint8_t*>(r.region_.data()) + kDataOffset;
        r.capacity_ = r.cb_->capacity;
        r.mask_ = r.capacity_ - 1;
        return r;
    }

    [[nodiscard]] size_t capacity() const { return capacity_; }

    [[nodiscard]] size_t max_record_size() const { return capacity_ / 2; }

    bool try_push(const RecordHeader& hdr, const void* payload) {
        const size_t record_bytes = record_size(hdr.payload_size);
        uint64_t head = cb_->head.load(std::memory_order_relaxed);
        const size_t offset = head & mask_;
        const size_t bytes_to_end = capacity_ - offset;
        const size_t needed = (record_bytes <= bytes_to_end)
                                  ? record_bytes
                                  : bytes_to_end + record_bytes;

        if (free_space(head) < needed) {
            cached_tail_ = cb_->tail.load(std::memory_order_acquire);
            if (free_space(head) < needed) return false;
        }

        uint8_t* dst;
        if (record_bytes <= bytes_to_end) {
            dst = data_ + offset;
        } else {
            std::memcpy(data_ + offset, &kWrapMarker, sizeof(kWrapMarker));
            head += bytes_to_end;
            dst = data_;
        }
        std::memcpy(dst, &hdr, sizeof(hdr));
        std::memcpy(dst + sizeof(hdr), payload, hdr.payload_size);
        cb_->head.store(head + record_bytes, std::memory_order_release);
        return true;
    }

    const RecordHeader* try_pop_begin() {
        for (;;) {
            const uint64_t tail = cb_->tail.load(std::memory_order_relaxed);
            if (cached_head_ == tail) {
                cached_head_ = cb_->head.load(std::memory_order_acquire);
                if (cached_head_ == tail) return nullptr;
            }
            const size_t offset = tail & mask_;
            uint32_t payload_size;
            std::memcpy(&payload_size, data_ + offset, sizeof(payload_size));
            if (payload_size == kWrapMarker) {
                cb_->tail.store(tail + (capacity_ - offset), std::memory_order_release);
                continue;
            }
            pending_pop_size_ = record_size(payload_size);
            return reinterpret_cast<const RecordHeader*>(data_ + offset);
        }
    }

    void pop_end() {
        cb_->tail.store(cb_->tail.load(std::memory_order_relaxed) + pending_pop_size_,
                        std::memory_order_release);
    }

private:
    RingBuffer() = default;

    [[nodiscard]] size_t free_space(uint64_t head) const {
        return capacity_ - (head - cached_tail_);
    }

    MappedRegion region_;
    ControlBlock* cb_ = nullptr;
    uint8_t* data_ = nullptr;
    size_t capacity_ = 0;
    size_t mask_ = 0;

    uint64_t cached_tail_ = 0;
    uint64_t cached_head_ = 0;
    size_t pending_pop_size_ = 0;
};

}

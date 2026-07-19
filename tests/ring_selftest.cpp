// Self-test for the SPSC ring: a producer thread pushes records of random
// sizes through a small ring (forcing plenty of wrap-arounds and full-ring
// waits), a consumer thread validates size, seq, CRC and byte content.

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <random>
#include <thread>
#include <vector>

#include "control.hpp"
#include "crc32c.hpp"
#include "ring_buffer.hpp"

namespace {

constexpr uint64_t kPackets = 2'000'000;
constexpr size_t kRingBytes = 1 << 20;  // 1 MiB — small on purpose
constexpr size_t kMaxPayload = 3000;

uint8_t byte_for(uint64_t seq, size_t i) {
    return static_cast<uint8_t>(seq * 131 + i * 7);
}

size_t payload_size_for(uint64_t seq) {
    // Deterministic pseudo-random size in [1, kMaxPayload], reproducible on
    // both threads without sharing state.
    return 1 + (seq * 2654435761u) % kMaxPayload;
}

std::atomic<uint64_t> errors{0};

void check(bool ok, const char* what, uint64_t seq) {
    if (!ok) {
        std::fprintf(stderr, "FAIL: %s at seq=%llu\n", what,
                     static_cast<unsigned long long>(seq));
        errors.fetch_add(1, std::memory_order_relaxed);
    }
}

}  // namespace

int main() {
    // CRC32C known-answer test (RFC 3720 check value) and cross-check of the
    // optimized path against the portable table-driven implementation on
    // lengths around every block-size boundary.
    if (ipc::crc32c("123456789", 9) != 0xE3069283u) {
        std::fprintf(stderr, "FAIL: crc32c known-answer test\n");
        return 1;
    }
    {
        std::vector<uint8_t> buf(70000);
        std::mt19937_64 rng(1);
        for (auto& b : buf) b = static_cast<uint8_t>(rng());
        for (size_t len : {0, 1, 7, 8, 9, 255, 256, 767, 768, 769, 4096,
                           8191, 8192, 24575, 24576, 24577, 65536, 70000}) {
            if (ipc::crc32c(buf.data(), len) != ipc::crc32c_sw(buf.data(), len) ||
                ipc::crc32c(buf.data() + 1, len ? len - 1 : 0) !=
                    ipc::crc32c_sw(buf.data() + 1, len ? len - 1 : 0)) {
                std::fprintf(stderr, "FAIL: crc32c mismatch at len=%zu\n", len);
                return 1;
            }
        }
    }

    auto ring = ipc::SpscRing::create("/pkt_ring_selftest", kRingBytes);

    std::thread producer([&] {
        std::vector<uint8_t> buf(kMaxPayload);
        for (uint64_t seq = 0; seq < kPackets; ++seq) {
            const size_t n = payload_size_for(seq);
            for (size_t i = 0; i < n; ++i) buf[i] = byte_for(seq, i);
            ipc::RecordHeader hdr{};
            hdr.payload_size = static_cast<uint32_t>(n);
            hdr.seq = seq;
            hdr.timestamp_ns = ipc::now_ns();
            hdr.crc = ipc::crc32c(buf.data(), n);
            ipc::Backoff bo;
            while (!ring.try_push(hdr, buf.data())) bo.wait();
        }
    });

    ipc::Backoff bo;
    for (uint64_t seq = 0; seq < kPackets; ++seq) {
        const ipc::RecordHeader* hdr;
        bo.reset();
        while ((hdr = ring.try_pop_begin()) == nullptr) bo.wait();

        check(hdr->seq == seq, "sequence", seq);
        check(hdr->payload_size == payload_size_for(seq), "payload size", seq);
        const uint8_t* p =
            reinterpret_cast<const uint8_t*>(hdr) + sizeof(ipc::RecordHeader);
        check(ipc::crc32c(p, hdr->payload_size) == hdr->crc, "crc", seq);
        bool content_ok = true;
        for (size_t i = 0; i < hdr->payload_size; ++i) {
            if (p[i] != byte_for(seq, i)) { content_ok = false; break; }
        }
        check(content_ok, "content", seq);
        ring.pop_end();

        if (errors.load(std::memory_order_relaxed) > 10) break;
    }

    producer.join();
    if (errors.load() == 0) {
        std::printf("PASS: %llu packets, ring %zu KiB, payloads 1..%zu B\n",
                    static_cast<unsigned long long>(kPackets), kRingBytes / 1024,
                    kMaxPayload);
        return 0;
    }
    std::printf("FAILED with %llu errors\n",
                static_cast<unsigned long long>(errors.load()));
    return 1;
}

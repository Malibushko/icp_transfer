#pragma once

#include <cstddef>
#include <cstdint>

namespace ipc {

// Wire format of one packet inside the ring: RecordHeader immediately
// followed by `payload_size` bytes of payload, the whole record padded to
// 8-byte alignment.
//
// `payload_size` is deliberately the first field: the consumer reads it to
// frame the record, and the special value kWrapMarker in its place tells the
// consumer that the rest of the ring up to the wrap point is padding and the
// next record starts at offset 0.
struct RecordHeader {
    uint32_t payload_size;
    uint32_t crc;           // CRC32C of the payload
    uint64_t seq;           // monotonic sequence number, starts at 0
    uint64_t timestamp_ns;  // CLOCK_MONOTONIC at production time
};
static_assert(sizeof(RecordHeader) == 24);
static_assert(alignof(RecordHeader) == 8);

constexpr uint32_t kWrapMarker = 0xFFFFFFFFu;

constexpr size_t align8(size_t n) {
    return (n + 7) & ~size_t{7};
}

constexpr size_t record_size(size_t payload_size) {
    return align8(sizeof(RecordHeader) + payload_size);
}

}  // namespace ipc

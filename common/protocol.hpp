#pragma once

#include <cstddef>

namespace ipc {

    struct RecordHeader {
        uint32_t payload_size;
        uint32_t crc;
        uint64_t seq;
        uint64_t timestamp_ns;
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

}

#pragma once

// CRC32C (Castagnoli polynomial). Uses the SSE4.2 hardware instruction when
// available (~1 byte per cycle per lane, effectively >20 GB/s); otherwise
// falls back to a portable table-driven implementation.

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>

#if defined(__SSE4_2__)
#include <nmmintrin.h>
#endif

namespace ipc {

#if defined(__SSE4_2__)

inline uint32_t crc32c(const void* data, size_t len) {
    const uint8_t* p = static_cast<const uint8_t*>(data);
    uint64_t crc = 0xFFFFFFFFu;
    while (len >= 8) {
        uint64_t chunk;
        std::memcpy(&chunk, p, 8);
        crc = _mm_crc32_u64(crc, chunk);
        p += 8;
        len -= 8;
    }
    uint32_t c = static_cast<uint32_t>(crc);
    while (len--) {
        c = _mm_crc32_u8(c, *p++);
    }
    return ~c;
}

#else

inline uint32_t crc32c(const void* data, size_t len) {
    static const std::array<uint32_t, 256> table = [] {
        std::array<uint32_t, 256> t{};
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int k = 0; k < 8; ++k) {
                c = (c & 1) ? (c >> 1) ^ 0x82F63B78u : (c >> 1);
            }
            t[i] = c;
        }
        return t;
    }();
    const uint8_t* p = static_cast<const uint8_t*>(data);
    uint32_t crc = 0xFFFFFFFFu;
    while (len--) {
        crc = table[(crc ^ *p++) & 0xFFu] ^ (crc >> 8);
    }
    return ~crc;
}

#endif

}  // namespace ipc

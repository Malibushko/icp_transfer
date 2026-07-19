#pragma once

// CRC32C (Castagnoli polynomial).
//
// Hardware path (SSE4.2): the crc32 instruction has a 3-cycle latency and a
// serial dependency chain, which caps a naive loop at ~2.7 bytes/cycle. To
// go past that, three independent CRC streams are computed over three
// adjacent blocks in parallel (saturating the crc32 execution unit) and then
// merged with a precomputed "advance CRC by N zero bytes" GF(2) operator —
// the classic scheme from Mark Adler's crc32c.c. This roughly triples CRC
// throughput on large buffers.
//
// Software fallback: portable table-driven implementation.

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>

#if defined(__SSE4_2__)
#include <nmmintrin.h>
#endif

namespace ipc {

// Portable table-driven CRC32C. Always available (used as a cross-check in
// tests even when the hardware path is enabled).
inline uint32_t crc32c_sw(const void* data, size_t len) {
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

#if defined(__SSE4_2__)

namespace detail {

// GF(2) 32x32 matrix times vector: mat[i] is the image of bit i.
inline uint32_t gf2_matrix_times(const uint32_t* mat, uint32_t vec) {
    uint32_t sum = 0;
    while (vec) {
        if (vec & 1) sum ^= *mat;
        vec >>= 1;
        ++mat;
    }
    return sum;
}

inline void gf2_matrix_square(uint32_t* square, const uint32_t* mat) {
    for (int n = 0; n < 32; ++n) square[n] = gf2_matrix_times(mat, mat[n]);
}

// Build the operator that advances a CRC by `len` zero bytes.
// `len` must be a power of two.
inline void crc32c_zeros_op(uint32_t* even, size_t len) {
    uint32_t odd[32];  // operator for one zero bit
    odd[0] = 0x82F63B78u;
    uint32_t row = 1;
    for (int n = 1; n < 32; ++n) {
        odd[n] = row;
        row <<= 1;
    }
    gf2_matrix_square(even, odd);  // two zero bits
    gf2_matrix_square(odd, even);  // four zero bits
    do {
        gf2_matrix_square(even, odd);  // eight, thirty-two, ... zero bits
        len >>= 1;
        if (len == 0) return;
        gf2_matrix_square(odd, even);
        len >>= 1;
    } while (len);
    for (int n = 0; n < 32; ++n) even[n] = odd[n];
}

// Expand the operator into byte-indexed tables for fast application.
using ZerosTable = std::array<std::array<uint32_t, 256>, 4>;

inline ZerosTable make_zeros_table(size_t len) {
    uint32_t op[32];
    crc32c_zeros_op(op, len);
    ZerosTable z{};
    for (uint32_t n = 0; n < 256; ++n) {
        z[0][n] = gf2_matrix_times(op, n);
        z[1][n] = gf2_matrix_times(op, n << 8);
        z[2][n] = gf2_matrix_times(op, n << 16);
        z[3][n] = gf2_matrix_times(op, n << 24);
    }
    return z;
}

inline uint32_t crc32c_shift(const ZerosTable& z, uint32_t crc) {
    return z[0][crc & 0xFFu] ^ z[1][(crc >> 8) & 0xFFu] ^
           z[2][(crc >> 16) & 0xFFu] ^ z[3][crc >> 24];
}

constexpr size_t kLongBlock = 8192;
constexpr size_t kShortBlock = 256;

inline uint64_t crc_u64(uint64_t crc, const uint8_t* p) {
    uint64_t chunk;
    std::memcpy(&chunk, p, 8);
    return _mm_crc32_u64(crc, chunk);
}

}  // namespace detail

inline uint32_t crc32c(const void* data, size_t len) {
    static const detail::ZerosTable long_shift =
        detail::make_zeros_table(detail::kLongBlock);
    static const detail::ZerosTable short_shift =
        detail::make_zeros_table(detail::kShortBlock);

    const uint8_t* next = static_cast<const uint8_t*>(data);
    uint64_t crc0 = 0xFFFFFFFFu;

    while (len && (reinterpret_cast<uintptr_t>(next) & 7) != 0) {
        crc0 = _mm_crc32_u8(static_cast<uint32_t>(crc0), *next++);
        --len;
    }

    // Three independent dependency chains over three adjacent blocks, merged
    // by advancing crc0/crc1 over the bytes they did not see.
    while (len >= 3 * detail::kLongBlock) {
        uint64_t crc1 = 0, crc2 = 0;
        const uint8_t* end = next + detail::kLongBlock;
        do {
            crc0 = detail::crc_u64(crc0, next);
            crc1 = detail::crc_u64(crc1, next + detail::kLongBlock);
            crc2 = detail::crc_u64(crc2, next + 2 * detail::kLongBlock);
            next += 8;
        } while (next < end);
        crc0 = detail::crc32c_shift(long_shift, static_cast<uint32_t>(crc0)) ^ crc1;
        crc0 = detail::crc32c_shift(long_shift, static_cast<uint32_t>(crc0)) ^ crc2;
        next += 2 * detail::kLongBlock;
        len -= 3 * detail::kLongBlock;
    }
    while (len >= 3 * detail::kShortBlock) {
        uint64_t crc1 = 0, crc2 = 0;
        const uint8_t* end = next + detail::kShortBlock;
        do {
            crc0 = detail::crc_u64(crc0, next);
            crc1 = detail::crc_u64(crc1, next + detail::kShortBlock);
            crc2 = detail::crc_u64(crc2, next + 2 * detail::kShortBlock);
            next += 8;
        } while (next < end);
        crc0 = detail::crc32c_shift(short_shift, static_cast<uint32_t>(crc0)) ^ crc1;
        crc0 = detail::crc32c_shift(short_shift, static_cast<uint32_t>(crc0)) ^ crc2;
        next += 2 * detail::kShortBlock;
        len -= 3 * detail::kShortBlock;
    }
    while (len >= 8) {
        crc0 = detail::crc_u64(crc0, next);
        next += 8;
        len -= 8;
    }
    uint32_t c = static_cast<uint32_t>(crc0);
    while (len--) {
        c = _mm_crc32_u8(c, *next++);
    }
    return ~c;
}

#else

inline uint32_t crc32c(const void* data, size_t len) {
    return crc32c_sw(data, len);
}

#endif

}  // namespace ipc

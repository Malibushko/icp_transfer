# ipc-packets

Two CLI apps that stream data packets through a **lock-free SPSC ring buffer in
POSIX shared memory** (`shm_open` + `mmap`, no third-party IPC layer).

- **producer** — generates fixed-size payloads, stamps each with a sequence
  number, monotonic timestamp and CRC32C, and pushes them into the ring.
- **consumer** — reads packets in place, verifies CRC and sequence continuity,
  and reports throughput once per second.

## Build

Requires Linux, GCC/Clang with C++20, CMake ≥ 3.25, and [Conan 2](https://conan.io)
(which fetches the dependencies: CLI11, spdlog, crc32c).

```sh
conan install . --output-folder=build --build=missing -s build_type=Release
cmake -S . -B build -DCMAKE_TOOLCHAIN_FILE=build/conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

## Run

```sh
# terminal 1          # terminal 2
./build/consumer      ./build/producer 4096
```

```
producer <payload_bytes> [shm_name] [ring_mib]
consumer [shm_name]
```

Defaults: shm name `/pkt_ring`, ring capacity 64 MiB (must be a power of two).
Start order doesn't matter — the consumer waits for the producer.

## Controls

Both apps respond to the same controls:

| Action | Signal | Key |
|---|---|---|
| pause / resume | `SIGUSR1` | any key |
| quit | `SIGINT` / `SIGTERM` | `q` |

Pausing the consumer applies **backpressure with no loss**: the ring fills, the
producer's `try_push` blocks (spin → yield → sleep), and the stream resumes from
the exact packet where it stopped — `seq_gap` stays 0 and the final totals match.

## How it works

**Why shared memory + SPSC ring.** Pipes and sockets copy every byte through the
kernel and cost a syscall per operation. A shared-memory ring is written once by
the producer and read in place by the consumer — one `memcpy` in, zero copies
out, **no syscalls on the hot path**. With exactly one writer and one reader the
queue needs no locks, just two atomic indices.

**Ring buffer** ([common/ipc_ring_buffer.hpp](common/ipc_ring_buffer.hpp)). The
segment holds a one-page control block (magic, capacity, `ready` flag, `head`,
`tail`) followed by the data area. `head`/`tail` are 64-bit monotonic counters
masked by `capacity-1`, so free/used space is a plain subtraction with no
full-vs-empty ambiguity. They sit on separate cache lines and are
published/consumed with release/acquire stores; each side caches the other's
index to avoid cache-line ping-pong. Records are always contiguous — if one
won't fit before the end of the buffer, a 4-byte wrap marker is written and it
restarts at offset 0 — which lets the consumer checksum the payload in place.

**Packet format** ([common/protocol.hpp](common/protocol.hpp)):

```
RecordHeader { u32 payload_size; u32 crc; u64 seq; u64 timestamp_ns; } + payload, padded to 8 bytes
```

The payload is random bytes generated once at startup (per-packet RNG would
benchmark the RNG, not the transport); the sequence number is stamped into each
packet so content and CRC differ from packet to packet. CRC32C uses the
hardware-accelerated `crc32c` library.

## Performance

Sustains **tens of GB/s** of payload throughput on a modern desktop — roughly
~20 GB/s at 4 KiB payloads and ~5 GB/s (10M packets/s) at 512 B on an i9-12900,
with `crc_err=0` / `seq_gap=0` and producer/consumer totals equal. Full sweep in
[docs/bench.log](docs/bench.log); `scripts/bench.sh` reproduces it.

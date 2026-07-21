# ipc-packets

Two CLI apps that stream data packets through a **lock-free SPSC ring buffer in
POSIX shared memory** (`shm_open` + `mmap`, no third-party IPC layer).

- **producer** — generates fixed-size payloads, stamps each with a sequence
  number, monotonic timestamp and CRC32C, and pushes them into the ring.
- **consumer** — reads packets in place, verifies CRC and sequence continuity,
  and reports throughput once per second.

## Build

Needs Linux, GCC/Clang with C++20, and CMake ≥ 3.25. Dependencies (CLI11,
spdlog, crc32c) are managed by [Conan 2](https://conan.io) and fetched
automatically on the first `conan install`.

```sh
# one-time Conan setup
pip install "conan>=2.0"
conan profile detect

# build
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
producer's `try_push` blocks, and the stream resumes from the exact packet where
it stopped — `seq_gap` stays 0 and the final totals match.

## How it works

The producer writes fixed-size records — a 24-byte header (payload size, CRC32C,
sequence number, timestamp) followed by the payload padded to 8 bytes — into a
shared-memory ring and publishes each with a release store; the consumer reads
them in place and validates CRC and sequence continuity. The ring is a lock-free
single-producer/single-consumer queue keyed on 64-bit monotonic `head`/`tail`
counters (masked by `capacity-1`, on separate cache lines, each side caching the
other's index to avoid contention), so steady state is one `memcpy` in, zero
copies out, and no syscalls; a wrap marker keeps every record contiguous. Source
is under [src/](src/), with the shared primitives in `src/include/common/`.

## Performance

Sustains tens of GB/s of payload throughput on a modern desktop (~20 GB/s at
4 KiB payloads, ~5 GB/s / 10M packets/s at 512 B on an i9-12900), with
`crc_err=0` / `seq_gap=0`. See [docs/bench.log](docs/bench.log).

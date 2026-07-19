# ipc-packets

Two CLI applications exchanging data packets through a **lock-free SPSC ring
buffer in POSIX shared memory** (`shm_open` + `mmap` — plain POSIX API, no
third-party IPC libraries).

- **producer** — generates packets of a given payload size, attaches metadata
  (sequence number, monotonic timestamp, CRC32C) and pushes them into the ring.
- **consumer** — receives packets, validates the checksum and sequence
  continuity, and once per second reports total packets, packets/s and bytes/s.

Peak measured throughput: **~21.5 GB/s of payload (5.2M packets/s) at 4 KiB
payload**, and **10.2M packets/s** at 512 B payload
(i9-12900, WSL2, GCC 14, see [docs/bench.log](docs/bench.log)).

## Build & run

Requires a POSIX system, GCC or Clang with C++20, CMake ≥ 3.16.
No external dependencies.

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build

./build/ring_selftest                  # optional: correctness self-test

# terminal 1                           # terminal 2
./build/consumer                       ./build/producer 4096
```

```
Usage: producer <payload_bytes> [shm_name] [ring_mib]
Usage: consumer [shm_name]
```

Defaults: shm name `/pkt_ring`, ring capacity 64 MiB (power of two required;
8 MiB is the sweet spot on the benchmark machine — small enough to stay in L3).
`scripts/bench.sh` sweeps payload and ring sizes and prints the peak numbers.

## Pause / resume / quit

Both applications support the same controls:

| Action | Signal | Key |
|---|---|---|
| pause / resume (toggle) | `SIGUSR1` | any key |
| quit | `SIGINT` / `SIGTERM` | `q` |

Signal handlers only flip a `sig_atomic_t` flag; the main loops observe it.
Keyboard input uses non-canonical termios (restored on exit) and is polled
without blocking, amortized to once per 1024 packets on the hot path.

### What happens to the data while the consumer is paused

**Backpressure, no loss.** A paused consumer stops draining the ring; the ring
fills up; the producer's `try_push` starts failing and the producer waits
(spin → yield → sleep backoff) for free space. When the consumer resumes, the
stream continues from the exact packet where it stopped — nothing is dropped
or duplicated. The consumer verifies this: `seq_gap` stays 0 across
pause/resume cycles, and the producer's final packet count matches the
consumer's total. A producer pause interrupts its wait loop and the unsent
packet is retried with the same sequence number after resume.

## Design

### Transport: why shared memory + SPSC ring

Pipes, FIFOs and UNIX sockets copy every byte into the kernel and back and pay
a syscall per operation. A shared-memory ring is written by the producer and
read (validated) by the consumer *in place* — one `memcpy` into the ring on
the producing side, zero copies on the consuming side, and **no syscalls at
all on the hot path**. With exactly one writer and one reader, the queue is
wait-free with two atomic indices and needs no locks.

### Ring buffer (`include/ring_buffer.hpp`)

- Layout in the segment: one page of control block (magic, capacity, `ready`
  flag, `head`, `tail`), then the data area.
- `head`/`tail` are monotonically increasing 64-bit counters; the buffer
  position is `index & (capacity-1)` (capacity is a power of two). This makes
  free/used space a plain subtraction and removes the classic full-vs-empty
  ambiguity.
- `head` and `tail` live on **separate cache lines**; the producer publishes a
  record with a release store, the consumer acquires it (and symmetrically for
  tail). Each side keeps a *cached* copy of the other side's index and re-reads
  the shared atomic only when the cached value is insufficient — this removes
  almost all cache-line ping-pong from the hot path.
- Records are always contiguous: if a record does not fit before the end of
  the buffer, a 4-byte wrap marker is written and the record starts at
  offset 0. Every record is padded to 8 bytes, so there is always room for the
  marker. Contiguity is what lets the consumer compute the CRC in place.
- The producer creates the segment (removing a stale one after a crash) and
  unlinks it on exit; the consumer waits for the segment and its `ready` flag,
  so start order does not matter.

### Packet format (`include/protocol.hpp`)

```
RecordHeader { u32 payload_size; u32 crc; u64 seq; u64 timestamp_ns; }
+ payload, padded to 8 bytes
```

The payload is random bytes generated once at startup (generating fresh random
data per packet would benchmark the RNG, not the transport), with the sequence
number stamped into the head of each packet so the content and CRC differ from
packet to packet.

### CRC32C (`include/crc32c.hpp`)

CRC32C via the SSE4.2 `crc32` instruction. The instruction has a 3-cycle
latency on a serial dependency chain (~2.7 B/cycle), which capped the whole
pipeline at ~11 GB/s — with both processes computing a checksum, CRC was the
bottleneck, not the transport. The implementation therefore runs **three
independent CRC streams over adjacent blocks** and merges them with a
precomputed advance-by-N-zero-bytes GF(2) operator (Mark Adler's scheme),
which roughly doubled end-to-end throughput (11 → 21.5 GB/s). A portable
table-driven fallback is used on non-SSE4.2 targets, and the self-test
cross-checks both implementations against each other and the RFC 3720
known-answer value.

### Correctness

`./build/ring_selftest` pushes 2M records of pseudo-random sizes through a
deliberately small 1 MiB ring (forcing wrap-arounds and full-ring waits) and
verifies sequence, size, CRC and every payload byte. The consumer continuously
validates CRC and sequence continuity of the live stream.

## Benchmark

Machine: i9-12900 (24 threads), WSL2 (Linux 6.18), GCC 14.2, `-O3
-march=native`, producer and consumer pinned to different physical cores.
Full log: [docs/bench.log](docs/bench.log).

Peak per-second windows by payload size (8 MiB ring):

| payload | packets/s | payload throughput |
|---:|---:|---:|
| 512 B | 10 213 788 | 5.2 GB/s |
| 4 KiB | 5 247 747 | **21.5 GB/s** |
| 16 KiB | 1 282 963 | 21.0 GB/s |
| 64 KiB | 308 593 | 20.2 GB/s |
| 256 KiB | 78 607 | 20.6 GB/s |

Sustained 10-second run at 4 KiB payload: 51 172 836 packets, 209.6 GB,
**~21 GB/s**, `crc_err=0`, `seq_gap=0`, producer and consumer totals equal.

At small payloads the cost is per-packet (~100 ns/packet: header, timestamp,
CRC setup, index publication); at 4 KiB+ the pipeline is limited by per-byte
work (CRC on both sides + one memcpy) and inter-core L3 bandwidth.

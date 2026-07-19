# ipc-packets

Two CLI applications (producer / consumer) exchanging data packets over a
lock-free SPSC ring buffer in POSIX shared memory.

Work in progress — see commit history.

## Build

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

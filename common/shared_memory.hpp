#pragma once

#include <cerrno>
#include <csignal>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "clock.hpp"

namespace ipc {

class MappedRegion {
public:
    MappedRegion() = default;

    MappedRegion(const MappedRegion&) = delete;
    MappedRegion& operator=(const MappedRegion&) = delete;

    MappedRegion(MappedRegion&& other) noexcept { *this = std::move(other); }
    MappedRegion& operator=(MappedRegion&& other) noexcept {
        if (this != &other) {
            reset();
            addr_ = other.addr_;
            len_ = other.len_;
            name_ = std::move(other.name_);
            owner_ = other.owner_;
            other.addr_ = nullptr;
            other.owner_ = false;
        }
        return *this;
    }

    ~MappedRegion() { reset(); }

    static MappedRegion create(const std::string& name, size_t size) {
        shm_unlink(name.c_str());
        int fd = shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
        if (fd < 0) {
            throw std::system_error(errno, std::generic_category(), "shm_open(create)");
        }
        if (ftruncate(fd, static_cast<off_t>(size)) != 0) {
            int e = errno;
            ::close(fd);
            shm_unlink(name.c_str());
            throw std::system_error(e, std::generic_category(), "ftruncate");
        }
        void* mem = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        ::close(fd);
        if (mem == MAP_FAILED) {
            shm_unlink(name.c_str());
            throw std::system_error(errno, std::generic_category(), "mmap");
        }
        return MappedRegion(mem, size, name, true);
    }

    static MappedRegion open(const std::string& name, size_t min_size,
                             const volatile std::sig_atomic_t* stop = nullptr) {
        int fd = -1;
        size_t total = 0;
        for (;;) {
            if (stop && *stop) throw std::runtime_error("interrupted");
            fd = shm_open(name.c_str(), O_RDWR, 0);
            if (fd >= 0) {
                struct stat st{};
                if (fstat(fd, &st) == 0 &&
                    st.st_size >= static_cast<off_t>(min_size)) {
                    total = static_cast<size_t>(st.st_size);
                    break;
                }
                ::close(fd);
            } else if (errno != ENOENT) {
                throw std::system_error(errno, std::generic_category(), "shm_open(open)");
            }
            sleep_ms(50);
        }
        void* mem = mmap(nullptr, total, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        ::close(fd);
        if (mem == MAP_FAILED) {
            throw std::system_error(errno, std::generic_category(), "mmap");
        }
        return MappedRegion(mem, total, name, false);
    }

    [[nodiscard]] void* data() const { return addr_; }
    [[nodiscard]] size_t size() const { return len_; }

private:
    MappedRegion(void* addr, size_t len, std::string name, bool owner)
        : addr_(addr), len_(len), name_(std::move(name)), owner_(owner) {}

    void reset() {
        if (addr_) {
            munmap(addr_, len_);
            if (owner_) shm_unlink(name_.c_str());
            addr_ = nullptr;
        }
    }

    void* addr_ = nullptr;
    size_t len_ = 0;
    std::string name_;
    bool owner_ = false;
};

}

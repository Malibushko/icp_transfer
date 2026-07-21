#include <string>

#include "cli.hpp"
#include "consumer.hpp"
#include "control.hpp"
#include "ipc_ring_buffer.hpp"
#include "logging.hpp"
#include "terminal.hpp"

int main(int argc, char** argv) {
    ipc::init_logging("consumer");
    const std::string shm_name = consumer::parse_args(argc, argv);

    ipc::install_signal_handlers();
    ipc::RingBuffer ring = consumer::open_ring(shm_name);
    ipc::RawTerminal terminal;

    consumer::run_consumer_loop(ring, terminal);
    return 0;
}

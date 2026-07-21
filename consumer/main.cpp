#include <string>

#include "cli.hpp"
#include "consumer.hpp"
#include "control.hpp"
#include "ipc_ring_buffer.hpp"
#include "logging.hpp"

int main(int argc, char** argv) {
    ipc::init_logging("consumer");
    const std::string shm_name = consumer::parse_args(argc, argv);

    ipc::install_signal_handlers();
    ipc::RawTerminal term;

    ipc::RingBuffer ring = consumer::open_ring(shm_name);
    consumer::run_consumer_loop(ring, term);
    return 0;
}

#include "cli.hpp"
#include "control.hpp"
#include "ipc_ring_buffer.hpp"
#include "logging.hpp"
#include "producer.hpp"
#include "terminal.hpp"

int main(int argc, char** argv) {
    ipc::init_logging("producer");
    const producer::Options opt = producer::parse_args(argc, argv);

    ipc::install_signal_handlers();
    ipc::RawTerminal term;
    ipc::RingBuffer ring = producer::create_ring(opt);

    producer::run_producer_loop(ring, term, opt);
    return 0;
}

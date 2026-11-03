# SPSC packet batching

A C++20 single-producer/single-consumer example. The producer emits variable-size
packets using a distribution of 98% at 1500 bytes and 2% at 60 bytes, at
40 Mbit/s. The consumer batches packets until adding the next packet would reach 65,000
bytes, or until `T` microseconds have elapsed since the first packet.

## Build and run

```sh
make
./spsc_packets [timeout_microseconds] [run_seconds]
```

Or use `make run` for the default demonstration.

The defaults are `T=1000` microseconds and a five-second run. Replace
`consume()` with the real batch processing operation.


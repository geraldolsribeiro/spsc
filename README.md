# SPSC packet batching

A C++20 single-producer/single-consumer example. The producer emits 1500-byte
packets at 40 Mbit/s (about one packet every 300 microseconds). The consumer
batches packets until the batch is full or until `T` microseconds have elapsed
since the first packet in the batch.


Because the limit is strictly less than 65,000 bytes, a full batch contains 43
packets (64,500 bytes).

## Build and run

```sh
make
./spsc_packets [timeout_microseconds] [run_seconds]
```

Or use `make run` for the default demonstration.

The defaults are `T=1000` microseconds and a five-second run. Replace
`consume()` with the real batch processing operation.


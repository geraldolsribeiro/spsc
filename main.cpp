#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <random>
#include <span>
#include <stop_token>
#include <thread>
#include <tuple>
#include <vector>

namespace {
using namespace std::chrono_literals;

// Architecture:
//
//   +----------+       Packet queue        +----------+      65 kB batch
//   | Producer | --push(Packet)----------> | Consumer | ------------------+
//   +----------+                           +----------+                    |
//        |                                      |                          v
//        | 98% x 1500 B, 2% x 60 B              | BlockAssembler
//        | 40 Mbit/s                             v
//        +------------------------------> [11,200 B][11,200 B]...[tail+0]
//                                                        |
//                                                        v
//                                               consume_block(block)
//
// The program models a byte stream made of packets. A consumer batch is kept
// below 65,000 bytes, then the stream is repacked into fixed 11,200-byte
// blocks. These are separate limits: batches control waiting/latency, while
// blocks control the format delivered to the downstream consumer.
// Packet-size distribution: 98% standard packets and 2% small packets.
constexpr std::size_t   standard_packet_size = 1500;
constexpr std::size_t   small_packet_size    = 60;
constexpr std::size_t   batch_limit          = 65'000; // Strictly less than 65 kB.
constexpr std::size_t   block_size           = 11'200;
constexpr std::uint64_t producer_rate_bps    = 40'000'000; // 40 Mbit/s.

struct Packet {
  std::vector<std::byte> payload;

  explicit Packet(std::size_t size) : payload(size) {}
};

// Queue used by exactly one producer and one consumer. The mutex makes the
// example easy to integrate safely; the single-writer/single-reader design
// means no additional producers or consumers should be added.
// Queue ownership and synchronization:
//
//   producer thread                         consumer thread
//        |                                      |
//        +-- lock -> [ deque<Packet> ] <- lock-+
//                         |
//                    condition_variable
//
// Only one thread calls push() and only one thread calls take_batch().
class SpscQueue {
public:
  // The producer transfers ownership of the packet into the queue. The lock
  // protects both the deque and the condition-variable state, and is released
  // before notifying the consumer.
  void push(Packet packet) {
    {
      std::lock_guard lock(mutex_);
      queue_.push_back(std::move(packet));
    }
    not_empty_.notify_one();
  }

  // Waits for the first packet. Once a batch has started, waits no longer
  // than timeout for more packets, or returns immediately when it is full.
  std::vector<Packet> take_batch(std::chrono::microseconds timeout, std::stop_token stop) {
    std::vector<Packet> batch;
    // Reserve for the worst case (60-byte packets produce the most entries).
    // This is only a capacity hint; packet sizes remain variable.
    batch.reserve(batch_limit / small_packet_size);
    std::unique_lock lock(mutex_);

    // Waiting avoids polling. The predicate also handles spurious wake-ups.
    auto wake = [&] { return !queue_.empty() || closed_ || stop.stop_requested(); };
    not_empty_.wait(lock, wake);
    // An empty result means shutdown or a stop request.
    if (queue_.empty())
      return batch;

    // The timeout starts when the first packet becomes available. This avoids
    // delaying the final partial batch indefinitely during a quiet period.
    auto        deadline = std::chrono::steady_clock::now() + timeout;
    std::size_t bytes    = 0;
    // The strict '<' prevents a batch from reaching exactly 65,000 bytes.
    while (!queue_.empty() && bytes + queue_.front().payload.size() < batch_limit) {
      bytes += queue_.front().payload.size();
      batch.push_back(std::move(queue_.front()));
      queue_.pop_front();
    }

    // Keep filling the batch, rather than processing one packet at a time.
    // The deadline starts at the first packet and is not reset per packet.
    while (bytes + small_packet_size < batch_limit && !closed_ && !stop.stop_requested()) {
      if (!not_empty_.wait_until(lock, deadline, [&] { return !queue_.empty() || closed_ || stop.stop_requested(); })) {
        break;
      }
      while (!queue_.empty() && bytes + queue_.front().payload.size() < batch_limit) {
        bytes += queue_.front().payload.size();
        batch.push_back(std::move(queue_.front()));
        queue_.pop_front();
      }
      // The next packet does not fit under the strict limit. Return now;
      // otherwise the predicate remains true and this loop would spin.
      if (!queue_.empty() && bytes + queue_.front().payload.size() >= batch_limit)
        break;
    }
    return batch;
  }

  void close() {
    {
      std::lock_guard lock(mutex_);
      closed_ = true;
    }
    not_empty_.notify_all();
  }

private:
  std::mutex              mutex_;
  std::condition_variable not_empty_;
  std::deque<Packet>      queue_;
  bool                    closed_ = false;
};

// This represents the actual downstream operation. The checksum is only a
// placeholder that makes the example touch every byte.
void consume_block(std::span<const std::byte> block) {
  // Replace this with the actual fixed-size block operation.
  std::uint8_t checksum = 0;
  for (const auto byte : block)
    checksum ^= std::to_integer<std::uint8_t>(byte);
  std::ignore = checksum;
}

// Batch-to-block conversion (partial data is carried across batches):
//
//   batch 1: [packet][packet][packet]----+
//   batch 2: [packet][packet]------------+--> continuous byte stream
//                                             |                 |
//                                             +--> [11200 B] --> consume_block()
//                                             +--> [11200 B] --> consume_block()
//                                             +--> [tail][zero padding]
class BlockAssembler {
public:
  // Keep the partial block between consumer batches. This is important: if
  // every batch were padded independently, each batch could lose bytes.
  void add(std::span<const Packet> packets) {
    // Packets are treated as a continuous byte stream; a packet may cross a
    // block boundary and is never padded individually.
    for (const auto &packet : packets) {
      std::size_t offset = 0;
      while (offset < packet.payload.size()) {
        // Copy only what fits. A remaining packet suffix is copied on the
        // next iteration into the next block.
        const auto count = std::min(block_size - used_, packet.payload.size() - offset);
        std::copy_n(packet.payload.data() + offset, count, block_.data() + used_);
        used_ += count;
        offset += count;
        // Full blocks are delivered immediately. used_ persists across
        // consumer batches, which minimizes padding.
        if (used_ == block_size) {
          consume_block(block_);
          std::fill(block_.begin(), block_.end(), std::byte{0});
          used_ = 0;
        }
      }
    }
  }

  void finish() {
    // Only the final partial block is padded, minimizing both block count and
    // the total number of zero-padding bytes for the complete stream.
    if (used_ != 0)
      consume_block(block_);
  }

private:
  std::vector<std::byte> block_{block_size}; // Value-initialized zero padding.
  std::size_t            used_ = 0;
};
} // namespace

int main(int argc, char **argv) {
  // argv[1] is the batch timeout in microseconds; argv[2] is run duration
  // in seconds. Defaults are 1,000 microseconds and 5 seconds.
  const auto timeout_us  = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 1'000;
  const auto run_seconds = argc > 2 ? std::strtoull(argv[2], nullptr, 10) : 5;
  // The interval is calculated per packet: variable packet sizes still average
  // the requested 40 Mbit/s. steady_clock avoids wall-clock adjustments.

  SpscQueue                  queue;
  std::atomic<std::uint64_t> produced{0}, produced_bytes{0};
  std::atomic<std::uint64_t> consumed{0}, consumed_bytes{0};
  std::atomic<std::uint64_t> batches{0};

  // jthread automatically joins, while stop_token provides cooperative
  // shutdown. sleep_until keeps the producer from accumulating drift.
  std::jthread producer([&](std::stop_token stop) {
    std::mt19937                       generator(std::random_device{}());
    std::uniform_int_distribution<int> packet_kind(0, 99);
    auto                               next = std::chrono::steady_clock::now();
    while (!stop.stop_requested()) {
      // 98 values select 1500 bytes; 2 values select 60 bytes.
      const auto size = packet_kind(generator) < 98 ? standard_packet_size : small_packet_size;
      queue.push(Packet(size));
      ++produced;
      produced_bytes += size;
      // Larger packets need longer intervals to maintain 40 Mbps.
      const auto interval = std::chrono::duration<double>(static_cast<double>(size * 8) / producer_rate_bps);
      next += std::chrono::duration_cast<std::chrono::steady_clock::duration>(interval);
      std::this_thread::sleep_until(next);
    }
    queue.close();
  });

  // The consumer processes a full batch whenever possible. If traffic is
  // sparse, take_batch wakes at the deadline and returns the partial batch.
  //
  // Shutdown sequence:
  //   main -> stop producer -> producer closes queue -> consumer drains queue
  //        -> consumer finishes/pads final block -> both threads join.
  std::jthread consumer([&](std::stop_token stop) {
    BlockAssembler assembler;
    while (!stop.stop_requested()) {
      auto batch = queue.take_batch(std::chrono::microseconds(timeout_us), stop);
      if (batch.empty())
        break;
      assembler.add(batch);
      consumed += batch.size();
      for (const auto &packet : batch)
        consumed_bytes += packet.payload.size();
      ++batches;
    }
    assembler.finish();
  });

  const auto    report_start            = std::chrono::steady_clock::now();
  auto          previous_time           = report_start;
  std::uint64_t previous_produced_bytes = 0, previous_consumed_bytes = 0;
  std::uint64_t previous_batches = 0;
  while (std::chrono::steady_clock::now() - report_start < std::chrono::seconds(run_seconds)) {
    std::this_thread::sleep_for(500ms);
    const auto   now     = std::chrono::steady_clock::now();
    const double elapsed = std::chrono::duration<double>(now - previous_time).count();
    const auto   p = produced_bytes.load(), c = consumed_bytes.load();
    const auto   batch_count = batches.load();
    const double fill_ratio  = batch_count == previous_batches ? 0.0
                                                               : static_cast<double>(c - previous_consumed_bytes) /
                                                                     ((batch_count - previous_batches) * batch_limit);
    const auto   mbps        = [elapsed](std::uint64_t current, std::uint64_t previous) {
      return (current - previous) * 8.0 / elapsed / 1'000'000.0;
    };
    std::cout << std::fixed << std::setprecision(2) << "bitrate: produced=" << mbps(p, previous_produced_bytes)
              << " Mbps, consumed=" << mbps(c, previous_consumed_bytes) << " | fill_ratio=" << fill_ratio * 100.0 << "%"
              << " | packets=" << produced.load() << "/" << consumed.load() << " batches=" << batch_count << '\n';
    previous_time           = now;
    previous_produced_bytes = p;
    previous_consumed_bytes = c;
    previous_batches        = batch_count;
  }
  producer.request_stop();
  producer.join();
  consumer.request_stop();
  queue.close();
  consumer.join();

  const auto   final_batches = batches.load();
  const double final_fill_ratio =
      final_batches == 0 ? 0.0 : static_cast<double>(consumed_bytes.load()) / (final_batches * batch_limit);
  std::cout << "final: produced_packets=" << produced.load() << " consumed_packets=" << consumed.load()
            << " produced_bytes=" << produced_bytes.load() << " consumed_bytes=" << consumed_bytes.load()
            << " batches=" << final_batches << " fill_ratio=" << final_fill_ratio * 100.0 << "%"
            << " timeout_us=" << timeout_us << '\n';
}

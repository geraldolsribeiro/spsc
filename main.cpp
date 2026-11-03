#include <array>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <iostream>
#include <mutex>
#include <span>
#include <stop_token>
#include <thread>
#include <tuple>
#include <vector>

namespace {
using namespace std::chrono_literals;
constexpr std::size_t   packet_size       = 1500;
constexpr std::size_t   batch_limit       = 65'000; // The batch must remain strictly below this.
constexpr std::uint64_t producer_rate_bps = 40'000'000;

struct Packet {
  std::array<std::byte, packet_size> payload{};
};

class SpscQueue {
public:
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
    batch.reserve(batch_limit / packet_size);
    std::unique_lock lock(mutex_);

    auto wake = [&] { return !queue_.empty() || closed_ || stop.stop_requested(); };
    not_empty_.wait(lock, wake);
    if (queue_.empty())
      return batch;

    auto        deadline = std::chrono::steady_clock::now() + timeout;
    std::size_t bytes    = 0;
    while (!queue_.empty() && bytes + packet_size < batch_limit) {
      batch.push_back(std::move(queue_.front()));
      queue_.pop_front();
      bytes += packet_size;
    }

    // Keep filling the batch, rather than waking and processing a small
    // batch for every packet. The deadline starts at the first packet.
    while (bytes + packet_size < batch_limit && !closed_ && !stop.stop_requested()) {
      if (!not_empty_.wait_until(lock, deadline, [&] { return !queue_.empty() || closed_ || stop.stop_requested(); })) {
        break;
      }
      while (!queue_.empty() && bytes + packet_size < batch_limit) {
        batch.push_back(std::move(queue_.front()));
        queue_.pop_front();
        bytes += packet_size;
      }
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

void consume(std::span<const Packet> batch) {
  // Replace this with the actual batch operation. Touching the data prevents
  // an optimizing compiler from treating the simulated work as irrelevant.
  std::uint8_t checksum = 0;
  for (const auto &packet : batch)
    checksum ^= std::to_integer<std::uint8_t>(packet.payload[0]);
  std::ignore = checksum;
}
} // namespace

int main(int argc, char **argv) {
  const auto timeout_us      = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 1'000;
  const auto run_seconds     = argc > 2 ? std::strtoull(argv[2], nullptr, 10) : 5;
  const auto packet_interval = std::chrono::duration<double>(static_cast<double>(packet_size * 8) / producer_rate_bps);

  SpscQueue     queue;
  std::uint64_t produced = 0;
  std::uint64_t consumed = 0;
  std::uint64_t batches  = 0;

  std::jthread producer([&](std::stop_token stop) {
    auto next = std::chrono::steady_clock::now();
    while (!stop.stop_requested()) {
      queue.push(Packet{});
      ++produced;
      next += std::chrono::duration_cast<std::chrono::steady_clock::duration>(packet_interval);
      std::this_thread::sleep_until(next);
    }
    queue.close();
  });

  std::jthread consumer([&](std::stop_token stop) {
    while (!stop.stop_requested()) {
      auto batch = queue.take_batch(std::chrono::microseconds(timeout_us), stop);
      if (batch.empty())
        break;
      consume(batch);
      consumed += batch.size();
      ++batches;
    }
  });

  std::this_thread::sleep_for(std::chrono::seconds(run_seconds));
  producer.request_stop();
  producer.join();
  consumer.request_stop();
  queue.close();
  consumer.join();

  std::cout << "produced=" << produced << " consumed=" << consumed << " batches=" << batches
            << " timeout_us=" << timeout_us << '\n';
}

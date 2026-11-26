#include "delta_compression.h"
#include "chunk/chunk.h"
#include "chunk/fast_cdc.h"
#include "config.h"
#include "delta_policy.h"
#include "index/adaptive_odess_subfeature_index.h"
#include "storage/storage.h"
#include <chrono>
#include <climits>
#include <filesystem>
#include <glog/logging.h>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

namespace Delta {

void DeltaCompression::RecordChainDepth(chunk_id id, uint16_t depth) {
  if (chain_depth_by_id_.size() <= id) {
    chain_depth_by_id_.resize(static_cast<size_t>(id) + 1, 0);
  }
  chain_depth_by_id_[id] = depth;
}

Feature DeltaCompression::CalculateFeatureTimed(
    const std::shared_ptr<Chunk> &chunk) {
  const auto start = std::chrono::steady_clock::now();
  Feature feature = (*feature_)(chunk);
  feature_calculation_time_ns_ += static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - start)
          .count());
  feature_calculation_count_++;
  return feature;
}

std::vector<std::pair<chunk_id, uint32_t>>
DeltaCompression::QueryTopKTimed(const Feature &feature, int k) {
  const auto start = std::chrono::steady_clock::now();
  auto result = index_->GetTopKCandidates(feature, k);
  feature_index_query_time_ns_ += static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - start)
          .count());
  feature_index_query_count_++;
  return result;
}

void DeltaCompression::InsertFeatureTimed(const Feature &feature,
                                          chunk_id id) {
  const auto start = std::chrono::steady_clock::now();
  index_->AddFeature(feature, id);
  feature_index_insertion_time_ns_ += static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - start)
          .count());
  feature_index_insertion_count_++;
}

void DeltaCompression::PrintSimilarityDetectionTelemetry() const {
  const uint64_t core_time_ns =
      feature_calculation_time_ns_ + feature_index_query_time_ns_ +
      feature_index_insertion_time_ns_;
  const double unique_mib =
      static_cast<double>(total_size_deduplicated_) / 1048576.0;
  const auto milliseconds = [](uint64_t nanoseconds) {
    return static_cast<double>(nanoseconds) / 1000000.0;
  };
  const auto average_microseconds =
      [](uint64_t nanoseconds, uint64_t operations) {
        return operations == 0
            ? 0.0
            : static_cast<double>(nanoseconds) /
                  static_cast<double>(operations) / 1000.0;
      };
  const auto speed_mib_per_second =
      [unique_mib](uint64_t nanoseconds) {
        return nanoseconds == 0
            ? 0.0
            : unique_mib /
                  (static_cast<double>(nanoseconds) / 1000000000.0);
      };

  std::cout << "Similarity detection speed telemetry:" << std::endl;
  std::cout << "  scope: non-duplicate bytes; feature calculation + "
               "index query + index insertion"
            << std::endl;
  std::cout << "  excludes: chunking, exact deduplication, xdelta, and "
               "storage I/O"
            << std::endl;
  std::cout << "  non-duplicate chunks/bytes: "
            << base_chunk_count_ + delta_chunk_count_ << "/"
            << total_size_deduplicated_ << std::endl;
  std::cout << "  feature calculations/time total ms/avg us: "
            << feature_calculation_count_ << "/" << std::fixed
            << std::setprecision(3)
            << milliseconds(feature_calculation_time_ns_) << "/"
            << average_microseconds(feature_calculation_time_ns_,
                                    feature_calculation_count_)
            << std::endl;
  std::cout << "  index queries/time total ms/avg us: "
            << feature_index_query_count_ << "/"
            << milliseconds(feature_index_query_time_ns_) << "/"
            << average_microseconds(feature_index_query_time_ns_,
                                    feature_index_query_count_)
            << std::endl;
  std::cout << "  index insertions/time total ms/avg us: "
            << feature_index_insertion_count_ << "/"
            << milliseconds(feature_index_insertion_time_ns_) << "/"
            << average_microseconds(feature_index_insertion_time_ns_,
                                    feature_index_insertion_count_)
            << std::endl;
  std::cout << "  core detection time ms/speed MiB/s: "
            << milliseconds(core_time_ns) << "/"
            << speed_mib_per_second(core_time_ns) << std::endl;
  std::cout << std::defaultfloat << std::setprecision(6);
}

void DeltaCompression::AddFile(const std::string &file_name) {
  FileMeta file_meta;
  file_meta.file_name = file_name;
  file_meta.start_chunk_id = -1;
  if (!this->chunker_->ReinitWithFile(file_name)) {
    LOG(ERROR) << "failed to initialize chunker with file: " << file_name;
    return;
  }

  const uint64_t size_before = total_size_origin_;
  const auto t_start = std::chrono::steady_clock::now();

  while (true) {
    auto chunk = chunker_->GetNextChunk();
    if (nullptr == chunk)
      break;
    if (-1 == file_meta.start_chunk_id)
      file_meta.start_chunk_id = chunk->id();
    // Every chunk type belongs to the file.  Updating this only for delta
    // chunks leaves end_chunk_id uninitialized when a file ends in a base or
    // duplicate chunk and makes full recovery read arbitrary chunk ranges.
    file_meta.end_chunk_id = chunk->id();
    uint32_t dedup_base_id = dedup_->ProcessChunk(chunk);
    total_size_origin_ += chunk->len();
    // duplicate chunk
    if (dedup_base_id != chunk->id()) {
      storage_->WriteDuplicateChunk(chunk, dedup_base_id);
      duplicate_chunk_count_++;
      continue;
    }
    total_size_deduplicated_ += chunk->len();

    auto write_base_chunk = [this](const std::shared_ptr<Chunk> &chunk) {
      storage_->WriteBaseChunk(chunk);
      base_chunk_count_++;
      total_size_compressed_ += chunk->len();
      RecordChainDepth(chunk->id(), 0);
    };

    auto write_delta_chunk = [this](const std::shared_ptr<Chunk> &chunk,
                                    const std::shared_ptr<Chunk> &delta_chunk,
                                    const uint32_t base_chunk_id) {
      chunk_size_before_delta_ += chunk->len();
      storage_->WriteDeltaChunk(delta_chunk, base_chunk_id);
      delta_chunk_count_++;
      chunk_size_after_delta_ += delta_chunk->len();
      total_size_compressed_ += delta_chunk->len();
      uint16_t parent_depth = 0;
      if (base_chunk_id < chain_depth_by_id_.size()) {
        parent_depth = chain_depth_by_id_[base_chunk_id];
      }
      const uint16_t depth = parent_depth == UINT16_MAX
          ? UINT16_MAX
          : static_cast<uint16_t>(parent_depth + 1);
      RecordChainDepth(chunk->id(), depth);
    };

    auto feature = CalculateFeatureTimed(chunk);
    {
      auto candidates = QueryTopKTimed(feature, kTopK);
      if (candidates.empty()) {
        InsertFeatureTimed(feature, chunk->id());
        write_base_chunk(chunk);
        continue;
      }

      chunk_id best_id = candidates.front().first;

      {
        if (best_id >= chain_depth_by_id_.size()) {
          LOG(FATAL) << "Missing chain depth for selected parent " << best_id;
        }
        const uint16_t parent_depth = chain_depth_by_id_[best_id];
        if (parent_depth >= max_chain_depth_) {
          InsertFeatureTimed(feature, chunk->id());
          write_base_chunk(chunk);
          continue;
        }
      }

      std::shared_ptr<Chunk> best_delta;
      size_t best_delta_len = SIZE_MAX;
      const size_t short_circuit_threshold =
          static_cast<size_t>(chunk->len() * kShortCircuitDeltaRatio);
      for (const auto &candidate : candidates) {
        const chunk_id cand_id = candidate.first;
        auto trial = storage_->GetDeltaEncodedChunk(chunk, cand_id);
        if (trial->len() < best_delta_len) {
          best_delta_len = trial->len();
          best_delta = trial->DeepCopy();
          best_id = cand_id;
        }
        if (best_delta_len <= short_circuit_threshold) {
          break;
        }
      }
      const RatioControlledStorage ratio_storage =
          DecideRatioControlledStorage(best_delta_len, chunk->len());

      if (ratio_storage == RatioControlledStorage::kBaseIndex) {
        InsertFeatureTimed(feature, chunk->id());
        write_base_chunk(chunk);
        continue;
      }
      write_delta_chunk(chunk, best_delta, best_id);
      if (ratio_storage == RatioControlledStorage::kDeltaIndex) {
        InsertFeatureTimed(feature, chunk->id());
      }
      file_meta.end_chunk_id = chunk->id();
      continue;
    }

  }

  const auto t_end = std::chrono::steady_clock::now();
  const uint64_t elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(t_end - t_start)
          .count();
  const uint64_t file_bytes = total_size_origin_ - size_before;
  total_input_bytes_ += file_bytes;
  total_elapsed_ms_ += elapsed_ms;
  total_files_processed_++;

  const double mb = file_bytes / 1048576.0;
  const double secs = elapsed_ms / 1000.0;
  const double throughput = (secs > 0.0) ? (mb / secs) : 0.0;
  if (print_file_timing_) {
    std::cout << "[timing] AddFile " << file_name << ": "
              << std::fixed << std::setprecision(1)
              << mb << " MB in " << elapsed_ms << " ms"
              << " (" << throughput << " MB/s)" << std::endl;
  }

  file_meta_writer_.Write(file_meta);

}

DeltaCompression::~DeltaCompression() {
  // Cumulative throughput summary.
  if (total_files_processed_ > 0) {
    const double total_mb = total_input_bytes_ / 1048576.0;
    const double total_secs = total_elapsed_ms_ / 1000.0;
    const double avg_throughput = (total_secs > 0.0) ? (total_mb / total_secs) : 0.0;
    std::cout << "[timing] Total: " << total_files_processed_ << " file(s), "
              << std::fixed << std::setprecision(1)
              << total_mb << " MB in " << total_elapsed_ms_ << " ms"
              << " (avg " << avg_throughput << " MB/s)" << std::endl;
  }
  PrintSimilarityDetectionTelemetry();

  auto print_ratio = [](size_t a, size_t b) {
    if (b == 0) {
      std::cout << "(N/A)" << std::endl;
      return;
    }
    double ratio = (double)a / (double)b;
    std::cout << std::fixed << std::setprecision(1);
    std::cout << "(" << ratio * 100 << "%)" << std::endl;
    std::cout << std::defaultfloat;
  };
  uint32_t chunk_count =
      base_chunk_count_ + delta_chunk_count_ + duplicate_chunk_count_;
  std::cout << "Total chunk count: " << chunk_count << std::endl;
  std::cout << "Base chunk count: " << base_chunk_count_;
  print_ratio(base_chunk_count_, chunk_count);
  std::cout << "Delta chunk count: " << delta_chunk_count_;
  print_ratio(delta_chunk_count_, chunk_count);
  std::cout << "Duplicate chunk count: " << duplicate_chunk_count_;
  print_ratio(duplicate_chunk_count_, chunk_count);
  // DRR captures the combined reduction from exact deduplication and delta
  // encoding. DCR uses the post-deduplication logical size as its numerator so
  // that it reports only the resemblance/delta-compression stage.
  std::cout << "DRR (Data Reduction Ratio): ";
  print_ratio(total_size_origin_, total_size_compressed_);
  std::cout << "DCR (Delta Compression Ratio): ";
  print_ratio(total_size_deduplicated_, total_size_compressed_);
  std::cout << "before " << total_size_origin_
            << " after: " << total_size_compressed_ << std::endl;
  std::cout << "deduplicated size: " << total_size_deduplicated_ << std::endl;
  std::cout << "DCE (Delta Compression Efficiency): ";
  print_ratio(chunk_size_after_delta_, chunk_size_before_delta_);
}

DeltaCompression::DeltaCompression() {
  auto config = Config::Instance().get();
  auto chunk_data_path = *config->get_as<std::string>("chunk_data_path");
  auto chunk_meta_path = *config->get_as<std::string>("chunk_meta_path");
  auto file_meta_path = *config->get_as<std::string>("file_meta_path");
  for (const auto &path : {chunk_data_path, chunk_meta_path, file_meta_path}) {
    const auto parent = std::filesystem::path(path).parent_path();
    if (!parent.empty()) std::filesystem::create_directories(parent);
  }

  auto chunker = config->get_table("chunker");
  const auto min_chunk_size = *chunker->get_as<int64_t>("min_chunk_size");
  const auto max_chunk_size = *chunker->get_as<int64_t>("max_chunk_size");
  const auto stop_mask = *chunker->get_as<int64_t>("stop_mask");
  chunker_ =
      std::make_unique<FastCDC>(min_chunk_size, max_chunk_size, stop_mask);

  auto pacsi_config = config->get_table("pacsi");
  const uint32_t pacsi_confident_match = pacsi_config
      ? static_cast<uint32_t>(pacsi_config->get_as<int64_t>(
            "confident_match_count").value_or(4))
      : 4;
  const uint32_t pacsi_confidence_margin = pacsi_config
      ? static_cast<uint32_t>(pacsi_config->get_as<int64_t>(
            "confidence_margin").value_or(1))
      : 1;
  const size_t pacsi_fallback_initial = pacsi_config
      ? static_cast<size_t>(pacsi_config->get_as<int64_t>(
            "fallback_initial_budget").value_or(8))
      : 8;
  const size_t pacsi_fallback_max = pacsi_config
      ? static_cast<size_t>(pacsi_config->get_as<int64_t>(
            "fallback_max_budget").value_or(48))
      : 48;
  const int64_t pacsi_split_threshold = pacsi_config
      ? pacsi_config->get_as<int64_t>("split_threshold").value_or(32)
      : 32;
  const int64_t pacsi_max_chain_depth = pacsi_config
      ? pacsi_config->get_as<int64_t>("max_chain_depth").value_or(8)
      : 8;
  const int64_t pacsi_depth_match_tolerance = pacsi_config
      ? pacsi_config->get_as<int64_t>("depth_match_tolerance").value_or(1)
      : 1;
  if (pacsi_max_chain_depth < 1 || pacsi_max_chain_depth > UINT16_MAX) {
    LOG(FATAL) << "[pacsi].max_chain_depth must be in [1, "
               << UINT16_MAX << "] for EvoRef chain control";
  }
  if (pacsi_split_threshold < 1 || pacsi_split_threshold > 1048576) {
    LOG(FATAL) << "[pacsi].split_threshold must be in [1, 1048576]";
  }
  if (pacsi_depth_match_tolerance < 0 || pacsi_depth_match_tolerance > 8) {
    LOG(FATAL) << "[pacsi].depth_match_tolerance must be in [0, 8]";
  }
  print_file_timing_ =
      config->get_as<bool>("print_file_timing").value_or<bool>(true);
  max_chain_depth_ = static_cast<uint16_t>(pacsi_max_chain_depth);

  feature_ = std::make_unique<OdessSubfeatures>();
  auto adaptive_index = std::make_unique<AdaptiveOdessSubfeatureIndex>(
      pacsi_confident_match, pacsi_confidence_margin,
      pacsi_fallback_initial, pacsi_fallback_max, true,
      static_cast<uint32_t>(pacsi_depth_match_tolerance),
      static_cast<size_t>(pacsi_split_threshold));
  adaptive_index->SetChainDepthView(&chain_depth_by_id_);
  index_ = std::move(adaptive_index);

  dedup_ = std::make_unique<Dedup>();

  auto storage = config->get_table("storage");
  auto cache_size = *storage->get_as<int64_t>("cache_size");
  storage_ = std::make_unique<Storage>(chunk_data_path, chunk_meta_path, true,
                                       cache_size);
  file_meta_writer_.Init(file_meta_path);
}

} // namespace Delta

#include "recover_file.h"
#include "chunk/chunk.h"
#include "config.h"
#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <limits>
#include <random>
#include <vector>
namespace Delta {

namespace {

struct RandomRequestStats {
  std::vector<uint64_t> latency_ns;
  uint64_t decode_count = 0;
  uint64_t content_requests = 0;
  uint64_t payload_bytes = 0;
  uint64_t recovered_bytes = 0;
};

uint64_t MixRecoveryFingerprint(uint64_t value) {
  value += UINT64_C(0x9e3779b97f4a7c15);
  value = (value ^ (value >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
  value = (value ^ (value >> 27)) * UINT64_C(0x94d049bb133111eb);
  return value ^ (value >> 31);
}

} // namespace
void RecoverFile::UpdateRecoveryChecksums(chunk_id id, const Chunk &chunk) {
  const uint8_t first = chunk.len() == 0 ? 0 : chunk.buf()[0];
  const uint8_t last = chunk.len() == 0 ? 0 : chunk.buf()[chunk.len() - 1];
  UpdateRecoveryOrderedChecksum(id, static_cast<uint32_t>(chunk.len()),
                                first, last);
  UpdateRecoveryFingerprint(id, static_cast<uint32_t>(chunk.len()),
                            first, last);
}

void RecoverFile::UpdateRecoveryOrderedChecksum(chunk_id id, uint32_t length,
                                                uint8_t first,
                                                uint8_t last) {
  recovery_checksum_ ^= static_cast<uint64_t>(id) << 32;
  recovery_checksum_ ^= static_cast<uint64_t>(length);
  if (length != 0) {
    recovery_checksum_ ^= first;
    recovery_checksum_ *= UINT64_C(1099511628211);
    recovery_checksum_ ^= last;
  }
}

void RecoverFile::UpdateRecoveryFingerprint(chunk_id id, uint32_t length,
                                            uint8_t first, uint8_t last) {
  uint64_t fingerprint_input =
      (static_cast<uint64_t>(id) << 32) ^ static_cast<uint64_t>(length);
  if (length != 0) {
    fingerprint_input ^= static_cast<uint64_t>(first) << 16;
    fingerprint_input ^= static_cast<uint64_t>(last) << 24;
  }
  // XOR makes this validation fingerprint independent of recovery policy and
  // traversal order. The chunk id prevents equal-content logical chunks from
  // cancelling merely because their bytes match.
  recovery_order_independent_fingerprint_ ^=
      MixRecoveryFingerprint(fingerprint_input);
}

void RecoverFile::RecoverSingle(const FileMeta &meta, const std::string &path) {
  const auto start = std::chrono::steady_clock::now();
  std::ofstream out;
  if (materialize_output_) {
    const std::filesystem::path output_path(path);
    if (!output_path.parent_path().empty()) {
      std::filesystem::create_directories(output_path.parent_path());
    }
    out.open(path, std::ios::out | std::ios::binary | std::ios::trunc);
    if (!out.is_open()) {
      LOG(FATAL) << "Fail to create recovery output " << path;
    }
  }

  uint64_t file_bytes = 0;
  uint64_t file_chunks = 0;
  for (uint64_t i = meta.start_chunk_id; i <= meta.end_chunk_id; i++) {
    auto chunk = storage_->GetChunkContent(static_cast<chunk_id>(i));
    if (materialize_output_) {
      out.write(reinterpret_cast<char *>(chunk->buf()), chunk->len());
    }
    UpdateRecoveryChecksums(static_cast<chunk_id>(i), *chunk);
    file_bytes += chunk->len();
    file_chunks++;
  }
  if (materialize_output_) out.close();
  const uint64_t elapsed_ns = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - start)
          .count());
  recovered_files_++;
  recovered_chunks_ += file_chunks;
  recovered_bytes_ += file_bytes;
  recovery_time_ns_ += elapsed_ns;
  if (print_file_timing_) {
    const double mb = file_bytes / 1048576.0;
    const double seconds = elapsed_ns / 1000000000.0;
    std::cout << "[recovery timing] " << meta.file_name << ": "
              << std::fixed << std::setprecision(1) << mb << " MB in "
              << elapsed_ns / 1000000.0 << " ms ("
              << (seconds == 0.0 ? 0.0 : mb / seconds) << " MB/s)"
              << std::defaultfloat << std::setprecision(6) << std::endl;
  }
  LOG(INFO) << "Recover file " << meta.file_name << " finish.";
}

void RecoverFile::RecoverSingleOars(const FileMeta &meta) {
  struct TargetDigest {
    uint32_t length = 0;
    uint8_t first = 0;
    uint8_t last = 0;
  };
  struct ForestNode {
    chunk_id id = 0;
    int32_t parent = -1;
    bool target = false;
    bool cached_root = false;
    std::vector<uint32_t> children;
  };
  struct TraversalFrame {
    uint32_t node = 0;
    size_t next_child = 0;
    std::shared_ptr<Chunk> content;
    bool entered = false;
    bool owns_active_payload = false;
  };

  const auto file_start = std::chrono::steady_clock::now();
  const auto build_start = file_start;
  if (meta.start_chunk_id > meta.end_chunk_id ||
      meta.end_chunk_id >= forest_metadata_.size()) {
    LOG(FATAL) << "Invalid chunk range for dependency forest: "
               << meta.start_chunk_id << ".." << meta.end_chunk_id;
  }

  std::vector<ForestNode> nodes;
  nodes.reserve(static_cast<size_t>(meta.end_chunk_id) -
                meta.start_chunk_id + 1);
  std::vector<TargetDigest> target_digests(
      static_cast<size_t>(meta.end_chunk_id) - meta.start_chunk_id + 1);

  // The online policy learns the no-cache closure of only the file currently
  // being requested. It never scans later recovery requests. This pass is
  // telemetry-only and also establishes the causal per-file baseline.
  std::vector<chunk_id> full_closure;
  full_closure.reserve(target_digests.size());
  for (uint64_t target = meta.start_chunk_id;
       target <= meta.end_chunk_id; ++target) {
    chunk_id current = static_cast<chunk_id>(target);
    while (forest_local_index_[current] < 0) {
      forest_local_index_[current] = 0;
      full_closure.push_back(current);
      forest_full_node_occurrences_++;
      const ChunkMeta &chunk_meta = forest_metadata_[current];
      if (chunk_meta.type == DeltaChunk) forest_full_delta_occurrences_++;
      if (chunk_meta.type == BaseChunk) break;
      current = chunk_meta.base_chunk_id;
    }
  }
  for (chunk_id id : full_closure) forest_local_index_[id] = -1;
  auto add_node = [&](chunk_id id, bool cached_root) {
    const int32_t local = static_cast<int32_t>(nodes.size());
    forest_local_index_[id] = local;
    ForestNode node;
    node.id = id;
    node.target = id >= meta.start_chunk_id && id <= meta.end_chunk_id;
    node.cached_root = cached_root;
    nodes.push_back(std::move(node));
  };

  // Build only the part of this file's dependency forest that is not already
  // covered by a decoded result retained from an earlier file. Cached cut
  // points are pinned until this file finishes so capacity admissions cannot
  // invalidate the clipped plan while it is being traversed.
  for (uint64_t target = meta.start_chunk_id;
       target <= meta.end_chunk_id; ++target) {
    chunk_id current = static_cast<chunk_id>(target);
    while (forest_local_index_[current] < 0) {
      const bool cached = storage_->PinOarsCachedChunk(current);
      if (cached) {
        add_node(current, true);
        forest_cached_roots_++;
        break;
      }
      forest_cache_probe_misses_++;
      add_node(current, false);
      const ChunkMeta &chunk_meta = forest_metadata_[current];
      if (chunk_meta.type == BaseChunk) break;
      current = chunk_meta.base_chunk_id;
    }
  }

  for (size_t local = 0; local < nodes.size(); ++local) {
    ForestNode &node = nodes[local];
    const ChunkMeta &chunk_meta = forest_metadata_[node.id];
    if (node.cached_root || chunk_meta.type == BaseChunk) continue;
    const int32_t parent = forest_local_index_[chunk_meta.base_chunk_id];
    if (parent < 0) {
      LOG(FATAL) << "Dependency-forest parent " << chunk_meta.base_chunk_id
                 << " missing for chunk " << node.id;
    }
    node.parent = parent;
    nodes[static_cast<size_t>(parent)].children.push_back(
        static_cast<uint32_t>(local));
  }
  uint64_t file_workspace_bytes =
      nodes.capacity() * sizeof(ForestNode) +
      target_digests.capacity() * sizeof(TargetDigest);
  for (const ForestNode &node : nodes) {
    file_workspace_bytes +=
        node.children.capacity() * sizeof(uint32_t);
  }
  forest_clipped_node_occurrences_ += nodes.size();
  const uint64_t build_ns = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - build_start)
          .count());
  forest_file_build_time_ns_ += build_ns;

  uint64_t file_bytes = 0;
  uint64_t file_chunks = 0;
  const auto traversal_start = std::chrono::steady_clock::now();
  std::vector<TraversalFrame> stack;
  for (size_t root = 0; root < nodes.size(); ++root) {
    if (nodes[root].parent >= 0) continue;
    stack.push_back(
        TraversalFrame{static_cast<uint32_t>(root), 0, nullptr, false, false});
    while (!stack.empty()) {
      TraversalFrame &frame = stack.back();
      ForestNode &node = nodes[frame.node];
      const ChunkMeta &chunk_meta = forest_metadata_[node.id];
      if (!frame.entered) {
        if (node.cached_root) {
          frame.content = storage_->GetOarsCachedChunk(node.id);
        } else {
          const std::shared_ptr<Chunk> parent_content = stack.size() > 1
              ? stack[stack.size() - 2].content
              : nullptr;
          frame.content = storage_->ReconstructChunkFromMetadata(
              node.id, chunk_meta, parent_content);
          if (chunk_meta.type == BaseChunk) {
            forest_reconstructed_base_nodes_++;
            frame.owns_active_payload = true;
          } else if (chunk_meta.type == DeltaChunk) {
            forest_reconstructed_delta_nodes_++;
            frame.owns_active_payload = true;
          } else {
            forest_reconstructed_duplicate_nodes_++;
          }
          if (frame.owns_active_payload) {
            forest_active_payload_bytes_ += frame.content->len();
            forest_peak_active_payload_bytes_ = std::max(
                forest_peak_active_payload_bytes_,
                forest_active_payload_bytes_);
          }
        }
        if (!frame.content) {
          LOG(FATAL) << "Dependency forest failed to reconstruct chunk "
                     << node.id;
        }
        if (node.target) {
          TargetDigest &digest =
              target_digests[node.id - meta.start_chunk_id];
          digest.length = static_cast<uint32_t>(frame.content->len());
          if (digest.length != 0) {
            digest.first = frame.content->buf()[0];
            digest.last = frame.content->buf()[digest.length - 1];
          }
          UpdateRecoveryFingerprint(node.id, digest.length, digest.first,
                                    digest.last);
          file_bytes += frame.content->len();
          file_chunks++;
        }
        frame.entered = true;
      }

      if (frame.next_child < node.children.size()) {
        const uint32_t child = node.children[frame.next_child++];
        stack.push_back(TraversalFrame{child, 0, nullptr, false, false});
        continue;
      }

      storage_->AddOarsCachedChunk(node.id, frame.content);
      if (frame.owns_active_payload) {
        forest_active_payload_bytes_ -= frame.content->len();
      }
      frame.content.reset();
      stack.pop_back();
    }
  }
  file_workspace_bytes += stack.capacity() * sizeof(TraversalFrame);
  forest_peak_file_workspace_bytes_ = std::max(
      forest_peak_file_workspace_bytes_, file_workspace_bytes);
  const uint64_t traversal_ns = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - traversal_start)
          .count());
  forest_traversal_time_ns_ += traversal_ns;

  // OARS traversal is dependency ordered. Replay compact target digests in
  // logical order to keep the checksum comparable with normal LRU recovery.
  for (size_t offset = 0; offset < target_digests.size(); ++offset) {
    const TargetDigest &digest = target_digests[offset];
    UpdateRecoveryOrderedChecksum(
        static_cast<chunk_id>(meta.start_chunk_id + offset), digest.length,
        digest.first, digest.last);
  }

  for (const ForestNode &node : nodes) forest_local_index_[node.id] = -1;
  const uint64_t elapsed_ns = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - file_start)
          .count());
  recovered_files_++;
  recovered_chunks_ += file_chunks;
  recovered_bytes_ += file_bytes;
  recovery_time_ns_ += elapsed_ns;
  if (file_chunks != static_cast<uint64_t>(meta.end_chunk_id) -
                         meta.start_chunk_id + 1) {
    LOG(FATAL) << "Dependency forest recovered " << file_chunks
               << " targets, expected "
               << static_cast<uint64_t>(meta.end_chunk_id) -
                      meta.start_chunk_id + 1;
  }
  if (print_file_timing_) {
    const double mb = file_bytes / 1048576.0;
    const double seconds = elapsed_ns / 1000000000.0;
    std::cout << "[recovery timing] " << meta.file_name << ": "
              << std::fixed << std::setprecision(1) << mb << " MB in "
              << elapsed_ns / 1000000.0 << " ms ("
              << (seconds == 0.0 ? 0.0 : mb / seconds) << " MB/s)"
              << std::defaultfloat << std::setprecision(6) << std::endl;
  }
}

void RecoverFile::RecoverSingleFile() {
  auto config = Config::Instance().get();
  auto file_to_recover = *config->get_as<std::string>("file_to_recover");
  auto output_dir = *config->get_as<std::string>("output_dir");
  auto file_meta_path = *config->get_as<std::string>("file_meta_path");

  auto meta = GetFileMetaByName(file_meta_path, file_to_recover);
  if (!meta.has_value())
    LOG(FATAL) << "Fail to recover file " << file_to_recover;
  const std::filesystem::path output_file_path =
      std::filesystem::path(output_dir) /
      std::filesystem::path(meta->file_name).filename();
  RecoverSingle(meta.value(), output_file_path.string());
}

void RecoverFile::RecoverAllFiles() {
  auto config = Config::Instance().get();
  auto output_dir = *config->get_as<std::string>("output_dir");
  auto file_meta_path = *config->get_as<std::string>("file_meta_path");

  FileMetaReader meta_reader(file_meta_path);
  while (auto meta = meta_reader.Next()) {
    if (!meta.has_value())
      break;
    if (use_oars_) {
      RecoverSingleOars(meta.value());
      storage_->ReleaseOarsCachePins();
      storage_->NoteOarsFileBoundary();
    } else {
      const std::filesystem::path output_file_path =
          std::filesystem::path(output_dir) /
          std::filesystem::path(meta->file_name).filename();
      RecoverSingle(meta.value(), output_file_path.string());
    }
  }
}

void RecoverFile::ConfigureOars(
    double decode_weight_bytes) {
  const auto plan_start = std::chrono::steady_clock::now();
  forest_metadata_ = storage_->ReadAllChunkMeta();
  const size_t chunk_count = forest_metadata_.size();
  if (chunk_count == 0) LOG(FATAL) << "No chunks available for recovery";

  std::vector<double> reconstruction_payload(chunk_count, 0.0);
  std::vector<uint32_t> reconstruction_decodes(chunk_count, 0);
  std::vector<double> descendant_references(chunk_count, 0.0);
  for (size_t i = 0; i < chunk_count; ++i) {
    const ChunkMeta &meta = forest_metadata_[i];
    if ((meta.type == DeltaChunk || meta.type == DuplicateChunk) &&
        meta.base_chunk_id >= i) {
      LOG(FATAL) << "Non-backward chunk reference " << meta.base_chunk_id
                 << " at chunk " << i;
    }
    if (meta.type == BaseChunk) {
      sequential_base_chunks_++;
      reconstruction_payload[i] = meta.size;
    } else if (meta.type == DeltaChunk) {
      sequential_delta_chunks_++;
      reconstruction_payload[i] =
          reconstruction_payload[meta.base_chunk_id] + meta.size;
      reconstruction_decodes[i] =
          reconstruction_decodes[meta.base_chunk_id] + 1;
    } else if (meta.type == DuplicateChunk) {
      sequential_duplicate_chunks_++;
      reconstruction_payload[i] = reconstruction_payload[meta.base_chunk_id];
      reconstruction_decodes[i] = reconstruction_decodes[meta.base_chunk_id];
    } else {
      LOG(FATAL) << "Unknown chunk type " << static_cast<int>(meta.type)
                 << " at chunk " << i;
    }
  }

  // This is a property of the immutable dependency graph, not of the future
  // recovery request stream. A node receives one unit for every stored
  // descendant whose reconstruction could reuse it.
  for (size_t reverse = chunk_count; reverse-- > 0;) {
    const ChunkMeta &meta = forest_metadata_[reverse];
    if (meta.type == DeltaChunk || meta.type == DuplicateChunk) {
      descendant_references[meta.base_chunk_id] +=
          1.0 + descendant_references[reverse];
    }
  }

  std::vector<double> static_benefit(chunk_count, 0.0);
  for (size_t i = 0; i < chunk_count; ++i) {
    if (descendant_references[i] <= 0.0) continue;
    const double reconstruction_cost = reconstruction_payload[i] +
        decode_weight_bytes * reconstruction_decodes[i];
    // log1p limits the influence of broad early roots. GreedyDual later adds
    // observed hit frequency and aging, so stale high-fanout nodes do not
    // remain immortal merely because the stored graph is large.
    static_benefit[i] =
        reconstruction_cost * std::log1p(descendant_references[i]);
  }

  forest_local_index_.assign(chunk_count, -1);
  recovery_cache_profile_workspace_bytes_ =
      forest_metadata_.capacity() * sizeof(ChunkMeta) +
      reconstruction_payload.capacity() * sizeof(double) +
      reconstruction_decodes.capacity() * sizeof(uint32_t) +
      descendant_references.capacity() * sizeof(double) +
      static_benefit.capacity() * sizeof(double) +
      forest_local_index_.capacity() * sizeof(int32_t);
  storage_->ConfigureOarsCache(std::move(static_benefit));
  file_aware_plan_time_ns_ = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - plan_start)
          .count());

}

void RecoverFile::RecoverRandomChunks() {
  auto config = Config::Instance().get();
  const auto random_config = config->get_table("random_recovery");
  if (!random_config) {
    LOG(FATAL) << "recover_random requires a [random_recovery] table";
  }
  const int64_t requested_samples =
      random_config->get_as<int64_t>("sample_count").value_or(20000);
  const int64_t requested_warmup =
      random_config->get_as<int64_t>("warmup_count").value_or(0);
  const int64_t requested_seed =
      random_config->get_as<int64_t>("seed").value_or(20260716);
  const bool clear_each_request =
      random_config->get_as<bool>("clear_cache_each_request")
          .value_or(false);
  const bool include_duplicates =
      random_config->get_as<bool>("include_duplicates").value_or(false);
  const bool warm_os_cache =
      random_config->get_as<bool>("warm_os_cache").value_or(true);
  if (requested_samples <= 0 || requested_warmup < 0) {
    LOG(FATAL) << "sample_count must be positive and warmup_count must be "
                  "non-negative";
  }

  const std::vector<ChunkMeta> metadata = storage_->ReadAllChunkMeta();
  if (metadata.empty()) LOG(FATAL) << "No chunks available for recovery";
  const size_t chunk_count = metadata.size();
  std::vector<uint16_t> depths(chunk_count, 0);
  std::vector<chunk_id> candidates;
  candidates.reserve(chunk_count);

  for (size_t i = 0; i < chunk_count; ++i) {
    const ChunkMeta &meta = metadata[i];
    if (meta.type != BaseChunk && meta.type != DeltaChunk &&
        meta.type != DuplicateChunk) {
      LOG(FATAL) << "Unknown chunk type " << static_cast<int>(meta.type)
                 << " at chunk " << i;
    }
    if ((meta.type == DeltaChunk || meta.type == DuplicateChunk) &&
        meta.base_chunk_id >= i) {
      LOG(FATAL) << "Non-backward chunk reference " << meta.base_chunk_id
                 << " at chunk " << i;
    }
    if (meta.type == DeltaChunk) {
      const size_t parent = meta.base_chunk_id;
      depths[i] = depths[parent] == std::numeric_limits<uint16_t>::max()
          ? depths[parent]
          : static_cast<uint16_t>(depths[parent] + 1);
    } else if (meta.type == DuplicateChunk) {
      const size_t parent = meta.base_chunk_id;
      depths[i] = depths[parent];
    }
    if (include_duplicates || meta.type != DuplicateChunk) {
      candidates.push_back(static_cast<chunk_id>(i));
    }
  }
  const uint64_t policy_workspace_bytes =
      depths.capacity() * sizeof(uint16_t) +
      metadata.capacity() * sizeof(ChunkMeta);

  uint64_t os_warmed_bytes = 0;
  uint64_t os_warm_time_ns = 0;
  if (warm_os_cache) {
    const auto warm_start = std::chrono::steady_clock::now();
    os_warmed_bytes = storage_->WarmStorageFiles();
    os_warm_time_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - warm_start)
            .count());
  }

  std::mt19937_64 random(static_cast<uint64_t>(requested_seed));
  std::uniform_int_distribution<size_t> distribution(0,
                                                      candidates.size() - 1);
  auto next_id = [&]() { return candidates[distribution(random)]; };

  storage_->ClearChunkCache(true);
  if (!clear_each_request) {
    for (int64_t i = 0; i < requested_warmup; ++i) {
      storage_->GetChunkContent(next_id());
    }
    storage_->ResetCostTelemetry();
  }

  RandomRequestStats total;
  total.latency_ns.reserve(static_cast<size_t>(requested_samples));
  std::vector<RandomRequestStats> by_depth;
  uint64_t checksum = UINT64_C(1469598103934665603);
  const auto benchmark_start = std::chrono::steady_clock::now();
  uint64_t reconstruction_time_ns = 0;
  for (int64_t sample = 0; sample < requested_samples; ++sample) {
    const chunk_id id = next_id();
    if (clear_each_request) storage_->ClearChunkCache(false);
    const uint64_t decode_before = storage_->decode_invocations();
    const uint64_t requests_before = storage_->content_requests();
    const uint64_t payload_before = storage_->payload_bytes_read();
    const auto start = std::chrono::steady_clock::now();
    const auto chunk = storage_->GetChunkContent(id);
    const uint64_t elapsed_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - start)
            .count());
    reconstruction_time_ns += elapsed_ns;
    const uint64_t decodes = storage_->decode_invocations() - decode_before;
    const uint64_t requests = storage_->content_requests() - requests_before;
    const uint64_t payload = storage_->payload_bytes_read() - payload_before;
    const uint16_t depth = depths[id];
    if (by_depth.size() <= depth) {
      by_depth.resize(static_cast<size_t>(depth) + 1);
    }
    auto record = [&](RandomRequestStats &stats) {
      stats.latency_ns.push_back(elapsed_ns);
      stats.decode_count += decodes;
      stats.content_requests += requests;
      stats.payload_bytes += payload;
      stats.recovered_bytes += chunk->len();
    };
    record(total);
    record(by_depth[depth]);
    checksum ^= static_cast<uint64_t>(id) << 32;
    checksum ^= chunk->len();
    if (chunk->len() != 0) {
      checksum ^= chunk->buf()[0];
      checksum *= UINT64_C(1099511628211);
      checksum ^= chunk->buf()[chunk->len() - 1];
    }
  }
}

void RecoverFile::VerifyAllFiles() {
  auto config = Config::Instance().get();
  const auto file_meta_path = *config->get_as<std::string>("file_meta_path");
  FileMetaReader meta_reader(file_meta_path);
  while (auto maybe_meta = meta_reader.Next()) {
    if (!maybe_meta.has_value()) break;
    const FileMeta &meta = maybe_meta.value();
    std::ifstream original(meta.file_name, std::ios::in | std::ios::binary);
    if (!original.is_open()) {
      LOG(FATAL) << "Fail to open original file for verification "
                 << meta.file_name;
    }
    for (uint64_t id = meta.start_chunk_id; id <= meta.end_chunk_id; ++id) {
      const auto chunk =
          storage_->GetChunkContent(static_cast<chunk_id>(id));
      std::vector<uint8_t> expected(chunk->len());
      original.read(reinterpret_cast<char *>(expected.data()), chunk->len());
      if (original.gcount() != static_cast<std::streamsize>(chunk->len()) ||
          std::memcmp(expected.data(), chunk->buf(), chunk->len()) != 0) {
        LOG(FATAL) << "Verification mismatch in " << meta.file_name
                   << " at chunk " << id;
      }
    }
    char extra = 0;
    if (original.read(&extra, 1)) {
      LOG(FATAL) << "Recovered chunk range is shorter than original file "
                 << meta.file_name;
    }
  }
}

RecoverFile::~RecoverFile() = default;

RecoverFile::RecoverFile() {
  auto config = Config::Instance().get();
  print_file_timing_ =
      config->get_as<bool>("print_file_timing").value_or<bool>(true);
  materialize_output_ =
      config->get_as<bool>("materialize_output").value_or<bool>(true);
  auto chunk_data_path = *config->get_as<std::string>("chunk_data_path");
  auto chunk_meta_path = *config->get_as<std::string>("chunk_meta_path");
  auto storage = config->get_table("storage");
  auto cache_size = *storage->get_as<int64_t>("cache_size");
  storage_ = std::make_unique<Storage>(chunk_data_path, chunk_meta_path, false,
                                       cache_size);

  const std::string task =
      config->get_as<std::string>("task").value_or<std::string>("");
  // recover_random owns a separate per-workload cache configuration because
  // it may exclude Duplicate chunks and can clear the application cache for
  // every request. Other recovery tasks use this common policy table.
  if (task != "recover_random") {
    const auto recovery_cache = config->get_table("recovery_cache");
    const std::string recovery_policy = recovery_cache
        ? recovery_cache->get_as<std::string>("policy")
              .value_or<std::string>("lru")
        : "lru";
    if (recovery_policy != "lru" && recovery_policy != "oars") {
      LOG(FATAL) << "Unknown recovery cache policy " << recovery_policy;
    }
    use_oars_ = recovery_policy == "oars";
    if (use_oars_ && task != "recover_all") {
      LOG(FATAL) << "OARS is available only for recover_all";
    }
    if (use_oars_ && materialize_output_) {
      LOG(FATAL) << "OARS currently supports only materialize_output=false";
    }
    if (use_oars_) {
      const double decode_weight_bytes = recovery_cache
          ? recovery_cache->get_as<double>("decode_weight_bytes")
                .value_or(65536.0)
          : 65536.0;
      if (decode_weight_bytes < 0.0) {
        LOG(FATAL) << "decode_weight_bytes must be non-negative";
      }
      ConfigureOars(decode_weight_bytes);
    }
    const bool warm_os_cache = recovery_cache
        ? recovery_cache->get_as<bool>("warm_os_cache").value_or(false)
        : false;
    if (warm_os_cache) {
      const auto warm_start = std::chrono::steady_clock::now();
      recovery_os_warm_bytes_ = storage_->WarmStorageFiles();
      recovery_os_warm_time_ns_ = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::chrono::steady_clock::now() - warm_start)
              .count());
    }
  }
}
} // namespace Delta

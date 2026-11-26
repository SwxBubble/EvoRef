#pragma once
#include "chunk/fast_cdc.h"
#include "index/adaptive_odess_subfeature_index.h"
#include "storage/file_meta.h"
#include "storage/storage.h"
#include "dedup/dedup.h"
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>
namespace Delta {
class DeltaCompression {
public:
  DeltaCompression();
  virtual ~DeltaCompression();
  virtual void AddFile(const std::string &file_name);

protected:
  void RecordChainDepth(chunk_id id, uint16_t depth);
  Feature CalculateFeatureTimed(const std::shared_ptr<Chunk> &chunk);
  std::vector<std::pair<chunk_id, uint32_t>>
  QueryTopKTimed(const Feature &feature, int k);
  void InsertFeatureTimed(const Feature &feature, chunk_id id);
  void PrintSimilarityDetectionTelemetry() const;

  std::unique_ptr<FastCDC> chunker_;
  std::unique_ptr<AdaptiveOdessSubfeatureIndex> index_;
  std::unique_ptr<Dedup> dedup_;
  std::unique_ptr<Storage> storage_;
  std::unique_ptr<OdessSubfeatures> feature_;

  FileMetaWriter file_meta_writer_;

  uint32_t base_chunk_count_ = 0;
  uint32_t delta_chunk_count_ = 0;
  uint32_t duplicate_chunk_count_ = 0;

  size_t total_size_origin_ = 0;
  // Sum of the original lengths of unique chunks only. Exact duplicate chunks
  // are excluded, so this is the logical input size after deduplication and
  // before resemblance-based delta encoding.
  size_t total_size_deduplicated_ = 0;
  size_t total_size_compressed_ = 0;
  size_t chunk_size_before_delta_ = 0;
  size_t chunk_size_after_delta_ = 0;

  // Throughput timing — accumulated across all AddFile() calls.
  uint64_t total_input_bytes_ = 0;
  uint64_t total_elapsed_ms_ = 0;
  uint32_t total_files_processed_ = 0;
  bool print_file_timing_ = true;

  // Paper-compatible similarity-detection timing. Only non-duplicate chunks
  // reach these stages. The core metric is feature calculation + index query
  // + index insertion; xdelta, storage I/O, chunking, and exact deduplication
  // are deliberately excluded. Version-boundary index maintenance is
  // reported separately and also in an inclusive speed.
  uint64_t feature_calculation_time_ns_ = 0;
  uint64_t feature_index_query_time_ns_ = 0;
  uint64_t feature_index_insertion_time_ns_ = 0;
  uint64_t feature_calculation_count_ = 0;
  uint64_t feature_index_query_count_ = 0;
  uint64_t feature_index_insertion_count_ = 0;

  // Base depth is 0 and a Delta referencing X has depth depth(X)+1.
  std::vector<uint16_t> chain_depth_by_id_;

  // A parent at this depth forces the current chunk to become a new Base.
  uint16_t max_chain_depth_ = 8;
};
} // namespace Delta

#pragma once
#include "encoder/xdelta.h"
#include "storage/cache.h"
#include <cstdio>
#include <memory>
#include <string>
#include <vector>
namespace Delta {
constexpr uint8_t BaseChunk = 1;
constexpr uint8_t DeltaChunk = 2;
constexpr uint8_t DuplicateChunk = 3;
using chunk_id = uint32_t;
class Chunk;
struct ChunkMeta {
  uint64_t offset;
  chunk_id base_chunk_id;
  uint16_t size;
  uint8_t type;
} __attribute__((aligned(16)));

class Storage {
public:
  Storage(std::string DataPath, std::string MetaPath,
          bool compress_mode, size_t cache_size);
  ~Storage();
  void WriteBaseChunk(std::shared_ptr<Chunk> chunk);
  int WriteDeltaChunk(std::shared_ptr<Chunk> chunk, chunk_id base_chunk_id);
  void WriteDuplicateChunk(std::shared_ptr<Chunk> chunk,
                           chunk_id base_chunk_id);
  std::shared_ptr<Chunk> GetChunkContent(chunk_id id);
  std::shared_ptr<Chunk> ReconstructChunkFromMetadata(
      chunk_id id, const ChunkMeta &meta,
      const std::shared_ptr<Chunk> &base_chunk);
  std::shared_ptr<Chunk> GetDeltaEncodedChunk(std::shared_ptr<Chunk> chunk,
                                              chunk_id base_chunk_id);

  // B=2: cost telemetry — number of xdelta decode invocations and encode
  // invocations that have happened since Storage was constructed.
  uint64_t decode_invocations() const { return decode_invocations_; }
  uint64_t encode_invocations() const { return encode_invocations_; }
  uint64_t content_requests() const { return content_requests_; }
  uint64_t payload_bytes_read() const { return payload_bytes_read_; }
  uint64_t cache_hits() const { return cache_.hits(); }
  uint64_t cache_misses() const { return cache_.misses(); }
  uint64_t cache_evictions() const { return cache_.evictions(); }
  uint64_t cache_rejected_admissions() const {
    return cache_.rejected_admissions();
  }
  size_t cache_priority_metadata_bytes() const {
    return cache_.priority_metadata_bytes();
  }
  void ClearChunkCache(bool reset_statistics = false) {
    cache_.clear(reset_statistics);
  }
  void ResetCostTelemetry();
  std::vector<ChunkMeta> ReadAllChunkMeta();
  uint64_t WarmStorageFiles();
  void ConfigureOarsCache(
      std::vector<double> reconstruction_benefit) {
    cache_.configure_oars(std::move(reconstruction_benefit));
  }
  void NoteOarsFileBoundary() { cache_.note_oars_file_boundary(); }
  bool PinOarsCachedChunk(chunk_id id) {
    return cache_.pin_oars(id);
  }
  std::shared_ptr<Chunk> GetOarsCachedChunk(chunk_id id) {
    return cache_.get_oars_pinned(id);
  }
  void AddOarsCachedChunk(chunk_id id, std::shared_ptr<Chunk> chunk) {
    cache_.add(id, std::move(chunk));
  }
  void ReleaseOarsCachePins() {
    cache_.release_oars_pins();
  }

private:
  XDelta encoder_;
  ChunkCache cache_;
  FILE *data_ = nullptr;
  FILE *meta_ = nullptr;
  uint64_t decode_invocations_ = 0;
  uint64_t encode_invocations_ = 0;
  uint64_t content_requests_ = 0;
  uint64_t payload_bytes_read_ = 0;
  uint64_t delta_encode_request_time_ns_ = 0;
  uint64_t encode_engine_time_ns_ = 0;
  uint64_t decode_engine_time_ns_ = 0;
};
} // namespace Delta

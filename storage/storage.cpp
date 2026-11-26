#include "storage/storage.h"
#include "chunk/chunk.h"
#include <chrono>
#include <cstdlib>
#include <glog/logging.h>
namespace Delta {

Storage::~Storage() {
  if (data_) fclose(data_);
  if (meta_) fclose(meta_);
}


Storage::Storage(std::string DataPath, std::string MetaPath,
                 bool compress_mode, size_t cache_size)
    : cache_(cache_size) {
  char fopen_flag[3] = {'r', '+', '\0'};
  if (compress_mode) {
    fopen_flag[0] = 'w';
  }
  data_ = fopen(DataPath.c_str(), fopen_flag);
  meta_ = fopen(MetaPath.c_str(), fopen_flag);
  if (!data_ || !meta_) {
    LOG(FATAL) << "Fail to open " << DataPath << " or " << MetaPath;
  }
  fseek(data_, 0, SEEK_END);
  fseek(meta_, 0, SEEK_END);
}

void Storage::ResetCostTelemetry() {
  decode_invocations_ = 0;
  encode_invocations_ = 0;
  content_requests_ = 0;
  payload_bytes_read_ = 0;
  delta_encode_request_time_ns_ = 0;
  encode_engine_time_ns_ = 0;
  decode_engine_time_ns_ = 0;
  cache_.reset_statistics();
}

std::vector<ChunkMeta> Storage::ReadAllChunkMeta() {
  fseek(meta_, 0, SEEK_END);
  const long metadata_bytes = ftell(meta_);
  if (metadata_bytes < 0 ||
      metadata_bytes % static_cast<long>(sizeof(ChunkMeta)) != 0) {
    LOG(FATAL) << "Invalid chunk metadata file size " << metadata_bytes;
  }
  const size_t count =
      static_cast<size_t>(metadata_bytes) / sizeof(ChunkMeta);
  std::vector<ChunkMeta> result(count);
  fseek(meta_, 0, SEEK_SET);
  if (count != 0 && fread(result.data(), sizeof(ChunkMeta), count, meta_) !=
                        count) {
    LOG(FATAL) << "Failed to read all chunk metadata";
  }
  fseek(meta_, 0, SEEK_END);
  return result;
}

uint64_t Storage::WarmStorageFiles() {
  constexpr size_t kBufferBytes = 4 * 1024 * 1024;
  std::vector<uint8_t> buffer(kBufferBytes);
  uint64_t warmed_bytes = 0;
  auto warm = [&](FILE *file) {
    clearerr(file);
    fseek(file, 0, SEEK_SET);
    while (true) {
      const size_t count = fread(buffer.data(), 1, buffer.size(), file);
      warmed_bytes += count;
      if (count < buffer.size()) break;
    }
    clearerr(file);
    fseek(file, 0, SEEK_END);
  };
  warm(data_);
  warm(meta_);
  return warmed_bytes;
}

void Storage::WriteBaseChunk(std::shared_ptr<Chunk> chunk) {
  ChunkMeta meta{};
  meta.offset = ftell(data_);
  meta.base_chunk_id = chunk->id();
  meta.size = chunk->len();
  meta.type = BaseChunk;

  fwrite(chunk->buf(), 1, chunk->len(), data_);
  fwrite(&meta, sizeof(ChunkMeta), 1, meta_);
  LOG(INFO) << "write base chunk " << chunk->id() << ", len " << chunk->len();
}

std::shared_ptr<Chunk>
Storage::GetDeltaEncodedChunk(std::shared_ptr<Chunk> chunk,
                              chunk_id base_chunk_id) {
  const auto request_start = std::chrono::steady_clock::now();
  auto base_chunk = cache_.get(base_chunk_id);
  if (nullptr == base_chunk) {
    base_chunk = GetChunkContent(base_chunk_id);
    cache_.add(base_chunk_id, base_chunk);
  }
  encode_invocations_++;
  const auto encode_start = std::chrono::steady_clock::now();
  auto delta_chunk = encoder_.encode(base_chunk, chunk);
  const auto request_end = std::chrono::steady_clock::now();
  encode_engine_time_ns_ += static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          request_end - encode_start)
          .count());
  delta_encode_request_time_ns_ += static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          request_end - request_start)
          .count());
  return delta_chunk;
}

int Storage::WriteDeltaChunk(std::shared_ptr<Chunk> delta_chunk,
                             chunk_id base_chunk_id) {
  ChunkMeta meta{};
  meta.offset = ftell(data_);
  meta.base_chunk_id = base_chunk_id;
  meta.size = delta_chunk->len();
  meta.type = DeltaChunk;

  fwrite(delta_chunk->buf(), 1, delta_chunk->len(), data_);
  fwrite(&meta, sizeof(ChunkMeta), 1, meta_);
  LOG(INFO) << "write delta chunk " << delta_chunk->id() << ", len "
            << delta_chunk->len();
  return delta_chunk->len();
}

std::shared_ptr<Chunk> Storage::GetChunkContent(chunk_id id) {
  content_requests_++;
  uint64_t meta_offset = id * sizeof(ChunkMeta);
  fseek(meta_, meta_offset, SEEK_SET);
  ChunkMeta meta{};
  if (fread(&meta, sizeof(ChunkMeta), 1, meta_) != 1) {
    LOG(FATAL) << "Failed to read metadata for chunk " << id;
  }

  // Duplicate chunks have no payload of their own.  Resolve the reference
  // before attempting to read from the data file.
  if (meta.type == DuplicateChunk) {
    return GetChunkContent(meta.base_chunk_id);
  }

  fseek(data_, meta.offset, SEEK_SET);
  auto raw_content = Chunk::FromFileStream(data_, meta.size, id);
  payload_bytes_read_ += meta.size;
  fseek(meta_, 0, SEEK_END);
  fseek(data_, 0, SEEK_END);
  if (meta.type == DeltaChunk) {
    auto base_chunk = cache_.get(meta.base_chunk_id);
    if (nullptr == base_chunk) {
      base_chunk = GetChunkContent(meta.base_chunk_id);
      cache_.add(meta.base_chunk_id, base_chunk);
    }
    // XDelta::decode returns a FromMemoryRef into a shared internal buffer.
    // Deep-copy so the result is independent of any subsequent encode/decode
    // call that would overwrite the buffer (top-K trials, recursive decodes,
    // cached entries reused later, etc).
    decode_invocations_++;
    const auto decode_start = std::chrono::steady_clock::now();
    auto decoded = encoder_.decode(base_chunk, raw_content)->DeepCopy();
    decode_engine_time_ns_ += static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - decode_start)
            .count());
    return decoded;
  }
  return raw_content;
}

std::shared_ptr<Chunk> Storage::ReconstructChunkFromMetadata(
    chunk_id id, const ChunkMeta &meta,
    const std::shared_ptr<Chunk> &base_chunk) {
  content_requests_++;
  if (meta.type == DuplicateChunk) {
    if (!base_chunk) {
      LOG(FATAL) << "Duplicate chunk " << id << " has no reconstructed base";
    }
    return base_chunk;
  }
  if (meta.type != BaseChunk && meta.type != DeltaChunk) {
    LOG(FATAL) << "Unknown chunk type " << static_cast<int>(meta.type)
               << " at chunk " << id;
  }

  fseek(data_, meta.offset, SEEK_SET);
  auto raw_content = Chunk::FromFileStream(data_, meta.size, id);
  payload_bytes_read_ += meta.size;
  fseek(data_, 0, SEEK_END);
  if (meta.type == BaseChunk) return raw_content;
  if (!base_chunk) {
    LOG(FATAL) << "Delta chunk " << id << " has no reconstructed base";
  }

  decode_invocations_++;
  const auto decode_start = std::chrono::steady_clock::now();
  auto decoded = encoder_.decode(base_chunk, raw_content)->DeepCopy();
  decode_engine_time_ns_ += static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - decode_start)
          .count());
  return decoded;
}

void Storage::WriteDuplicateChunk(std::shared_ptr<Chunk> chunk,
                                  chunk_id base_chunk_id) {
  ChunkMeta meta{};
  meta.base_chunk_id = base_chunk_id;
  meta.size = chunk->len();
  meta.type = DuplicateChunk;
  fseek(meta_, static_cast<uint64_t>(base_chunk_id) * sizeof(ChunkMeta),
        SEEK_SET);
  ChunkMeta base_meta{};
  if (fread(&base_meta, sizeof(ChunkMeta), 1, meta_) != 1) {
    LOG(FATAL) << "Failed to read duplicate base metadata for chunk "
               << base_chunk_id;
  }
  meta.offset = base_meta.offset;
  fseek(meta_, 0, SEEK_END);
  fwrite(&meta, sizeof(ChunkMeta), 1, meta_);
  LOG(INFO) << "write duplicate chunk " << chunk->id() << ", base id "
            << base_chunk_id;
}
} // namespace Delta

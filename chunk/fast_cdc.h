#pragma once
#include "utils/mapped_file.h"
#include <cstdint>
#include <memory>
#include <string>
namespace Delta {
class Chunk;
class FastCDC {
public:
  FastCDC(int min_ck_sz, int max_ck_sz, uint64_t stop_mask)
      : min_chunk_size(min_ck_sz), max_chunk_size(max_ck_sz), mask(stop_mask) {}
  bool ReinitWithFile(std::string file_name);
  std::shared_ptr<Chunk> GetNextChunk();

private:
  const int min_chunk_size;
  const int max_chunk_size;
  const uint64_t mask;

  mapped_file file;
  uint8_t *file_read_ptr = nullptr;
  uint64_t remaining_file_len = 0;
  uint32_t next_chunk_id_ = 0;
  uint32_t get_next_chunk_id() { return next_chunk_id_++; }
};
} // namespace Delta

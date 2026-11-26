#pragma once
#include <cstring>
#include <cstdio>
#include <memory>
#include <stdexcept>
namespace Delta {
using chunk_id = uint32_t;
class Chunk {
public:
  Chunk() = delete;
  Chunk(uint8_t *buf, bool needs_to_free, int length, chunk_id id)
      : buf_(buf), needs_to_free_(needs_to_free), length_(length), id_(id) {}
  ~Chunk() {
    if (buf_ != nullptr && needs_to_free_)
      delete[] buf_;
  }
  static std::shared_ptr<Chunk> FromMemory(void *start, size_t size, uint32_t id) {
    auto buf = new uint8_t[size];
    memcpy(buf, start, size);
    return std::make_shared<Chunk>(buf, true, size, id);
  }
  static std::shared_ptr<Chunk> FromFileStream(FILE *fp, size_t size, uint32_t id) {
    auto buf = new uint8_t[size];
    if (fread(buf, 1, size, fp) != size) {
      delete[] buf;
      throw std::runtime_error("failed to read stored chunk payload");
    }
    return std::make_shared<Chunk>(buf, true, size, id);
  }
  std::shared_ptr<Chunk> DeepCopy() {
    return FromMemory(buf_, length_, id_);
  }
  int len() const { return length_; }
  uint8_t *buf() const { return buf_; }
  chunk_id id() const { return id_; }
  // This view does not own the buffer. Use FromMemory when the bytes must
  // outlive their backing mapping or encoder workspace.
  static std::shared_ptr<Chunk> FromMemoryRef(void *start, size_t size, uint32_t id) {
    return std::make_shared<Chunk>(static_cast<uint8_t *>(start), false, size,
                                   id);
  }

private:
  uint8_t *buf_ = nullptr;
  bool needs_to_free_ = false;
  int length_;
  chunk_id id_;
};
} // namespace Delta

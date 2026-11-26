#include <unordered_map>
#include "utils/sha1.h"
#include "chunk/chunk.h"
namespace Delta {
class Dedup {
public:
  // return the base chunk id
  uint32_t ProcessChunk(std::shared_ptr<Chunk> &chunk) {
    auto digest = sha1_hash(chunk->buf(), chunk->len());
    if (mp_.count(digest)) {
      return mp_[digest];
    }
    mp_[digest] = chunk->id();
    return chunk->id();
  }
private:
  std::unordered_map<SHA1_digest, uint32_t> mp_;
};
} 

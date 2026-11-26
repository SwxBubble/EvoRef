#pragma once
#include <memory>
#include <vector>
namespace Delta {
constexpr uint32_t kOdessSamplingMask = (1U << 7) - 1;
class Chunk;
using Feature = std::vector<uint64_t>;

// EvoRef's twelve sampled ODESS subfeatures. The implementation deliberately
// retains the original uint32_t fingerprint and transform overflow behavior.
class OdessSubfeatures {
public:
  Feature operator()(const std::shared_ptr<Chunk> &chunk) const;
};
} // namespace Delta

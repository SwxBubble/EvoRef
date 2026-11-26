#pragma once
#include <cstdint>
#include <cstddef>
namespace Delta {

// Ratio-only storage and reference-admission policy. Feature-match votes are
// used to select a candidate, but never to decide whether the resulting chunk
// is admitted to the resemblance index.
enum class RatioControlledStorage {
  kDeltaNoIndex,
  kDeltaIndex,
  kBaseIndex,
};

inline RatioControlledStorage DecideRatioControlledStorage(
    uint64_t delta_bytes, uint64_t original_bytes) {
  if (original_bytes == 0) return RatioControlledStorage::kBaseIndex;

  // Exact widened products make the 30% and 60% boundaries unambiguous and
  // avoid overflow for large byte counts.
  using Wide = unsigned __int128;
  const Wide scaled_delta = static_cast<Wide>(100) * delta_bytes;
  if (scaled_delta >= static_cast<Wide>(60) * original_bytes) {
    return RatioControlledStorage::kBaseIndex;
  }
  if (scaled_delta < static_cast<Wide>(5) * original_bytes) {
    return RatioControlledStorage::kDeltaNoIndex;
  }
  return RatioControlledStorage::kDeltaIndex;
}

// Index returns up to kTopK candidates; writer xdelta-encodes against each and
// picks the one yielding the smallest delta.
constexpr int kTopK = 1;

// Short-circuit: if the best trial delta so far is already <= chunk_size *
// kShortCircuitDeltaRatio, stop trying remaining candidates. Trades a tiny
// bit of compression for K-fold CPU savings on "easy" chunks where the first
// candidate already produces a tiny delta.
constexpr double kShortCircuitDeltaRatio = 0.05;

} // namespace Delta

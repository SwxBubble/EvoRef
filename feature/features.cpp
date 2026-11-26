#include "feature/features.h"

#include "chunk/chunk.h"
#include "utils/gear.h"

#include <cstdint>

namespace Delta {
namespace {

// These constants are part of EvoRef's feature format and must not change.
constexpr uint32_t kTransformM[] = {
    0x5b49898a, 0xe4f94e27, 0x95f658b2, 0x8f9c99fc,
    0xeba8d4d8, 0xba2c8e92, 0xa868aeb4, 0xd767df82,
    0x843606a4, 0xc1e70129, 0x32d9d1b0, 0xeb91e53c,
};

constexpr uint32_t kTransformA[] = {
    0x0ff4be8c, 0x6f485986, 0x012843ff, 0x5b47dc4d,
    0x7faa9b8a, 0xd547b8ba, 0xf9979921, 0x4f5400da,
    0x725f79a9, 0x3c9321ac, 0x0032716d, 0x3f5adf5d,
};

constexpr int kFeatureCount = 12;

} // namespace

Feature OdessSubfeatures::operator()(
    const std::shared_ptr<Chunk> &chunk) const {
  Feature subfeatures(kFeatureCount, 0);
  uint32_t fingerprint = 0;

  for (size_t offset = 0; offset < chunk->len(); ++offset) {
    fingerprint = (fingerprint << 1) + GEAR_TABLE[chunk->buf()[offset]];
    if ((fingerprint & kOdessSamplingMask) != 0) continue;

    for (int feature = 0; feature < kFeatureCount; ++feature) {
      // Multiplication and addition intentionally occur in uint32_t before
      // promotion to uint64_t, matching the published implementation.
      const uint64_t transform =
          kTransformM[feature] * fingerprint + kTransformA[feature];
      if (subfeatures[feature] == 0 ||
          subfeatures[feature] >= transform) {
        subfeatures[feature] = transform;
      }
    }
  }
  return subfeatures;
}

} // namespace Delta

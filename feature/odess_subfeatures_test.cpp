#include "chunk/chunk.h"
#include "feature/features.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>

namespace Delta {
namespace {

TEST(OdessSubfeaturesTest, PreservesPublishedIntegerArithmetic) {
  std::array<uint8_t, 8192> input{};
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<uint8_t>(i * 37 + 11);
  }
  const auto chunk = Chunk::FromMemoryRef(input.data(), input.size(), 0);
  const Feature actual = OdessSubfeatures{}(chunk);
  const Feature expected = {
      0x7940ef8c, 0x6fd0df06, 0x3be7b8ff, 0xc72fe24d,
      0x63d4878a, 0xd6ae5dba, 0x7e393321, 0xd93d3dda,
      0x2a400ba9, 0x6d27a42c, 0x2c23e96d, 0x8d563d5d,
  };
  EXPECT_EQ(actual, expected);
}

} // namespace
} // namespace Delta

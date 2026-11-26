#include "delta_policy.h"

#include <gtest/gtest.h>
#include <limits>

namespace Delta {
namespace {

TEST(RatioControlledStorageTest, UsesExactThirtyAndSixtyPercentBoundaries) {
  EXPECT_EQ(DecideRatioControlledStorage(0, 0),
            RatioControlledStorage::kBaseIndex);
  EXPECT_EQ(DecideRatioControlledStorage(0, 100),
            RatioControlledStorage::kDeltaNoIndex);
  EXPECT_EQ(DecideRatioControlledStorage(2999, 10000),
            RatioControlledStorage::kDeltaNoIndex);
  EXPECT_EQ(DecideRatioControlledStorage(3000, 10000),
            RatioControlledStorage::kDeltaIndex);
  EXPECT_EQ(DecideRatioControlledStorage(5999, 10000),
            RatioControlledStorage::kDeltaIndex);
  EXPECT_EQ(DecideRatioControlledStorage(6000, 10000),
            RatioControlledStorage::kBaseIndex);
  EXPECT_EQ(DecideRatioControlledStorage(10000, 10000),
            RatioControlledStorage::kBaseIndex);
}

TEST(RatioControlledStorageTest, AvoidsOverflowForLargeByteCounts) {
  constexpr uint64_t original =
      std::numeric_limits<uint64_t>::max() - 15;
  static_assert(original % 100 == 0);
  EXPECT_EQ(DecideRatioControlledStorage(original / 100 * 29, original),
            RatioControlledStorage::kDeltaNoIndex);
  EXPECT_EQ(DecideRatioControlledStorage(original / 100 * 30, original),
            RatioControlledStorage::kDeltaIndex);
  EXPECT_EQ(DecideRatioControlledStorage(original / 100 * 60, original),
            RatioControlledStorage::kBaseIndex);
}

} // namespace
} // namespace Delta

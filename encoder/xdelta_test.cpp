#include <gtest/gtest.h>
#include "chunk/chunk.h"
#include "encoder/xdelta.h"
#include <array>
using namespace Delta;
TEST(XDELTA_TEST, BasicEncodeDecodeTest) {
    constexpr size_t test_size = 1024;
    std::array<uint8_t, test_size> buf1{}, buf2{};
    for (size_t i = 0; i < test_size; i++) {
        buf2[i] = buf1[i] = i % 256;
    }
    for (size_t i = 0; i < test_size; i += 16) {
        buf2[i] = static_cast<uint8_t>(256 - (i % 256));
    }
    auto chunk1 = Chunk::FromMemoryRef(buf1.data(), test_size, 1);
    auto chunk2 = Chunk::FromMemoryRef(buf2.data(), test_size, 2);
    XDelta xdelta;
    auto delta_chunk = xdelta.encode(chunk1, chunk2)->DeepCopy();
    auto decoded_chunk = xdelta.decode(chunk1, delta_chunk);
    auto result_buf = decoded_chunk->buf();
    for (int i = 0; i < test_size; i++) {
        ASSERT_EQ(result_buf[i], buf2[i]);
    }
}

#include "crc32.h"

#include "gtest/gtest.h"

// https://www.lammertbies.nl/comm/info/crc-calculation
// http://chrisballance.com/wp-content/uploads/2015/10/CRC-Primer.html

TEST(crc32, all) {
  char buffer[] = "123456789";
  std::uint32_t result = crc32(buffer, 9);
  EXPECT_EQ(result, 0xCBF43926);
}
/*
 * Copyright (c) 2019-2022 ExpoLab, UC Davis
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *
 */

#include "durable_layer/leveldb_durable.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

using ::testing::Test;

class LevelDBDurableTest : public Test {
 public:
  int Set(const std::string& key, const std::string& value) {
    ResConfigData config_data;
    config_data.mutable_leveldb_info()->set_path(path_);
    LevelDurable leveldb_layer(NULL, config_data);

    leveldb_layer.setDurable(key, value);
    return 0;
  }

  std::string Get(const std::string& key) {
    ResConfigData config_data;
    config_data.mutable_leveldb_info()->set_path(path_);
    LevelDurable leveldb_layer(NULL, config_data);

    std::string value = leveldb_layer.getDurable(key);
    return value;
  }

 private:
  std::string path_ = "/tmp/leveldb_test";
};

TEST_F(LevelDBDurableTest, GetEmptyValue) { EXPECT_EQ(Get("empty_key"), ""); }

TEST_F(LevelDBDurableTest, SetValue) {
  EXPECT_EQ(Set("test_key", "test_value"), 0);
}

TEST_F(LevelDBDurableTest, GetValue) {
  EXPECT_EQ(Get("test_key"), "test_value");
}

TEST_F(LevelDBDurableTest, SetNewValue) {
  EXPECT_EQ(Set("test_key", "new_value"), 0);
}

TEST_F(LevelDBDurableTest, GetNewValue) {
  EXPECT_EQ(Get("test_key"), "new_value");
}

}  // namespace

/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
#include <thread>
#include <vector>
#include <gtest/gtest.h>
#include <LatestByKeyOperatorHandler.hpp>

namespace NES
{
TEST(LatestByKeyOperatorHandlerTest, VersionsAreIndependentPerKey)
{
    LatestByKeyOperatorHandler state;
    EXPECT_TRUE(state.accept(1, 100));
    EXPECT_TRUE(state.accept(2, 1));
    EXPECT_TRUE(state.accept(1, 101));
    EXPECT_FALSE(state.accept(1, 100));
    EXPECT_FALSE(state.accept(1, 101));
    EXPECT_TRUE(state.accept(2, 2));
}

TEST(LatestByKeyOperatorHandlerTest, ZeroAndMaximumVersions)
{
    LatestByKeyOperatorHandler state;
    EXPECT_TRUE(state.accept(0, 0));
    EXPECT_FALSE(state.accept(0, 0));
    EXPECT_TRUE(state.accept(0, UINT64_MAX));
    EXPECT_FALSE(state.accept(0, UINT64_MAX - 1));
}

TEST(LatestByKeyOperatorHandlerTest, CapacityFailsWithoutEvictingOldKeys)
{
    LatestByKeyOperatorHandler state(1);
    EXPECT_TRUE(state.accept(1, 10));
    EXPECT_THROW(state.accept(2, 1), std::runtime_error);
    EXPECT_FALSE(state.accept(1, 9));
    EXPECT_TRUE(state.accept(1, 11));
}

TEST(LatestByKeyOperatorHandlerTest, IndependentQueries)
{
    LatestByKeyOperatorHandler first;
    LatestByKeyOperatorHandler second;
    EXPECT_TRUE(first.accept(1, 10));
    EXPECT_TRUE(second.accept(1, 1));
}

TEST(LatestByKeyOperatorHandlerTest, ConcurrentUpdatesNeverRollBack)
{
    LatestByKeyOperatorHandler state;
    std::vector<std::thread> threads;
    for (uint64_t t = 0; t < 4; ++t)
    {
        threads.emplace_back(
            [&state, t]
            {
                for (uint64_t v = t; v < 1000; v += 4)
                {
                    state.accept(1, v);
                }
            });
    }
    for (auto& thread : threads)
    {
        thread.join();
    }
    EXPECT_FALSE(state.accept(1, 998));
    EXPECT_FALSE(state.accept(1, 999));
    EXPECT_TRUE(state.accept(1, 1000));
}
}

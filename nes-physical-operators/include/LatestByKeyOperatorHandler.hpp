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
#pragma once

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <stdexcept>
#include <unordered_map>
#include <Runtime/Execution/OperatorHandler.hpp>

namespace NES
{
/// Per-query version state. Equal versions are first-wins; no silent eviction,
/// since evicting a key would allow a stale version to become current again.
/// The output is an upsert changelog, not a retracting table or ordered stream.
class LatestByKeyOperatorHandler final : public OperatorHandler
{
public:
    explicit LatestByKeyOperatorHandler(size_t capacity = 100000) : capacity(capacity) { }

    void start(PipelineExecutionContext&) override { }

    void stop(QueryTerminationType, PipelineExecutionContext&) override { }

    bool accept(uint64_t key, uint64_t version)
    {
        const std::lock_guard lock(mutex);
        const auto found = versions.find(key);
        if (found != versions.end())
        {
            if (version <= found->second)
            {
                return false;
            }
            found->second = version;
            return true;
        }
        if (versions.size() >= capacity)
        {
            throw std::runtime_error("LatestByKey key capacity exceeded; restart or partition the query");
        }
        versions.emplace(key, version);
        return true;
    }

private:
    const size_t capacity;
    std::mutex mutex;
    std::unordered_map<uint64_t, uint64_t> versions;
};
}

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
#include <cstdint>
#include <DataTypes/VarVal.hpp>
#include <Interface/Record.hpp>
#include <ExecutionContext.hpp>
#include <LatestByKeyOperatorHandler.hpp>
#include <LatestByKeyPhysicalOperator.hpp>
#include <function.hpp>

namespace NES
{
namespace
{
bool acceptLatest(OperatorHandler* handler, uint64_t key, uint64_t version)
{
    return static_cast<LatestByKeyOperatorHandler*>(handler)->accept(key, version);
}
}

void LatestByKeyPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    const auto keyValue = key.execute(record, ctx.pipelineMemoryProvider.arena).getRawValueAs<nautilus::val<uint64_t>>();
    const auto versionValue = version.execute(record, ctx.pipelineMemoryProvider.arena).getRawValueAs<nautilus::val<uint64_t>>();
    if (nautilus::invoke(acceptLatest, ctx.getGlobalOperatorHandler(handlerId), keyValue, versionValue))
    {
        executeChild(ctx, record);
    }
}
}

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
#include <optional>
#include <utility>
#include <Functions/PhysicalFunction.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{
class LatestByKeyPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    LatestByKeyPhysicalOperator(PhysicalFunction key, PhysicalFunction version, OperatorHandlerId handlerId)
        : key(std::move(key)), version(std::move(version)), handlerId(handlerId)
    {
    }

    void execute(ExecutionContext& ctx, Record& record) const override;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override { return child; }

    void setChild(PhysicalOperator next) override { child = std::move(next); }

private:
    PhysicalFunction key;
    PhysicalFunction version;
    OperatorHandlerId handlerId;
    std::optional<PhysicalOperator> child;
};
}

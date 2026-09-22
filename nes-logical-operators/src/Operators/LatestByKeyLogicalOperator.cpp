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

#include <Operators/LatestByKeyLogicalOperator.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <Operators/LogicalOperatorFwd.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <fmt/format.h>

#include <Configurations/Descriptor.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
LatestByKeyLogicalOperator::LatestByKeyLogicalOperator(WeakLogicalOperator self, LogicalFunction key, LogicalFunction version)
    : ManagedByOperator(std::move(self)), key(std::move(key)), version(std::move(version))
{
}

LatestByKeyLogicalOperator::LatestByKeyLogicalOperator(
    WeakLogicalOperator self, LogicalOperator child, LogicalFunction key, LogicalFunction version)
    : ManagedByOperator(std::move(self)), child(std::move(child)), key(std::move(key)), version(std::move(version))
{
    inferLocalSchema();
}

TypedLogicalOperator<LatestByKeyLogicalOperator> LatestByKeyLogicalOperator::create(LogicalFunction key, LogicalFunction version)
{
    return TypedLogicalOperator<LatestByKeyLogicalOperator>{std::move(key), std::move(version)};
}

TypedLogicalOperator<LatestByKeyLogicalOperator>
LatestByKeyLogicalOperator::create(LogicalOperator child, LogicalFunction key, LogicalFunction version)
{
    return TypedLogicalOperator<LatestByKeyLogicalOperator>{std::move(child), std::move(key), std::move(version)};
}

std::string_view LatestByKeyLogicalOperator::getName() const noexcept
{
    return NAME;
}

LogicalFunction LatestByKeyLogicalOperator::getKey() const
{
    return key;
}

LogicalFunction LatestByKeyLogicalOperator::getVersion() const
{
    return version;
}

bool LatestByKeyLogicalOperator::operator==(const LatestByKeyLogicalOperator& rhs) const
{
    return key == rhs.key && version == rhs.version && outputSchema == rhs.outputSchema && traitSet == rhs.traitSet;
};

std::string LatestByKeyLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId opId) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "LATEST_BY_KEY(opId: {}, key: {}, version: {}, traitSet: {})",
            opId,
            key.explain(verbosity),
            version.explain(verbosity),
            traitSet.explain(verbosity));
    }
    return fmt::format("LATEST_BY_KEY({}, version: {})", key.explain(verbosity), version.explain(verbosity));
}

void LatestByKeyLogicalOperator::inferLocalSchema()
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    const auto inputSchema = child->getOutputSchema();
    key = key.withInferredDataType(inputSchema);
    version = version.withInferredDataType(inputSchema);
    if (not key.getDataType().isType(DataType::Type::UINT64) || key.getDataType().nullable
        || not version.getDataType().isType(DataType::Type::UINT64) || version.getDataType().nullable)
    {
        throw CannotInferSchema("LatestByKey requires non-nullable UINT64 key and version");
    }
    outputSchema = unbind(inputSchema);
}

LatestByKeyLogicalOperator LatestByKeyLogicalOperator::withInferredSchema() const
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    auto copy = *this;
    copy.child = copy.child->withInferredSchema();
    copy.inferLocalSchema();
    return copy;
}

TraitSet LatestByKeyLogicalOperator::getTraitSet() const
{
    return traitSet;
}

LatestByKeyLogicalOperator LatestByKeyLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

LatestByKeyLogicalOperator LatestByKeyLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for latest-by-key, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    return copy;
}

LatestByKeyLogicalOperator LatestByKeyLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for latest-by-key, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    copy.inferLocalSchema();
    return copy;
}

Schema<Field, Unordered> LatestByKeyLogicalOperator::getOutputSchema() const
{
    INVARIANT(outputSchema.has_value(), "Accessed output schema before calling schema inference");
    return NES::bindToOperator(self.lock(), outputSchema.value());
}

std::vector<LogicalOperator> LatestByKeyLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

LogicalOperator LatestByKeyLogicalOperator::getChild() const
{
    PRECONDITION(child.has_value(), "Child not set when trying to retrieve child");
    return child.value();
}

Reflected Reflector<TypedLogicalOperator<LatestByKeyLogicalOperator>>::operator()(
    const TypedLogicalOperator<LatestByKeyLogicalOperator>& op, const ReflectionContext& context) const
{
    return context.reflect(
        detail::ReflectedLatestByKeyLogicalOperator{.operatorId = op.getId(), .key = op->getKey(), .version = op->getVersion()});
}

Unreflector<TypedLogicalOperator<LatestByKeyLogicalOperator>>::Unreflector(ContextType operatorMapping) : plan(std::move(operatorMapping))
{
}

TypedLogicalOperator<LatestByKeyLogicalOperator>
Unreflector<TypedLogicalOperator<LatestByKeyLogicalOperator>>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto [id, key, version] = context.unreflect<detail::ReflectedLatestByKeyLogicalOperator>(rfl);
    auto children = plan->getChildrenFor(id, context);
    if (children.size() != 1)
    {
        throw CannotDeserialize("LatestByKeyLogicalOperator requires exactly one child, but got {}", children.size());
    }
    return LatestByKeyLogicalOperator::create(children.at(0), key, version);
}
}

uint64_t std::hash<NES::LatestByKeyLogicalOperator>::operator()(const NES::LatestByKeyLogicalOperator& op) const noexcept
{
    return std::hash<NES::LogicalFunction>{}(op.getKey()) ^ (std::hash<NES::LogicalFunction>{}(op.getVersion()) << 1);
}

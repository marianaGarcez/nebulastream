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

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// Emits a keyed changelog: only records with a strictly newer UINT64 version pass.
class LatestByKeyLogicalOperator : public ManagedByOperator
{
public:
    explicit LatestByKeyLogicalOperator(WeakLogicalOperator self, LogicalFunction key, LogicalFunction version);
    LatestByKeyLogicalOperator(WeakLogicalOperator self, LogicalOperator child, LogicalFunction key, LogicalFunction version);

    static TypedLogicalOperator<LatestByKeyLogicalOperator> create(LogicalFunction key, LogicalFunction version);
    static TypedLogicalOperator<LatestByKeyLogicalOperator> create(LogicalOperator child, LogicalFunction key, LogicalFunction version);

    [[nodiscard]] LogicalFunction getKey() const;
    [[nodiscard]] LogicalFunction getVersion() const;

    [[nodiscard]] bool operator==(const LatestByKeyLogicalOperator& rhs) const;

    [[nodiscard]] LatestByKeyLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] LatestByKeyLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] LatestByKeyLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] LogicalOperator getChild() const;
    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] LatestByKeyLogicalOperator withInferredSchema() const;

private:
    static constexpr std::string_view NAME = "LatestByKey";
    std::optional<LogicalOperator> child;
    LogicalFunction key;
    LogicalFunction version;

    void inferLocalSchema();
    /// Set during schema inference
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;

    TraitSet traitSet;
    friend struct std::hash<LatestByKeyLogicalOperator>;
};

namespace detail
{
struct ReflectedLatestByKeyLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    LogicalFunction key;
    LogicalFunction version;
};
}

template <>
struct Reflector<TypedLogicalOperator<LatestByKeyLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<LatestByKeyLogicalOperator>& op, const ReflectionContext& context) const;
};

template <>
struct Unreflector<TypedLogicalOperator<LatestByKeyLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType operatorMapping);
    TypedLogicalOperator<LatestByKeyLogicalOperator> operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<LatestByKeyLogicalOperator>);
}

template <>
struct std::hash<NES::LatestByKeyLogicalOperator>
{
    uint64_t operator()(const NES::LatestByKeyLogicalOperator& op) const noexcept;
};

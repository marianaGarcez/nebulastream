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

#include <NadTgeoStboxLogicalFunction.hpp>

#include <ranges>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

NadTgeoStboxLogicalFunction::NadTgeoStboxLogicalFunction(const LogicalFunction& leftChild, const LogicalFunction& rightChild)
    : dataType(DataType::Type::UNDEFINED, DataType::NULLABLE::NOT_NULLABLE), leftChild(leftChild), rightChild(rightChild)
{
}

DataType NadTgeoStboxLogicalFunction::getDataType() const
{
    return dataType;
}

NadTgeoStboxLogicalFunction NadTgeoStboxLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

LogicalFunction NadTgeoStboxLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChildren = getChildren() | std::views::transform([&schema](auto& c) { return c.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();
    INVARIANT(newChildren.size() == 2, "NadTgeoStboxLogicalFunction expects exactly two child functions but has {}", newChildren.size());
    const auto leftType = newChildren[0].getDataType();
    const auto rightType = newChildren[1].getDataType();

    /// The right operand must be a SpatioTemporalBox, i.e. the struct laid out by the plugin data type.
    if (rightType.type != DataType::Type::STRUCT || rightType.fields.size() != 6 || rightType.fields[0].first != "xmin"
        || rightType.fields[0].second.type != DataType::Type::FLOAT64 || rightType.fields[1].first != "xmax"
        || rightType.fields[1].second.type != DataType::Type::FLOAT64 || rightType.fields[2].first != "ymin"
        || rightType.fields[2].second.type != DataType::Type::FLOAT64 || rightType.fields[3].first != "ymax"
        || rightType.fields[3].second.type != DataType::Type::FLOAT64 || rightType.fields[4].first != "tsmin"
        || rightType.fields[4].second.type != DataType::Type::UINT64 || rightType.fields[5].first != "tsmax"
        || rightType.fields[5].second.type != DataType::Type::UINT64)
    {
        throw DifferentFieldTypeExpected("NadTgeoStbox expects a SpatioTemporalBox as right type, instead got: ", rightType);
    }
    const auto nullable = leftType.nullable || rightType.nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE;

    /// MEOS nad_tgeo_stbox returns a distance, so the result is always a double.
    auto newDataType = DataType{DataType::Type::FLOAT64, nullable};
    return withDataType(newDataType).withChildren(newChildren);
}

std::vector<LogicalFunction> NadTgeoStboxLogicalFunction::getChildren() const
{
    return {leftChild, rightChild};
}

NadTgeoStboxLogicalFunction NadTgeoStboxLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 2, "NadTgeoStboxLogicalFunction requires exactly two children, but got {}", children.size());
    auto copy = *this;
    copy.leftChild = children[0];
    copy.rightChild = children[1];
    return copy;
}

std::string_view NadTgeoStboxLogicalFunction::getType() const
{
    return NAME;
}

bool NadTgeoStboxLogicalFunction::operator==(const NadTgeoStboxLogicalFunction& rhs) const
{
    return leftChild == rhs.leftChild && rightChild == rhs.rightChild;
}

std::string NadTgeoStboxLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "NadTgeoStboxLogicalFunction({}, {} : {})", leftChild.explain(verbosity), rightChild.explain(verbosity), dataType);
    }
    return fmt::format("nad_tgeo_stbox({}, {})", leftChild.explain(verbosity), rightChild.explain(verbosity));
}

Reflected Reflector<NadTgeoStboxLogicalFunction>::operator()(const NadTgeoStboxLogicalFunction& function) const
{
    return reflect(detail::ReflectedNadTgeoStboxLogicalFunction{.left = function.leftChild, .right = function.rightChild});
}

NadTgeoStboxLogicalFunction Unreflector<NadTgeoStboxLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [left, right] = unreflect<detail::ReflectedNadTgeoStboxLogicalFunction>(reflected);
    if (!left.has_value() || !right.has_value())
    {
        throw CannotDeserialize("NadTgeoStboxLogicalFunction is missing its child");
    }
    return NadTgeoStboxLogicalFunction(left.value(), right.value());
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterNAD_TGEO_STBOXLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<NadTgeoStboxLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 2)
    {
        throw CannotDeserialize("NadTgeoStboxLogicalFunction requires exactly two children, but got {}", arguments.children.size());
    }
    return NadTgeoStboxLogicalFunction(arguments.children[0], arguments.children[1]);
}
}

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

#include <EdwithinTgeoGeoLogicalFunction.hpp>

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

EdwithinTgeoGeoLogicalFunction::EdwithinTgeoGeoLogicalFunction(
    const LogicalFunction& leftChild, const LogicalFunction& middleChild, const LogicalFunction& rightChild)
    : dataType(DataType::Type::UNDEFINED, DataType::NULLABLE::NOT_NULLABLE)
    , leftChild(leftChild)
    , middleChild(middleChild)
    , rightChild(rightChild)
{
}

DataType EdwithinTgeoGeoLogicalFunction::getDataType() const
{
    return dataType;
}

EdwithinTgeoGeoLogicalFunction EdwithinTgeoGeoLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

LogicalFunction EdwithinTgeoGeoLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChildren = getChildren() | std::views::transform([&schema](auto& c) { return c.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();
    INVARIANT(
        newChildren.size() == 3, "EdwithinTgeoGeoLogicalFunction expects exactly three child functions but has {}", newChildren.size());
    const auto leftType = newChildren[0].getDataType();
    const auto middleType = newChildren[1].getDataType();
    const auto rightType = newChildren[2].getDataType();

    /// The distance operand must be numeric.
    if (rightType.type != DataType::Type::FLOAT64 && rightType.type != DataType::Type::FLOAT32)
    {
        throw DifferentFieldTypeExpected("EdwithinTgeoGeo expects a floating point distance as third argument, instead got: ", rightType);
    }

    const auto nullable = leftType.nullable || middleType.nullable || rightType.nullable ? DataType::NULLABLE::IS_NULLABLE
                                                                                        : DataType::NULLABLE::NOT_NULLABLE;
    auto newDataType = DataType{DataType::Type::BOOLEAN, nullable};
    return withDataType(newDataType).withChildren(newChildren);
}

std::vector<LogicalFunction> EdwithinTgeoGeoLogicalFunction::getChildren() const
{
    return {leftChild, middleChild, rightChild};
}

EdwithinTgeoGeoLogicalFunction EdwithinTgeoGeoLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 3, "EdwithinTgeoGeoLogicalFunction requires exactly three children, but got {}", children.size());
    auto copy = *this;
    copy.leftChild = children[0];
    copy.middleChild = children[1];
    copy.rightChild = children[2];
    return copy;
}

std::string_view EdwithinTgeoGeoLogicalFunction::getType() const
{
    return NAME;
}

bool EdwithinTgeoGeoLogicalFunction::operator==(const EdwithinTgeoGeoLogicalFunction& rhs) const
{
    return leftChild == rhs.leftChild && middleChild == rhs.middleChild && rightChild == rhs.rightChild;
}

std::string EdwithinTgeoGeoLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "EdwithinTgeoGeoLogicalFunction({}, {}, {} : {})",
            leftChild.explain(verbosity),
            middleChild.explain(verbosity),
            rightChild.explain(verbosity),
            dataType);
    }
    return fmt::format(
        "edwithin_tgeo_geo({}, {}, {})", leftChild.explain(verbosity), middleChild.explain(verbosity), rightChild.explain(verbosity));
}

Reflected Reflector<EdwithinTgeoGeoLogicalFunction>::operator()(const EdwithinTgeoGeoLogicalFunction& function) const
{
    return reflect(detail::ReflectedEdwithinTgeoGeoLogicalFunction{
        .left = function.leftChild, .middle = function.middleChild, .right = function.rightChild});
}

EdwithinTgeoGeoLogicalFunction Unreflector<EdwithinTgeoGeoLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [left, middle, right] = unreflect<detail::ReflectedEdwithinTgeoGeoLogicalFunction>(reflected);
    if (!left.has_value() || !middle.has_value() || !right.has_value())
    {
        throw CannotDeserialize("EdwithinTgeoGeoLogicalFunction is missing a child");
    }
    return EdwithinTgeoGeoLogicalFunction(left.value(), middle.value(), right.value());
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterEDWITHIN_TGEO_GEOLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<EdwithinTgeoGeoLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 3)
    {
        throw CannotDeserialize("EdwithinTgeoGeoLogicalFunction requires exactly three children, but got {}", arguments.children.size());
    }
    return EdwithinTgeoGeoLogicalFunction(arguments.children[0], arguments.children[1], arguments.children[2]);
}
}

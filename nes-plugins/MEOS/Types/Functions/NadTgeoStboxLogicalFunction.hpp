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
#include <string>
#include <string_view>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// Nearest approach distance between a moving geometry and a SpatioTemporalBox.
/// Returns the minimum distance ever attained between the temporal geometry and the box.
/// This is the typed counterpart of the demo's distancetpointstbox (SIGMOD '25 Q5).
class NadTgeoStboxLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "NAD_TGEO_STBOX";

    explicit NadTgeoStboxLogicalFunction(const LogicalFunction& leftChild, const LogicalFunction& rightChild);

    [[nodiscard]] bool operator==(const NadTgeoStboxLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] NadTgeoStboxLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] NadTgeoStboxLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    DataType dataType;
    LogicalFunction leftChild;
    LogicalFunction rightChild;

    friend Reflector<NadTgeoStboxLogicalFunction>;
};

template <>
struct Reflector<NadTgeoStboxLogicalFunction>
{
    Reflected operator()(const NadTgeoStboxLogicalFunction& function) const;
};

template <>
struct Unreflector<NadTgeoStboxLogicalFunction>
{
    NadTgeoStboxLogicalFunction operator()(const Reflected& reflected) const;
};

static_assert(LogicalFunctionConcept<NadTgeoStboxLogicalFunction>);

}

namespace NES::detail
{
struct ReflectedNadTgeoStboxLogicalFunction
{
    std::optional<LogicalFunction> left;
    std::optional<LogicalFunction> right;
};
}

FMT_OSTREAM(NES::NadTgeoStboxLogicalFunction);

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

/// Ever-dwithin between a moving geometry and a static geometry: true if the temporal
/// geometry is ever within `dist` of the geometry during its lifetime.
/// Typed counterpart of the demo's tedwithin (SIGMOD '25 Q3, Q4, Q8) and of
/// MobilityNebula's edwithin_tgeo_geo(lon, lat, ts, wkt, dist).
///
/// Takes three children — MovingPoint, Polygon, distance. The physical function is
/// registered as EDWITHIN_TGEO_GEO_MovingPoint_Polygon_FLOAT64, matching the arity-generic
/// specialized dispatch in FunctionProvider.


class EdwithinTgeoGeoLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "EDWITHIN_TGEO_GEO";

    explicit EdwithinTgeoGeoLogicalFunction(
        const LogicalFunction& leftChild, const LogicalFunction& middleChild, const LogicalFunction& rightChild);

    [[nodiscard]] bool operator==(const EdwithinTgeoGeoLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] EdwithinTgeoGeoLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] EdwithinTgeoGeoLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    DataType dataType;
    LogicalFunction leftChild;
    LogicalFunction middleChild;
    LogicalFunction rightChild;

    friend Reflector<EdwithinTgeoGeoLogicalFunction>;
};

template <>
struct Reflector<EdwithinTgeoGeoLogicalFunction>
{
    Reflected operator()(const EdwithinTgeoGeoLogicalFunction& function) const;
};

template <>
struct Unreflector<EdwithinTgeoGeoLogicalFunction>
{
    EdwithinTgeoGeoLogicalFunction operator()(const Reflected& reflected) const;
};

static_assert(LogicalFunctionConcept<EdwithinTgeoGeoLogicalFunction>);

}

namespace NES::detail
{
struct ReflectedEdwithinTgeoGeoLogicalFunction
{
    std::optional<LogicalFunction> left;
    std::optional<LogicalFunction> middle;
    std::optional<LogicalFunction> right;
};
}

FMT_OSTREAM(NES::EdwithinTgeoGeoLogicalFunction);

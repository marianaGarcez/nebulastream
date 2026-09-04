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

#include <TpointEdwithinPolygonPhysicalFunction.hpp>

#include <cstdint>
#include <iostream>
#include <utility>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/FixedSizedData.hpp>
#include <Nautilus/DataTypes/StructData.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/function.hpp>
#include <nautilus/val.hpp>

#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <MEOSWrapper.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <val_concepts.hpp>

namespace NES
{

bool edwithin_proxy(double lLonVal, double lLatVal, uint64_t lTsVal, int8_t* rPtr, uint64_t rSize, double distVal)
{
    try
    {
        MEOS::Meos::ensureMeosInitialized();

        /// Not const: TemporalInstant::getGeometry() is not const-qualified.
        MEOS::Meos::TemporalInstant lInstant(lLonVal, lLatVal, lTsVal);
        if (!lInstant.getGeometry())
        {
            std::cout << "EdwithinTgeoGeo: left temporal geometry is null" << std::endl;
            return false;
        }

        /// Build the static geometry from the polygon's WKB payload, as the eintersects plugin does.
        MEOS::Meos::StaticGeometry rPolygon(3, rPtr, rSize);
        if (!rPolygon.getGeometry())
        {
            std::cout << "EdwithinTgeoGeo: right geometry is null" << std::endl;
            return false;
        }

        return static_cast<bool>(MEOS::Meos::safe_edwithin_tgeo_geo(lInstant.getGeometry(), rPolygon.getGeometry(), distVal));
    }
    catch (const std::exception& e)
    {
        std::cout << "MEOS exception in EdwithinTgeoGeo: " << e.what() << std::endl;
        return false;
    }
    catch (...)
    {
        std::cout << "Unknown error in EdwithinTgeoGeo" << std::endl;
        return false;
    }
}

TpointEdwithinPolygonPhysicalFunction::TpointEdwithinPolygonPhysicalFunction(
    PhysicalFunction leftPhysicalFunction, PhysicalFunction middlePhysicalFunction, PhysicalFunction rightPhysicalFunction)
    : leftPhysicalFunction(std::move(leftPhysicalFunction))
    , middlePhysicalFunction(std::move(middlePhysicalFunction))
    , rightPhysicalFunction(std::move(rightPhysicalFunction))
{
}

VarVal TpointEdwithinPolygonPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    /// Retrieve MovingPoint field values
    const auto leftPoint = leftPhysicalFunction.execute(record, arena).getRawValueAs<StructData>();
    const auto lLon = leftPoint.at("lon").getRawValueAs<nautilus::val<double>>();
    const auto lLat = leftPoint.at("lat").getRawValueAs<nautilus::val<double>>();
    const auto lTs = leftPoint.at("ts").getRawValueAs<nautilus::val<uint64_t>>();

    /// Retrieve the polygon's vertex buffer
    const auto middlePolygon = middlePhysicalFunction.execute(record, arena).getRawValueAs<StructData>();
    const auto verticesVector = middlePolygon.at("vertices").getRawValueAs<VarArrayData>();
    const nautilus::val<int8_t*> verticesPtr = verticesVector.getRawPtr();
    const nautilus::val<uint64_t> verticesSize = verticesVector.getTotalSizeInBytes();

    /// Retrieve the distance threshold
    const auto dist = rightPhysicalFunction.execute(record, arena).getRawValueAs<nautilus::val<double>>();

    const auto result = nautilus::invoke(edwithin_proxy, lLon, lLat, lTs, verticesPtr, verticesSize, dist);

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterEDWITHIN_TGEO_GEO_MovingPoint_Polygon_FLOAT64PhysicalFunction(PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 3, "EdwithinTgeoGeo expects exactly three children functions");
    return TpointEdwithinPolygonPhysicalFunction(
        arguments.childFunctions[0], arguments.childFunctions[1], arguments.childFunctions[2]);
}

}

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

#include <TpointNadStboxPhysicalFunction.hpp>

#include <cstdint>
#include <iostream>
#include <limits>
#include <utility>
#include <Functions/PhysicalFunction.hpp>
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

/// Returns infinity when the distance cannot be computed, so that downstream comparisons
/// such as `nad_tgeo_stbox(...) < 3` reject the record rather than accepting it by accident.
double nad_stbox_proxy(
    double lLonVal,
    double lLatVal,
    uint64_t lTsVal,
    double xminVal,
    double xMaxVal,
    double yminVal,
    double yMaxVal,
    uint64_t tsminVal,
    uint64_t tsmaxVal)
{
    try
    {
        MEOS::Meos::ensureMeosInitialized();

        /// Not const: TemporalInstant::getGeometry() is not const-qualified.
        MEOS::Meos::TemporalInstant lInstant(lLonVal, lLatVal, lTsVal);
        if (!lInstant.getGeometry())
        {
            std::cout << "NadTgeoStbox: left temporal geometry is null" << std::endl;
            return std::numeric_limits<double>::infinity();
        }

        const MEOS::Meos::SpatioTemporalBox stbox(xminVal, xMaxVal, yminVal, yMaxVal, tsminVal, tsmaxVal);
        if (!stbox.getBox())
        {
            std::cout << "NadTgeoStbox: right spatio-temporal box is null" << std::endl;
            return std::numeric_limits<double>::infinity();
        }

        return MEOS::Meos::safe_nad_tgeo_stbox(lInstant.getGeometry(), stbox.getBox());
    }
    catch (const std::exception& e)
    {
        std::cout << "MEOS exception in NadTgeoStbox: " << e.what() << std::endl;
        return std::numeric_limits<double>::infinity();
    }
    catch (...)
    {
        std::cout << "Unknown error in NadTgeoStbox" << std::endl;
        return std::numeric_limits<double>::infinity();
    }
}

TpointNadStboxPhysicalFunction::TpointNadStboxPhysicalFunction(
    PhysicalFunction leftPhysicalFunction, PhysicalFunction rightPhysicalFunction)
    : leftPhysicalFunction(std::move(leftPhysicalFunction)), rightPhysicalFunction(std::move(rightPhysicalFunction))
{
}

VarVal TpointNadStboxPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    /// Retrieve MovingPoint field values
    const auto leftPoint = leftPhysicalFunction.execute(record, arena).getRawValueAs<StructData>();
    const auto lLon = leftPoint.at("lon").getRawValueAs<nautilus::val<double>>();
    const auto lLat = leftPoint.at("lat").getRawValueAs<nautilus::val<double>>();
    const auto lTs = leftPoint.at("ts").getRawValueAs<nautilus::val<uint64_t>>();

    /// Retrieve SpatioTemporalBox field values
    const auto stbox = rightPhysicalFunction.execute(record, arena).getRawValueAs<StructData>();
    const auto xmin = stbox.at("xmin").getRawValueAs<nautilus::val<double>>();
    const auto xmax = stbox.at("xmax").getRawValueAs<nautilus::val<double>>();
    const auto ymin = stbox.at("ymin").getRawValueAs<nautilus::val<double>>();
    const auto ymax = stbox.at("ymax").getRawValueAs<nautilus::val<double>>();
    const auto tsmin = stbox.at("tsmin").getRawValueAs<nautilus::val<uint64_t>>();
    const auto tsmax = stbox.at("tsmax").getRawValueAs<nautilus::val<uint64_t>>();

    const auto result = nautilus::invoke(nad_stbox_proxy, lLon, lLat, lTs, xmin, xmax, ymin, ymax, tsmin, tsmax);

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterNAD_TGEO_STBOX_MovingPoint_SpatioTemporalBoxPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 2, "NadTgeoStbox expects exactly two children functions");
    return TpointNadStboxPhysicalFunction(arguments.childFunctions[0], arguments.childFunctions[1]);
}

}

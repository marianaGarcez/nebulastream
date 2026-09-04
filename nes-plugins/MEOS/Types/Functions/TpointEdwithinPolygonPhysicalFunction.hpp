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

#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Arena.hpp>

namespace NES
{

/// Checks whether a MovingPoint is ever within a given distance of a Polygon.
class TpointEdwithinPolygonPhysicalFunction final
{
public:
    explicit TpointEdwithinPolygonPhysicalFunction(
        PhysicalFunction leftPhysicalFunction, PhysicalFunction middlePhysicalFunction, PhysicalFunction rightPhysicalFunction);
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const;

private:
    PhysicalFunction leftPhysicalFunction;
    PhysicalFunction middlePhysicalFunction;
    PhysicalFunction rightPhysicalFunction;
};

static_assert(PhysicalFunctionConcept<TpointEdwithinPolygonPhysicalFunction>);

}

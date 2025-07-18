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

#include <typeinfo>
#include <Traits/OriginIdAssignerTrait.hpp>
#include <Traits/Trait.hpp>
#include <SerializableTrait.pb.h>

namespace NES
{

bool OriginIdAssignerTrait::operator==(const TraitConcept& other) const
{
    return typeid(other) == typeid(*this);
}

const std::type_info& OriginIdAssignerTrait::getType() const
{
    return typeid(this);
}

SerializableTrait OriginIdAssignerTrait::serialize() const
{
    SerializableTrait trait;
    trait.set_trait_type(getType().name());
    return trait;
}
}

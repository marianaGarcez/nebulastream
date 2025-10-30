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

#include <Traits/OutputOriginIdsTrait.hpp>

#include <cstddef>
#include <ranges>
#include <string>
#include <string_view>
#include <typeinfo>
#include <utility>
#include <variant>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <folly/hash/Hash.h>
#include <ErrorHandling.hpp>
#include <SerializableTrait.pb.h>
#include <SerializableVariantDescriptor.pb.h>
#include <TraitRegisty.hpp>

namespace NES
{

const std::type_info& OutputOriginIdsTrait::getType() const
{
    return typeid(OutputOriginIdsTrait);
}

std::string_view OutputOriginIdsTrait::getName() const
{
    return NAME;
}

SerializableTrait OutputOriginIdsTrait::serialize() const
{
    SerializableTrait trait;
    auto* serializedDescriptorConfig = trait.mutable_config();
    SerializableVariantDescriptor serializedVariant{};
    // Encode origin IDs as a comma-separated string for portability with current proto
    std::string encoded;
    for (size_t i = 0; i < originIds.size(); ++i) {
        if (i) encoded.push_back(',');
        encoded += std::to_string(originIds[i].getRawValue());
    }
    serializedVariant.set_string_value(encoded);
    (*serializedDescriptorConfig)["outputOriginIds"] = serializedVariant;
    return trait;
}

bool OutputOriginIdsTrait::operator==(const TraitConcept& other) const
{
    const auto* const casted = dynamic_cast<const OutputOriginIdsTrait*>(&other);
    if (casted == nullptr)
    {
        return false;
    }
    return this->originIds == casted->originIds;
}

size_t OutputOriginIdsTrait::hash() const
{
    return folly::hash::commutative_hash_combine_range_generic(
        7, folly::hash::StdHasher{}, originIds.begin(), originIds.end()); ///NOLINT(readability-magic-numbers)
}

OutputOriginIdsTrait::OutputOriginIdsTrait(std::vector<OriginId> originIds) : originIds(std::move(originIds))
{
}

std::string OutputOriginIdsTrait::explain(ExplainVerbosity) const
{
    return fmt::format(
        "OutputOriginIdsTrait: {}",
        fmt::join(originIds | std::views::transform([](const auto& originId) { return originId.getRawValue(); }), ", "));
}

auto OutputOriginIdsTrait::begin() const -> decltype(std::declval<std::vector<OriginId>>().cbegin())
{
    return originIds.cbegin();
}

auto OutputOriginIdsTrait::end() const -> decltype(std::declval<std::vector<OriginId>>().cend())
{
    return originIds.cend();
}

OriginId& OutputOriginIdsTrait::operator[](size_t index)
{
    return originIds.at(index);
}

const OriginId& OutputOriginIdsTrait::operator[](size_t index) const
{
    return originIds.at(index);
}

size_t OutputOriginIdsTrait::size() const
{
    return originIds.size();
}

TraitRegistryReturnType TraitGeneratedRegistrar::RegisterOutputOriginIdsTrait(TraitRegistryArguments arguments)
{
    const auto configIter = arguments.config.find("outputOriginIds");
    if (configIter == arguments.config.end())
    {
        throw CannotDeserialize("OutputOriginIdsTrait is missing in configuration");
    }
    if (!std::holds_alternative<std::string>(configIter->second))
    {
        throw CannotDeserialize("OutputOriginIdsTrait is not of type string");
    }

    const auto& encoded = std::get<std::string>(configIter->second);
    std::vector<OriginId> originIdList;
    size_t start = 0;
    while (start <= encoded.size()) {
        size_t pos = encoded.find(',', start);
        auto token = encoded.substr(start, pos == std::string::npos ? std::string::npos : pos - start);
        if (!token.empty()) {
            if (auto parsed = NES::Util::from_chars<uint64_t>(token)) {
                originIdList.emplace_back(parsed.value());
            }
        }
        if (pos == std::string::npos) break;
        start = pos + 1;
    }
    return OutputOriginIdsTrait{originIdList};
}
}

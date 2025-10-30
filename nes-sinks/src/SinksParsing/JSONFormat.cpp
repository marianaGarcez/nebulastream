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

#include <SinksParsing/JSONFormat.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <ranges>
#include <span>
#include <sstream>
#include <string>
#include <utility>
#include <DataTypes/Schema.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <MemoryLayout/VariableSizedAccess.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>

namespace NES
{

JSONFormat::JSONFormat(Schema pSchema) : Format(std::move(pSchema))
{
    PRECONDITION(schema.getNumberOfFields() != 0, "Formatter expected a non-empty schema");
    size_t offset = 0;
    for (const auto& field : schema.getFields())
    {
        const auto physicalType = field.dataType;
        formattingContext.offsets.push_back(offset);
        offset += physicalType.getSizeInBytes();
        formattingContext.physicalTypes.emplace_back(physicalType);
        formattingContext.names.emplace_back(field.name);
    }
    formattingContext.schemaSizeInBytes = schema.getSizeOfSchemaInBytes();
}

std::string JSONFormat::getFormattedBuffer(const Memory::TupleBuffer& inputBuffer) const
{
    return tupleBufferToFormattedJSONString(inputBuffer, formattingContext);
}

std::string JSONFormat::tupleBufferToFormattedJSONString(Memory::TupleBuffer tbuffer, const FormattingContext& formattingContext)
{
    std::stringstream ss;
    const auto numberOfTuples = tbuffer.getNumberOfTuples();
    auto dataSpan = tbuffer.getAvailableMemoryArea<char>();
    auto buffer = std::span<const char>(dataSpan.data(), numberOfTuples * formattingContext.schemaSizeInBytes);
    for (size_t i = 0; i < numberOfTuples; i++)
    {
        auto tuple = buffer.subspan(i * formattingContext.schemaSizeInBytes, formattingContext.schemaSizeInBytes);
        auto fields
            = std::views::iota(static_cast<size_t>(0), formattingContext.offsets.size())
            | std::views::transform(
                  [&](const auto& index)
                  {
                      auto type = formattingContext.physicalTypes[index];
                      auto offset = formattingContext.offsets[index];
                      if (type.type == DataType::Type::VARSIZED)
                      {
                          auto combined = *std::bit_cast<const uint64_t*>(&tuple[offset]);
                          auto value = MemoryLayout::readVarSizedDataAsString(tbuffer, VariableSizedAccess(combined));
                          return fmt::format(R"("{}":"{}")", formattingContext.names.at(index), value);
                      }
                      return fmt::format("\"{}\":{}", formattingContext.names.at(index), type.formattedBytesToString(&tuple[offset]));
                  });

        ss << fmt::format("{{{}}}\n", fmt::join(fields, ","));
    }
    return ss.str();
}

std::ostream& operator<<(std::ostream& out, const JSONFormat& format)
{
    return out << fmt::format("JSONFormat(Schema: {})", format.schema);
}

}

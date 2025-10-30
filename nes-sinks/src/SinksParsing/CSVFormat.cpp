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

#include <SinksParsing/CSVFormat.hpp>

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <ranges>
#include <span>
#include <sstream>
#include <string>
#include <DataTypes/Schema.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <MemoryLayout/VariableSizedAccess.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SinksParsing/Format.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
CSVFormat::CSVFormat(const Schema& schema) : CSVFormat(schema, false)
{
}

CSVFormat::CSVFormat(const Schema& pSchema, const bool escapeStrings) : Format(pSchema), escapeStrings(escapeStrings)
{
    PRECONDITION(schema.getNumberOfFields() != 0, "Formatter expected a non-empty schema");
    size_t offset = 0;
    for (const auto& field : schema.getFields())
    {
        const auto physicalType = field.dataType;
        formattingContext.offsets.push_back(offset);
        offset += physicalType.getSizeInBytes();
        formattingContext.physicalTypes.emplace_back(physicalType);
    }
    formattingContext.schemaSizeInBytes = schema.getSizeOfSchemaInBytes();
}

std::string CSVFormat::getFormattedBuffer(const Memory::TupleBuffer& inputBuffer) const
{
    return tupleBufferToFormattedCSVString(inputBuffer, formattingContext);
}

std::string CSVFormat::tupleBufferToFormattedCSVString(Memory::TupleBuffer tbuffer, const FormattingContext& formattingContext) const
{
    std::stringstream ss;
    const auto numberOfTuples = tbuffer.getNumberOfTuples();
    const auto dataSpan = tbuffer.getAvailableMemoryArea<std::byte>();
    const auto buffer = std::span<const std::byte>(dataSpan.data(), numberOfTuples * formattingContext.schemaSizeInBytes);
    for (size_t i = 0; i < numberOfTuples; i++)
    {
        auto tuple = buffer.subspan(i * formattingContext.schemaSizeInBytes, formattingContext.schemaSizeInBytes);
        auto fields = std::views::iota(static_cast<size_t>(0), formattingContext.offsets.size())
            | std::views::transform(
                          [&formattingContext, &tuple, &tbuffer, copyOfEscapeStrings = escapeStrings](const auto& index)
                          {
                              const auto physicalType = formattingContext.physicalTypes[index];
                              const auto offset = formattingContext.offsets[index];
                              if (physicalType.type == DataType::Type::VARSIZED)
                              {
                                  const auto combined = *std::bit_cast<const uint64_t*>(&tuple[offset]);
                                  const auto str = MemoryLayout::readVarSizedDataAsString(tbuffer, VariableSizedAccess(combined));
                                  // If the VARSIZED content is non-printable (binary), render as BINARY(<size>)
                                  const bool isPrintable = std::ranges::all_of(
                                      str,
                                      [](unsigned char c)
                                      {
                                          // printable ASCII range excluding control characters
                                          return c >= 32 && c <= 126;
                                      });
                                  if (!isPrintable)
                                  {
                                      return fmt::format("BINARY({})", str.size());
                                  }
                                  if (copyOfEscapeStrings)
                                  {
                                      return "\"" + str + "\"";
                                  }
                                  return str;
                              }
                              return physicalType.formattedBytesToString(&tuple[offset]);
                          });
        ss << fmt::format("{}\n", fmt::join(fields, ","));
    }
    return ss.str();
}

std::ostream& operator<<(std::ostream& out, const CSVFormat& format)
{
    return out << fmt::format("CSVFormat(Schema: {})", format.schema);
}

}

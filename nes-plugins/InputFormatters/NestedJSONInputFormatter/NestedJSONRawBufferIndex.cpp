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

#include <NestedJSONRawBufferIndex.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <simdjson.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/Record.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatIndexer.hpp>
#include <RawBufferIndex.hpp>
#include <RawTupleBuffer.hpp>
#include <NestedJSONInputFormatIndexer.hpp>
#include <JsonValueParser.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>
#include <common/FunctionAttributes.hpp>

namespace NES
{
namespace
{

/// Walks a slash-separated JSON path (e.g. "EXTRA_KEY/NAME" or "MILK/CYCLES/LEFT")
/// using `find_field_unordered` at every level. We deliberately avoid `at_pointer`
/// because it calls `rewind()`, which would invalidate every `string_view` simdjson
/// has handed out for the current tuple.
inline simdjson::simdjson_result<simdjson::ondemand::value>
navigateNestedField(simdjson::simdjson_result<simdjson::ondemand::document_reference>& doc, const std::string_view fieldPath)
{
    auto slashPos = fieldPath.find('/');
    if (slashPos == std::string_view::npos)
    {
        return doc.find_field_unordered(fieldPath);
    }

    auto result = doc.find_field_unordered(fieldPath.substr(0, slashPos));
    auto path = fieldPath.substr(slashPos + 1);
    for (slashPos = path.find('/'); slashPos != std::string_view::npos; slashPos = path.find('/'))
    {
        result = result.find_field_unordered(path.substr(0, slashPos));
        path = path.substr(slashPos + 1);
    }
    return result.find_field_unordered(path);
}

struct NestedJsonTraits
{
    using BufferIndex = NestedJSONRawBufferIndex;
    using Indexer = NestedJSONInputFormatIndexer;

    static simdjson::simdjson_result<simdjson::ondemand::value>
    navigate(simdjson::simdjson_result<simdjson::ondemand::document_reference>& doc, const std::string_view fieldName)
    {
        return navigateNestedField(doc, fieldName);
    }

    static constexpr bool supportsFixedSized = true;
};

using NestedParser = JsonValueParser::JsonRecordParser<NestedJsonTraits>;

}


NestedJSONRawBufferIndex::NestedJSONRawBufferIndex()
{
    INVARIANT(
        static_cast<void*>(this) == static_cast<void*>(static_cast<RawBufferIndex*>(this)),
        "RawBufferIndex base subobject must lay out at offset 0 in NestedJSONRawBufferIndex");
}

[[nodiscard]] nautilus::val<bool>
NestedJSONRawBufferIndex::hasNext(const nautilus::val<uint64_t>&, const nautilus::val<RawBufferIndex*>& rawBufferIndex) const
{
    const nautilus::val<bool> lastTuple = readValueFromMemRef<bool>(getMemberRef(rawBufferIndex, &NestedJSONRawBufferIndex::isAtLastTuple));
    return not lastTuple;
}

Record NestedJSONRawBufferIndex::readSpanningRecord(
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const nautilus::val<int8_t*>&,
    const nautilus::val<uint64_t>&,
    const InputFormatIndexer& indexer,
    nautilus::val<RawBufferIndex*> rawBufferIndex,
    const TupleBufferRef& bufferRef, ArenaRef& arena) const
{
    Record record;
    const auto numberOfFields = bufferRef.getAllDataTypes().size();
    for (nautilus::static_val<uint64_t> i = 0; i < numberOfFields; ++i)
    {
        const auto fieldName = bufferRef.getAllFieldNames().at(i);

        if (std::ranges::find(projections, fieldName) == projections.end())
        {
            continue;
        }

        auto fieldIndex = static_cast<nautilus::val<FieldIndex>>(i);
        const auto fieldDataType = bufferRef.getAllDataTypes().at(i);
        NestedParser::writeValueToRecord(
            fieldDataType, record, fieldName, fieldIndex, rawBufferIndex, nautilus::val<const InputFormatIndexer*>(&indexer), arena);
    }
    /// Increment iterator and return record
    nautilus::invoke(
        +[](RawBufferIndex* rawBufferIndexPtr)
        {
            PRECONDITION(
                dynamic_cast<NestedJSONRawBufferIndex*>(rawBufferIndexPtr) != nullptr, "rawBufferIndex must be a NestedJSONRawBufferIndex");
            /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast): type verified by PRECONDITION above.
            auto* simdJsonBufferIndex = static_cast<NestedJSONRawBufferIndex*>(rawBufferIndexPtr);
            auto& activeIterator
                = simdJsonBufferIndex->useExtraJSON ? simdJsonBufferIndex->extraDocStreamIterator : simdJsonBufferIndex->docStreamIterator;
            ++activeIterator;
            if (not simdJsonBufferIndex->useExtraJSON and activeIterator.at_end() and simdJsonBufferIndex->hasExtraJSON)
            {
                simdJsonBufferIndex->useExtraJSON = true;
                simdJsonBufferIndex->isAtLastTuple = simdJsonBufferIndex->extraDocStreamIterator.at_end();
                return;
            }
            simdJsonBufferIndex->isAtLastTuple = activeIterator.at_end();
        },
        rawBufferIndex);
    return record;
}

/// Marks the buffer as containing no tuple delimiters by setting both offsets to `max()`.
void NestedJSONRawBufferIndex::markNoTupleDelimiters()
{
    this->offsetOfFirstTuple = std::numeric_limits<FieldIndex>::max();
    this->offsetOfLastTuple = std::numeric_limits<FieldIndex>::max();
}

void NestedJSONRawBufferIndex::markWithTupleDelimiters(const FieldIndex offsetToFirstTuple, const std::optional<FieldIndex> offsetToLastTuple)
{
    this->offsetOfFirstTuple = offsetToFirstTuple;
    this->offsetOfLastTuple = offsetToLastTuple.value_or(std::numeric_limits<FieldIndex>::max());
}

std::pair<bool, FieldIndex> NestedJSONRawBufferIndex::indexJSON(const std::string_view jsonSV)
{
    return indexJSON(jsonSV, simdjson::ondemand::DEFAULT_BATCH_SIZE);
}

std::pair<bool, FieldIndex> NestedJSONRawBufferIndex::indexJSON(const std::string_view jsonSV, size_t batchSize)
{
    const simdjson::padded_string_view paddedJSONSV{jsonSV.data(), jsonSV.size(), jsonSV.size() + simdjson::SIMDJSON_PADDING};
    this->varSizedValues.clear();
    this->parser = std::make_shared<simdjson::ondemand::parser>();
    this->parser->threaded = false;
    if (jsonSV.size() > batchSize)
    {
        throw CannotFormatSourceData("Size of raw buffer: {} exceeds NestedJSONs configured batch_size: {}", jsonSV.size(), batchSize);
    }
    docStream = std::make_shared<simdjson::ondemand::document_stream>(parser->iterate_many(paddedJSONSV, batchSize));
    docStreamIterator = docStream->begin();
    isAtLastTuple = docStreamIterator == docStream->end();
    return {docStreamIterator.at_end(), docStream->truncated_bytes()};
}

void NestedJSONRawBufferIndex::indexExtraJSON(const std::string_view jsonSV, const size_t batchSize)
{
    if (jsonSV.size() > batchSize)
    {
        throw CannotFormatSourceData("Size of raw buffer: {} exceeds NestedJSONs configured batch_size: {}", jsonSV.size(), batchSize);
    }
    extraJSON = simdjson::padded_string(jsonSV);
    extraParser = std::make_shared<simdjson::ondemand::parser>();
    extraParser->threaded = false;
    extraDocStream = std::make_shared<simdjson::ondemand::document_stream>(extraParser->iterate_many(extraJSON, batchSize));
    extraDocStreamIterator = extraDocStream->begin();
    hasExtraJSON = not extraDocStreamIterator.at_end();
    if (isAtLastTuple and hasExtraJSON)
    {
        useExtraJSON = true;
        isAtLastTuple = false;
    }
}

}

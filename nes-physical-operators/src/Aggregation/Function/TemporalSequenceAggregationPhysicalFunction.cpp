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

#include <Aggregation/Function/TemporalSequenceAggregationPhysicalFunction.hpp>
#include <ErrorHandling.hpp>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <utility>

#include <MemoryLayout/ColumnLayout.hpp>
#include <nautilus/Interface/MemoryProvider/ColumnTupleBufferMemoryProvider.hpp>
#include <nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <nautilus/Interface/PagedVector/PagedVector.hpp>
#include <nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <nautilus/Interface/Record.hpp>
#include <nautilus/function.hpp>

#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <val.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{

using nautilus::val;

constexpr static std::string_view LonFieldName = "longitude";
constexpr static std::string_view LatFieldName = "latitude";
constexpr static std::string_view TimestampFieldName = "timestamp";

TemporalSequenceAggregationPhysicalFunction::TemporalSequenceAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction lonFunction,
    PhysicalFunction latFunction,
    PhysicalFunction timestampFunction,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memProviderPagedVector)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(lonFunction), std::move(resultFieldIdentifier))
    , lonFunction(std::move(lonFunction))
    , latFunction(std::move(latFunction))
    , timestampFunction(std::move(timestampFunction))
    , memProviderPagedVector(std::move(memProviderPagedVector))
{
}

void TemporalSequenceAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, ExecutionContext& executionContext, const Nautilus::Record& record)
{
    const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState);
    
    // Execute all three input functions to get longitude, latitude, and timestamp
    auto lonValue = lonFunction.execute(record, executionContext.pipelineMemoryProvider.arena);
    auto latValue = latFunction.execute(record, executionContext.pipelineMemoryProvider.arena);
    auto timestampValue = timestampFunction.execute(record, executionContext.pipelineMemoryProvider.arena);
    
    // Create a record with all three fields
    Record aggregateStateRecord({
        {std::string(LonFieldName), lonValue},
        {std::string(LatFieldName), latValue},
        {std::string(TimestampFieldName), timestampValue}
    });
    
    const Nautilus::Interface::PagedVectorRef pagedVectorRef(memArea, memProviderPagedVector);
    pagedVectorRef.writeRecord(aggregateStateRecord, executionContext.pipelineMemoryProvider.bufferProvider);
}

void TemporalSequenceAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    /// Calling the copyFrom function of the paged vector to combine the two paged vectors by copying the content of the second paged vector to the first paged vector
    nautilus::invoke(
        +[](AggregationState* state1, AggregationState* state2) -> void
        {
            /// Reinterpret the aggregation states as PagedVector pointers
            auto* vector1 = reinterpret_cast<Nautilus::Interface::PagedVector*>(state1);
            auto* vector2 = reinterpret_cast<Nautilus::Interface::PagedVector*>(state2);
            vector1->copyFrom(*vector2);
        },
        aggregationState1,
        aggregationState2);
}

Nautilus::Record TemporalSequenceAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    /// Get the PagedVector pointer from the aggregation state
    const auto pagedVectorPtr = nautilus::invoke(
        +[](AggregationState* state) -> Nautilus::Interface::PagedVector*
        {
            return reinterpret_cast<Nautilus::Interface::PagedVector*>(state);
        },
        aggregationState);
    const Nautilus::Interface::PagedVectorRef pagedVectorRef(pagedVectorPtr, memProviderPagedVector);
    const auto allFieldNames = memProviderPagedVector->getMemoryLayout()->getSchema().getFieldNames();
    const auto numberOfEntries = invoke(
        +[](const Nautilus::Interface::PagedVector* pagedVector)
        {
            const auto numberOfEntriesVal = pagedVector->getTotalNumberOfEntries();
            return numberOfEntriesVal;
        },
        pagedVectorPtr);

    // Handle empty trajectory case
    if (numberOfEntries == val<size_t>(0))
    {
        // Return empty trajectory with just the count header (0)
        auto variableSized = pipelineMemoryProvider.arena.allocateVariableSizedData(sizeof(uint64_t));
        auto current = variableSized.getContent();
        *static_cast<nautilus::val<uint64_t*>>(current) = val<uint64_t>(0);
        
        Nautilus::Record resultRecord;
        resultRecord.write(resultFieldIdentifier, variableSized);
        return resultRecord;
    }
    
    // Each point consists of: lon (8 bytes) + lat (8 bytes) + timestamp (8 bytes) = 24 bytes
    constexpr size_t pointSize = sizeof(double) + sizeof(double) + sizeof(uint64_t);
    // Header: count (8 bytes) + data
    auto totalSize = sizeof(uint64_t) + (numberOfEntries * pointSize);

    auto variableSized = pipelineMemoryProvider.arena.allocateVariableSizedData(totalSize);

    // Write number of points as header
    auto current = variableSized.getContent();
    *static_cast<nautilus::val<uint64_t*>>(current) = numberOfEntries;
    current += sizeof(uint64_t);

    // Write each point (lon, lat, timestamp)
    const auto endIt = pagedVectorRef.end(allFieldNames);
    for (auto candidateIt = pagedVectorRef.begin(allFieldNames); candidateIt != endIt; ++candidateIt)
    {
        const auto itemRecord = *candidateIt;
        
        // Read longitude
        const auto lonValue = itemRecord.read(std::string(LonFieldName));
        lonValue.customVisit(
            [&]<typename T>(const T& type) -> VarVal
            {
                if constexpr (std::is_same_v<T, nautilus::val<double>>)
                {
                    *static_cast<nautilus::val<double*>>(current) = type;
                    current += sizeof(double);
                }
                else if constexpr (std::is_same_v<T, nautilus::val<float>>)
                {
                    // Convert float to double using nautilus casting
                    *static_cast<nautilus::val<double*>>(current) = val<double>(type);
                    current += sizeof(double);
                }
                return type;
            });
        
        // Read latitude
        const auto latValue = itemRecord.read(std::string(LatFieldName));
        latValue.customVisit(
            [&]<typename T>(const T& type) -> VarVal
            {
                if constexpr (std::is_same_v<T, nautilus::val<double>>)
                {
                    *static_cast<nautilus::val<double*>>(current) = type;
                    current += sizeof(double);
                }
                else if constexpr (std::is_same_v<T, nautilus::val<float>>)
                {
                    // Convert float to double using nautilus casting
                    *static_cast<nautilus::val<double*>>(current) = val<double>(type);
                    current += sizeof(double);
                }
                return type;
            });
        
        // Read timestamp
        const auto timestampValue = itemRecord.read(std::string(TimestampFieldName));
        timestampValue.customVisit(
            [&]<typename T>(const T& type) -> VarVal
            {
                if constexpr (std::is_same_v<T, nautilus::val<uint64_t>>)
                {
                    *static_cast<nautilus::val<uint64_t*>>(current) = type;
                    current += sizeof(uint64_t);
                }
                else if constexpr (std::is_same_v<T, nautilus::val<unsigned long>> && !std::is_same_v<unsigned long, uint64_t>)
                {
                    // Convert unsigned long to uint64_t using nautilus casting
                    *static_cast<nautilus::val<uint64_t*>>(current) = val<uint64_t>(type);
                    current += sizeof(uint64_t);
                }
                return type;
            });
    }

    Nautilus::Record resultRecord;
    resultRecord.write(resultFieldIdentifier, variableSized);

    return resultRecord;
}

void TemporalSequenceAggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    nautilus::invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            /// Ensure proper alignment before placement new
            INVARIANT(reinterpret_cast<uintptr_t>(pagedVectorMemArea) % alignof(Nautilus::Interface::PagedVector) == 0,
                      "PagedVector memory must be properly aligned");
            /// Allocates a new PagedVector in the memory area provided by the pointer to the pagedvector
            auto* pagedVector = reinterpret_cast<Nautilus::Interface::PagedVector*>(pagedVectorMemArea);
            new (pagedVector) Nautilus::Interface::PagedVector();
        },
        aggregationState);
}

size_t TemporalSequenceAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    // Ensure proper alignment for std::vector inside PagedVector
    constexpr size_t alignment = alignof(std::max_align_t);
    size_t size = sizeof(Nautilus::Interface::PagedVector);
    // Round up to nearest multiple of alignment
    return ((size + alignment - 1) / alignment) * alignment;
}
void TemporalSequenceAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
{
    nautilus::invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            /// Calls the destructor of the PagedVector
            auto* pagedVector = reinterpret_cast<Nautilus::Interface::PagedVector*>(
                pagedVectorMemArea); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            pagedVector->~PagedVector();
        },
        aggregationState);
}


AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterTemporalSequenceAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments)
{
    // TEMPORAL_SEQUENCE stores lon, lat, timestamp for each point
    auto memoryLayoutSchema = Schema()
        .addField(std::string(LonFieldName), DataType::Type::FLOAT64)
        .addField(std::string(LatFieldName), DataType::Type::FLOAT64)
        .addField(std::string(TimestampFieldName), DataType::Type::UINT64);
    
    auto layout = std::make_shared<Memory::MemoryLayouts::ColumnLayout>(8192, memoryLayoutSchema);
    const std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider
        = std::make_shared<Nautilus::Interface::MemoryProvider::ColumnTupleBufferMemoryProvider>(layout);

    // Note: This registration function doesn't have access to the three separate functions
    // The actual creation happens in LowerToPhysicalWindowedAggregation.cpp
    // For now, we'll return nullptr to indicate this should be handled specially
    return nullptr;
}

}

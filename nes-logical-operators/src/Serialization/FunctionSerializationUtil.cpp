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

#include <Serialization/FunctionSerializationUtil.hpp>

#include <memory>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES::FunctionSerializationUtil
{

LogicalFunction deserializeFunction(const SerializableFunction& serializedFunction)
{
    const auto& functionType = serializedFunction.function_type();

    std::vector<LogicalFunction> deserializedChildren;
    for (const auto& child : serializedFunction.children())
    {
        deserializedChildren.emplace_back(deserializeFunction(child));
    }

    auto dataType = DataTypeSerializationUtil::deserializeDataType(serializedFunction.data_type());

    DescriptorConfig::Config functionDescriptorConfig{};
    for (const auto& [key, value] : serializedFunction.config())
    {
        functionDescriptorConfig[key] = protoToDescriptorConfigType(value);
    }

    auto argument = LogicalFunctionRegistryArguments(functionDescriptorConfig, deserializedChildren, dataType);

    if (auto function = LogicalFunctionRegistry::instance().create(functionType, argument))
    {
        return function.value();
    }
    throw CannotDeserialize("Logical Function: {}", serializedFunction.DebugString());
}

std::shared_ptr<WindowAggregationLogicalFunction>
deserializeWindowAggregationFunction(const SerializableAggregationFunction& serializedFunction)
{
    const auto& type = serializedFunction.type();
    auto onField = deserializeFunction(serializedFunction.on_field());
    auto asField = deserializeFunction(serializedFunction.as_field());

    // Default path: single-input aggregations encoded as on_field + as_field
    if (auto fieldAccess = onField.tryGet<FieldAccessLogicalFunction>())
    {
        if (auto asFieldAccess = asField.tryGet<FieldAccessLogicalFunction>())
        {
            AggregationLogicalFunctionRegistryArguments args;

            // Special-case deserialization for TemporalSequence with extra inputs
            if (type == std::string("TemporalSequence"))
            {
                const auto& onFieldProto = serializedFunction.on_field();
                auto it = onFieldProto.config().find("temporal_sequence_inputs");
                if (it == onFieldProto.config().end() || !it->second.has_function_list())
                {
                    throw UnknownLogicalOperator();
                }
                const auto& fnList = it->second.function_list();
                if (fnList.functions_size() != 2)
                {
                    throw UnknownLogicalOperator();
                }
                // Deserialize lat and timestamp from the function list
                auto latFn = deserializeFunction(fnList.functions(0));
                auto tsFn = deserializeFunction(fnList.functions(1));
                auto latAccess = latFn.tryGet<FieldAccessLogicalFunction>();
                auto tsAccess = tsFn.tryGet<FieldAccessLogicalFunction>();
                if (!latAccess || !tsAccess)
                {
                    throw UnknownLogicalOperator();
                }
                args.fields = {fieldAccess.value(), latAccess.value(), tsAccess.value(), asFieldAccess.value()};
            }
            else
            {
                args.fields = {fieldAccess.value(), asFieldAccess.value()};
            }

            if (auto function = AggregationLogicalFunctionRegistry::instance().create(type, args))
            {
                return function.value();
            }
        }
    }
    throw UnknownLogicalOperator();
}

}

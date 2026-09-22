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
#include <Interface/RecordLayoutUtil.hpp>

#include <cstdint>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <DataTypes/StructData.hpp>
#include <DataTypes/FixedSizedData.hpp>
#include <DataTypes/VarArrayData.hpp>
#include <Interface/Record.hpp>
#include <ErrorHandling.hpp>
#include <static.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{
/// Array payloads may be copied verbatim only if they contain no indirect values.
bool hasIndirectValues(const DataType& type)
{
    if (type.type == DataType::Type::VARSIZED || type.type == DataType::Type::VARARRAY)
    {
        return true;
    }
    if (type.type == DataType::Type::STRUCT)
    {
        for (const auto& field : type.fields)
        {
            if (hasIndirectValues(field.second))
            {
                return true;
            }
        }
    }
    if (type.type == DataType::Type::FIXEDSIZED)
    {
        return hasIndirectValues(type.elementType.at(0));
    }
    return false;
}

/// Reads a field honoring the leading null byte. Indirect struct fields are resolved lazily;
/// their stored references must remain unchanged for subsequent readers.
VarVal readFieldValue(const DataType& dataType, const nautilus::val<int8_t*>& address, const VarSizedLoadFn& loadVarSized)
{
    /// For now, we store the null byte before the actual VarVal
    nautilus::val<bool> null = false;
    nautilus::val<int8_t*> varValRef = address;
    if (dataType.nullable)
    {
        /// Reading the first byte (null) and then incrementing the memref by 1 byte to read the actual value
        null = readValueFromMemRef<bool>(address);
        varValRef += 1;
    }
    if (dataType.type == DataType::Type::STRUCT && hasIndirectValues(dataType))
    {
        auto loader = [loadVarSized](const DataType& type, const nautilus::val<int8_t*>& ptr)
        {
            auto nestedType = type;
            nestedType.nullable = false; // Existing composite layout omits nested null bytes.
            return readFieldValue(nestedType, ptr, loadVarSized);
        };
        return VarVal{StructData{varValRef, dataType.fields, std::move(loader)}, dataType.nullable, null};
    }
    if (dataType.type == DataType::Type::VARARRAY)
    {
        if (hasIndirectValues(dataType.elementType.at(0)))
            throw NotImplemented("Stored variable arrays with indirect elements are not supported");
        nautilus::val<int8_t*> ptr = nullptr;
        nautilus::val<uint64_t> len = 0;
        if (!null)
        {
            const auto loaded = loadVarSized(varValRef);
            ptr = loaded.first;
            len = loaded.second;
        }
        return VarVal{VarArrayData{ptr, dataType.elementType.at(0), len}, dataType.nullable, null};
    }
    if (dataType.type == DataType::Type::FIXEDSIZED && hasIndirectValues(dataType))
        throw NotImplemented("Stored fixed arrays with indirect elements are not supported");
    if (dataType.type != DataType::Type::VARSIZED)
    {
        return VarVal::readVarValFromMemory(varValRef, dataType, null);
    }
    const auto [ptr, len] = loadVarSized(varValRef);
    return VarVal{VariableSizedData{ptr, len}, dataType.nullable, null};
}

/// Writes inline composite fields recursively and copies indirect payloads through the layout's storage callback.
void writeFieldValue(
    const DataType& dataType, const nautilus::val<int8_t*>& address, const VarVal& value, const VarSizedStoreFn& storeVarSized)
{
    /// For now, we store the null byte before the actual VarVal
    nautilus::val<int8_t*> addressToWriteValue = address;
    if (dataType.nullable)
    {
        /// Writing the null value to the first byte and then incrementing the memref by 1 byte to store the actual value
        VarVal{value.isNull()}.writeToMemory(addressToWriteValue);
        addressToWriteValue += 1;
    }
    if (dataType.type == DataType::Type::STRUCT)
    {
        const auto composite = value.getRawValueAs<StructData>();
        uint64_t offset = 0;
        if (!value.isNull())
        {
            for (nautilus::static_val<size_t> i = 0; i < dataType.fields.size(); ++i)
            {
                const auto& fieldType = dataType.fields[i].second;
                if (fieldType.nullable)
                {
                    throw NotImplemented("Stored structs with nullable nested fields are not supported");
                }
                writeFieldValue(fieldType, addressToWriteValue + nautilus::val<uint64_t>{offset}, composite.at(i), storeVarSized);
                offset += fieldType.getSizeInBytesWithoutNull();
            }
        }
        return;
    }
    if (dataType.type == DataType::Type::FIXEDSIZED)
    {
        if (hasIndirectValues(dataType))
            throw NotImplemented("Stored fixed arrays with indirect elements are not supported");
        if (!value.isNull())
            value.writeToMemory(addressToWriteValue);
        return;
    }
    if (dataType.type == DataType::Type::VARARRAY)
    {
        if (hasIndirectValues(dataType.elementType.at(0)))
            throw NotImplemented("Stored variable arrays with indirect elements are not supported");
        const auto array = value.getRawValueAs<VarArrayData>();
        if (!value.isNull())
            storeVarSized(addressToWriteValue, VarVal{VariableSizedData{array.getRawPtr(), array.getTotalSizeInBytes()}});
        return;
    }
    if (dataType.type != DataType::Type::VARSIZED)
    {
        /// We might have to cast the value to the correct type, e.g. VarVal could be a INT8 but the type we have to write is of type INT16.
        /// We get the correct function to call via a unordered_map
        if (const auto storeFunction = storeValueFunctionMap.find(dataType.type); storeFunction != storeValueFunctionMap.end())
        {
            storeFunction->second(value, addressToWriteValue);
            return;
        }
        throw UnknownDataType("Physical Type: {} is currently not supported", dataType);
    }
    storeVarSized(addressToWriteValue, value);
}
}

Record readRecordFields(const std::vector<FieldAccess>& fields, const VarSizedLoadFn& loadVarSized)
{
    Record record;
    for (nautilus::static_val<uint64_t> i = 0; i < fields.size(); ++i)
    {
        const auto& [name, dataType, address] = fields.at(i);
        record.write(name, readFieldValue(dataType, address, loadVarSized));
    }
    return record;
}

void writeRecordFields(const std::vector<FieldAccess>& fields, const Record& record, const VarSizedStoreFn& storeVarSized)
{
    for (nautilus::static_val<uint64_t> i = 0; i < fields.size(); ++i)
    {
        const auto& [name, dataType, address] = fields.at(i);
        if (record.hasField(name))
        {
            writeFieldValue(dataType, address, record.read(name), storeVarSized);
        }
    }
}

}

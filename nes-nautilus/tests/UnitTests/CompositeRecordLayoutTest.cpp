#include <array>
#include <cstring>
#include <vector>
#include <DataTypes/StructData.hpp>
#include <DataTypes/VarArrayData.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Identifiers/Identifier.hpp>
#include <Interface/RecordLayoutUtil.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <nautilus/function.hpp>

namespace NES
{
class CompositeRecordLayoutTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("CompositeRecordLayoutTest.log", LogLevel::LOG_DEBUG);
    }
};

TEST_F(CompositeRecordLayoutTest, PolygonReadTwiceAndCopyDoesNotModifyStoredReferences)
{
    const DataType number{DataType::Type::FLOAT64, DataType::NULLABLE::NOT_NULLABLE};
    const DataType point{DataType::Type::STRUCT, DataType::NULLABLE::NOT_NULLABLE,
                         std::string{"Point"}, {{"x", number}, {"y", number}}};
    const DataType vertices{DataType::Type::VARARRAY, DataType::NULLABLE::NOT_NULLABLE, point};
    const DataType polygon{DataType::Type::STRUCT, DataType::NULLABLE::NOT_NULLABLE,
                           std::string{"Polygon"}, {{"vertices", vertices}}};
    std::array<double, 4> coordinates{1.0, 2.0, 3.0, 4.0};
    std::array<int8_t, 16> input{}, stored{}, copied{};
    const auto name = QualifiedIdentifier::parse("polygon");
    const auto ptr = [](auto& bytes) { return nautilus::val<int8_t*>{reinterpret_cast<int8_t*>(bytes.data())}; };
    VarVal{VarArrayData{ptr(coordinates), point, nautilus::val<uint64_t>{sizeof(coordinates)}}}.writeToMemory(ptr(input));
    Record record;
    record.write(name, VarVal{StructData{ptr(input), polygon.fields}});
    std::vector<std::vector<int8_t>> payloads;
    const VarSizedStoreFn store = [&payloads](auto slot, const VarVal& value)
    {
        const auto bytes = value.getRawValueAs<VariableSizedData>();
        nautilus::invoke(+[](std::vector<std::vector<int8_t>>* values, int8_t* destination, int8_t* source, uint64_t size)
        {
            values->emplace_back(source, source + size);
            const std::array<uint64_t, 2> reference{values->size() - 1, size};
            std::memcpy(destination, reference.data(), sizeof(reference));
        }, nautilus::val<std::vector<std::vector<int8_t>>*>{&payloads}, slot, bytes.getContent(), bytes.getSize());
    };
    const VarSizedLoadFn load = [&payloads](auto slot)
    {
        const auto data = nautilus::invoke(+[](std::vector<std::vector<int8_t>>* values, int8_t* source)
        {
            uint64_t index;
            std::memcpy(&index, source, sizeof(index));
            return values->at(index).data();
        }, nautilus::val<std::vector<std::vector<int8_t>>*>{&payloads}, slot);
        const auto size = nautilus::invoke(+[](int8_t* source)
        {
            uint64_t length;
            std::memcpy(&length, source + sizeof(uint64_t), sizeof(length));
            return length;
        }, slot);
        return std::pair{data, size};
    };
    const std::vector<FieldAccess> fields{{name, polygon, ptr(stored)}};
    writeRecordFields(fields, record, store);
    const auto originalReference = stored;
    coordinates.fill(-1.0); // Stored payload must own a copy, not retain the input pointer.
    for (int i = 0; i < 2; ++i)
    {
        const auto read = readRecordFields(fields, load);
        const auto array = read.read(name).getRawValueAs<StructData>().at("vertices").getRawValueAs<VarArrayData>();
        EXPECT_EQ(array.at(nautilus::val<uint64_t>{1}).getRawValueAs<StructData>().at("x").getRawValueAs<nautilus::val<double>>(), 3.0);
        writeRecordFields({{name, polygon, ptr(copied)}}, read, store);
        EXPECT_EQ(stored, originalReference);
        const auto second = readRecordFields({{name, polygon, ptr(copied)}}, load);
        const auto secondArray = second.read(name).getRawValueAs<StructData>().at("vertices").getRawValueAs<VarArrayData>();
        EXPECT_EQ(secondArray.at(nautilus::val<uint64_t>{0}).getRawValueAs<StructData>().at("y").getRawValueAs<nautilus::val<double>>(), 2.0);
    }
}
}

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

#include <FileSource.hpp>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <format>
#include <fstream>
#include <ios>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <InlineDataRegistry.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>


namespace NES::Sources
{

FileSource::FileSource(const SourceDescriptor& sourceDescriptor) : filePath(sourceDescriptor.getFromConfig(ConfigParametersCSV::FILEPATH))
{
}

void FileSource::open()
{
    const auto realCSVPath = std::unique_ptr<char, decltype(std::free)*>{realpath(this->filePath.c_str(), nullptr), std::free};
    this->inputFile = std::ifstream(realCSVPath.get(), std::ios::binary);
    if (not this->inputFile)
    {
        throw InvalidConfigParameter("Could not determine absolute pathname: {} - {}", this->filePath.c_str(), std::strerror(errno));
    }
}

void FileSource::close()
{
    this->inputFile.close();
}
size_t FileSource::fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer, Memory::AbstractBufferProvider&, const std::stop_token&)
{
    auto span = tupleBuffer.getAvailableMemoryArea<char>();
    this->inputFile.read(span.data(), static_cast<std::streamsize>(tupleBuffer.getBufferSize()));
    const auto numBytesRead = this->inputFile.gcount();
    this->totalNumBytesRead += numBytesRead;
    return numBytesRead;
}

NES::DescriptorConfig::Config FileSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return NES::DescriptorConfig::validateAndFormat<ConfigParametersCSV>(std::move(config), NAME);
}

std::ostream& FileSource::toString(std::ostream& str) const
{
    str << std::format("\nFileSource(filepath: {}, totalNumBytesRead: {})", this->filePath, this->totalNumBytesRead.load());
    return str;
}

}


namespace NES::SourceGeneratedRegistrar {
SourceRegistryReturnType RegisterFileSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<NES::Sources::FileSource>(sourceRegistryArguments.sourceDescriptor);
}
}

namespace NES::InlineDataGeneratedRegistrar {
InlineDataRegistryReturnType RegisterFileInlineData(InlineDataRegistryArguments args)
{
    if (!args.tuples.empty())
    {
        if (auto it = args.physicalSourceConfig.sourceConfig.find(std::string("filePath"));
            it != args.physicalSourceConfig.sourceConfig.end())
        {
            it->second = args.testFilePath;
            if (std::ofstream testFile(args.testFilePath); testFile.is_open())
            {
                for (const auto& tuple : args.tuples)
                {
                    testFile << tuple << "\n";
                }
                testFile.flush();
                return args.physicalSourceConfig;
            }
            throw TestException("Could not open source file \"{}\"", args.testFilePath);
        }
        throw InvalidConfigParameter("A FileSource config must contain filePath parameter");
    }
    throw TestException("Inline data: tuples vector is empty.");
}
}

namespace NES::FileDataGeneratedRegistrar {
FileDataRegistryReturnType RegisterFileFileData(FileDataRegistryArguments args)
{
    if (auto it = args.physicalSourceConfig.sourceConfig.find(std::string("filePath"));
        it != args.physicalSourceConfig.sourceConfig.end())
    {
        it->second = args.testFilePath;
        return args.physicalSourceConfig;
    }
    throw InvalidConfigParameter("A FileSource config must contain filePath parameter.");
}
}

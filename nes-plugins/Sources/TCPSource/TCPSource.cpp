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

#include <TCPSource.hpp>

#include <cerrno> /// For socket error
#include <chrono>
#include <cstring>
#include <exception>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <sys/select.h>
#include <poll.h>
#include <algorithm>
#include <stdexcept>

#include <cstdio>
#include <fcntl.h>
#include <netdb.h>
#include <unistd.h> /// For read
#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifier.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <asm-generic/socket.h>
#include <bits/types/struct_timeval.h>
#include <cpptrace/from_current.hpp>
#include <sys/socket.h> /// For socket functions
#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <InlineDataRegistry.hpp>
#include <TCPDataServer.hpp>

namespace NES
{

TCPSource::TCPSource(const SourceDescriptor& sourceDescriptor)
    : errBuffer{}
    , socketHost(sourceDescriptor.getFromConfig(ConfigParametersTCP::HOST))
    , socketPort(std::to_string(sourceDescriptor.getFromConfig(ConfigParametersTCP::PORT)))
    , socketType(sourceDescriptor.getFromConfig(ConfigParametersTCP::TYPE))
    , socketDomain(sourceDescriptor.getFromConfig(ConfigParametersTCP::DOMAIN))
    , tupleDelimiter(sourceDescriptor.getFromConfig(ConfigParametersTCP::SEPARATOR))
    , socketBufferSize(sourceDescriptor.getFromConfig(ConfigParametersTCP::SOCKET_BUFFER_SIZE))
    , bytesUsedForSocketBufferSizeTransfer(sourceDescriptor.getFromConfig(ConfigParametersTCP::SOCKET_BUFFER_TRANSFER_SIZE))
    , flushIntervalInMs(sourceDescriptor.getFromConfig(ConfigParametersTCP::FLUSH_INTERVAL_MS))
    , connectionTimeout(sourceDescriptor.getFromConfig(ConfigParametersTCP::CONNECT_TIMEOUT))
{
    NES_TRACE("Init TCPSource.");
}

std::ostream& TCPSource::toString(std::ostream& str) const
{
    str << "\nTCPSource(";
    str << "\n  generated tuples: " << this->generatedTuples;
    str << "\n  generated buffers: " << this->generatedBuffers;
    str << "\n  connection: " << this->connection;
    str << "\n  timeout: " << connectionTimeout << " seconds";
    str << "\n  socketHost: " << socketHost;
    str << "\n  socketPort: " << socketPort;
    str << "\n  socketType: " << socketType;
    str << "\n  socketDomain: " << socketDomain;
    str << "\n  tupleDelimiter: " << tupleDelimiter;
    str << "\n  socketBufferSize: " << socketBufferSize;
    str << "\n  bytesUsedForSocketBufferSizeTransfer" << bytesUsedForSocketBufferSizeTransfer;
    str << "\n  flushIntervalInMs" << flushIntervalInMs;
    str << ")\n";
    return str;
}

bool TCPSource::tryToConnect(const addrinfo* result, const int flags)
{
    const std::chrono::seconds socketConnectDefaultTimeout{connectionTimeout};

    /// we try each addrinfo until we successfully create a socket
    while (result != nullptr)
    {
        sockfd = socket(result->ai_family, result->ai_socktype, result->ai_protocol);

        if (sockfd != -1)
        {
            break;
        }
        result = result->ai_next;
    }

    /// check if we found a vaild address
    if (result == nullptr)
    {
        NES_ERROR("No valid address found to create socket.");
        return false;
    }

    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg, hicpp-signed-bitwise) - POSIX API requires varargs
    fcntl(sockfd, F_SETFL, flags | O_NONBLOCK);

    /// set timeout for both blocking receive and send calls
    /// if timeout is set to zero, then the operation will never timeout
    /// (https://linux.die.net/man/7/socket)
    /// as a workaround, we implicitly add one microsecond to the timeout
    timeval timeout{.tv_sec = socketConnectDefaultTimeout.count(), .tv_usec = IMPLICIT_TIMEOUT_USEC};
    setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(sockfd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    connection = connect(sockfd, result->ai_addr, result->ai_addrlen);

    /// if the TCPSource did not establish a connection, try with timeout
    if (connection < 0)
    {
        if (errno != EINPROGRESS)
        {
            close();
            /// if connection was unsuccessful, throw an exception with context using errno
            const auto strerrorResult = strerror_r(errno, errBuffer.data(), errBuffer.size());
            throw CannotOpenSource("Could not connect to: {}:{}. {}", socketHost, socketPort, strerrorResult);
        }

        /// Set the timeout for the connect attempt
        fd_set fdset;
        timeval timeValue{.tv_sec = socketConnectDefaultTimeout.count(), .tv_usec = IMPLICIT_TIMEOUT_USEC};

        FD_ZERO(&fdset);
        FD_SET(sockfd, &fdset);

        connection = select(sockfd + 1, nullptr, &fdset, nullptr, &timeValue);
        if (connection <= 0)
        {
            /// Timeout or error
            errno = ETIMEDOUT;
            close();
            const auto strerrorResult = strerror_r(errno, errBuffer.data(), errBuffer.size());
            throw CannotOpenSource("Could not connect to: {}:{}. {}", socketHost, socketPort, strerrorResult);
        }

        /// Check if connect succeeded
        int error = 0;
        socklen_t len = sizeof(error);
        if (getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &error, &len) < 0 || (error != 0))
        {
            errno = error;
            close();
            const auto strerrorResult = strerror_r(errno, errBuffer.data(), errBuffer.size());
            throw CannotOpenSource("Could not connect to: {}:{}. {}", socketHost, socketPort, strerrorResult);
        }
    }
    return true;
}

void TCPSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    NES_TRACE("TCPSource::open: Trying to create socket and connect.");

    addrinfo hints{};
    addrinfo* result = nullptr;

    hints.ai_family = socketDomain;
    hints.ai_socktype = socketType;
    hints.ai_flags = 0; /// use default behavior
    hints.ai_protocol
        = 0; /// specifying 0 in this field indicates that socket addresses with any protocol can be returned by getaddrinfo() ;

    const auto errorCode = getaddrinfo(socketHost.c_str(), socketPort.c_str(), &hints, &result);
    if (errorCode != 0)
    {
        throw CannotOpenSource("Failed getaddrinfo with error: {}", gai_strerror(errorCode));
    }

    /// make sure that result is cleaned up automatically (RAII)
    const std::unique_ptr<addrinfo, decltype(&freeaddrinfo)> resultGuard(result, freeaddrinfo);

    const int flags = fcntl(sockfd, F_GETFL, 0);

    CPPTRACE_TRY
    {
        tryToConnect(result, flags);
    }
    CPPTRACE_CATCH(...)
    {
        ::close(sockfd); /// close socket to clean up state
        throw wrapExternalException("Could not establich connection!");
    }

    /// Set connection to non-blocking again to enable a timeout in the 'read()' call
    fcntl(sockfd, F_SETFL, flags); /// NOLINT(cppcoreguidelines-pro-type-vararg) - POSIX API requires varargs

    NES_TRACE("TCPSource::open: Connected to server.");
}

Source::FillTupleBufferResult TCPSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    size_t received = 0;
    const auto capacity = tupleBuffer.getBufferSize();
    auto firstByteAt = std::chrono::steady_clock::now();
    while (!stopToken.stop_requested() && received < capacity)
    {
        int timeoutMs = 100; // Bound cancellation latency even on an idle connection.
        if (received > 0 && flushIntervalInMs > 0)
        {
            const auto elapsed = std::chrono::duration<float, std::milli>(
                std::chrono::steady_clock::now() - firstByteAt).count();
            if (elapsed >= flushIntervalInMs)
            {
                break;
            }
            timeoutMs = std::max(1, static_cast<int>(std::min(100.0F, flushIntervalInMs - elapsed)));
        }
        pollfd descriptor{.fd = sockfd, .events = POLLIN, .revents = 0};
        const auto ready = poll(&descriptor, 1, timeoutMs);
        if (ready == 0 || (ready < 0 && errno == EINTR))
        {
            continue;
        }
        if (ready < 0 || (descriptor.revents & POLLNVAL))
        {
            throw std::runtime_error("TCP source failed while waiting for input");
        }
        const auto bytes = recv(sockfd, tupleBuffer.getAvailableMemoryArea().data() + received, capacity - received, MSG_DONTWAIT);
        if (bytes == 0)
        {
            break; // EOF: emit any final partial buffer first.
        }
        if (bytes < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
            {
                continue;
            }
            throw std::runtime_error("TCP source failed while receiving input");
        }
        if (received == 0)
        {
            firstByteAt = std::chrono::steady_clock::now();
        }
        received += static_cast<size_t>(bytes);
    }
    return received == 0 ? FillTupleBufferResult::eos() : FillTupleBufferResult::withBytes(received);
}

DescriptorConfig::Config TCPSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersTCP>(std::move(config), name());
}

void TCPSource::close()
{
    NES_DEBUG("Trying to close connection.");
    if (connection >= 0)
    {
        ::close(sockfd);
        NES_TRACE("Connection closed.");
    }
}

InlineDataRegistryReturnType TCPSource::provideInlineData(InlineDataRegistryArguments systestAdaptorArguments)
{
    std::unordered_map<Identifier, std::string> defaultSourceConfig{{Identifier::parse("flush_interval_ms"), "100"}};
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.merge(defaultSourceConfig);

    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(Identifier::parse(ConfigParametersTCP::PORT)))
    {
        throw InvalidConfigParameter("Cannot use mock implementation if config already contains a port");
    }
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(Identifier::parse(ConfigParametersTCP::HOST)))
    {
        throw InvalidConfigParameter("Cannot use mock implementation if config already contains a host");
    }

    auto mockTCPServer = std::make_unique<TCPDataServer>(std::move(systestAdaptorArguments.tuples));

    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(
        Identifier::parse(ConfigParametersTCP::PORT), std::to_string(mockTCPServer->getPort()));
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(Identifier::parse(ConfigParametersTCP::HOST), "localhost");

    auto serverThread = std::jthread([server = std::move(mockTCPServer)](const std::stop_token& stopToken) { server->run(stopToken); });
    systestAdaptorArguments.serverThreads->push_back(std::move(serverThread));

    return systestAdaptorArguments.physicalSourceConfig;
}

FileDataRegistryReturnType TCPSource::provideFileData(FileDataRegistryArguments systestAdaptorArguments)
{
    std::unordered_map<Identifier, std::string> defaultSourceConfig{{Identifier::parse("flush_interval_ms"), "100"}};
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.merge(defaultSourceConfig);

    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(Identifier::parse(ConfigParametersTCP::PORT)))
    {
        throw InvalidConfigParameter("Cannot use mock implementation if config already contains a port");
    }
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(Identifier::parse(ConfigParametersTCP::HOST)))
    {
        throw InvalidConfigParameter("Cannot use mock implementation if config already contains a host");
    }


    auto mockTCPServer = std::make_unique<TCPDataServer>(systestAdaptorArguments.testFilePath);

    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(
        Identifier::parse(ConfigParametersTCP::PORT), std::to_string(mockTCPServer->getPort()));
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(Identifier::parse(ConfigParametersTCP::HOST), "localhost");

    auto serverThread = std::jthread([server = std::move(mockTCPServer)](const std::stop_token& stopToken) { server->run(stopToken); });
    systestAdaptorArguments.serverThreads->push_back(std::move(serverThread));

    return systestAdaptorArguments.physicalSourceConfig;
}
}

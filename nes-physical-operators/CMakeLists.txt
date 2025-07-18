# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# include registrar generation functions
include(${PROJECT_SOURCE_DIR}/cmake/PluginRegistrationUtil.cmake)

# Set Sources and Headers
add_subdirectory(src)
get_source(nes-physical-operators NES_PHYSICAL_OPERATORS_SOURCE_FILES)

# Add Library
add_library(nes-physical-operators ${NES_PHYSICAL_OPERATORS_SOURCE_FILES})
target_link_libraries(nes-physical-operators PUBLIC nes-sources nes-sinks nes-nautilus)
target_include_directories(nes-physical-operators PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

create_registries_for_component(PhysicalFunction AggregationPhysicalFunction)

add_tests_if_enabled(tests)

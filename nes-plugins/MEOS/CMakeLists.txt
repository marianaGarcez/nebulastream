# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Ensure MEOS is located for this subdirectory
find_package(meos REQUIRED)

# Create MEOS plugin library
add_library(nes-meos STATIC MEOSWrapper.cpp)

# Set include directories
target_include_directories(nes-meos
        PUBLIC include
        PUBLIC .
        PUBLIC ${meos_INCLUDE_DIR}
)

# Link with MEOS library using the imported target
target_link_libraries(nes-meos PUBLIC meos::meos)

# Link with NebulaStream dependencies  
target_link_libraries(nes-meos PUBLIC nes-common nes-data-types)

# Add to build_all_plugins target
add_dependencies(build_all_plugins nes-meos)

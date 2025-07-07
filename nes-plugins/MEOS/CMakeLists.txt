# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Create MEOS plugin library
add_library(nes-meos STATIC MEOSWrapper.cpp)

# Set include directories
target_include_directories(nes-meos
        PUBLIC include
        PRIVATE .
)

# Debug: Check if MEOS variables are set
message(STATUS "MEOS variables - meos_FOUND: ${meos_FOUND}, meos_LIBRARY: ${meos_LIBRARY}, meos_INCLUDE_DIR: ${meos_INCLUDE_DIR}")

# Link with MEOS library using the imported target
if(TARGET meos::meos)
    target_link_libraries(nes-meos PUBLIC meos::meos)
    message(STATUS "Linking nes-meos with meos::meos imported target")
    # Also print the imported location
    get_target_property(MEOS_LOCATION meos::meos IMPORTED_LOCATION)
    message(STATUS "meos::meos imported location: ${MEOS_LOCATION}")
else()
    # Fallback to direct library linking
    if(meos_LIBRARY)
        target_link_libraries(nes-meos PUBLIC ${meos_LIBRARY})
        target_include_directories(nes-meos PUBLIC ${meos_INCLUDE_DIR})
        message(STATUS "Linking nes-meos with ${meos_LIBRARY}")
    else()
        message(FATAL_ERROR "MEOS library not found - meos_LIBRARY is not set")
    endif()
endif()

# Link with NebulaStream dependencies  
target_link_libraries(nes-meos PUBLIC nes-common nes-data-types)

# Add to build_all_plugins target
add_dependencies(build_all_plugins nes-meos)
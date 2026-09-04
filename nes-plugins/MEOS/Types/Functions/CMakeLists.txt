# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(${PROJECT_SOURCE_DIR}/cmake/PluginRegistrationUtil.cmake)

add_plugin_as_library(NEAREST_APPROACH_DISTANCE LogicalFunction nes-logical-operators-registry nearest_approach_distance_logical_function NearestApproachDistanceLogicalFunction.cpp)
add_plugin_as_library(NEAREST_APPROACH_DISTANCE_MovingPoint_MovingPoint PhysicalFunction nes-physical-operators-registry nearest_approach_distance_movingpoint_movingpoint_physical_function PointPointNearestApproachDistancePhysicalFunction.cpp)

add_plugin_as_library(TGEO_AT_STBOX LogicalFunction nes-logical-operators-registry tgeo_at_stbox_logical_function TgeoAtStboxLogicalFunction.cpp)
add_plugin_as_library(TGEO_AT_STBOX_MovingPoint_SpatioTemporalBox PhysicalFunction nes-physical-operators-registry tgeo_at_stbox_movingpoint_physical_function TpointAtStboxPhysicalFunction.cpp)

add_plugin_as_library(TEMPORAL_EINTERSECTS_GEOMETRY LogicalFunction nes-logical-operators-registry temporal_eintersects_geometry_logical_function TemporalEIntersectsGeometryLogicalFunction.cpp)
add_plugin_as_library(TEMPORAL_EINTERSECTS_GEOMETRY_MovingPoint_Polygon PhysicalFunction nes-physical-operators-registry temporal_eintersects_geometry_movingpoint_polygon_physical_function TPointEIntersectsPolygonPhysicalFunction.cpp)

add_plugin_as_library(EDWITHIN_TGEO_GEO LogicalFunction nes-logical-operators-registry edwithin_tgeo_geo_logical_function EdwithinTgeoGeoLogicalFunction.cpp)
add_plugin_as_library(EDWITHIN_TGEO_GEO_MovingPoint_Polygon_FLOAT64 PhysicalFunction nes-physical-operators-registry edwithin_tgeo_geo_physical_function TpointEdwithinPolygonPhysicalFunction.cpp)

add_plugin_as_library(NAD_TGEO_STBOX LogicalFunction nes-logical-operators-registry nad_tgeo_stbox_logical_function NadTgeoStboxLogicalFunction.cpp)
add_plugin_as_library(NAD_TGEO_STBOX_MovingPoint_SpatioTemporalBox PhysicalFunction nes-physical-operators-registry nad_tgeo_stbox_movingpoint_physical_function TpointNadStboxPhysicalFunction.cpp)



target_include_directories(nearest_approach_distance_logical_function
        PUBLIC include
        PRIVATE .
)
target_include_directories(nearest_approach_distance_movingpoint_movingpoint_physical_function
        PUBLIC include
        PRIVATE .
)
target_link_libraries(nearest_approach_distance_movingpoint_movingpoint_physical_function PUBLIC nes-meos)

target_include_directories(tgeo_at_stbox_logical_function
        PUBLIC include
        PRIVATE .
)
target_include_directories(tgeo_at_stbox_movingpoint_physical_function
        PUBLIC include
        PRIVATE .
)
target_link_libraries(tgeo_at_stbox_movingpoint_physical_function PUBLIC nes-meos)

target_include_directories(temporal_eintersects_geometry_logical_function
        PUBLIC include
        PRIVATE .
)
target_include_directories(temporal_eintersects_geometry_movingpoint_polygon_physical_function
        PUBLIC include
        PRIVATE .
)
target_link_libraries(temporal_eintersects_geometry_movingpoint_polygon_physical_function PUBLIC nes-meos)

target_include_directories(nad_tgeo_stbox_logical_function
        PUBLIC include
        PRIVATE .
)
target_include_directories(nad_tgeo_stbox_movingpoint_physical_function
        PUBLIC include
        PRIVATE .
)
target_link_libraries(nad_tgeo_stbox_movingpoint_physical_function PUBLIC nes-meos)


target_include_directories(edwithin_tgeo_geo_logical_function
        PUBLIC include
        PRIVATE .
)
target_include_directories(edwithin_tgeo_geo_physical_function
        PUBLIC include
        PRIVATE .
)
target_link_libraries(edwithin_tgeo_geo_physical_function PUBLIC nes-meos)

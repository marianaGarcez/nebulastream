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

#ifndef NES_PLUGINS_MEOS_HPP
#define NES_PLUGINS_MEOS_HPP

#include <string>
#include <vector>

// Require MEOS headers and C API
extern "C" {
    #include <meos.h>
    #include <meos_geo.h>
}

namespace MEOS {

class Meos {
  public:
    /**
     * @brief Initialize MEOS library
     */
    Meos();

    /**
     * @brief Finalize MEOS library, free the timezone cache
     */
    ~Meos();

    class SpatioTemporalBox {
    public:
        /**
         * @brief Create SpatioTemporal from WKT string
         * @param wkt_string String in format "SRID=4326;SpatioTemporal X((3.5, 50.5),(4.5, 51.5))"
         */
        explicit SpatioTemporalBox(const std::string& wkt_string);
        ~SpatioTemporalBox();
        STBox* getBox() const;


    private:
        void* stbox_ptr; 
    };

    class TemporalInstant {
    public:
        explicit TemporalInstant(double lon, double lat, long long ts, int srid=4326);
        ~TemporalInstant();

        bool intersects(const TemporalInstant& point) const;

    private:
        Temporal* instant;
    };
    
    class TemporalSequence {
    public:
        explicit TemporalSequence(double lon, double lat, int t_out);
        // Constructor for creating trajectory from multiple points
        explicit TemporalSequence(const std::vector<TemporalInstant*>& instants);
        
        // Constructor for creating trajectory from coordinate arrays
        TemporalSequence(const std::vector<double>& longitudes, 
                        const std::vector<double>& latitudes, 
                        const std::vector<long long>& timestamps, 
                        int srid = 4326);
        
        ~TemporalSequence();

        // Serialization methods
        std::string serialize() const;
        std::string toMFJSON() const;
        
        // bool intersects(const TemporalInstant& point) const;
        // double distance(const TemporalInstant& point) const;
        double length(const TemporalInstant& instant) const;

    private:
        Temporal* sequence;
    };

    // New helpers and types used by MEOS-dependent code
    // Forward declaration so TemporalGeometry can reference it in signatures
    class StaticGeometry;

    class TemporalGeometry {
    public:
        explicit TemporalGeometry(const std::string& wkt_string);
        ~TemporalGeometry();
        Temporal* getGeometry() const;

        int intersects(const TemporalGeometry& geom) const;
        int contains(const TemporalGeometry& geom) const;
        int intersectsStatic(const StaticGeometry& static_geom) const;
        int aintersects(const TemporalGeometry& geom) const;
        int aintersectsStatic(const StaticGeometry& static_geom) const;
        int containsStatic(const StaticGeometry& static_geom) const;

    private:
        Temporal* geometry{nullptr};
    };

    class StaticGeometry {
    public:
        explicit StaticGeometry(const std::string& wkt_string);
        ~StaticGeometry();
        GSERIALIZED* getGeometry() const;
        int containsTemporal(const TemporalGeometry& temporal_geom) const;

    private:
        GSERIALIZED* geometry{nullptr};
    };

    class TemporalHolder {
    public:
        explicit TemporalHolder(Temporal* temporalPtr);
        ~TemporalHolder();
        Temporal* get() const;

    private:
        Temporal* temporal{nullptr};
    };

    static std::string convertSecondsToTimestamp(long long seconds);
    static std::string convertEpochToTimestamp(unsigned long long epochLike);

    static void* parseTemporalPoint(const std::string& trajStr);
    static void freeTemporalObject(void* temporal);
    static uint8_t* temporalToWKB(void* temporal, size_t& size);

    static void ensureMeosInitialized();

    static int safe_edwithin_tgeo_geo(const Temporal* temp, const GSERIALIZED* gs, double dist);
    static int safe_eintersects_tgeo_geo(const Temporal* temp, const GSERIALIZED* gs);
    static Temporal* safe_tgeo_at_stbox(const Temporal* temp, const STBox* box, bool border_inc);

    bool finalized=false;

};

}
#endif // NES_PLUGINS_MEOS_HPP

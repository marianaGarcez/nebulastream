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

namespace MEOS {
extern "C" {
    #include <meos.h>
}

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


    private:
        void* stbox_ptr; 
    };

    class TemporalInstant {
    public:
        explicit TemporalInstant(const std::string& mf_string);
        ~TemporalInstant();

        bool intersects(const TemporalInstant& point) const;

    private:
        Temporal* instant;
    };
    
    class TemporalSequence {
    public:
        explicit TemporalSequence(double lon, double lat, int t_out);
        ~TemporalSequence();

        // bool intersects(const TemporalInstant& point) const;
        // double distance(const TemporalInstant& point) const;
        double length(const TemporalInstant& instant) const;

    private:
        Temporal* sequence;
    };

    std::string convertSecondsToTimestamp(long long seconds);
    bool finalized=false;

};

}
#endif // NES_PLUGINS_MEOS_HPP
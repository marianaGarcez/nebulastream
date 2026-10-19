# Porting the SIGMOD 2025 Demo onto Leo's Extensible Data Types

**Date:** 2026-08-03
**Target branch:** `extensible-data-types-geotemporal`
**Status:** plan — nothing here is implemented yet

Kept under `docs/demo/` rather than `docs/design/` on purpose: this is fork-specific
planning, and keeping it out of the upstream doc directories reduces conflicts when the
branch is rebased.

## Scope

Reproduce the demonstration from *Mobility Stream Processing on NebulaStream and MEOS*
(SIGMOD-Companion '25, pp. 79–82) on the current NebulaStream, expressing its queries
through the DataType plugin system rather than through flat columns and WKT string
literals.

### Source artefacts and their authority

| Artefact | Contains | Role |
|---|---|---|
| **`nebulastream-private` @ `meos-nes-demo`** (HEAD: *"ready for sigmod demo"*) | `MeosSNCBQuery1..8.cpp` — **all eight demo queries** | **authoritative reference implementation** |
| SIGMOD '25 paper | query descriptions, throughput targets | specification |
| `NebulaMEOSVisu` | Deck.gl frontend, Kafka pipeline, demo logic in JS | visualisation target |
| `nebulaQueryAPI` | old-API experiments and Raspberry Pi benchmark logs | benchmark baselines; **not** the demo queries |
| `MobilityNebula` / `SNCBQueries` | EDBT '26 query set, MEOS operator implementations | **operator port source**; different query set |

`nebulastream-private` supersedes `nebulaQueryAPI` as the query reference. The latter
holds exploratory variants (`queryCFA`, `querytrysamewindow`, brake range experiments)
that differ from what the demo actually ran.

The trajectory machinery of the EDBT paper — `topK`, `knn_agg`, Kalman,
`temporal_sequence`, `VAR` — is **not used by any demo query** and is out of scope.

## Verdict

Reproducible, and the scope is smaller than the papers suggest. Four things that looked
like blockers are not:

1. **No new sink.** The Deck.gl frontend polls a JSON file over HTTP; the existing File
   sink can write it.
2. **No aggregation plugin, and no `VAR`.** Across all eight queries the only aggregates
   used are `Avg` (Q1) and `Max` (Q2). Both exist. Six of eight queries aggregate nothing
   at all — they filter and project.
3. **The queries exist**, in full, in the old Query API, and translate mechanically.
4. **Joins are already supported**, and three queries need them.

The real work is **three missing MEOS predicates** and the type-system re-expression.

Phase 0 has since been executed and all six probes pass — see
[Phase 0](#phase-0--de-risk--done-all-green). Only one new constraint surfaced (shared
window-timestamp naming across join inputs), and it is schema design rather than code.

## Codebase relationships

| Repo | Base | Holds |
|---|---|---|
| `~/dev/nebulastream` @ `extensible-data-types-geotemporal` | upstream `4d44b1681e`, 2026-05-06, +19 commits | DataType plugins, variable-sized arrays |
| `nebulastream-private` @ `meos-nes-demo` | old NebulaStream (pre-rewrite: `nes-coordinator`, `nes-expressions`, `nes-operators`) | the demo system |
| `SNCB/MobilityNebula` | upstream `fde1abbb2b`, 2026-03-11 | MEOS operators, MQTT, EDBT queries |

Leo's four MEOS commits are absent from MobilityNebula; both fork from upstream about two
months apart, Leo's base newer. **Port MobilityNebula's operators onto Leo's branch** —
newer base, only 19 commits to rebase, and his type system is the target architecture.

## The eight demo queries

Extracted from `nes-plugins/meos/tests/Integration/MeosSNCBQuery{1..8}.cpp`.

| Q | Paper name | MEOS predicate | Join | Aggregation | Other logic |
|---|---|---|---|---|---|
| 1 | Location-Based Alert Filtering | `teintersects == 1` | — | `Avg(speed)` | `Code1 != 0 \|\| Code2 != 0`; sliding window |
| 2 | Location-Based Noise Monitoring | `tpointatstbox == 1` | — | `Max(PCFA_mbar)` | `speed*0.5 + PCFA_mbar*0.5 > 2`; tumbling 500 ms |
| 3 | Dynamic Speed Limit | `tedwithin == 1` | ✅ | — | `highriskArea` stream joined to `sncb` on timestamp; `ABS(Δlat/Δlon) <= slackDegrees`; sliding 30 s/10 s |
| 4 | Weather-Based Speed Zones | `tedwithin == 1` | ✅ | — | weather filters (temp, snowfall, wind, precipitation); `adjusted_speed_limit = 15.0`; tumbling 10 ms |
| 5 | Battery Monitoring | `distancetpointstbox < 3` | — | — | `workshops` subquery; `ABS(Δlat/Δlon) <= 0.5`; `Vbat < 30.0 && speed > 0`; sliding 60 s/30 s |
| 6 | Heavy Passenger Load | `tpointatstbox == 1` | — | — | `passenger_count = (T1+T2+PCF1+PCF2)/4 > 3.15`; `adjusted_temp/light`; tumbling 20 ms |
| 7 | Unscheduled Stops | `tpointatsworkshop == 0` | — | — | `speed < 0.002`; sliding window |
| 8 | Monitoring Brakes | `tedwithin == 1` | ✅ | — | pressure thresholds on `Code1`, `PCFA/PCFF/PCF1/PCF2_mbar`; tumbling 200 ms |

**Aggregation summary: `Avg` once, `Max` once, nothing else.** This closes the VAR
question definitively — the demo never used it. (An earlier draft of this plan proposed a
`Max − Min` range for Q8 based on `nebulaQueryAPI/query6.cpp`; that was an exploratory
variant, not the demo. Q8 uses threshold filters plus a join.)

### MEOS predicate coverage — the actual gap

| Demo predicate | Leo's branch | MobilityNebula | Used by |
|---|---|---|---|
| `teintersects` | ✅ `temporal_eintersects_geometry` | ✅ | Q1 |
| `tpointatstbox` | ✅ `tgeo_at_stbox` | ✅ | Q2, Q6 |
| `tedwithin` | ❌ | ✅ `edwithin_tgeo_geo` | **Q3, Q4, Q8** |
| `distancetpointstbox` | ❌ | ✅ `nad_tgeo_stbox` | Q5 |
| `tpointatsworkshop` | ❌ | ❌ (custom `MeosTernaryFunctionAtWorkshop`) | Q7 |

Two of five predicates exist; two can be ported from MobilityNebula; one is bespoke.

`tpointatsworkshop` has no equivalent anywhere, but it is only *"is the point inside any
workshop box?"* — the workshop bounding boxes are the same axis-aligned lon/lat rectangles
hardcoded in `NebulaMEOSVisu`'s `AlertQuery.js`, `BatteryQuery.js` and `StopQuery.js`.
It can be expressed as a disjunction of `tgeo_at_stbox` calls over `SpatioTemporalBox`
constants — **no new operator required**, just a more verbose query.

## Capability inventory

Verified by inspection on 2026-08-03.

| Capability | Leo's branch | MobilityNebula |
|---|---|---|
| Types: MovingPoint, MovingPolygon, Polygon, SpatioTemporalBox | yes | none (flat columns) |
| `tgeo_at_stbox`, `temporal_eintersects_geometry`, `nearest_approach_distance` | yes | yes |
| `edwithin_tgeo_geo` | **no** | yes |
| `nad_tgeo_stbox` | **no** | yes |
| Aggregations | Avg, Count, Max, Median, Min, Sum | + VAR, temporal_sequence, knn_agg, Array |
| Windows: tumbling, sliding, threshold, count | yes (all in `AntlrSQL.g4`) | yes |
| Join | yes | yes |
| Sinks | File, Print, Network, Checksum, Void | + MQTT |
| Sources | File, TCP, Generator | + MQTT |

### Three calling conventions, one target

Old demo API — flat columns, geometry hidden inside the operator:

```cpp
tedwithin(Attribute("longitude"), Attribute("latitude"), Attribute("timestamp")) == 1
```

MobilityNebula — flat columns, WKT string literal:

```sql
edwithin_tgeo_geo(gps_lon, gps_lat, time_utc, 'SRID=4326;POINT(4.3658 50.6456)', FLOAT64(2.0))
```

Leo's branch — typed plugin values:

```sql
tgeo_at_stbox(MovingPoint(gps_lon, gps_lat, ts),
              SpatioTemporalBox(FLOAT64(4.0), FLOAT64(4.6), FLOAT64(50.0), FLOAT64(50.8),
                                UINT64(1722470400), UINT64(1754006400)))
```

The demo's geometry was **compiled into the operator**; the port must lift it into an
explicit typed argument. That is the substance of the work, and it is also the demo's
contribution — each ported predicate needs a logical function, a physical function,
registry entries, and a systest.

## Visualisation integration

`NebulaMEOSVisu` runs: CSV → producer → Kafka → `consumer.py` → `outputConsumer.json` →
`cors_http_server.py` → frontend polls over HTTP.

```js
// src/constants.js
export const DATA_URL = 'http://localhost:8003/outputConsumer.json';
export const UPDATE_INTERVAL = 1000;
// src/App.js:224
const interval = setInterval(fetchTrainData, UPDATE_INTERVAL);
```

**The frontend never talks to Kafka.** It polls a static JSON file once a second, so the
existing File sink with `INTO FILE('JSON' as \`SINK\`.OUTPUT_FORMAT)` can feed it directly
and the Kafka + producer + consumer stack becomes optional. **No MQTT or Kafka sink
required.**

Two format details to reconcile:

- NES emits **newline-delimited JSON**; the frontend calls `response.json()` and expects a
  **JSON array**. Wrap it in the data server or adjust the fetch — this is `consumer.py`'s
  current job.
- NES uppercases and source-prefixes field names (`SNCB_STREAM$START`); the frontend wants
  `gps_lon`, `device_id`, `PCFA_mbar`. Needs a mapping layer.

### The query logic must move out of JavaScript

Every class in `src/queries/` computes its predicate client-side in `processData`; nothing
in the frontend references NebulaStream. Porting means moving each predicate into NES SQL
and reducing the JS to presentation, which `getVisualizationProps` already separates.

The workshop and high-risk-zone constants hardcoded there are the same rectangles the
demo's `tpointatsworkshop` and `tedwithin` used, and they map exactly onto
`SpatioTemporalBox(minLon, maxLon, minLat, maxLat, tMin, tMax)` — no geometry work, and no
dependence on the unfinished in-query array syntax.

## Gaps, ranked

1. **Translate 8 old-API queries to SQL** against typed constructors, lifting hidden
   geometry into explicit arguments.
2. **Port `edwithin_tgeo_geo`** — blocks Q3, Q4, Q8. The highest-value single item.
3. **Port `nad_tgeo_stbox`** for Q5's `distancetpointstbox`. `nearest_approach_distance`
   may substitute.
4. **NDJSON/array and field-name adaptation** — small, but on the critical path for
   anything visible.
5. **Express `tpointatsworkshop`** as a disjunction of `tgeo_at_stbox` over workshop
   boxes (Q7). Query-authoring, not C++.
6. **VAR / aggregation plugins, `topK`, `knn_agg`, Kalman, MQTT sink, threshold windows.**
   Not required by any demo query. Cut.

## Phased plan

### Phase 0 — de-risk — **DONE, all green**

Executed 2026-08-03 via `nes-systests/demo/Phase0.test` (6 probes, all passing).

| Probe | Question | Result |
|---|---|---|
| 1 | JSON sink shape and field naming | ✅ valid NDJSON, `SOURCE$FIELD` names |
| 2 | Q2 shape: `tgeo_at_stbox` + arithmetic + tumbling + `MAX` | ✅ executes |
| 3 | Q7 workshop containment as `tgeo_at_stbox` disjunction | ✅ executes |
| 4 | Join + window (Q3/Q4/Q8 shape) | ✅ executes |
| 5 | Join with a MEOS predicate on one side (Q8 shape) | ✅ executes |
| 6 | Join on the event timestamp — the demo's literal key | ✅ executes |

SNCB CSV replay through the File source is confirmed by every probe. **Not yet tested:**
TCP source replay, and throughput against the paper's targets.

**Constraint discovered — affects Q3, Q4 and Q8.** The window timestamp field must exist
in *both* join inputs under the same name. Joining `sncb_stream` to a zone stream whose
timestamp was named `zts` fails at schema inference:

```
cannot infer schema; field TS is not part of the schema Schema(fields(
  Field(name: ZONE_STREAM$ZTS, …)))
```

The fix is schema design, not code: give the secondary stream a `ts` field for the window,
plus a separate copy (`zts`) if the timestamp is also the join key — a bare `ts` on both
sides is ambiguous in the `ON` clause. `guide/guide11.test` follows the same pattern.

**Confirmed Q2 result** (the Phase 1 query), over `small/input_sncb.csv`:

```
1722520000,1722520500,4.983
```

**Confirmed JSON sink output**, one object per line:

```json
{"SNCB_STREAM$TS":1722520348,"SNCB_STREAM$DEVICEID":3,"SNCB_STREAM$GPS_LAT":50.6456,"SNCB_STREAM$GPS_LON":4.3658,"SNCB_STREAM$GSP_SPEED":1.4671}
```

This pins the two adaptations the frontend needs: wrap the newline-delimited objects in a
JSON array, and map `SNCB_STREAM$GPS_LAT` → `gps_lat`.

**Probe 3 settles the `tpointatsworkshop` question**: a disjunction of `tgeo_at_stbox`
over `SpatioTemporalBox` constants works, so Q7 needs no new operator.

### Phase 1 — one query, end to end, on screen

**Q2** is the right first query: it needs only `tgeo_at_stbox` (already working), one
`Max` aggregate, a tumbling window, and no join. It isolates the schema-and-format
integration risk with zero new C++.

CSV replay → `tgeo_at_stbox` → tumbling 500 ms → `Max(PCFA_mbar)` → File sink JSON → data
server → Deck.gl, with `SoundQuery.js` reduced to presentation.

### Phase 2 — the rest of the no-new-C++ queries

Q1 (`teintersects` → `temporal_eintersects_geometry`, `Avg`), Q6 (`tgeo_at_stbox`), and Q7
(workshop boxes as a `tgeo_at_stbox` disjunction). As `.test` files first, then deployment
YAML, then JS reduction.

### Phase 3 — the ported predicates

`edwithin_tgeo_geo` as a typed plugin function, unblocking Q3, Q4 and Q8 together; then
`nad_tgeo_stbox` for Q5.

Phases 2 and 3 are independent — query authoring needs no C++ work.

### Phase 4 — demo polish

`QuerySelector` and `DeviceSelector` already implement the paper's "user selects a query
and a train". Remaining: alert colouring per Figure 3, welcome overlay.

## Recommendations and risks

**Rebase Leo's branch onto current upstream before starting.** Cheap now, expensive after
operators and a visualisation integration land on top.

**Fix the NES-to-frontend schema contract in Phase 1.** Every query's output flows through
it; changing it later touches all eight queries and all eight JS classes.

**Published acceptance criteria.** The paper commits to 2.24 MB at 20K events/s for Q1–Q4,
and per query for Q5–Q8: 0.61 MB / 8K e/s, 3.68 MB / 32K e/s, 0.40 MB / 10K e/s,
2.24 MB / 20K e/s. `nebulaQueryAPI/benchmark_results/` holds the original Raspberry Pi
measurement logs — the correct baseline to compare against.

## References

- `nebulastream-private` @ `meos-nes-demo`:
  - `nes-plugins/meos/tests/Integration/MeosSNCBQuery{1..8}.cpp` — the eight demo queries
  - `nes-expressions/src/Expressions/Functions/MeosTernaryFunction{Tdwithin,AtStBox,AtWorkshop,Distance}.cpp` — predicate implementations
  - `nes-execution/include/Execution/Expressions/Functions/Meos*Expression.hpp` — execution layer
  - `nes-benchmark/src/DataGeneration/SNCBDataGenerator.cpp` — synthetic SNCB data
- `~/dev/nebulastream`:
  - `nes-systests/formatter/JSON_OUTPUT/GeoTemporalTypes.test` — current plugin-type surface
  - `nes-plugins/MEOS/Types/` — MovingPoint, MovingPolygon, Polygon, SpatioTemporalBox
- MobilityNebula `nes-logical-operators/src/Functions/Meos/` — `edwithin`, `nad_tgeo_stbox` to port
- `SNCBQueries/*.yaml` — deployment YAML and sink configuration reference
- `NebulaMEOSVisu/src/queries/*.js`, `src/constants.js`, `consumer/consumer.py` — integration surface
- `nebulaQueryAPI/benchmark_results/` — original Raspberry Pi baselines

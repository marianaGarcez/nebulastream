# SIGMOD 2025 demo queries — deployable form

The eight demo queries from *Mobility Stream Processing on NebulaStream and MEOS*
(SIGMOD '25), expressed with the extensible DataType plugins.


| File | Query | 
|---|---|---|
| `Q1.yaml` | Location-Based Alert Filtering | 
| `Q2.yaml` | Location-Based Noise Monitoring | 
| `Q3.yaml` | Dynamic Speed Limit |
| `Q4.yaml` | Weather-Based Speed Zones | 
| `Q5.yaml` | Battery Monitoring | 
| `Q6.yaml` | Heavy Passenger Load | 
| `Q7.yaml` | Unscheduled Stops |
| `Q8.yaml` | Monitoring Brakes | 

Each YAML carries a comment naming its old-API original in
`nebulastream-private/nes-plugins/meos/tests/Integration/MeosSNCBQuery{1..8}.cpp`, plus any
deviation forced by the current engine.

## Deploying

```sh
./Queries/deploy.sh              # all eight
./Queries/deploy.sh Q3 Q7        # a subset
```

Or by hand, with a worker already listening:

```sh
nes-single-node-worker --grpc=localhost:8080 --data_address=localhost:9090 &
nebucli -t Queries/Q1.yaml start
nebucli status
nebucli stop <queryId>
```

`nebucli -t Queries/Q1.yaml dump` validates a query and prints its plan without needing a
worker — the fastest way to check a YAML edit.

Note the config flags take `--key=value`; a bare `--key value` is rejected with
`Unrecognized configuration key`.

## Data

All queries read `nes-systests/testdata/small/sncb_demo.csv` — a contiguous two-hour slice
(1722657600..1722664800) of the dataset the demo itself used
(`M6_data_csv/selected_columns_df_original.csv`, 4.1M rows). 1683 rows, 4 devices, chosen
because it contains both event classes the demo needs: 29 rows with `gps_speed < 0.002`
for Q7 and 307 with `code1 > 0` for Q8.

Q3 and Q4 additionally read the zone and weather streams in `Queries/data/*.jsonl`, which
carry the geofence as a `Polygon` column (in-query polygon literals are not yet supported).

To run against the full dataset, point the `file_path` under `physical:` at it — but note
the `SpatioTemporalBox` time bounds in each query are set to the slice's window and would
need widening.

## Output

Results are written to `Output/*.json` as newline-delimited JSON.

**The first line is a CSV-style schema header, not JSON** — the File sink emits it
regardless of `output_format`. Anything consuming these files must skip line 1.

Field names are source-qualified and upper-cased (`SNCB_STREAM$GPS_LAT`), so feeding the
Deck.gl frontend needs a mapping to its expected lower-case names (`gps_lat`), plus
wrapping the newline-delimited objects into a JSON array.

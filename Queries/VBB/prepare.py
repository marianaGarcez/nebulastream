#!/usr/bin/env python3

import argparse
import csv
import hashlib
import io
import json
import re
import zipfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from google.transit import gtfs_realtime_pb2 as pb


def static_tables(path):
    with zipfile.ZipFile(path) as archive:
        def rows(name):
            with io.TextIOWrapper(archive.open(name + ".txt"), encoding="utf-8-sig", newline="") as stream:
                yield from csv.DictReader(stream)
        agencies = {r["agency_id"]: r["agency_name"] for r in rows("agency")}
        routes = {r["route_id"]: r for r in rows("routes")}
        trips = {r["trip_id"]: r["route_id"] for r in rows("trips")}
        stops = {r["stop_id"]: r for r in rows("stops")}
    return agencies, routes, trips, stops


def event(stop, name):
    value = getattr(stop, name)
    # NO_DATA/SKIPPED must not be treated as predictions.
    if stop.schedule_relationship != pb.TripUpdate.StopTimeUpdate.SCHEDULED:
        return None
    return value if value.HasField("time") else None


def estimate(update, stops, at, max_segment=600):
    """Reuse the original estimator's bracketing and linear interpolation."""
    updates = update.stop_time_update
    for current, following in zip(updates, updates[1:]):
        departure, arrival = event(current, "departure"), event(following, "arrival")
        if departure is None or arrival is None:
            continue
        duration = arrival.time - departure.time
        if not (0 < duration <= max_segment and departure.time <= at < arrival.time):
            continue
        a, b = stops.get(current.stop_id), stops.get(following.stop_id)
        if a is None or b is None:
            continue
        fraction = (at - departure.time) / duration
        lon = float(a["stop_lon"]) + fraction * (float(b["stop_lon"]) - float(a["stop_lon"]))
        lat = float(a["stop_lat"]) + fraction * (float(b["stop_lat"]) - float(a["stop_lat"]))
        return round(lon, 6), round(lat, 6), following, arrival
    return None


def snapshot(feed, tables, lines, max_age):
    agencies, routes, trips, stops = tables
    at = int(feed.header.timestamp)
    if not at or feed.header.incrementality != pb.FeedHeader.FULL_DATASET:
        raise ValueError("A timestamped FULL_DATASET snapshot is required")
    # Revisions replace earlier entities for the same trip instance. Never append snapshots.
    latest, counts = {}, Counter()
    for entity in feed.entity:
        for kind in ("trip_update", "vehicle", "alert"):
            counts[kind] += entity.HasField(kind)
        if entity.is_deleted or not entity.HasField("trip_update"):
            continue
        update = entity.trip_update
        trip = update.trip
        route = routes.get(trip.route_id)
        if not route or route["route_short_name"] not in lines:
            continue
        if "Berliner Verkehrsbetriebe" not in agencies.get(route["agency_id"], ""):
            counts["non_bvg"] += 1
            continue
        if trips.get(trip.trip_id) != trip.route_id or not trip.start_date:
            counts["unmatched_trip_or_missing_date"] += 1
            continue
        key = (trip.trip_id, trip.start_date, trip.start_time)
        previous = latest.get(key)
        if previous is None or update.timestamp >= previous.timestamp:
            latest[key] = update
    rows, details = [], []
    for key, update in sorted(latest.items()):
        if update.trip.schedule_relationship != pb.TripDescriptor.SCHEDULED:
            counts["excluded_trip_relationship"] += 1
            continue
        if not update.HasField("timestamp") or not 0 <= at - update.timestamp <= max_age:
            counts["stale_or_missing_update_timestamp"] += 1
            continue
        position = estimate(update, stops, at)
        if position is None:
            counts["no_valid_bracketing_segment"] += 1
            continue
        lon, lat, stop, arrival = position
        delay = int(arrival.delay) if arrival.HasField("delay") else None
        rows.append({"ID": len(rows) + 1, "TS": at, "LON": lon, "LAT": lat, "DELAY_S": delay})
        details.append({"id": len(rows), "trip_id": key[0], "service_date": key[1],
                        "start_time": key[2], "line": routes[update.trip.route_id]["route_short_name"],
                        "next_stop": stops[stop.stop_id]["stop_name"], "next_stop_id": stop.stop_id,
                        "predicted_arrival": int(arrival.time), "delay_s": delay,
                        "scheduled_arrival_from_rt": int(arrival.time) - delay if delay is not None else None,
                        "update_timestamp": int(update.timestamp), "update_age_s": at - update.timestamp,
                        "position_source": "gtfs_rt_stop_time_linear_interpolation", "position_is_observed": False})
    return rows, details, dict(counts)


def write_input(output, rows, details, metadata):
    output.mkdir(parents=True, exist_ok=True)
    def save(name, value):
        (output / name).write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")
    features = [{"type": "Feature", "geometry": {"type": "Point", "coordinates": [r["LON"], r["LAT"]]},
                 "properties": d} for r, d in zip(rows, details)]
    (output / "positions.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
    save("metadata.json", metadata)
    save("positions.geojson", {"type": "FeatureCollection", "metadata": metadata, "features": features})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--static", type=Path, required=True)
    parser.add_argument("--realtime", type=Path, required=True)
    parser.add_argument("--headers", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("Output/vbb/input"))
    parser.add_argument("--line", action="append", dest="lines")
    parser.add_argument("--max-update-age", type=int, default=300)
    args = parser.parse_args()
    feed = pb.FeedMessage()
    raw = args.realtime.read_bytes()
    feed.ParseFromString(raw)
    lines = set(args.lines or ["M41", "M10"])
    rows, details, counts = snapshot(feed, static_tables(args.static), lines, args.max_update_age)
    variant = re.search(r"schedule_sha256=([^;\s]+)", args.headers.read_text())
    with args.static.open("rb") as stream:
        static_hash = hashlib.file_digest(stream, "sha256").hexdigest()
    metadata = {"source": "VBB (CC BY 4.0)", "feed_timestamp": int(feed.header.timestamp),
                "prepared_at": datetime.now(timezone.utc).isoformat(), "lines": sorted(lines),
                "realtime_sha256": hashlib.sha256(raw).hexdigest(), "static_zip_sha256": static_hash,
                "rt_schedule_variant": variant[1] if variant else None,
                "static_compatibility": "trip/route/stop IDs checked; schedule hash equivalence not established",
                "schedule_source": "arrival.time - arrival.delay, when delay is explicitly present; not a static timetable join",
                "position_is_observed": False, "estimated_count": len(rows), "counts": counts,
                "max_update_age_s": args.max_update_age,
                "warning": "Recorded snapshot; incomplete coverage; interpolated positions are not GPS or ground truth"}
    write_input(args.output, rows, details, metadata)
    print(json.dumps(metadata, indent=2))


if __name__ == "__main__":
    main()

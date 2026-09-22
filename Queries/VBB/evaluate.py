"""Bounded correctness and timing evaluation of native NES queries on one worker.

Synthetic cases test the current >180-second threshold and configured rectangle.
Timings include CLI deployment, compilation, polling and sink reading, not just
operator execution. This does not evaluate ETA or position accuracy.
"""
import json
import tempfile
import time
from pathlib import Path

from prepare import write_input
from serve import HERE, ROOT, command, run_queries, worker


def main():
    output = ROOT / 'Output/vbb/evaluation'
    output.mkdir(parents=True, exist_ok=True)
    west, east, south, north = json.loads((HERE / 'map-region.json').read_text())['bbox']
    inside = ((west + east) / 2, (south + north) / 2)
    outside = (east + 0.01, north + 0.01)
    timestamp = 1790074306
    cases = [
        ('mixed', [(inside, 240), (inside, 180), (outside, 300), (outside, None)],
         [{1, 3}, {1, 2}, {1}]),
        ('revised', [(outside, 0), (inside, None), (inside, 181)],
         [{3}, {2, 3}, {3}]),
        ('no_matches', [(outside, 0)], [set(), set(), set()]),
        ('empty_snapshot', [], [set(), set(), set()]),
    ]
    results = []
    with worker() as container:
        for name, observations, expected in cases:
            rows = [{'ID': i, 'TS': timestamp, 'LON': point[0], 'LAT': point[1], 'DELAY_S': delay}
                    for i, (point, delay) in enumerate(observations, 1)]
            with tempfile.TemporaryDirectory(dir=output) as directory:
                stage = Path(directory)
                write_input(stage, rows, [{'id': r['ID']} for r in rows], {'feed_timestamp': timestamp})
                run_queries(stage, container)
                data = json.loads((stage / 'positions.geojson').read_text())
                for key, ids in zip(('nes_late', 'nes_in_region', 'nes_late_in_region'), expected):
                    actual = {f['properties']['id'] for f in data['features'] if f['properties'][key]}
                    if actual != ids:
                        raise AssertionError(f'{name} {key}: expected {ids}, got {actual}')
                results.append({'case': name, 'passed': True,
                                'query_cycle_ms': data['metadata']['query_cycle_ms']})
        # An idle post-run sample, not peak CPU or peak memory during query execution.
        resources = command(['docker', 'stats', '--no-stream', '--format', '{{json .}}', container])
        report = {'recorded_at': time.time(), 'worker_id': container, 'cases': results,
                  'resource_sample_after_queries': json.loads(resources),
                  'scope': 'synthetic query correctness and orchestration timing; no ETA accuracy claim'}
    path = output / f'report-{time.time_ns()}.json'
    path.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))
    print(f'Report: {path}')


if __name__ == '__main__':
    main()

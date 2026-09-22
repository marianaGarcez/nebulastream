"""Exercise one running native query across revisions, duplicates and idle time."""
import json
import time

from continuous import NativePipeline
from serve import ROOT, command, worker


def row(key, at, update, delay, state=1, lon=13.4, lat=52.52):
    return {'ID': key, 'TS': at, 'UPDATE_TS': update, 'STATE': state,
            'LON': lon, 'LAT': lat, 'DELAY_S': delay}


def main():
    output = ROOT / 'Output/vbb/evaluation'
    with worker(single_thread=True) as container, NativePipeline(container, output) as pipeline:
        pipeline.send([row(1, 1000, 900, 300), row(2, 1000, 900, 0, lon=13.5),
                       row(3, 1000, 900, None)], 1000)
        assert pipeline.results[1]['NES_LATE'] is True
        assert pipeline.results[2]['NES_IN_REGION'] is False
        assert pipeline.results[3]['NES_LATE'] is None

        pipeline.send([row(1, 1001, 901, 60), row(1, 1001, 901, 500),
                       row(2, 1001, 901, None, state=2), row(3, 1001, 899, 300)], 1001)
        assert pipeline.results[1]['NES_LATE'] is False, 'on-time revision must clear alert; duplicate must not overwrite it'
        assert pipeline.results[2]['STATE'] == 2
        assert pipeline.results[2]['NES_LATE'] is False
        assert pipeline.results[3]['TS'] == 1000, 'older VBB revision must not replace newer state'
        pipeline.send([], 1002)
        assert len(pipeline.results) == 3, 'absence is not an implicit cancellation'

        # Longer than the old TCP receive timeout, on the same socket/query.
        time.sleep(11)
        pipeline.send([row(1, 1003, 901, 60, lon=13.5)], 1003)
        assert pipeline.results[1]['TS'] == 1003
        assert pipeline.results[1]['NES_IN_REGION'] is False
        # A boundary must not acknowledge a partially materialized multi-buffer feed.
        keys = set(range(10, 1010))
        pipeline.send([row(key, 1004, 902, 240) for key in sorted(keys)], 1004)
        actual = {key for key, result in pipeline.results.items() if result['TS'] == 1004}
        assert actual == keys, 'feed boundary overtook data buffers'
        statuses = json.loads(command(pipeline.cli + ['status', pipeline.query_id]))
        assert statuses and all(s['query_status'] not in ('Failed', 'Stopped', 'Error') for s in statuses), statuses
        report = {'query_id': pipeline.query_id, 'worker_id': container, 'passed': True,
                  'checks': ['late-to-on-time', 'duplicate', 'out-of-order', 'explicit cancellation',
                             'unknown delay', 'empty feed', 'same-version newer feed', 'idle TCP survives',
                             'multi-buffer feed completion'],
                  'scope': 'native keyed version state and NES geographic/late flags, not ETA accuracy'}
        path = output / f'latest-by-key-{time.time_ns()}.json'
        path.write_text(json.dumps(report, indent=2) + '\n')
        print(json.dumps(report, indent=2))
        print(f'Report: {path}')


if __name__ == '__main__':
    main()

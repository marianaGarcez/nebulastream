"""One native LatestByKey query for the lifetime of the local demo.

Python decodes snapshots and materializes the NES output for display. Version
acceptance and geographic/late classifications are evaluated inside NES.
This is a single-worker, single-execution-thread prototype, without recovery.
"""
import json
import socket
import tempfile
import time
from collections import Counter
from pathlib import Path

import yaml
from serve import HERE, ROOT, command

STATES = {'active': 1, 'cancelled': 2, 'stale': 3, 'unsupported': 4, 'no_position': 5}
FLAGS = ('nes_late', 'nes_in_region', 'nes_late_in_region')


def input_rows(stage):
    data = json.loads((stage / 'positions.geojson').read_text())
    at = data['metadata']['feed_timestamp']
    if not 0 < at < 2**32:
        raise ValueError('Native version encoding requires a 32-bit unsigned feed timestamp')
    positions = {f['properties']['id']: f for f in data['features']}
    rows = []
    for trip in json.loads((stage / 'trips.json').read_text()):
        update = trip['update_timestamp'] or 0
        if not (0 <= update < 2**32 and 0 < at < 2**32):
            raise ValueError('Native version encoding requires 32-bit unsigned timestamps')
        if trip['id'] == 0:
            raise ValueError('Trip ID zero is reserved for the feed boundary')
        feature = positions.get(trip['id'])
        lon, lat = feature['geometry']['coordinates'] if feature else (0., 0.)
        rows.append({'ID': trip['id'], 'TS': at, 'UPDATE_TS': update,
                     'STATE': STATES[trip['status']], 'LON': lon, 'LAT': lat,
                     'DELAY_S': feature['properties']['delay_s'] if feature else None})
    return data, rows


class NativePipeline:
    HEADER = ('ID:UINT64:NOT_NULLABLE,TS:UINT64:NOT_NULLABLE,UPDATE_TS:UINT64:NOT_NULLABLE,'
              'STATE:UINT64:NOT_NULLABLE,NES_LATE:BOOLEAN:IS_NULLABLE,'
              'NES_IN_REGION:BOOLEAN:NOT_NULLABLE,NES_LATE_IN_REGION:BOOLEAN:IS_NULLABLE')

    def __init__(self, container, output):
        self.container, self.output = container, output
        self.results = {}
        self.last_feed = 0
        self.offset = 0
        self.pending = b''
        self.connection = None
        self.query_id = None
        self.failed = False
        self.header_seen = False
        self.last_fingerprint = None

    def __enter__(self):
        self.output.mkdir(parents=True, exist_ok=True)
        self.directory = tempfile.TemporaryDirectory(prefix='native-', dir=self.output)
        self.stage = Path(self.directory.name)
        self.sink = self.stage / 'results.jsonl'
        self.listener = socket.socket()
        try:
            self.listener.bind(('127.0.0.1', 0))
            self.listener.listen(1)
            self.listener.settimeout(30)
            config = yaml.safe_load((HERE / 'QLatest.yaml').read_text())
            config['physical'][0]['source_config']['socket_port'] = str(self.listener.getsockname()[1])
            container_stage = Path('/workspace') / self.stage.relative_to(ROOT)
            config['sinks'][0]['config']['file_path'] = str(container_stage / self.sink.name)
            path = self.stage / 'QLatest.yaml'
            path.write_text(yaml.safe_dump(config, sort_keys=False))
            self.cli = ['docker', 'exec', self.container,
                        '/workspace/build-main-meos/nes-frontend/apps/nes-cli',
                        '-t', str(container_stage / path.name)]
            self.query_id = command(self.cli + ['start'])
            self.connection, _ = self.listener.accept()
            self.connection.settimeout(20)
            print(f'NES continuous query: {self.query_id}', flush=True)
            return self
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def __exit__(self, *_):
        if self.connection:
            self.connection.close()
        self.listener.close()
        try:
            if self.query_id:
                command(self.cli + ['stop', self.query_id])
        finally:
            self.directory.cleanup()

    def send(self, rows, at):
        if self.failed:
            raise RuntimeError('Native stream needs a restart after an incomplete feed')
        if at < self.last_feed:
            raise ValueError('Out-of-order feed timestamp')
        if at == self.last_feed:
            return
        boundary = {'ID': 0, 'TS': at, 'UPDATE_TS': at, 'STATE': 0,
                    'LON': 0., 'LAT': 0., 'DELAY_S': None}
        payload = ''.join(json.dumps(r) + '\n' for r in [*rows, boundary])
        # Any transport/parse/timeout failure poisons this session. Retrying a
        # partially processed boundary would otherwise make completion ambiguous.
        self.failed = True
        self.connection.sendall(payload.encode())
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if self.sink.exists():
                with self.sink.open('rb') as stream:
                    stream.seek(self.offset)
                    chunk = stream.read()
                    self.offset = stream.tell()
                self.pending += chunk
                lines = self.pending.split(b'\n')
                self.pending = lines.pop()
                complete = False
                for line in lines:
                    if not line.strip():
                        continue
                    if not self.header_seen:
                        if line.decode() != self.HEADER:
                            raise ValueError(f'Unexpected NES sink schema: {line!r}')
                        self.header_seen = True
                        continue
                    row = json.loads(line)
                    if row['ID'] == 0:
                        complete |= row['TS'] == at
                    else:
                        prior = self.results.get(row['ID'])
                        if prior is None or (row['UPDATE_TS'], row['TS']) > (prior['UPDATE_TS'], prior['TS']):
                            self.results[row['ID']] = row
                if complete:
                    self.last_feed = at
                    self.failed = False
                    return
            time.sleep(.05)
        raise TimeoutError('NES did not acknowledge this feed; restart the server to resynchronize')

    def run(self, stage):
        started = time.monotonic()
        data, rows = input_rows(stage)
        at = data['metadata']['feed_timestamp']
        if not -60 <= time.time() - at <= 300:
            raise ValueError('VBB feed is stale or in the future')
        fingerprint = (data['metadata'].get('realtime_sha256'), data['metadata'].get('static_zip_sha256'))
        if at == self.last_feed and self.last_fingerprint is not None and fingerprint != self.last_fingerprint:
            raise ValueError('Conflicting snapshot with the same feed timestamp; refusing to mix it with prior NES results')
        self.send(rows, at)
        self.last_fingerprint = fingerprint
        accepted = {key: r for key, r in self.results.items() if r['TS'] == at}
        features = []
        for feature in data['features']:
            result = accepted.get(feature['properties']['id'])
            if result and result['STATE'] == STATES['active']:
                for flag in FLAGS:
                    feature['properties'][flag] = result[flag.upper()] is True
                features.append(feature)
        data['features'] = features
        names = {value: key for key, value in STATES.items()}
        counts = {flag: sum(f['properties'][flag] for f in features) for flag in FLAGS}
        data['metadata'].update(
            query_engine='NebulaStream (continuous LatestByKey)',
            query_id=self.query_id, worker_id=self.container,
            query_cycle_ms=round((time.monotonic() - started) * 1000, 2),
            native_input_rows=len(rows), native_current_rows=len(accepted),
            query_result_counts=counts,
            state_counts=dict(Counter(names[r['STATE']] for r in accepted.values())),
            bbox=json.loads((HERE / 'map-region.json').read_text())['bbox'],
            state_scope='NES version state; current-feed view; absence is not cancellation; no restart recovery')
        for flag in FLAGS:
            selected = sorted(f['properties']['id'] for f in features if f['properties'][flag])
            print(f'NES continuous result ({flag}): {selected} — {len(selected)} matching rows', flush=True)
        (stage / 'positions.geojson').write_text(json.dumps(data, ensure_ascii=False))

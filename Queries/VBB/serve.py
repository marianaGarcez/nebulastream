"""Local auto-refresh demo. Reuses prepare.py; exposes only map and latest data."""
import argparse
import json
import subprocess
import sys
import tempfile
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.request import Request, urlopen
import yaml

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent


def deploy_queries(stage):
    """Deploy NES YAML unchanged except per-snapshot input/output paths."""
    container_stage = Path('/workspace') / stage.resolve().relative_to(ROOT)
    def command(args):
        result = subprocess.run(args, capture_output=True, text=True, timeout=60)
        if result.returncode:
            raise RuntimeError((result.stdout + result.stderr)[-3000:])
        return result.stdout.strip()
    container = command(['docker', 'run', '-d', '--rm', '--entrypoint',
               '/workspace/build-main-meos/nes-single-node-worker/nes-single-node-worker',
               '-v', f'{ROOT}:/workspace', '-w', '/workspace',
               'nebulastream/nes-development:mobility-twin-main-meos',
               '--', '--grpc=0.0.0.0:8080', '--data_address=localhost:9090'])
    try:
        # Wait for worker readiness, without retrying query submissions.
        for attempt in range(40):
            ready = subprocess.run(['docker', 'exec', container, 'bash', '-c',
                                    'exec 3<>/dev/tcp/127.0.0.1/8080'],
                                   capture_output=True, timeout=5)
            if ready.returncode == 0:
                break
            time.sleep(0.25)
        else:
            raise RuntimeError('NES worker did not become ready')
        for number in (1, 2, 3):
            config = yaml.safe_load((HERE / f'Q{number}.yaml').read_text())
            config['physical'][0]['source_config']['file_path'] = str(container_stage / 'positions.jsonl')
            config['sinks'][0]['config']['file_path'] = str(container_stage / f'Q{number}.csv')
            (stage / f'Q{number}.yaml').write_text(yaml.safe_dump(config, sort_keys=False))
            cli = ['docker', 'exec', container, '/workspace/build-main-meos/nes-frontend/apps/nes-cli',
                   '-t', str(container_stage / f'Q{number}.yaml')]
            query_id = command(cli + ['start'])
            deadline = time.monotonic() + 45
            while time.monotonic() < deadline:
                states = json.loads(command(cli + ['status', query_id]))
                statuses = [s['query_status'] for s in states]
                if statuses and all(s == 'Stopped' for s in statuses):
                    break
                if any(s in ('Failed', 'Unreachable', 'Error') for s in statuses):
                    raise RuntimeError(f'NES query failed: {states}')
                time.sleep(0.25)
            else:
                raise TimeoutError(f'NES query {query_id} did not finish')
    finally:
        command(['docker', 'stop', '--time', '2', container])


def run_queries(stage):
    """Read CSV sinks and attach their classifications to map features."""
    deploy_queries(stage)
    data = json.loads((stage / 'positions.geojson').read_text())
    data['metadata']['bbox'] = json.loads((HERE / 'map-region.json').read_text())['bbox']
    known = {f['properties']['id'] for f in data['features']}
    counts = {}
    for number, key in enumerate(('nes_late', 'nes_in_region', 'nes_late_in_region'), 1):
        lines = (stage / f'Q{number}.csv').read_text().splitlines()
        if not lines or lines[0] != 'ID:UINT64:NOT_NULLABLE':
            raise ValueError('Unexpected NebulaStream CSV schema')
        selected = {int(line) for line in lines[1:] if line.strip()}
        if not selected <= known:
            raise ValueError('NebulaStream returned IDs outside this snapshot')
        counts[key] = len(selected)
        print(f'NES query result Q{number} ({key}): '
              f'{sorted(selected)} — {len(selected)} matching rows', flush=True)
        for feature in data['features']:
            feature['properties'][key] = feature['properties']['id'] in selected
    data['metadata']['query_engine'] = 'NebulaStream (periodic snapshot queries)'
    data['metadata']['query_result_counts'] = counts
    (stage / 'positions.geojson').write_text(json.dumps(data, ensure_ascii=False))


def refresh(static, output):
    output.mkdir(parents=True, exist_ok=True)
    # Publish only a complete, successfully prepared snapshot, on the same filesystem.
    with tempfile.TemporaryDirectory(dir=output) as directory:
        stage = Path(directory)
        request = Request('https://production.gtfsrt.vbb.de/data', headers={
            'User-Agent': 'MobilityNebula-BerlinTwin/0.1 (research prototype)'})
        with urlopen(request, timeout=20) as response:
            (stage / 'realtime.pb').write_bytes(response.read())
            (stage / 'realtime.headers').write_text(str(response.headers))
        subprocess.run([sys.executable, str(HERE / 'prepare.py'),
                        '--static', str(static), '--realtime', str(stage / 'realtime.pb'),
                        '--headers', str(stage / 'realtime.headers'), '--output', str(stage)],
                       check=True, timeout=60, stdout=subprocess.DEVNULL)
        run_queries(stage)
        (stage / 'positions.geojson').replace(output / 'latest.geojson')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, default=8000)
    parser.add_argument('--static', type=Path, default=ROOT / 'Output/vbb/static.zip')
    args = parser.parse_args()
    if not args.static.is_file():
        parser.error('Static GTFS ZIP is missing')
    output = ROOT / 'Output/vbb/live'

    class Handler(BaseHTTPRequestHandler):
        last_attempt = float('-inf')
        error = None

        def do_GET(self):
            if self.path in ('/', '/map.html'):
                payload, mime = (HERE / 'map.html').read_bytes(), 'text/html; charset=utf-8'
            elif self.path == '/latest.geojson':
                if time.monotonic() - Handler.last_attempt >= 30:
                    Handler.last_attempt = time.monotonic()
                    try:
                        refresh(args.static, output)
                        Handler.error = None
                    except Exception as error:
                        Handler.error = str(error)
                        print(f'Refresh failed: {error}', flush=True)
                if Handler.error or not (output / 'latest.geojson').exists():
                    self.send_error(503, 'Refresh failed; retained snapshot is not current')
                    return
                payload = (output / 'latest.geojson').read_bytes()
                mime = 'application/geo+json'
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header('Content-Type', mime)
            self.send_header('Cache-Control', 'no-store')
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    print(f'Open http://127.0.0.1:{args.port} — refresh while map is open; Ctrl+C stops.', flush=True)
    try:
        with HTTPServer(('127.0.0.1', args.port), Handler) as server:
            server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()

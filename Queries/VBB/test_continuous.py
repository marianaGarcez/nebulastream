import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from continuous import NativePipeline, input_rows
from serve import HERE
import yaml


class ContinuousTest(unittest.TestCase):
    def test_live_query_region_matches_map(self):
        bounds = json.loads((HERE / 'map-region.json').read_text())['bbox']
        query = yaml.safe_load((HERE / 'QLatest.yaml').read_text())['query']
        expression = ', '.join(f'FLOAT64({v})' for v in bounds)
        self.assertIn(expression, query)

    def test_decoder_keeps_cancellation_and_checks_timestamp_range(self):
        with tempfile.TemporaryDirectory() as directory:
            stage = Path(directory)
            (stage / 'positions.geojson').write_text(json.dumps(
                {'features': [], 'metadata': {'feed_timestamp': 1000}}))
            trip = {'id': 42, 'status': 'cancelled', 'update_timestamp': 900}
            (stage / 'trips.json').write_text(json.dumps([trip]))
            _, rows = input_rows(stage)
            self.assertEqual(rows[0]['STATE'], 2)
            self.assertIsNone(rows[0]['DELAY_S'])
            trip['update_timestamp'] = 2**32
            (stage / 'trips.json').write_text(json.dumps([trip]))
            with self.assertRaises(ValueError):
                input_rows(stage)

    def test_sink_materialization_and_duplicate_feed(self):
        with tempfile.TemporaryDirectory() as directory:
            pipeline = NativePipeline('unused', Path(directory))
            pipeline.connection = Mock()
            pipeline.sink = Path(directory) / 'results.jsonl'
            pipeline.sink.write_text(
                NativePipeline.HEADER + '\n'
                + json.dumps({'ID': 4, 'TS': 1000, 'UPDATE_TS': 900, 'STATE': 1}) + '\n'
                + json.dumps({'ID': 0, 'TS': 1000}) + '\n')
            pipeline.send([], 1000)
            self.assertEqual(pipeline.results[4]['UPDATE_TS'], 900)
            pipeline.send([], 1000)
            pipeline.connection.sendall.assert_called_once()
            with self.assertRaises(ValueError):
                pipeline.send([], 999)

    def test_failed_session_is_not_retried(self):
        pipeline = NativePipeline('unused', Path('.'))
        pipeline.failed = True
        with self.assertRaises(RuntimeError):
            pipeline.send([], 1000)

    def test_empty_feed_timestamp_range_is_checked(self):
        with tempfile.TemporaryDirectory() as directory:
            stage = Path(directory)
            (stage / 'positions.geojson').write_text(json.dumps(
                {'features': [], 'metadata': {'feed_timestamp': 2**32}}))
            (stage / 'trips.json').write_text('[]')
            with self.assertRaises(ValueError):
                input_rows(stage)


if __name__ == '__main__':
    unittest.main()

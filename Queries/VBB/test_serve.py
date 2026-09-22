import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from serve import HERE, ROOT, run_queries, worker
import yaml


class QueryResultsTests(unittest.TestCase):
    def test_existing_worker_is_reused_without_stopping(self):
        with patch('serve.command') as invoke:
            with worker('existing-worker') as container:
                self.assertEqual(container, 'existing-worker')
            invoke.assert_not_called()

    def test_reads_sink_ids(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'Output/vbb') as directory:
            stage = Path(directory)
            (stage / 'positions.geojson').write_text(json.dumps({
                'metadata': {},
                'features': [{'properties': {'id': 1}}, {'properties': {'id': 2}}]}))

            def execute(*args, **kwargs):
                sinks = stage
                for number, ids in enumerate(('2\n', '1\n', ''), 1):
                    (sinks / f'Q{number}.csv').write_text('ID:UINT64:NOT_NULLABLE\n' + ids)

            with patch('serve.deploy_queries', side_effect=execute):
                run_queries(stage)
            data = json.loads((stage / 'positions.geojson').read_text())
            self.assertFalse(data['features'][0]['properties']['nes_late'])
            self.assertTrue(data['features'][1]['properties']['nes_late'])
            self.assertEqual(data['metadata']['query_result_counts']['nes_late_in_region'], 0)

    def test_engine_failure_is_not_published(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'Output/vbb') as directory:
            with patch('serve.deploy_queries', side_effect=RuntimeError('failed')):
                with self.assertRaises(RuntimeError):
                    run_queries(Path(directory))

    def test_display_bounds_match_native_queries(self):
        bounds = json.loads((HERE / 'map-region.json').read_text())['bbox']
        expected = 'SpatioTemporalBox(' + ', '.join(f'FLOAT64({v})' for v in bounds)
        for number in (2, 3):
            sql = yaml.safe_load((HERE / f'Q{number}.yaml').read_text())['query']
            self.assertIn(expected, ' '.join(sql.split()))

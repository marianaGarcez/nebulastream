import copy
import json
import unittest

from prepare import trip_identity
from trip_state import reconcile


def batch(at, status='active', delay=240, update=None):
    key, identifier = trip_identity(('trip', '20260922', ''))
    record = {'id': identifier, 'trip_key': key, 'trip_id': 'trip', 'status': status,
              'update_timestamp': update if update is not None else at - 5}
    feature = {'geometry': {'type': 'Point', 'coordinates': [13.4, 52.5]},
               'properties': {**record, 'delay_s': delay, 'predicted_arrival': at + 60,
                              'nes_late': delay is not None and delay > 180,
                              'nes_in_region': True,
                              'nes_late_in_region': delay is not None and delay > 180}}
    return {'features': [feature] if status == 'active' else [],
            'metadata': {'feed_timestamp': at, 'query_result_counts': {}}}, [record]


class TripStateTests(unittest.TestCase):
    def apply(self, before, at, **kwargs):
        data, incoming = batch(at, **kwargs)
        return reconcile(before, data, incoming, at)

    def test_stable_id_and_separate_service_days(self):
        first = trip_identity(('trip', '20260922', ''))[1]
        self.assertLess(first, 2 ** 53)
        self.assertEqual(first, trip_identity(('trip', '20260922', ''))[1])
        self.assertNotEqual(first, trip_identity(('trip', '20260923', ''))[1])

    def test_revision_clears_lateness(self):
        before = self.apply({}, 1000)
        after = self.apply(before, 1030, delay=0)
        self.assertEqual(before['features'][0]['properties']['id'], after['features'][0]['properties']['id'])
        self.assertEqual(after['metadata']['query_result_counts']['nes_late'], 0)

    def test_cancelled_and_missing_are_distinct(self):
        before = self.apply({}, 1000)
        cancelled = self.apply(before, 1030, status='cancelled')
        self.assertEqual(cancelled['features'], [])
        self.assertEqual(cancelled['metadata']['state_counts'], {'cancelled': 1})
        empty, _ = batch(1030)
        empty['features'] = []
        missing = reconcile(before, empty, [], 1030)
        self.assertEqual(missing['metadata']['state_counts'], {'missing': 1})
        self.assertEqual(missing['features'], [])

    def test_stale_trip_hides_marker(self):
        after = self.apply(self.apply({}, 1000), 1030, status='stale')
        self.assertEqual(after['features'], [])

    def test_out_of_order_trip_does_not_restore_old_prediction(self):
        before = self.apply({}, 1000)
        after = self.apply(before, 1030, update=980)
        self.assertEqual(after['features'], [])
        self.assertEqual(after['metadata']['state_counts'], {'out_of_order': 1})
        self.assertEqual(next(iter(after['trip_states'].values()))['accepted_update_timestamp'], 995)

    def test_duplicate_is_idempotent_and_old_feed_rejected(self):
        before = self.apply({}, 1000)
        self.assertEqual(before, self.apply(before, 1000, delay=0))
        with self.assertRaises(ValueError):
            self.apply(before, 990)

    def test_stale_feed_rejected_without_mutating_checkpoint(self):
        before = self.apply({}, 1000)
        saved = copy.deepcopy(before)
        data, incoming = batch(1030)
        with self.assertRaises(ValueError):
            reconcile(before, data, incoming, 2000)
        self.assertEqual(before, saved)

    def test_checkpoint_reload_and_reappearance(self):
        before = json.loads(json.dumps(self.apply({}, 1000, status='cancelled')))
        after = self.apply(before, 1030, delay=None)
        self.assertEqual(after['metadata']['state_counts'], {'active': 1})
        self.assertFalse(after['features'][0]['properties']['nes_late'])

    def test_old_inactive_records_expire(self):
        before = self.apply({}, 1000)
        data, _ = batch(90000)
        data['features'] = []
        after = reconcile(before, data, [], 90000)
        self.assertEqual(after['trip_states'], {})

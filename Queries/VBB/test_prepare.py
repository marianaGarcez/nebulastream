import unittest

from prepare import pb, snapshot


class SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.feed = pb.FeedMessage()
        self.feed.header.gtfs_realtime_version = "2.0"
        self.feed.header.timestamp = 1000
        self.update = self.feed.entity.add(id="one").trip_update
        self.update.timestamp = 990
        self.update.trip.trip_id = "trip"
        self.update.trip.route_id = "route"
        self.update.trip.start_date = "20260922"
        self.update.stop_time_update.add(stop_id="a").departure.time = 900
        arrival = self.update.stop_time_update.add(stop_id="b").arrival
        arrival.time = 1100
        arrival.delay = 240
        self.tables = ({"agency": "Berliner Verkehrsbetriebe"},
                       {"route": {"agency_id": "agency", "route_short_name": "M41"}},
                       {"trip": "route"},
                       {"a": {"stop_lon": "13", "stop_lat": "52", "stop_name": "A"},
                        "b": {"stop_lon": "14", "stop_lat": "53", "stop_name": "B"}})

    def result(self):
        return snapshot(self.feed, self.tables, {"M41"}, 300)

    def test_midpoint(self):
        rows, details, _ = self.result()
        self.assertEqual((rows[0]["LON"], rows[0]["LAT"]), (13.5, 52.5))
        self.assertEqual(rows[0]["DELAY_S"], 240)
        self.assertFalse(details[0]["position_is_observed"])

    def test_missing_delay_is_unknown(self):
        self.update.stop_time_update[1].arrival.ClearField("delay")
        self.assertIsNone(self.result()[0][0]["DELAY_S"])

    def test_stale_and_missing_timestamp(self):
        self.update.timestamp = 699
        self.assertEqual(self.result()[0], [])
        self.update.ClearField("timestamp")
        self.assertEqual(self.result()[0], [])

    def test_cancelled(self):
        self.update.trip.schedule_relationship = pb.TripDescriptor.CANCELED
        self.assertEqual(self.result()[0], [])
        states = []
        snapshot(self.feed, self.tables, {'M41'}, 300, states)
        self.assertEqual(states[0]['status'], 'cancelled')

    def test_id_survives_other_trips_being_added(self):
        identifier = self.result()[0][0]['ID']
        added = self.feed.entity.add(id='earlier').trip_update
        added.CopyFrom(self.update)
        added.trip.trip_id = 'a-trip'
        self.tables[2]['a-trip'] = 'route'
        rows, details, _ = self.result()
        actual = next(d['id'] for d in details if d['trip_id'] == 'trip')
        self.assertEqual(actual, identifier)

    def test_skipped(self):
        self.update.stop_time_update[1].schedule_relationship = pb.TripUpdate.StopTimeUpdate.SKIPPED
        self.assertEqual(self.result()[0], [])

    def test_newest_revision_wins(self):
        revision = self.feed.entity.add(id="two").trip_update
        revision.CopyFrom(self.update)
        revision.timestamp = 995
        revision.stop_time_update[1].arrival.delay = 300
        rows = self.result()[0]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["DELAY_S"], 300)

    def test_reject_differential(self):
        self.feed.header.incrementality = pb.FeedHeader.DIFFERENTIAL
        with self.assertRaises(ValueError):
            self.result()


if __name__ == "__main__":
    unittest.main()

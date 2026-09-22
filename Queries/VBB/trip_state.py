"""Reconcile FULL_DATASET snapshots without treating absence as cancellation.

Checkpoint state and NES map results together in latest.geojson. Retain inactive
trip records for one day, then discard them. This state lives in the adapter,
not inside NES; query classifications always come from the NES sinks.
"""
import copy
from collections import Counter


def reconcile(previous, current, incoming, now):
    at = current['metadata']['feed_timestamp']
    before = previous.get('metadata', {}).get('feed_timestamp', 0)
    if at < before:
        raise ValueError('Out-of-order VBB feed; refusing to roll back trip state')
    if not -60 <= now - at <= 300:
        raise ValueError('VBB feed is stale or timestamp is in the future')
    if at == before and previous.get('trip_states') is not None:
        return previous
    old = previous.get('trip_states', {})
    states, transitions = {}, []
    active = {f['properties']['trip_key']: f for f in current['features']}
    for item in incoming:
        state = dict(item)
        key = state['trip_key']
        prior = old.get(key, {})
        if prior and prior['id'] != state['id']:
            raise ValueError('Trip identity changed across snapshots')
        if state['update_timestamp'] is not None and state['update_timestamp'] < prior.get('accepted_update_timestamp', 0):
            state['status'] = 'out_of_order'
        state['accepted_update_timestamp'] = max(state['update_timestamp'] or 0,
                                                 prior.get('accepted_update_timestamp', 0))
        state['last_seen'] = at
        # Keep prediction history explicitly separate from current NES classifications.
        if state['status'] == 'active':
            props = active[key]['properties']
            state['delay_s'] = props['delay_s']
            state['predicted_arrival'] = props['predicted_arrival']
            state['nes_late'] = props['nes_late']
        else:
            active.pop(key, None)
        if not prior or any(state.get(k) != prior.get(k) for k in
                            ('status', 'delay_s', 'predicted_arrival', 'nes_late')):
            transitions.append({'trip_id': state['trip_id'], 'id': state['id'],
                                'from': prior.get('status'), 'to': state['status']})
        states[key] = state
    for key, prior in old.items():
        if key not in states and at - prior['last_seen'] <= 86400:
            state = dict(prior)
            # Preserve explicit cancellation while retaining the record.
            if state['status'] != 'cancelled':
                state['status'] = 'missing'
            state.pop('nes_late', None)
            state.pop('delay_s', None)
            state.pop('predicted_arrival', None)
            states[key] = state
            if prior['status'] != state['status']:
                transitions.append({'trip_id': state['trip_id'], 'id': state['id'],
                                    'from': prior['status'], 'to': state['status']})
    result = copy.deepcopy(current)
    result['features'] = list(active.values())
    result['trip_states'] = states
    result['metadata']['state_counts'] = dict(Counter(s['status'] for s in states.values()))
    result['metadata']['state_transitions'] = transitions
    # Preserve original engine counts; only count visible, accepted NES result rows here.
    result['metadata']['raw_query_result_counts'] = result['metadata']['query_result_counts']
    result['metadata']['query_result_counts'] = {
        k: sum(f['properties'][k] for f in result['features'])
        for k in ('nes_late', 'nes_in_region', 'nes_late_in_region')}
    return result

# LatestByKey prototype

## Semantics

```sql
SELECT id, revision, delay_s
FROM updates
LATEST BY id VERSION BY revision
INTO result_sink
```

The native logical/physical operator remembers the greatest accepted version per
key, for the lifetime of one query. It forwards the first record for each key and
subsequent strictly greater versions. Equal versions are first-wins, even when
payloads differ. Lower versions are discarded. Key and version expressions must
be non-nullable UINT64. Version zero is valid.

The output is an **upsert changelog**, not a final table or a retracting stream:
previous output rows are not removed. The operator stores versions, not a
queryable copy of every payload. Consumers materialize the latest accepted rows.
Do not apply ordinary COUNT/SUM to this stream and interpret it as the number or
sum of current trips. Output ordering across parallel execution threads is not
guaranteed; consumers must compare versions.

LATEST BY runs before WHERE. Predicate and projection pushdown stop at this
operator, using the optimizer's existing conservative fallback. A newer record
that fails a filter must still advance its key's version. To clear a dashboard
alert, project a Boolean flag for every accepted update, rather than filtering
out on-time updates.

## Limits

- First prototype: single-worker use; no distributed key routing or recovery.
- In-memory state only. Restarting the query loses prior versions.
- Maximum 100,000 distinct keys per operator. Exceeding this fails explicitly;
  there is no silent eviction that would let stale revisions reappear.
- No watermark, TTL, automatic cancellation, or missing-trip inference.
- Equal-version conflicts are not reconciled; producers need a meaningful version.

## VBB continuous mode

```sh
Queries/VBB/.venv/bin/python Queries/VBB/serve.py --continuous
```

Stop an older server on port 8000 first. The server deploys QLatest.yaml once.
Python fetches/decodes FULL_DATASET updates, performs the existing static-data
lookup and position interpolation, then sends normalized rows over a persistent
TCP connection. It does not decide which versions win or evaluate spatial/late
predicates. Explicit VBB cancellation/status labels are passed as data.

NES runs LatestByKey and the existing MovingPoint/SpatioTemporalBox predicates.
Its sink emits true **and false** classification flags. Python reads that sink
and publishes the current-feed map view; it bypasses trip_state.reconcile.
Trips absent from the current feed are not plotted, but are not declared
cancelled. Records rejected as older are likewise not plotted as current.

The version expression packs VBB update timestamp (high 32 bits) and feed timestamp
(low 32 bits). This allows estimated progress to refresh when the underlying VBB
revision is unchanged, while rejecting older VBB revisions even in newer feeds.
The adapter checks the timestamp range to prevent overflow. Missing update
timestamps are encoded as zero, not invented observations. Trip keys use the
existing stable trip-instance IDs; zero is reserved for a feed-boundary record.

The runner deliberately uses one execution thread and one ordered TCP source:
the boundary record acknowledges a feed. This is a demo protocol, not a
distributed barrier/checkpoint protocol. Any incomplete feed poisons the session
and requires a restart. Data is published atomically after acknowledgement.
The result file is session-local and append-only; it is removed when the session
closes. Long-term log rotation and checkpointing are future work.

TCP now polls with bounded waits, flushes partial buffers after the configured
interval without waiting for EOF or another message, and checks cancellation
while idle.

Continuous mode is the default; --snapshot selects the previous snapshot mode. Q1–Q3 and its
evaluation remain useful as a comparison, but are not long-running queries.

## Verification

```sh
Queries/VBB/.venv/bin/python -m unittest discover -s Queries/VBB -p 'test_*.py'
Queries/VBB/.venv/bin/python Queries/VBB/evaluate_latest.py
```

Native tests:

- LatestByKeyOperatorHandlerTest: versions, capacity, isolation and concurrency.
- nes-systests/operator/LatestByKey.test: SQL, lowering, execution and optimizer barriers.
- nes-systests/sources/TCP.test: existing transport regressions.

The integration evaluation uses one query across several feeds, checks stale and
duplicate revisions, late-to-on-time updates, explicit cancellation, unknown
delay, empty input, idle connection survival, and multi-buffer completion.
This evaluates query correctness, not ETA or GPS accuracy.

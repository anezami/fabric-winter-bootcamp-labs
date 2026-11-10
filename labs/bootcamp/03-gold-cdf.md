# Lab 3: Gold and both CDF processors

**15 minutes.** Prerequisite: Lab 2 passes.

Use `nb_bootcamp_gold_cdf` with its own imports/helpers. Generate in two
bounded steps. For a notebook job, approve and save **both steps** before one
run; an unfinished notebook would fail its final CDF check. Manual execution
may run the approved bootstrap and its check before generating step 2.

## 1. Generate the Gold bootstrap

```text
Use spark-cli and sections 6 and 8 of specs/bootcamp-lakehouse.spec.md.
Save a short Gold/CDF plan and numbered tasks under specs/plans/.
Generate new cells bootstrapping the Gold star from Silver: exact columns,
types, date coverage, table descriptions, column comments, order-line grain,
HALF_UP rounded revenue and the as-of customer join.
Bootstrap the Silver-to-Gold watermark before events and preserve the
Bronze-to-Silver watermark. Initialize customer targets and the fact snapshot
once; existing targets require valid durable watermarks.
Define imports/helpers locally. Do not apply events yet or alter the
spec/fixtures/validator. Show code and bounded write set, then stop for
approval. Do not submit a notebook job until the incremental step is ready.
```

After bootstrap, include this fixed check **before** the event cells:

```python
if not bootcamp_changes_started(spark):
    validate_bootcamp(spark, "gold")
```

This conditional is for completed-build replay. It must never suppress the
final CDF check. If a prior run stopped during events, resume the incremental
step; a partial event state is not a valid checkpoint.

## 2. Generate the incremental step

```text
Generate new cells applying the immutable customer event batch to
bronze.customers_current, then consuming Bronze CDF into Silver SCD2 and
Silver CDF into gold.dim_customer per section 6.
Expose process_bronze_to_silver() and process_silver_to_gold() with no arguments.
Read closed version ranges after durable watermarks through captured upper
versions, ignore update_preimage, and select the latest event per target key
for this bounded fixture. Print the actual consumed ranges.
U expires/inserts, I inserts, D expires without removing history. Stamp the
delete event's effective_at on Bronze state before removing it with keyed
MERGE ... WHEN MATCHED THEN DELETE; no subquery-based DELETE.
Target writes must be retry-safe before watermark advancement. Advance each
watermark only after target writes; fail on unavailable history. Never rebuild
targets from snapshots to pretend CDF was used. Keep facts as the initial
snapshot. Skip already-applied events and no-op source writes.
Whole-notebook replay must preserve data, keys, facts and watermark values.
Show code and write set, explain safety, then stop for approval.
```

Review the CDF reads, key selection, preimage filtering, write order and
watermark handling. After approval, save both steps and use
[Lab 1's execution prompt](01-bronze.md#2-execute-approved-code), or finish
the chosen manual execution path.

## 3. Verify events and replay

The unchanged final cell verifies completed state and reruns the processors:

```python
validate_bootcamp(spark, "cdf")
before = bootcamp_snapshot(spark)
process_bronze_to_silver()
process_silver_to_gold()
assert bootcamp_snapshot(spark) == before, "Replay changed persisted results"
```

Inspect the generated code and consumed version ranges. Output equality
alone does not establish that the processors used CDF.

## Definition of done

- Initial `gold` PASS and final `cdf` PASS, with actual bounded CDF ranges.
- Nine sales facts retain correct historical customer keys and total
  16 units / 516.73 net sales.
- Silver and Gold each contain eight customer-history rows, six current:
  C003 retains its expired Utrecht/Silver version plus current Amsterdam/Gold;
  C007 is current; C002 is expired with history retained.
- Both durable watermarks are current. Replaying both functions changes
  neither snapshots nor keys/facts/watermarks. Save evidence and tick tasks.

Use [repair and recovery](recovery.md) on failure. Do not overwrite history
or reset watermarks. Do not run the full pipeline until Lab 4 is ready.

**Next:** [Lab 4 - Materialized lake view](04-materialized-view.md).

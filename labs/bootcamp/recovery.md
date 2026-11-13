# Repair and recovery

Keep the fixed specification and checks. A failed stage is a debugging
exercise, not permission to change expected values or reset history. This page
is also the recovery plan for iterating in place: `lh_bootcamp` is an isolated
staging lakehouse rebuilt from immutable CSVs, with no clone-and-promote step.

## Recover setup safely

If the initial setup is interrupted, keep the returned item/operation IDs.
Ask Copilot to poll the accepted operation or rediscover only this approved
setup's items and bindings before retrying. Reuse matching items and skip
identical uploads; stop on conflicts. Do not duplicate creates, overwrite
files or reset another build. Ask the instructor about access/capacity blocks.

The [Lab 1 setup checkpoint](setup.md#4-wire-the-serial-pipeline) must be
complete before transformation recovery: supplied code cannot replace a
missing lakehouse, input files or notebook bindings.

## Repair your generated code

Wait for the current job to end and open its notebook run snapshot in Fabric
Monitor. Copy the actual assertion or exception, not just the generic REST
job failure reason. Ask Copilot:

```text
Fix only my generated implementation to satisfy the unchanged bootcamp spec
and validator. Use the actual error below and inspect persisted state.
Do not edit source CSVs/checks, drop tables, reset history or watermarks,
or create another workspace. Explain the root cause and show the smallest
patch and exact write set. Stop for approval before executing it.
```

Approve the patch, run only the affected stage and its unchanged checks, and
keep the error and result locally. Do not start a duplicate job while one is
queued or running.

## Use instructor-supplied recovery only explicitly

If blocked within the time box, ask the instructor for the relevant stage's
recovery cells. Record that stage as **supplied recovery**, not successful
live generation.

Before using them, confirm the same identity, workspace and lakehouse, and
stop any existing writer. Preserve your generated code/output. Replace only
the implementation cells in the existing notebook; keep the validator, baseline
and check cells, notebook ID and pipeline wiring. Recovery cells declare their
own session variables and read the persisted Delta tables.

Run that stage and its fixed checks, then continue downstream. Each stage
writes only its own layer and resumes from persisted Delta tables and its own
`lab_ops.cdf_watermarks` rows: Bronze creates only missing tables, Silver and
Gold consume the change feed after their watermarks, and the MLV stage
recreates its views. A watermark advances only after its target write, so
rerunning an interrupted stage is safe. Never reseed a missing watermark for
an existing target, reset watermarks or discard history. A malformed raw
Bronze table may be dropped only with explicit instructor approval, and only
before the change file has landed. Unavailable CDF history needs an explicit
instructor repair decision, not an automatic reset.

## Stop on time

Stop the live build after 45 minutes. Show available sales, history and
lineage evidence, and label any incomplete stage. Keep completed work and
stop Spark sessions you started.

After a complete build, pipeline replay must preserve customer history,
surrogate keys, facts and watermark values. In each Bronze, Silver and Gold
notebook the fixed baseline and check cells compare the layer before and
after a rerun, not only row counts: when the baseline finds the layer
current, the check fails if your generated cells changed any row or Delta
version.

[Back to the lab overview](../../README.md).

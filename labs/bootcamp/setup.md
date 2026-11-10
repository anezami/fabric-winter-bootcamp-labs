# Lab 1 setup: empty workspace to bound shells

The [initial Lab 1 prompt](01-bronze.md) performs this procedure after setup
approval. Start from the assigned empty workspace; its dedicated F2 capacity
is already assigned and Active. Do not perform these writes during pre-work.
Setup and cold Spark startup count inside Lab 1's 10-minute slot.

## 1. Discover, plan and approve

Use `spark-cli` and the signed-in CLI identity (`az account show`). Discover
the exact assigned workspace, its capacity and current items. On a first run
it must be empty. Show the identity, workspace, capacity, proposed lakehouse,
six upload destinations, four notebooks and pipeline; obtain setup approval
before the first remote write. Do not create/reassign workspaces or capacities,
change permissions or guess IDs.

On retry, reconcile only items and operations from this same confirmed setup.
Poll an accepted operation instead of resubmitting; rediscover items after
an uncertain response. Reuse an item only after confirming its identity,
definition/binding and ownership by this setup. Stop on duplicates, unexpected
items or conflicting files; do not reset existing work.

## 2. Create the lakehouse and stage inputs

Use the skill's CLI creation procedure in the confirmed workspace with this
Fabric item-create payload (`POST /v1/workspaces/{workspaceId}/items`):

```json
{
  "displayName": "lh_bootcamp",
  "type": "Lakehouse",
  "creationPayload": {"enableSchemas": true}
}
```

For `az rest`, use `--resource https://api.fabric.microsoft.com` and the
`x-ms-fabric-skill: spark-cli` header on every Fabric call, including polls.
Wait for successful creation, capture the returned lakehouse ID, and verify
schema support before proceeding. A default schema-disabled lakehouse is
not acceptable; do not silently substitute one.

Upload the five `data/bootcamp/initial/*.csv` files to
`lh_bootcamp/Files/bootcamp/initial/` and `customer_changes.csv` from
`data/bootcamp/changes/` to `lh_bootcamp/Files/bootcamp/changes/`, using the
skill's OneLake upload tooling. OneLake uses the storage token audience,
not the Fabric API audience. Verify all six paths and uploaded bytes against
the local files. On retry, skip identical uploads; stop on a mismatch.
Files are immutable after landing.

## 3. Generate and deploy empty shells

The local [artifact builder](prepare-artifacts.py) uses Python's standard
library. It writes JSON/Jupyter files under ignored `out/`; it does not
authenticate, discover resources, deploy items or run jobs.

From PowerShell in the repository root:

```powershell
$workspaceId = Read-Host "Discovered and confirmed workspace ID"
$lakehouseId = Read-Host "Discovered and confirmed lh_bootcamp ID"
python labs\bootcamp\prepare-artifacts.py shells --workspace-id $workspaceId --lakehouse-id $lakehouseId --output out\bootcamp-shells
```

The output folder must be new or empty. Each `.ipynb` includes the complete
[fixed validator](validate-lakehouse.py), a `NotImplementedError` placeholder
and its final check. No transformation code is supplied.

Within the approved setup write set, create exactly `nb_bootcamp_bronze`,
`nb_bootcamp_silver`, `nb_bootcamp_gold_cdf` and `nb_bootcamp_mlv` from the four
`out\bootcamp-shells` request definitions. Use Notebook item creation with
`definition.format` set to `ipynb`; the request's `definition` object is the
definition, not the whole request body. Capture actual IDs, wait for completion
and read back each definition to verify the default lakehouse, unchanged
validator, fail-fast placeholder and final fixed check. Do not run the shells.

Alternatively, import the four `.ipynb` files using Fabric's notebook import
and inspect their bindings. Do not create a second set of notebooks.

Save `out\bootcamp-notebook-ids.json` locally with exactly these four keys,
each mapped to the corresponding **discovered** notebook ID:

```json
{
  "bronze": "<discovered Bronze notebook ID>",
  "silver": "<discovered Silver notebook ID>",
  "gold_cdf": "<discovered Gold/CDF notebook ID>",
  "mlv": "<discovered MLV notebook ID>"
}
```

Replace every placeholder before proceeding; each stage needs a distinct ID.

## 4. Wire the serial pipeline

```powershell
python labs\bootcamp\prepare-artifacts.py pipeline --workspace-id $workspaceId --notebook-ids out\bootcamp-notebook-ids.json --output out\bootcamp-pipeline
```

Ask Copilot to create `pl_bootcamp_build` from
`out\bootcamp-pipeline\pl_bootcamp_build.request.json` as a DataPipeline item
with the supplied definition. Do not schedule or run it.

Inspect its activities in Fabric: the correct workspace/notebook IDs,
Bronze -> Silver -> Gold/CDF -> MLV, success-only dependencies, zero retries,
no parallel branches. A generated 15-minute activity timeout does not replace
the overall 45-minute live-build limit.

**Setup checkpoint:** one schema-enabled `lh_bootcamp`, six byte-identical
landed CSVs, four correctly bound empty shells with fixed checks, and one
unscheduled serial pipeline. Save actual item IDs and completion evidence
locally. No data tables or transformations have run; no warm session is required.

## 5. Generate, approve and execute

Generate and approve one notebook at a time. Replace only its implementation
placeholder, preserving both fixed cells. Save before submitting that
individual notebook, or run its cells manually. Keep actual job IDs and
check output. Never execute the full pipeline while later shells are empty.

All stages need their own imports/helpers. Preserve existing customer
state/history, fact snapshot and watermarks. Repair an interrupted Gold/CDF
run before replaying the full build.

After all four labs pass, the pipeline can replay the build. Capture
`bootcamp_snapshot(spark)` before and after the replay in a validation context,
and compare it. Move the replay after the session if it would exceed the
time box. Stop any previous writer before changing execution paths.

For an approved update to an existing notebook, pass only the emitted
`definition` object to `updateDefinition`, not the complete create request.
Keep existing item IDs so pipeline wiring remains valid.

References: [Notebook creation](https://learn.microsoft.com/en-us/rest/api/fabric/notebook/items/create-notebook),
[pipeline definitions](https://learn.microsoft.com/en-us/fabric/data-factory/pipeline-rest-api-capabilities),
[notebook activity](https://learn.microsoft.com/en-us/fabric/data-factory/notebook-activity).

**Next:** continue [Lab 1](01-bronze.md) with newly generated Bronze code
and separate code approval.

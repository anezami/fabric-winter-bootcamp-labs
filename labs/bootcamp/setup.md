# Lab 1 setup: empty workspace to bound shells

The [initial Lab 1 prompt](01-bronze.md) performs this procedure after setup
approval. Start from the assigned empty workspace; its dedicated F2 capacity
is already assigned and Active. Do not perform these writes during pre-work.
Setup and cold Spark startup count inside Lab 1's 10-minute slot.

## 1. Discover, plan and approve

Use `spark-cli` and the signed-in CLI identity (`az account show`). Discover
the exact assigned workspace, its capacity and current items. On a first run
it must be empty. Show the identity, workspace, capacity, proposed lakehouse,
five upload destinations, four notebooks and pipeline; obtain setup approval
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
`lh_bootcamp/Files/bootcamp/initial/` using the skill's OneLake upload
tooling. OneLake uses the storage token audience, not the Fabric API
audience. Verify all five paths and uploaded bytes against the local files.
On retry, skip identical uploads; stop on a mismatch. Files are immutable
after landing. Do not upload `data/bootcamp/changes/customer_changes.csv`
yet: Lab 4 lands it as new data.

On a connection timeout, retain the lakehouse ID and check reachability using
the skill's documented regional OneLake endpoint for the verified capacity
region. Keep the same identity and workspace/lakehouse path; byte-check any
existing file before resuming. Do not recreate resources or change network or
access policies. An authorization error is not a reason to try another target.

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

The output folder must be new or empty. Each `.ipynb` contains a markdown
description, only its applicable [fixed validator](validate-lakehouse.py) sections, a
`NotImplementedError` placeholder and the fixed stage check. No
transformation code is supplied.

Within the approved setup write set, create exactly `nb_bootcamp_bronze`,
`nb_bootcamp_silver`, `nb_bootcamp_gold` and `nb_bootcamp_mlv` from the four
`out\bootcamp-shells` request definitions. Use Notebook item creation with
`definition.format` set to `ipynb`; the request's `definition` object is the
definition, not the whole request body. Capture actual IDs, wait for completion
and read back each definition to verify the default lakehouse, description,
unchanged stage-specific validator, fail-fast placeholder and final fixed check.
Review every code cell: Bronze must contain no Silver/Gold helpers, Silver no
Gold helpers, and Gold no Bronze reads or Silver validators. Do not run the shells.

Alternatively, import the four `.ipynb` files using Fabric's notebook import
and inspect their bindings. Do not create a second set of notebooks.

Save `out\bootcamp-notebook-ids.json` locally with exactly these four keys,
each mapped to the corresponding **discovered** notebook ID:

```json
{
  "bronze": "<discovered Bronze notebook ID>",
  "silver": "<discovered Silver notebook ID>",
  "gold": "<discovered Gold notebook ID>",
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
Bronze -> Silver -> Gold -> MLV, success-only dependencies, zero retries,
no parallel branches. A generated 15-minute activity timeout does not replace
the overall 45-minute live-build limit.

**Setup checkpoint:** one schema-enabled `lh_bootcamp`, five byte-identical
landed CSVs, four correctly bound empty shells with fixed checks, and one
unscheduled serial pipeline. Save actual item IDs and completion evidence
locally. No data tables or transformations have run; no warm session is required.

## 5. Generate, approve and execute

Generate and approve one notebook at a time using Labs 1-4. Replace only its
implementation placeholder, preserving the description, validator and fixed
check cells. Save the notebook before submitting that individual notebook, or
run its cells manually. Keep actual job IDs and check output. Never execute
the full pipeline while later shells are unimplemented.

Run `pl_bootcamp_build` only once all four notebooks are implemented and
checked: Lab 4 uses one pipeline run to carry the change batch through every
layer. A failed stage blocks downstream stages. Stop any previous writer and
confirm it has ended before switching execution paths.

Every stage reloads its own imports and helpers and reads persisted Delta
state and watermarks; no Python state carries between notebooks. The fixed
checks find the expected state from the landed files: the initial load until
`Files/bootcamp/changes/customer_changes.csv` exists, then the changed state.
Each Bronze, Silver and Gold check also reruns its processor and requires an
identical `bootcamp_snapshot(spark, "<layer>")`. On failure, repair the
generated code and rerun that notebook; never reset tables or watermarks to
make a check pass.

For an approved update to an existing notebook, pass only the emitted
`definition` object to `updateDefinition`, not the complete create request.
Keep existing item IDs so pipeline wiring remains valid.

References: [Notebook creation](https://learn.microsoft.com/en-us/rest/api/fabric/notebook/items/create-notebook),
[pipeline definitions](https://learn.microsoft.com/en-us/fabric/data-factory/pipeline-rest-api-capabilities),
[notebook activity](https://learn.microsoft.com/en-us/fabric/data-factory/notebook-activity).

**Next:** continue [Lab 1](01-bronze.md) with newly generated Bronze code
and separate code approval.

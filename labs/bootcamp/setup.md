# Pre-work: notebook shells and pipeline

The local [artifact builder](prepare-artifacts.py) uses Python's standard
library. It writes JSON/Jupyter files under ignored `out/`; it does not
authenticate, discover resources, deploy items or run jobs.

## 1. Discover and confirm the target

Use Copilot with `spark-cli` and Fabric tools to confirm `az account show`,
your assigned workspace, active capacity and schema-enabled `lh_bootcamp`.
Discover the workspace and lakehouse IDs. Approve the target before any
remote write; do not guess IDs or paste credentials.

From PowerShell in the repository root:

```powershell
$workspaceId = Read-Host "Discovered and confirmed workspace ID"
$lakehouseId = Read-Host "Discovered and confirmed lh_bootcamp ID"
python labs\bootcamp\prepare-artifacts.py shells --workspace-id $workspaceId --lakehouse-id $lakehouseId --output out\bootcamp-shells
```

The output folder must be new or empty. Each `.ipynb` includes the complete
[fixed validator](validate-lakehouse.py), a `NotImplementedError` placeholder
and its final check. No transformation code is supplied.

## 2. Provision four notebooks once

Give Copilot this instruction and approve only the bounded setup write set:

```text
Use the public Fabric skills and my confirmed workspace/lh_bootcamp.
Create exactly nb_bootcamp_bronze, nb_bootcamp_silver,
nb_bootcamp_gold_cdf and nb_bootcamp_mlv from the four request definitions
in out/bootcamp-shells. Check for existing names first; stop on duplicates
or unexpected bindings. Use Notebook item creation with definition.format
ipynb. Each request's definition object is its definition, not the whole
request body. Do not run notebooks or generate transformations.
Capture actual item IDs, poll accepted operations to completion without
resubmitting, and verify every default lakehouse binding.
```

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

## 3. Wire the serial pipeline

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

## 4. Execute during the labs

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

**Next:** finish the [pre-work ready check](pre-work.md#ready-check).

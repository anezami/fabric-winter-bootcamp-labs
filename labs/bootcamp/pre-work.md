# Pre-work: get ready

Complete this before the session. The live 45 minutes are for designing,
generating and running the lakehouse, not installing tools.

## 1. Check tools and access

You need:

- Git, Python 3.10+ and Azure CLI.
- GitHub Copilot CLI with an active Copilot account.
- Your assigned Fabric workspace, permission to create notebooks/lakehouse
  items, and an active F2 capacity with Materialized Lake View support.
- The public [Skills for Fabric](https://github.com/microsoft/skills-for-fabric)
  bundle, including `spark-cli`.

From PowerShell:

```powershell
git clone https://github.com/anezami/fabric-winter-bootcamp-labs.git
cd fabric-winter-bootcamp-labs
az login
az account show
copilot
```

If your Fabric identity has no Azure subscription, use
`az login --allow-no-subscriptions`. Confirm the correct tenant and identity
with your instructor; do not paste tokens into chat.

Inside Copilot, check installed plugins. If the Fabric bundle is missing:

```text
/plugin marketplace add microsoft/skills-for-fabric
/plugin install fabric-skills@fabric-collection
```

Restart Copilot after installation and confirm `spark-cli` is available.
If you use another supported Copilot client, follow the public package's
client-specific setup. Do not copy CLI configuration into a different client.

## 2. Create and stage your target

Ask Copilot to discover your assigned workspace and confirm its active
capacity and your identity. Approve only creation of a **schema-enabled**
lakehouse named `lh_bootcamp` in that workspace.

If that lakehouse already contains another build, stop and ask the instructor
for an appropriate empty target. Do not delete, reset or overwrite it.

Upload the files from [the data folder](../../data/bootcamp/):

| Local folder/file | Destination inside `lh_bootcamp` |
|---|---|
| Five CSVs in `data/bootcamp/initial/` | `Files/bootcamp/initial/` |
| `data/bootcamp/changes/customer_changes.csv` | `Files/bootcamp/changes/customer_changes.csv` |

Use the Lakehouse explorer or approved Fabric OneLake upload tools. Preserve
the filenames, headers and values. Files remain read-only after upload.

## 3. Prepare empty notebooks and pipeline

Follow [notebook and pipeline setup](setup.md). It creates four bound shells
containing the fixed validator, an unimplemented placeholder and a final
check, plus `pl_bootcamp_build` with this success-only order:

```text
nb_bootcamp_bronze -> nb_bootcamp_silver -> nb_bootcamp_gold_cdf -> nb_bootcamp_mlv
```

Do not generate transformations, create tables, run all cells or trigger the
pipeline during pre-work. The implementation placeholder intentionally fails.

To warm one interactive session, run only the validator cell and this
temporary cell in the Bronze notebook, then remove the temporary cell:

```python
spark.sql("SELECT 1 AS ready").show()
```

Use one Spark workload at a time. Stop an idle interactive session before
submitting a separate notebook job. A warm session does not guarantee job or
pipeline session reuse; never submit duplicate runs while waiting.

## Ready check

- Correct signed-in identity, assigned workspace and active capacity.
- Empty schema-enabled `lh_bootcamp` with all six CSVs in the required paths.
- Four notebooks bound to that lakehouse, with unchanged validator and
  final-check cells; no transformations or tables yet.
- Pipeline references those notebook IDs in serial success-only order,
  with no schedule or parallel branches.
- At most one warm interactive session.

If anything is blocked, ask the instructor before starting. Do not change
workspaces, capacities or permissions yourself.

**Next:** [Lab 1 - Bronze](01-bronze.md).

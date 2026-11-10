"""Build local empty notebook and pipeline definitions; never deploy or run jobs.

Each stage notebook shell has four cells: a markdown description, the fixed
read-only validator, one implementation cell for the generated layer code and
a fixed check that validates the layer and proves a rerun changes nothing.
"""

import argparse
import base64
import json
from pathlib import Path
from uuid import UUID, uuid4

HERE = Path(__file__).resolve().parent
STAGES = ("bronze", "silver", "gold", "mlv")
NAMES = {stage: f"nb_bootcamp_{stage}" for stage in STAGES}
PROCESSORS = {
    "bronze": "load_bronze",
    "silver": "process_bronze_to_silver",
    "gold": "process_silver_to_gold",
}
DESCRIPTIONS = {
    "bronze": ("Bronze layer",
               "Loads the CSV files landed in `Files/bootcamp/` into `bronze.*` Delta tables "
               "as raw strings with audit columns and minimal cleansing, with Change Data Feed "
               "enabled. Applies a landed customer change batch to `bronze.customers_current`."),
    "silver": ("Silver layer",
               "Reads the Bronze Change Data Feed from each watermark and maintains the typed "
               "`silver.*` tables and the SCD2 `silver.customer_history`, with Change Data Feed "
               "enabled for Gold."),
    "gold": ("Gold layer",
             "Reads the Silver Change Data Feed from each watermark and maintains the documented "
             "Gold star schema: `dim_customer`, `dim_product`, `dim_store`, `dim_date` and "
             "`fact_sales`."),
    "mlv": ("Materialized lake views",
            "Defines `gold.sales_daily` and `gold.customer_current` as materialized lake views "
            "over the Silver tables."),
}


def guid(value):
    """Return a normalized GUID string; reject malformed or all-zero IDs."""
    parsed = UUID(value)
    if parsed.int == 0:
        raise ValueError("An actual nonempty discovered item ID is required")
    return str(parsed)


def lines(source):
    """Split cell source into notebook lines that keep their newline characters."""
    return [line + "\n" for line in source.rstrip().splitlines()]


def code_cell(source, tags=()):
    """Return one Fabric-compatible code cell with empty outputs and tags."""
    return {
        "id": uuid4().hex, "cell_type": "code", "execution_count": None,
        "metadata": {"tags": list(tags)}, "outputs": [], "source": lines(source),
    }


def markdown_cell(source):
    """Return one markdown cell."""
    return {"id": uuid4().hex, "cell_type": "markdown", "metadata": {}, "source": lines(source)}


def envelope(content, path, format=None):
    """Wrap item content as an inline Base64 Fabric definition part."""
    result = {"parts": [{
        "path": path, "payloadType": "InlineBase64",
        "payload": base64.b64encode(json.dumps(content).encode()).decode(),
    }]}
    if format:
        result["format"] = format
    return result


def description(stage):
    """Return the markdown description that opens a stage notebook."""
    title, text = DESCRIPTIONS[stage]
    return (f"# {NAMES[stage]}: {title}\n{text}\n\n"
            "Cells: (1) this description; (2) the fixed read-only validator; "
            "(3) the generated implementation, reviewed before running; "
            "(4) the fixed stage check. Do not edit the fixed cells.")


def placeholder(stage):
    """Return the implementation cell that must be replaced by generated code."""
    return (f"# Generated {stage} implementation: replace this cell with the reviewed code "
            "generated from the spec.\n"
            f'raise NotImplementedError("Generate and approve {stage} code live before '
            'running this shell")')


def check(stage):
    """Return the fixed stage check: validate, then prove that a rerun changes nothing."""
    if stage == "mlv":
        return ("# Fixed stage check: validate both materialized lake views against Silver.\n"
                'validate_bootcamp(spark, "mlv")')
    function = PROCESSORS[stage]
    return f'''# Fixed stage check: validate the {stage} contract, then prove a rerun changes nothing.
validate_bootcamp(spark, "{stage}")
before = bootcamp_snapshot(spark, "{stage}")
{function}()
assert bootcamp_snapshot(spark, "{stage}") == before, "Rerunning {function}() changed {stage}"
print("PASS: {stage} rerun is a no-op")'''


def notebook(stage, workspace_id, lakehouse_id):
    """Build one empty stage notebook shell bound to lh_bootcamp."""
    validator = (HERE / "validate-lakehouse.py").read_text(encoding="utf-8")
    cells = [
        markdown_cell(description(stage)),
        code_cell(validator, ["fixed-validator"]),
        code_cell(placeholder(stage), ["implementation"]),
        code_cell(check(stage), ["fixed-stage-check"]),
    ]
    return {
        "nbformat": 4, "nbformat_minor": 5, "cells": cells,
        "metadata": {
            "kernelspec": {"display_name": "Synapse PySpark", "language": "python", "name": "synapse_pyspark"},
            "language_info": {"name": "python"},
            "dependencies": {"lakehouse": {
                "default_lakehouse": guid(lakehouse_id),
                "default_lakehouse_name": "lh_bootcamp",
                "default_lakehouse_workspace_id": guid(workspace_id),
            }},
        },
    }


def pipeline(workspace_id, notebook_ids):
    """Build the serial pl_bootcamp_build definition: bronze, silver, gold, then mlv."""
    if set(notebook_ids) != set(STAGES):
        raise ValueError(f"Notebook mapping must have exactly {STAGES}")
    if len({guid(value) for value in notebook_ids.values()}) != len(STAGES):
        raise ValueError("Each stage must reference a different notebook")
    activities = []
    for index, stage in enumerate(STAGES):
        activities.append({
            "name": stage, "type": "TridentNotebook",
            "dependsOn": [] if index == 0 else [{
                "activity": STAGES[index - 1], "dependencyConditions": ["Succeeded"],
            }],
            "policy": {"timeout": "0.00:15:00", "retry": 0, "retryIntervalInSeconds": 30,
                       "secureInput": False, "secureOutput": False},
            "typeProperties": {
                "notebookId": guid(notebook_ids[stage]), "workspaceId": guid(workspace_id),
            },
        })
    return {"properties": {"activities": activities}}


def write_new(path, content):
    """Write JSON to a new file; never overwrite an existing artifact."""
    with path.open("x", encoding="utf-8") as target:
        json.dump(content, target, indent=2)
        target.write("\n")


def main():
    """Parse arguments and write the notebook shells or the pipeline locally."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("shells", "pipeline"))
    parser.add_argument("--workspace-id", required=True, type=guid)
    parser.add_argument("--lakehouse-id", type=guid)
    parser.add_argument("--notebook-ids", type=Path, help="Local JSON mapping stage names to discovered item IDs")
    parser.add_argument("--output", type=Path, required=True, help="New or empty local output folder")
    args = parser.parse_args()
    if args.output.exists() and any(args.output.iterdir()):
        parser.error("--output must be a new or empty folder; existing artifacts are not overwritten")
    if args.mode == "shells" and not args.lakehouse_id:
        parser.error("--lakehouse-id is required for notebooks")
    if args.mode == "pipeline" and not args.notebook_ids:
        parser.error("--notebook-ids is required for pipeline wiring")
    args.output.mkdir(parents=True, exist_ok=True)
    if args.mode == "pipeline":
        ids = json.loads(args.notebook_ids.read_text(encoding="utf-8"))
        content = pipeline(args.workspace_id, ids)
        write_new(args.output / "pipeline-content.json", content)
        write_new(args.output / "pl_bootcamp_build.request.json", {
            "displayName": "pl_bootcamp_build", "type": "DataPipeline",
            "definition": envelope(content, "pipeline-content.json"),
        })
    else:
        for stage in STAGES:
            content = notebook(stage, args.workspace_id, args.lakehouse_id)
            write_new(args.output / f"{NAMES[stage]}.ipynb", content)
            write_new(args.output / f"{NAMES[stage]}.request.json", {
                "displayName": NAMES[stage], "type": "Notebook",
                "definition": envelope(content, "notebook-content.ipynb", "ipynb"),
            })
    print(f"Local {args.mode} artifacts written to {args.output}; nothing deployed or executed")


if __name__ == "__main__":
    main()

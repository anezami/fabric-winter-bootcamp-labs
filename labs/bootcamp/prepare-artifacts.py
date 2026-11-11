"""Build local empty notebook and pipeline definitions; never deploy or run jobs.

Each stage notebook shell has five cells: a markdown description, the fixed
read-only validator, a fixed baseline cell, one Spark SQL placeholder cell for
the generated layer code and a fixed check that validates the layer and proves
a rerun changes nothing. The materialized-lake-view notebook has no baseline
cell because its views are rebuilt declaratively. Generated cells are native
Spark SQL (%%sql); only the fixed validator, baseline and check cells are PySpark.
"""

import argparse
import base64
import json
import re
from pathlib import Path
from uuid import UUID, uuid4

HERE = Path(__file__).resolve().parent
STAGES = ("bronze", "silver", "gold", "mlv")
NAMES = {stage: f"nb_bootcamp_{stage}" for stage in STAGES}
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


def code_cell(source, tags=(), sql=False):
    """Return one Fabric-compatible code cell; SQL cells start with %%sql and carry sparksql metadata."""
    metadata = {"tags": list(tags)}
    if sql:
        metadata["microsoft"] = {"language": "sparksql", "language_group": "synapse_pyspark"}
        source = "%%sql\n" + source
    return {
        "id": uuid4().hex, "cell_type": "code", "execution_count": None,
        "metadata": metadata, "outputs": [], "source": lines(source),
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
    cells = ["this description", "this layer's fixed read-only validator"]
    if stage != "mlv":
        cells.append("the fixed baseline, which records whether the layer is already current")
    cells += ["the generated Spark SQL implementation, reviewed before running",
              "the fixed stage check"]
    listed = "; ".join(f"({number}) {item}" for number, item in enumerate(cells, 1))
    return (f"# {NAMES[stage]}: {title}\n{text}\n\n"
            f"Cells: {listed}. Do not edit the fixed cells.")


def placeholder(stage):
    """Return the Spark SQL cell that fails fast until it is replaced by generated code."""
    return (f"-- Generated {stage} implementation: replace this cell with the reviewed Spark SQL "
            "generated from the spec.\n"
            f"SELECT raise_error('Generate and approve {stage} code live before running this shell')")


def baseline(stage):
    """Return the fixed baseline cell that runs before the generated implementation."""
    return (f"# Fixed baseline cell: record whether the {stage} layer is already current, so that "
            "rerunning the generated cell below must change nothing.\n"
            f'bootcamp_begin(spark, "{stage}")')


def check(stage):
    """Return the fixed stage check: validate the layer and, when it was current, the no-op rerun."""
    if stage == "mlv":
        return ("# Fixed stage check: validate both materialized lake views against Silver.\n"
                'validate_bootcamp(spark, "mlv")')
    return (f"# Fixed stage check: validate the {stage} contract and, when the baseline found the "
            "layer already current, prove that the rerun changed nothing.\n"
            f'validate_bootcamp(spark, "{stage}")')


def validator_source(stage):
    """Select unchanged common and stage-labelled checks, excluding other layers' code."""
    if stage not in STAGES:
        raise ValueError(f"Unknown bootcamp stage: {stage}")
    source = (HERE / "validate-lakehouse.py").read_text(encoding="utf-8")
    sections = re.split(r"^# Validation section: ([a-z ]+)\n", source, flags=re.MULTILINE)
    if len(sections) == 1:
        raise ValueError("The fixed validator has no labelled sections")
    selected = [sections[0]]
    for labels, content in zip(sections[1::2], sections[2::2]):
        names = set(labels.split())
        if not names or not names <= {"common", *STAGES}:
            raise ValueError(f"Unknown validation section: {labels}")
        if "common" in names or stage in names:
            selected.append(content)
    return "".join(selected)


def notebook(stage, workspace_id, lakehouse_id):
    """Build one empty stage notebook shell bound to lh_bootcamp."""
    cells = [markdown_cell(description(stage)),
             code_cell(validator_source(stage), ["fixed-validator"])]
    if stage != "mlv":
        cells.append(code_cell(baseline(stage), ["fixed-baseline"]))
    cells.append(code_cell(placeholder(stage), ["implementation"], sql=True))
    cells.append(code_cell(check(stage), ["fixed-stage-check"]))
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

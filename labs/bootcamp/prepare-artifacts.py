"""Build local empty notebook and pipeline definitions; never deploy or run jobs."""

import argparse
import base64
import json
from pathlib import Path
from uuid import UUID, uuid4

HERE = Path(__file__).resolve().parent
STAGES = ("bronze", "silver", "gold_cdf", "mlv")
NAMES = {stage: f"nb_bootcamp_{stage}" for stage in STAGES}


def guid(value):
    parsed = UUID(value)
    if parsed.int == 0:
        raise ValueError("An actual nonempty discovered item ID is required")
    return str(parsed)


def code_cell(source, tags=()):
    return {
        "id": uuid4().hex, "cell_type": "code", "execution_count": None,
        "metadata": {"tags": list(tags)}, "outputs": [],
        "source": [line + "\n" for line in source.rstrip().splitlines()],
    }


def envelope(content, path, format=None):
    result = {"parts": [{
        "path": path, "payloadType": "InlineBase64",
        "payload": base64.b64encode(json.dumps(content).encode()).decode(),
    }]}
    if format:
        result["format"] = format
    return result


def notebook(stage, workspace_id, lakehouse_id):
    validator = (HERE / "validate-lakehouse.py").read_text(encoding="utf-8")
    cells = [
        code_cell(validator, ["fixed-validator"]),
        code_cell(
            f'raise NotImplementedError("Generate and approve {stage} code live before running this shell")',
            ["implementation"],
        ),
    ]
    check = {
        "bronze": 'validate_bootcamp(spark, "bronze", after_changes=bootcamp_changes_started(spark))',
        "silver": 'validate_bootcamp(spark, "silver", after_changes=bootcamp_changes_started(spark))',
        "gold_cdf": '''validate_bootcamp(spark, "cdf")
before = bootcamp_snapshot(spark)
process_bronze_to_silver()
process_silver_to_gold()
assert bootcamp_snapshot(spark) == before, "Replay changed persisted results"
''',
        "mlv": 'validate_bootcamp(spark, "mlv")',
    }[stage]
    cells.append(code_cell(check, ["fixed-stage-check"]))
    return {
        "nbformat": 4, "nbformat_minor": 5, "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Synapse PySpark", "language": "python",
                "name": "synapse_pyspark",
            },
            "language_info": {"name": "python"},
            "dependencies": {"lakehouse": {
                "default_lakehouse": guid(lakehouse_id),
                "default_lakehouse_name": "lh_bootcamp",
                "default_lakehouse_workspace_id": guid(workspace_id),
            }},
        },
    }


def pipeline(workspace_id, notebook_ids):
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
            "policy": {
                "timeout": "0.00:15:00", "retry": 0, "retryIntervalInSeconds": 30,
                "secureInput": False, "secureOutput": False,
            },
            "typeProperties": {
                "notebookId": guid(notebook_ids[stage]), "workspaceId": guid(workspace_id),
            },
        })
    return {"properties": {"activities": activities}}


def write_new(path, content):
    with path.open("x", encoding="utf-8") as target:
        json.dump(content, target, indent=2)
        target.write("\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("shells", "pipeline"))
    parser.add_argument("--workspace-id", required=True, type=guid)
    parser.add_argument("--lakehouse-id", type=guid)
    parser.add_argument("--notebook-ids", type=Path,
                        help="Local JSON mapping stage names to discovered item IDs")
    parser.add_argument("--output", type=Path, required=True,
                        help="New or empty local output folder")
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

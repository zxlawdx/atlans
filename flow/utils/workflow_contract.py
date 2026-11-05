# flow/utils/workflow_contract.py
"""
Extracts a workflow's contract (public API) for use as a sub-workflow.

The contract is declared via the `ports` property (list of strings) on the
SubWorkflowInput and SubWorkflowOutput nodes. Edges leaving the Input and arriving
at the Output don't need `from_key`/`to_key` defined — the only edge
allowed on each side routes the whole dict via `inputs.update(...)` in the
executor (the "no key" path in flow/executor/core.py).
"""
from __future__ import annotations

import json
from typing import Any, Dict


_INPUT_NODE_NAME = "SubWorkflowInput"
_OUTPUT_NODE_NAME = "SubWorkflowOutput"
_PORTS_PROP = "ports"


def _node_properties(node: Dict[str, Any]) -> Dict[str, Any]:
    """Returns the node's properties accepting both canvas formats:
    `node["properties"]` or `node["data"]["properties"]`."""
    props = node.get("properties")
    if isinstance(props, dict):
        return props
    data = node.get("data") or {}
    return data.get("properties") or {}


def _parse_ports(raw: Any) -> list[str]:
    """Normalizes `ports` to list[str]. Accepts a native list or serialized JSON
    (the UI helper stores it via JSON.stringify for compatibility with setNodeField).
    Filters out empty values and non-strings."""
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (ValueError, TypeError):
            return []
    if not isinstance(raw, list):
        return []
    seen: set[str] = set()
    result: list[str] = []
    for item in raw:
        if not isinstance(item, str):
            continue
        key = item.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(key)
    return result


def extract_contract(definition: Dict[str, Any]) -> Dict[str, list]:
    """Returns {"inputs": [{name, ...}], "outputs": [...], "has_input_node", "has_output_node"}.

    Inputs/outputs are read from the `ports` property of the SubWorkflowInput
    and SubWorkflowOutput nodes. When the workflow doesn't have these nodes, returns
    empty lists and has_*_node flags False — the caller decides what to do.
    """
    nodes = definition.get("nodes") or []

    input_nodes = [n for n in nodes if n.get("name") == _INPUT_NODE_NAME]
    output_nodes = [n for n in nodes if n.get("name") == _OUTPUT_NODE_NAME]

    # Merges ports from all nodes of the type (rare to have more than 1, but defensive).
    input_keys: list[str] = []
    seen_in: set[str] = set()
    for n in input_nodes:
        for key in _parse_ports(_node_properties(n).get(_PORTS_PROP)):
            if key not in seen_in:
                seen_in.add(key)
                input_keys.append(key)

    output_keys: list[str] = []
    seen_out: set[str] = set()
    for n in output_nodes:
        for key in _parse_ports(_node_properties(n).get(_PORTS_PROP)):
            if key not in seen_out:
                seen_out.add(key)
                output_keys.append(key)

    return {
        "inputs": [{"name": k, "type": "any", "description": None} for k in input_keys],
        "outputs": [{"name": k, "type": "any", "description": None} for k in output_keys],
        "has_input_node": bool(input_nodes),
        "has_output_node": bool(output_nodes),
        # Counts: more than one contract node makes the return value ambiguous
        # (collect_subworkflow_output returns only the first).
        "input_node_count": len(input_nodes),
        "output_node_count": len(output_nodes),
    }


def validate_inputs_mapping(
    inputs_mapping: Dict[str, Any], contract: Dict[str, list]
) -> list[str]:
    """Returns a list of error messages for invalid keys.

    Compares the keys of `inputs_mapping` (which are sub-workflow keys) with
    the keys declared in `contract["inputs"]`. Accepts a partial mapping
    (a sub-workflow may receive a subset).

    Returns an empty list if OK; a list of messages otherwise.
    """
    if not isinstance(inputs_mapping, dict):
        return []
    declared = {p["name"] for p in (contract.get("inputs") or [])}
    if not declared:
        # Sub-workflow with no declared inputs; we have no way to validate
        return []
    errors: list[str] = []
    for key in inputs_mapping.keys():
        if key not in declared:
            errors.append(
                f"Chave '{key}' nao existe no contrato do sub-fluxo. "
                f"Declaradas: {sorted(declared) or '<nenhuma>'}."
            )
    return errors


def collect_subworkflow_references(definition: Dict[str, Any]) -> list[Dict[str, Any]]:
    """Lists SubWorkflow and ForEach nodes in the definition with their workflowHash
    + inputsMapping. Used for cross-workflow validation on save."""
    refs: list[Dict[str, Any]] = []
    nodes = definition.get("nodes") if isinstance(definition, dict) else None
    for n in nodes if isinstance(nodes, list) else []:
        # Tolerant on purpose: the PUT also reads the STORED definition here
        # (`_referencias_alteradas`), and one written by hand or by an older
        # client — a numeric hash, a node that is not an object — made every
        # save of that workflow a 500, the one that would fix it included.
        if not isinstance(n, dict) or n.get("name") not in ("SubWorkflow", "ForEach"):
            continue
        props = n.get("properties")
        props = props if isinstance(props, dict) else {}
        wh = props.get("workflowHash")
        if isinstance(wh, (int, float)) and not isinstance(wh, bool):
            wh = str(wh)
        wh = wh.strip() if isinstance(wh, str) else ""
        mapping = props.get("inputsMapping") or {}
        if not wh:
            continue
        ref = {
            "node_id": n.get("id"),
            "workflow_hash": wh,
            "inputs_mapping": mapping if isinstance(mapping, dict) else {},
        }
        if n.get("name") == "ForEach":
            # Keep the legacy SubWorkflow reference shape unchanged; only
            # ForEach needs the extra contract checks.
            ref.update(
                node_type="ForEach",
                item_key=str(props.get("itemKey") or "item").strip(),
                index_key=str(props.get("indexKey") or "index").strip(),
            )
        refs.append(ref)
    return refs


# Maximum depth of the sub-workflow chain. SubWorkflowNode imports it from here
# so it can EXPLAIN the overflow: without the shared constant, exceeding the limit
# reached the operator as "workflow nao encontrado" (workflow not found).
MAX_DEPTH = 10


async def collect_subworkflow_definitions_recursive(
    root_definition: Dict[str, Any],
    db,
    workspace_id: str | None = None,
    max_depth: int = MAX_DEPTH,
) -> Dict[str, Dict[str, Any]]:
    """Recursively collects the definitions of the referenced sub-workflows.

    Used by the server when building the job envelope: the executor has no access
    to the DB, so it must receive everything in the payload. Includes A -> B -> C -> D
    in a single pass.

    Limits depth to avoid huge payloads in case of an excessive
    chain (default 10 levels — generous for real use).

    Returns {hash: definition_decifrada}. Hashes not found or from
    other workspaces are silently ignored — `validate_subworkflow_
    references_against_db` already blocked those cases on save.
    """
    from sqlalchemy import select
    from app.models.models import Workflow
    from app.core.utils.encryption import decrypt_workflow_connections

    collected: Dict[str, Dict[str, Any]] = {}
    visited: set[str] = set()
    queue: list[tuple[Dict[str, Any], int]] = [(root_definition, 0)]

    while queue:
        definition, depth = queue.pop(0)
        if depth >= max_depth:
            continue

        refs = collect_subworkflow_references(definition)
        new_hashes = [r["workflow_hash"] for r in refs if r["workflow_hash"] not in visited]
        if not new_hashes:
            continue
        visited.update(new_hashes)

        stmt = select(Workflow).where(Workflow.id_hash.in_(new_hashes))
        if workspace_id:
            stmt = stmt.where(Workflow.workspace_id == workspace_id)
        rows = (await db.execute(stmt)).scalars().all()

        for wf in rows:
            if not wf.flag_ative:
                continue
            try:
                decrypted = decrypt_workflow_connections(wf.definition or {})
            except Exception:
                decrypted = wf.definition or {}
            collected[wf.id_hash] = decrypted
            queue.append((decrypted, depth + 1))

    return collected


async def validate_subworkflow_references_against_db(
    definition: Dict[str, Any],
    db,
    workspace_id: str | None = None,
) -> list[str]:
    """Validates that each SubWorkflow in `definition` references a workflow
    that (a) exists, (b) is active, (c) has a contract (SubWorkflowInput +
    SubWorkflowOutput), (d) `inputsMapping` only uses declared keys.

    Returns a list of error messages (empty if OK). Meant to be
    called before saving workflow A (POST/PUT) — blocks the save with
    422 if there are violations.

    Cross-workspace: if `workspace_id` is provided, the query already filters by
    the parent's workspace — a target from another workspace "doesn't exist", just
    like a made-up hash. Distinguishing "exists but belongs to another" from "doesn't
    exist" (or from "is deactivated") would be an oracle of the existence and state
    of other people's workflows for any member (the hash is uuid4, but the caller
    may be a workspace created just to probe).
    """
    refs = collect_subworkflow_references(definition)
    if not refs:
        return []

    from sqlalchemy import select
    from app.models.models import Workflow
    from app.core.utils.encryption import decrypt_workflow_connections

    # Loads ALL targets in a single query (avoids N+1 when workflow A
    # has multiple SubWorkflows).
    hashes = {ref["workflow_hash"] for ref in refs}
    stmt = select(Workflow).where(Workflow.id_hash.in_(hashes))
    if workspace_id:
        stmt = stmt.where(Workflow.workspace_id == workspace_id)
    rows = (await db.execute(stmt)).scalars().all()
    targets_by_hash: dict[str, Workflow] = {w.id_hash: w for w in rows}

    errors: list[str] = []
    for ref in refs:
        wh = ref["workflow_hash"]
        node_id = ref["node_id"]
        target = targets_by_hash.get(wh)
        node_type = ref.get("node_type", "SubWorkflow")

        # The query already filtered by workspace; the second condition is belt and
        # suspenders (a session that returns rows from outside) and reads as
        # nonexistent BEFORE looking at the state, so as not to leak even that.
        if target is None or (workspace_id and target.workspace_id != workspace_id):
            errors.append(
                f"Node {node_type} '{node_id}' aponta para workflow '{wh}' "
                "que nao existe."
            )
            continue
        if not target.flag_ative:
            errors.append(
                f"Node {node_type} '{node_id}' aponta para workflow '{wh}' "
                "que esta desativado."
            )
            continue

        try:
            target_def = decrypt_workflow_connections(target.definition or {})
        except Exception:
            target_def = target.definition or {}

        contract = extract_contract(target_def)

        # Only SubWorkflowOutput is mandatory — it defines the return value.
        # SubWorkflowInput is optional (a sub-workflow may receive nothing) and
        # empty `ports` means passthrough, not absence of a contract.
        if not contract.get("has_output_node"):
            errors.append(
                f"Node {node_type} '{node_id}' aponta para workflow '{wh}' "
                "sem node SubWorkflowOutput: não há saída declarada para consumir."
            )
            continue

        if contract.get("output_node_count", 0) > 1:
            errors.append(
                f"Node {node_type} '{node_id}' aponta para workflow '{wh}' com "
                f"{contract['output_node_count']} nodes SubWorkflowOutput. "
                "Apenas um é permitido."
            )
            continue

        # Parent sends a mapping to a child that has no entry point: the data
        # would be silently ignored at runtime.
        if ref["inputs_mapping"] and not contract.get("has_input_node"):
            errors.append(
                f"Node {node_type} '{node_id}' mapeia entradas para o workflow "
                f"'{wh}', que não tem SubWorkflowInput. Os dados não chegariam "
                "ao sub-fluxo — adicione o node de entrada ou remova o mapeamento."
            )
            continue

        # A repeated child MUST accept the item and its index. A missing
        # SubWorkflowInput (or missing named ports) discards the iteration data.
        if node_type == "ForEach":
            if not contract.get("has_input_node"):
                errors.append(
                    f"Node ForEach '{node_id}' exige SubWorkflowInput em '{wh}'."
                )
                continue
            expected = {ref["item_key"], ref["index_key"]}
            declared = {p["name"] for p in contract.get("inputs") or []}
            if declared and not expected.issubset(declared):
                errors.append(
                    f"Node ForEach '{node_id}' exige portas {sorted(expected)} "
                    f"em SubWorkflowInput de '{wh}'. Declaradas: {sorted(declared)}."
                )
                continue

        # Valida inputsMapping contra contrato
        mapping_errors = validate_inputs_mapping(ref["inputs_mapping"], contract)
        for msg in mapping_errors:
            errors.append(f"Node {node_type} '{node_id}' -> workflow '{wh}': {msg}")

    return errors

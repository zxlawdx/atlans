"""Regressões para ForEach: contratos, limites, iteração e compatibilidade."""
from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from flow.nodes.control.for_each import ForEachNode
from flow.nodes.control.sub_workflow import RESULT_KEY, SubWorkflowNode
from flow.utils.workflow_contract import collect_subworkflow_references


def _child(*, input_ports=None, output_ports=None, with_input=True) -> dict:
    nodes = [
        {
            "id": "out",
            "name": "SubWorkflowOutput",
            "properties": {"ports": output_ports if output_ports is not None else ["item"]},
        },
    ]
    if with_input:
        nodes.insert(0, {
            "id": "in",
            "name": "SubWorkflowInput",
            "properties": {"ports": input_ports if input_ports is not None else ["item", "index"]},
        })
    return {
        "nodes": nodes,
        "edges": (
            [{"source": "in", "target": "out", "from_key": "item", "to_key": "item"}]
            if with_input else []
        ),
    }


def _node(items=None, child=None, **kwargs) -> ForEachNode:
    params = {
        "workflowHash": "FILHO",
        "items": [] if items is None else items,
        "maxIterations": 1000,
        "timeoutSeconds": 30,
        **kwargs,
    }
    node = ForEachNode("foreach-1", params)
    node._task_id = "test-foreach"
    node._workspace_id = "ws-1"
    node._workflow_hash = "RAIZ"
    node.context = {
        "_subflow_ancestors": {"RAIZ"},
        "_subworkflow_definitions": {"FILHO": child or _child()},
        "_disabled_nodes": set(),
    }
    return node


@pytest.mark.asyncio
async def test_executes_real_subworkflow_for_each_item_in_order():
    node = _node(["cuc:a", "codef:b", "copam:c"])
    output = await node.execute({})
    assert output == {
        "results": [{"item": "cuc:a"}, {"item": "codef:b"}, {"item": "copam:c"}],
        "completed_count": 3,
        "failed_count": 0,
        "errors": [],
    }


@pytest.mark.asyncio
async def test_receives_items_from_upstream_without_using_static_list():
    node = _node(["ignorar"])
    output = await node.execute({"items": ["a", "b"]})
    assert [result["item"] for result in output["results"]] == ["a", "b"]


@pytest.mark.asyncio
async def test_accepts_anonymous_single_list_input():
    node = _node()
    output = await node.execute({"result": ["a"]})
    assert output["results"] == [{"item": "a"}]


@pytest.mark.asyncio
async def test_empty_list_does_not_execute_any_child():
    node = _node([])
    with patch.object(SubWorkflowNode, "execute", autospec=True) as mock:
        output = await node.execute({})
    mock.assert_not_called()
    assert output["completed_count"] == 0


@pytest.mark.asyncio
async def test_maximum_is_checked_before_running_any_item():
    node = _node(["a", "b"], maxIterations=1)
    with patch.object(SubWorkflowNode, "execute", autospec=True) as mock:
        with pytest.raises(ValueError, match="maxIterations"):
            await node.execute({})
    mock.assert_not_called()


@pytest.mark.asyncio
async def test_invalid_items_and_conflicting_keys_fail_fast():
    node = _node()
    with pytest.raises(ValueError, match="lista"):
        await node.execute({"items": {"camada": "a"}})
    node = _node(["a"], itemKey="index")
    with pytest.raises(ValueError, match="distintos"):
        await node.execute({})


@pytest.mark.asyncio
async def test_missing_input_contract_is_rejected_before_execution():
    node = _node(["a"], child=_child(with_input=False))
    with pytest.raises(ValueError, match="SubWorkflowInput"):
        await node.execute({})


@pytest.mark.asyncio
async def test_incomplete_input_ports_are_rejected():
    node = _node(["a"], child=_child(input_ports=["item"]))
    with pytest.raises(ValueError, match="portas"):
        await node.execute({})


@pytest.mark.asyncio
async def test_stop_on_error_does_not_attempt_later_items():
    called = []

    async def failing(self, inputs):
        called.append(inputs["item"])
        if inputs["item"] == "b":
            raise RuntimeError("WFS indisponível")
        return {RESULT_KEY: {"item": inputs["item"]}}

    node = _node(["a", "b", "c"], onError="stop")
    with patch.object(SubWorkflowNode, "execute", failing):
        with pytest.raises(RuntimeError, match="item 2/3"):
            await node.execute({})
    assert called == ["a", "b"]


@pytest.mark.asyncio
async def test_continue_on_error_reports_partial_results():
    async def failing(self, inputs):
        if inputs["item"] == "b":
            raise RuntimeError("WFS indisponível")
        return {RESULT_KEY: {"item": inputs["item"]}}

    node = _node(["a", "b", "c"], onError="continue")
    with patch.object(SubWorkflowNode, "execute", failing):
        output = await node.execute({})
    assert output["results"] == [{"item": "a"}, {"item": "c"}]
    assert output["completed_count"] == 2
    assert output["failed_count"] == 1
    assert output["errors"] == [{"index": 1, "error": "WFS indisponível"}]


@pytest.mark.asyncio
async def test_cancellation_propagates_without_being_swallowed():
    async def cancelled(self, inputs):
        raise asyncio.CancelledError()

    node = _node(["a"], onError="continue")
    with patch.object(SubWorkflowNode, "execute", cancelled):
        with pytest.raises(asyncio.CancelledError):
            await node.execute({})


def test_for_each_is_included_in_server_subworkflow_resolution():
    definition = {"nodes": [
        {"id": "normal", "name": "SubWorkflow",
         "properties": {"workflowHash": "SUB", "inputsMapping": {"a": "b"}}},
        {"id": "iter", "name": "ForEach",
         "properties": {"workflowHash": "FILHO", "itemKey": "camada", "indexKey": "pos"}},
    ]}
    refs = collect_subworkflow_references(definition)
    assert refs[0] == {
        "node_id": "normal",
        "workflow_hash": "SUB",
        "inputs_mapping": {"a": "b"},
    }
    assert refs[1]["workflow_hash"] == "FILHO"
    assert refs[1]["node_type"] == "ForEach"
    assert (refs[1]["item_key"], refs[1]["index_key"]) == ("camada", "pos")

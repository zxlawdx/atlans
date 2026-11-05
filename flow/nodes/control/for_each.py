"""ForEach — repete um sub-workflow para cada item de uma lista, sem ciclos no DAG.

A execução é sequencial e reutiliza SubWorkflowNode: autorização, contrato,
timeouts, cancelamento e publicação de eventos continuam sendo responsabilidade
do mesmo mecanismo utilizado por workflows existentes.
"""
from __future__ import annotations

from typing import Any, Dict

from flow.nodes.base import BaseNode
from flow.nodes.control.sub_workflow import SubWorkflowNode, RESULT_KEY
from flow.registry import register_node
from flow.utils.workflow_contract import extract_contract
from flow.utils.publisher.events import publish_stdout
from flow.utils.logger import get_logger

logger = get_logger(__name__)


@register_node
class ForEachNode(BaseNode):
    """Executa uma definição de sub-workflow uma vez para cada item."""

    # O sub-workflow pode gravar no banco, enviar mensagens ou criar arquivos.
    # O executor não deve repetir o ForEach inteiro por um erro transitório.
    TEM_EFEITO_COLATERAL = True

    @classmethod
    def description(cls) -> Dict[str, Any]:
        return {
            "name": "ForEach",
            "alias": "Para cada item (ForEach)",
            "type": "control",
            "description": (
                "Executa sequencialmente um sub-workflow para cada elemento de uma lista. "
                "O sub-workflow precisa ter SubWorkflowInput e SubWorkflowOutput. "
                "Use o nome completo da camada WFS como item, por exemplo "
                "'cuc:limites_ucs_estaduais'."
            ),
            "inputs": [
                {
                    "name": "items",
                    "type": "list",
                    "description": "Lista a iterar (opcional se preenchida em 'Itens').",
                },
            ],
            "properties": [
                {
                    "name": "workflowHash",
                    "label": "ID do sub-workflow",
                    "type": "string",
                    "default": "",
                    "required": True,
                    "description": "id_hash do workflow que será executado a cada item.",
                },
                {
                    "name": "items",
                    "label": "Itens (JSON)",
                    "type": "object",
                    "default": [],
                    "description": (
                        "Lista JSON, ex.: [\"cuc:limites_ucs_estaduais\", "
                        "\"cuc:zona_amortecimento_uc_estadual\"]. "
                        "Ignorada quando uma lista chega pela entrada 'items'."
                    ),
                },
                {
                    "name": "itemKey",
                    "label": "Chave do item no sub-workflow",
                    "type": "string",
                    "default": "item",
                    "description": "Nome da saída do SubWorkflowInput que receberá o item.",
                },
                {
                    "name": "indexKey",
                    "label": "Chave do índice no sub-workflow",
                    "type": "string",
                    "default": "index",
                    "description": "Nome da saída do SubWorkflowInput que receberá o índice (inicia em 0).",
                },
                {
                    "name": "maxIterations",
                    "label": "Máximo de iterações",
                    "type": "integer",
                    "default": 1000,
                    "description": "Limite de segurança. Se a lista exceder este valor, nada é executado.",
                },
                {
                    "name": "timeoutSeconds",
                    "label": "Tempo limite por item (s)",
                    "type": "integer",
                    "default": 300,
                    "description": "Tempo máximo de cada execução do sub-workflow.",
                },
                {
                    "name": "onError",
                    "label": "Se um item falhar",
                    "type": "select",
                    "default": "stop",
                    "options": [
                        {"value": "stop", "label": "Parar com erro"},
                        {"value": "continue", "label": "Registrar erro e continuar"},
                    ],
                    "description": (
                        "Por padrão aborta no primeiro erro. 'Continuar' permite resultados "
                        "parciais, identificados na saída errors."
                    ),
                },
            ],
            "outputs": [
                {
                    "name": "results",
                    "type": "list",
                    "description": "Saídas públicas dos sub-workflows bem-sucedidos, na ordem dos itens.",
                    "port": True,
                },
                {
                    "name": "completed_count",
                    "type": "number",
                    "description": "Quantidade de itens processados com sucesso.",
                    "port": True,
                },
                {
                    "name": "failed_count",
                    "type": "number",
                    "description": "Quantidade de itens que falharam.",
                    "port": True,
                },
                {
                    "name": "errors",
                    "type": "list",
                    "description": "Erros por índice, se onError for continue.",
                    "port": True,
                },
            ],
        }

    @staticmethod
    def _get_items(inputs: Dict[str, Any], configured: Any) -> tuple[list, dict]:
        """Aceita porta 'items', única entrada anônima ou lista JSON configurada."""
        if "items" in inputs:
            raw = inputs["items"]
            shared = {key: value for key, value in inputs.items() if key != "items"}
        elif len(inputs) == 1 and isinstance(next(iter(inputs.values())), list):
            # Uma única conexão anônima pode fornecer a lista.
            raw = next(iter(inputs.values()))
            shared = {}
        elif isinstance(configured, list) and configured:
            # Um ScheduleTrigger pode ligar-se ao ForEach só para iniciar a
            # execução; nesse caso seus metadados não substituem a lista
            # JSON configurada manualmente no nó.
            raw, shared = configured, {}
        elif len(inputs) == 1:
            raw = next(iter(inputs.values()))
            shared = {}
        elif inputs:
            raise ValueError(
                "ForEach recebeu múltiplas entradas sem a porta 'items'. "
                "Conecte a lista à entrada 'items'."
            )
        else:
            raw, shared = configured, {}
        if not isinstance(raw, list):
            raise ValueError(
                f"ForEach exige uma lista JSON em 'items'; recebido: {type(raw).__name__}."
            )
        return raw, shared

    async def execute(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        self.validate()

        workflow_hash = self.get_param("workflowHash", "").strip()
        item_key = self.get_param("itemKey", "item").strip()
        index_key = self.get_param("indexKey", "index").strip()
        max_iterations = self.get_param_int("maxIterations", 1000)
        timeout = self.get_param_int("timeoutSeconds", 300)
        on_error = self.get_param("onError", "stop")
        items, shared = self._get_items(inputs or {}, self.get_param("items", []))

        if max_iterations <= 0 or timeout <= 0:
            raise ValueError("maxIterations e timeoutSeconds devem ser positivos.")
        if len(items) > max_iterations:
            raise ValueError(
                f"ForEach recebeu {len(items)} itens, acima de maxIterations={max_iterations}. "
                "Nenhum sub-workflow foi executado."
            )
        if (
            not item_key or not index_key
            or item_key == index_key
            or item_key.startswith("__") or index_key.startswith("__")
            or item_key in shared or index_key in shared
        ):
            raise ValueError(
                "itemKey e indexKey devem ser nomes distintos, não reservados "
                "e sem conflito com outras entradas do ForEach."
            )

        # Reutiliza o nó original. Não cria outro executor, outro mecanismo de
        # credenciais, nem modifica a regra de grafos acíclicos do Atlans.
        caller = SubWorkflowNode(
            node_id=self.node_id,
            parameters={
                "workflowHash": workflow_hash,
                "inputsMapping": {},
                "timeoutSeconds": timeout,
            },
        )
        caller.context = self.context
        caller._publisher = self._publisher
        caller._task_id = self._task_id
        caller._workspace_id = self._workspace_id
        caller._workflow_hash = self._workflow_hash
        caller._debug_mode = self._debug_mode

        # Pré-validação antes de qualquer efeito colateral — inclusive em
        # onError=continue, erros estruturais não podem virar 'sucesso parcial'.
        caller._check_loop(workflow_hash)
        definition = caller._fetch_definition_from_snapshot(workflow_hash)
        caller._check_disabled_nodes_in_definition(workflow_hash, definition)
        caller._check_subworkflow_contract(workflow_hash, definition)
        contract = extract_contract(definition)
        if not contract["has_input_node"]:
            raise ValueError(
                f"Sub-workflow '{workflow_hash}' precisa de SubWorkflowInput "
                "para receber os itens do ForEach."
            )
        declared = {port["name"] for port in contract["inputs"]}
        if declared and not {item_key, index_key}.issubset(declared):
            raise ValueError(
                f"SubWorkflowInput de '{workflow_hash}' deve declarar as portas "
                f"'{item_key}' e '{index_key}' (ou deixar ports vazio). "
                f"Portas encontradas: {sorted(declared)}."
            )

        results: list[dict] = []
        errors: list[dict] = []
        for index, item in enumerate(items):
            logger.info("ForEach %s: item %d/%d", self.node_id, index + 1, len(items))
            publish_stdout(
                self._publisher, self._task_id, self.node_id,
                [f"ForEach: processando item {index + 1}/{len(items)}"],
            )
            try:
                public = await caller.execute({
                    **shared, item_key: item, index_key: index,
                })
                results.append(public[RESULT_KEY])
            except Exception as exc:
                if on_error == "stop":
                    raise RuntimeError(
                        f"ForEach falhou no item {index + 1}/{len(items)}: {exc}"
                    ) from exc
                logger.warning(
                    "ForEach %s: falha no item %d/%d: %s",
                    self.node_id, index + 1, len(items), exc,
                )
                errors.append({"index": index, "error": str(exc)})

        return {
            "results": results,
            "completed_count": len(results),
            "failed_count": len(errors),
            "errors": errors,
        }

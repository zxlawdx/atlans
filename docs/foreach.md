# ForEach — repetição de sub-workflow por item

Esta funcionalidade foi adicionada ao fork, de forma **aditiva**: não altera o
`Loop` (que particiona GeoDataFrames), o agendador ou a semântica de DAG.
O novo nó `ForEach`, na categoria **Controle**, reutiliza a execução do
`SubWorkflow` existente e os contratos `SubWorkflowInput` /
`SubWorkflowOutput`.

## Exemplo: 13 camadas WFS da SEDAM

Crie **dois workflows** na interface. O sub-workflow é configurado **uma única
vez** e recebe o identificador da camada em cada iteração.

### 1. Workflow filho — "Extrair uma camada"

1. Adicione `SubWorkflowInput` com **ports** `["item", "index"]`.
2. Conecte a saída `item` ao nó **WFS**. Configure a URL base do GeoServer.
   No campo **Camada (typeName)**, utilize:
   `{{ SubWorkflowInput.item }}`.
   Se você renomear o alias do nó de entrada, use esse alias na expressão.
3. Configure `maxFeatures` no WFS de acordo com o limite de feições por
   camada. A paginação interna do WFS permanece automática.
4. Faça o tratamento em `PythonScript` ou outros nós, conforme a necessidade.
5. Conecte os dados que serão devolvidos a `SubWorkflowOutput`.
   Exemplo: ports `["camada"]`, com a saída tratada ligada à porta `camada`.
   **A saída é obrigatória** para o chamador consumir o resultado.
6. Salve e copie o **id_hash** deste workflow.

Para grandes camadas, prefira gravar/exportar cada item dentro do filho,
com nomes únicos, e devolver apenas metadados ao ForEach. A saída `results`
fica em memória ao longo das iterações; acumular 13 GeoDataFrames enormes
pode esgotar a RAM.

### 2. Workflow principal — "Extrair 13 camadas"

Adicione o nó **ForEach** e preencha:

- **ID do sub-workflow:** id_hash copiado no passo anterior
- **Itens (JSON):** uma lista de nomes WFS, por exemplo:
  `["cuc:limites_ucs_estaduais", "cuc:zona_amortecimento_uc_estadual", "codef:vw_areas_codef_asv"]`
- **Chave do item:** `item`
- **Chave do índice:** `index`
- **Máximo de iterações:** 13 (ou mais, se necessário)
- **Tempo limite por item:** 300 s (ajuste ao servidor WFS)
- **Se um item falhar:** `stop` inicialmente, para não mascarar erro de consulta

Se preferir, um `PythonScript` anterior pode gerar uma lista chamada `items`,
que terá prioridade sobre os itens escritos no campo JSON do ForEach.

`ForEach` executa o filho sequencialmente: índice 0, 1, 2 e assim por diante.
Não é preciso conectar uma seta de volta ao começo do grafo.

### Saídas

- `results`: lista ordenada das saídas públicas das iterações que tiveram
  sucesso (os nomes são os declarados em `SubWorkflowOutput`)
- `completed_count`: total de execuções bem-sucedidas
- `failed_count`: total de execuções que falharam
- `errors`: lista de objetos `{"index": N, "error": "..."}`

No modo `continue`, os resultados são **parciais**; não assuma que
`results[i]` corresponde ao item de índice `i` se houver falha. A lista
`errors` identifica as posições perdidas.

### Cuidados

- O novo componente **não** remove o limite de `maxFeatures` de cada WFS;
  ele resolve a repetição entre **camadas**, não a paginação interna.
- Se uma camada devolver HTTP 400, verifique o nome real do `typeName` no
  `GetCapabilities` do GeoServer. O ForEach não corrige camadas inexistentes.
- O sub-workflow deve estar **ativo e no mesmo workspace**. `ForEach` exige
  `SubWorkflowInput` com as chaves `item` e `index`, ou portas vazias (passthrough).
- Cancelamentos propagam ao executor; o `maxIterations` impede listas
  excessivamente grandes; o `timeoutSeconds` limita **cada** sub-workflow.
- Operações de gravação podem ter efeitos parciais antes de uma falha.
  Use identificadores/nomes únicos ou operações idempotentes por item.
- O ForEach é marcado como tendo efeitos colaterais para impedir que um
  retry transitório repita todas as iterações já concluídas.

## Testes de desenvolvimento

Depois de clonar a branch do fork, configure um ambiente Python com as
dependências do projeto e rode:

```bash
python -m pytest tests/unit/test_for_each.py -q
python -m pytest tests/unit/test_sub_workflow.py tests/unit/test_sub_workflow_diagnostico.py -q
```

Para subir o Atlans completo, siga o README original: Docker Compose, make,
OpenSSL e PostgreSQL/PostGIS acessível. Na primeira instalação:

```bash
make bootstrap
make up-dev
```

Depois, abra http://localhost:3000 e confira o nó `ForEach` na categoria
de controle. O servidor/worker devem usar **a mesma branch** do código.
O Atlans original não é alterado.

## Arquivos da implementação

- `flow/nodes/control/for_each.py`: nó novo, composição com `SubWorkflowNode`
- `flow/utils/workflow_contract.py`: identifica e valida referências de
  sub-workflow em nós ForEach, para o servidor enviar a definição ao executor
- `tests/unit/test_for_each.py`: testes de comportamento e regressão

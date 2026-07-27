# Lições — Workshop Agentes Databricks

- Workshop começou como 2h30 e virou **dia inteiro** (~3h manhã + ~3h tarde). Sempre
  confirmar duração/formato antes de dimensionar a agenda.
- Domínio definido pelo usuário: **saúde** (beneficiários, hospitais, planos, coberturas,
  sinistros). Ancorar todos os notebooks nesse dicionário de dados único (`dados/README.md`).
- Material 100% em português; identificadores técnicos em inglês (padrão do perfil global).
- **Ambiente compartilhado**: isolar por usuário via `current_user()` → schema
  `saude_<slug>`. Pontos que causariam conflito e foram tratados:
  - schema/tabelas/volume/UC Functions/índice VS/modelo/deploy → todos dentro do schema do usuário;
  - `%%writefile agente_saude.py` (nome fixo no driver) → trocado por geração de arquivo
    por usuário com módulo único e schema injetado, gravado num **volume persistente do UC**
    (`/Volumes/<cat>/saude_<slug>/assets/`) em vez de `/tmp` (que some no restart do cluster
    e não é compartilhado entre sessões). nb 04 gera; nb 06 e 07 importam de lá (sem duplicar);
  - experimento MLflow: o **nome-folha** deve incluir o `USER_SLUG` (ex.:
    `workshop_agentes_avaliacao_<slug>`), não só a pasta `/Users/<usuario>/`. Motivo: se
    rodar como job/service principal, `current_user()` é igual para todos e colapsaria no
    mesmo experimento. Ancorar em `/Users/<usuario>/` com fallback para `/Shared/...`;
  - após `dbutils.library.restartPython()` é preciso RECALCULAR o slug (estado é perdido);
  - catálogo e endpoint de Vector Search permanecem compartilhados (caros, multiusuário seguro).
- **UC trace storage DESCARTADO do workshop** (nb 06): exige MLflow >= 3.9 + preview
  "OpenTelemetry on Databricks" + região us-east-1/us-west-2. Quando ligado sem atender os
  pré-requisitos, `set_experiment_trace_location` "aceita" mas os traces somem silenciosamente
  (search_traces retorna 0). Pré-requisitos demais para uma sala compartilhada — usar só o
  tracing normal do MLflow, que funciona em qualquer ambiente. NÃO reintroduzir.
- **Traces do evaluate**: para gerar 1 trace por linha, o autolog (`mlflow.langchain.autolog()`)
  precisa estar ativo NA SESSÃO do notebook (não só dentro do módulo do agente) e o
  `predict_fn` decorado com `@mlflow.trace`. Definir experimento ANTES da 1ª predição.
- **API MLflow correta**: `search_traces(locations=[...])` (não `experiment_ids=`, deprecado).
- **Versão com exercícios** (pedido do usuário): `notebooks/` tem lacunas `🧩 EXERCÍCIO`
  com prompt sugerido para o **Databricks Assistant** (ícone ✨ da célula); `respostas/`
  guarda o gabarito completo e funcional. Exercícios ficam nos pontos conceitualmente
  centrais (regras de negócio, UC Function, Genie/KA API, roteador MAS, scorer Guidelines),
  NÃO na infraestrutura (setup/isolamento/deploy) para não travar a turma. Ao editar a
  lógica, atualizar OS DOIS: a lacuna em notebooks/ e o gabarito em respostas/.
- **Formato de payload dos endpoints**: Knowledge Assistant, ResponsesAgent e MAS usam
  a chave `"input"` (formato Responses), NÃO `"messages"` (formato chat). Só os endpoints
  de Foundation Model/LLM usam `"messages"`. Ao chamar endpoints de agente, usar `"input"`
  (com fallback para `"messages"` quando o tipo é desconhecido). Corrigido em nb 03 e nb 05.
- **Attachments do Genie**: o primeiro attachment pode ser uma query SQL cujo `.text` é
  None — nunca acessar `attachments[0].text.content` direto; iterar procurando o texto.
- **`try/except Exception` genérico mascara bugs**: no nb 04 ele engoliu um NameError e
  imprimiu "já existe". Preferir checagem explícita de existência (list_endpoints/list_indexes).
- **Cada notebook tem sessão Python própria**: instalar as libs (`%pip install ... ;
  restartPython()`) no TOPO de cada notebook que as usa, antes do bloco de identidade.
- **Células autossuficientes**: recalcular identidade/imports em células que podem ser
  rodadas isoladamente (participantes pulam células). Ex.: `Row`, `N_BENEFICIARIOS`, VS_*.

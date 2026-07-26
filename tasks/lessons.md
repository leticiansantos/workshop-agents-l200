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
  - experimento MLflow `/Shared/...` → trocado por `/Users/<usuario>/...`;
  - após `dbutils.library.restartPython()` é preciso RECALCULAR o slug (estado é perdido);
  - catálogo e endpoint de Vector Search permanecem compartilhados (caros, multiusuário seguro).
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

# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Multi-Agent Supervisor (MAS)
# MAGIC
# MAGIC Um **supervisor** roteia a pergunta do usuário para o agente certo:
# MAGIC - **Genie** (`Sinistros de Saúde`) → perguntas quantitativas sobre dados.
# MAGIC - **Knowledge Assistant** (`Assistente de Coberturas`) → regras/cobertura/carência.
# MAGIC - **Agente de código** (`agente_saude`) → atendimento com tools + RAG.
# MAGIC
# MAGIC Mostramos **duas formas**: (A) MAS no-code do Agent Bricks e (B) supervisor com código.
# MAGIC
# MAGIC > O supervisor é o "cérebro" que decide quem responde o quê.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Bibliotecas
# MAGIC Cada notebook roda numa sessão própria. Instalamos MLflow + LangChain (usado pelo
# MAGIC `mlflow.langchain.autolog()`) e o SDK.

# COMMAND ----------

# MAGIC %pip install -U -qqq "mlflow[databricks]>=3.1" databricks-langchain langchain-core databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# ── Isolamento por usuário (ambiente compartilhado) ────────────────────────────
import re

dbutils.widgets.text("catalogo", "workshop_dev", "Catálogo (compartilhado)")
CATALOGO = dbutils.widgets.get("catalogo")
_usuario = spark.sql("SELECT current_user()").collect()[0][0]
USER_SLUG = re.sub(r"[^a-z0-9]+", "_", _usuario.split("@")[0].lower()).strip("_")
SCHEMA = f"agentes_saude_{USER_SLUG}"
LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"
print(f"Cada participante usa seus próprios agentes (Genie/KA/endpoint) — schema {SCHEMA}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Opção A · MAS no-code (Agent Bricks)
# MAGIC
# MAGIC ⚠️ **Ambiente compartilhado**: nomeie seu MAS com seu usuário e adicione os **seus**
# MAGIC agentes (o Genie, o KA e o endpoint que você mesmo criou nos notebooks anteriores).
# MAGIC
# MAGIC No menu: **Agents → Multi-Agent Supervisor → Create**.
# MAGIC
# MAGIC 1. **Nome**: `Atendimento Saúde — <seu_usuario>`
# MAGIC 2. **Adicione os agentes** (cada um com uma boa descrição de quando usar):
# MAGIC    - **Genie Space** `Sinistros de Saúde` — "Perguntas quantitativas: contagens,
# MAGIC      valores, taxas de negativa, rankings de hospitais/planos."
# MAGIC    - **Knowledge Assistant** `Assistente de Coberturas` — "Regras de cobertura,
# MAGIC      carência, reembolso, exclusões, rol de procedimentos."
# MAGIC    - **Serving endpoint** `agente_saude` — "Consultas de sinistros de um
# MAGIC      beneficiário específico e dúvidas combinando dados + documentos."
# MAGIC 3. **Instruções do supervisor**:
# MAGIC    ```
# MAGIC    Você coordena o atendimento de uma operadora de saúde. Escolha o agente mais
# MAGIC    adequado. Para números/estatísticas use o Genie; para regras use o Assistente
# MAGIC    de Coberturas; para dados de um beneficiário use o agente_saude. Sintetize a
# MAGIC    resposta final em português, de forma clara.
# MAGIC    ```
# MAGIC 4. Teste no playground e depois **deploy** — vira um serving endpoint próprio.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Opção B · Supervisor com código
# MAGIC Quando você precisa de lógica de roteamento custom, monta o supervisor no código.
# MAGIC Abaixo, um roteador simples baseado em LLM que decide entre Genie, KA e o agente.

# COMMAND ----------

dbutils.widgets.text("genie_space_id", "", "Genie Space ID")
dbutils.widgets.text("ka_endpoint", "", "Endpoint do KA")
dbutils.widgets.text("agente_endpoint", "", "Endpoint do agente_saude")

GENIE_SPACE_ID = dbutils.widgets.get("genie_space_id")
KA_ENDPOINT = dbutils.widgets.get("ka_endpoint")
AGENTE_ENDPOINT = dbutils.widgets.get("agente_endpoint")

# COMMAND ----------

import mlflow
from mlflow.deployments import get_deploy_client
from databricks.sdk import WorkspaceClient

mlflow.langchain.autolog()
client = get_deploy_client("databricks")
w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧩 EXERCÍCIO — Roteador do supervisor
# MAGIC O supervisor precisa decidir para qual agente enviar a pergunta. Complete o **prompt
# MAGIC de roteamento** e a **função `rotear`** que consulta o LLM.
# MAGIC
# MAGIC **Como preencher (use o Genie — ícone de lâmpada 💡 na célula):**
# MAGIC > "Escreva um prompt que classifique a pergunta do usuário em UMA de três categorias:
# MAGIC > GENIE (números, estatísticas, contagens, taxas, rankings), COBERTURA (regras,
# MAGIC > carência, reembolso, exclusões) ou BENEFICIARIO (histórico de um beneficiário
# MAGIC > específico, id BFxxxxxx). O modelo deve responder só com a palavra da categoria.
# MAGIC > Depois complete `rotear`: chame `client.predict(endpoint=LLM_ENDPOINT, inputs={...})`
# MAGIC > com `messages` (formato chat), `max_tokens=5` e `temperature=0`, e retorne o
# MAGIC > conteúdo em maiúsculas."
# MAGIC
# MAGIC 💡 `temperature=0` deixa a classificação determinística — ideal para roteamento.

# COMMAND ----------

ROTEADOR_PROMPT = ""  # 🧩 TODO: escreva o prompt de classificação (use {pergunta} como placeholder)

@mlflow.trace(name="rotear")
def rotear(pergunta: str) -> str:
    # 🧩 TODO: chame o LLM com ROTEADOR_PROMPT e retorne a categoria em maiúsculas.
    raise NotImplementedError("Complete a função rotear (veja o exercício acima).")

@mlflow.trace(name="chamar_genie")
def chamar_genie(pergunta):
    conv = w.genie.start_conversation_and_wait(GENIE_SPACE_ID, pergunta)
    # O primeiro attachment pode ser SQL (text=None); procuramos o texto entre todos.
    for att in (conv.attachments or []):
        if att.text and att.text.content:
            return att.text.content
    return "(sem resposta)"

@mlflow.trace(name="chamar_endpoint")
def chamar_endpoint(endpoint, pergunta):
    # KA e agente de código são ResponsesAgent → payload usa "input".
    # Tentamos o formato Responses e caímos para chat se o endpoint exigir "messages".
    msg = {"role": "user", "content": pergunta}
    try:
        r = client.predict(endpoint=endpoint, inputs={"input": [msg]})
    except Exception:
        r = client.predict(endpoint=endpoint, inputs={"messages": [msg]})
    # Cobre formatos chat e responses.
    if "choices" in r:
        return r["choices"][0]["message"]["content"]
    if "output" in r:
        return str(r["output"])
    return str(r)

@mlflow.trace(name="supervisor", span_type="AGENT")
def supervisor(pergunta: str) -> str:
    destino = rotear(pergunta)
    if "GENIE" in destino:
        return f"[Genie] {chamar_genie(pergunta)}"
    if "COBERTURA" in destino:
        return f"[Coberturas] {chamar_endpoint(KA_ENDPOINT, pergunta)}"
    return f"[Agente] {chamar_endpoint(AGENTE_ENDPOINT, pergunta)}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testar o supervisor com código

# COMMAND ----------

perguntas = [
    "Qual a taxa de negativa de sinistros por plano?",          # → GENIE
    "Qual a carência para cirurgias?",                           # → COBERTURA
    "Mostre o histórico de sinistros do beneficiário BF000001",  # → BENEFICIARIO
]
for p in perguntas:
    print(f"\n❓ {p}")
    print(supervisor(p))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discussão
# MAGIC - **No-code (MAS)**: rápido, bom para roteamento por descrição dos agentes.
# MAGIC - **Código**: controle total do roteamento, pré/pós-processamento e políticas.
# MAGIC - Repare que o `mlflow.trace` já capturou toda a árvore de chamadas — base para o
# MAGIC   próximo notebook de **avaliação e observabilidade**.
# MAGIC
# MAGIC Próximo: **`06_avaliacao`**.

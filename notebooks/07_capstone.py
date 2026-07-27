# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Capstone — Agente de atendimento ao beneficiário
# MAGIC
# MAGIC Hora de juntar tudo. O desafio: construir (ou estender) um **agente de atendimento**
# MAGIC que um beneficiário poderia usar num chat. Ele deve:
# MAGIC - Explicar **por que** um sinistro foi negado (dados + regra).
# MAGIC - Informar **carências** e regras de cobertura (documentos).
# MAGIC - Responder perguntas quantitativas se necessário (Genie).
# MAGIC
# MAGIC Este notebook é um **roteiro guiado** com TODOs. Trabalhe em duplas.
# MAGIC
# MAGIC > Não há uma única resposta certa — o objetivo é exercitar os padrões do dia.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Bibliotecas
# MAGIC Sessão própria — instalamos as libs usadas pelo agente.

# COMMAND ----------

# MAGIC %pip install -U -qqq databricks-langchain databricks-vectorsearch langchain-core "mlflow[databricks]>=3.1"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cenário
# MAGIC A beneficiária Ana teve uma ressonância negada e quer entender o motivo, além de
# MAGIC saber quando poderá refazer o pedido. O agente deve:
# MAGIC 1. Buscar os sinistros dela (UC Function `sinistros_do_beneficiario`).
# MAGIC 2. Identificar a negativa e o motivo.
# MAGIC 3. Explicar a **regra de carência** relacionada (busca em documentos).
# MAGIC 4. Dar uma resposta empática e acionável.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Desafio 1 — Reaproveite o agente do notebook 04
# MAGIC O `agente_saude` já tem as tools necessárias. Teste-o no cenário.

# COMMAND ----------

import os, re, sys, importlib
import mlflow
from mlflow.types.responses import ResponsesAgentRequest

# Importa o SEU agente (gerado no notebook 04, no volume persistente do UC).
# ⚠️ Use o MESMO catálogo do notebook 04 (o widget deve bater).
dbutils.widgets.text("catalogo", "workshop_dev", "Catálogo (compartilhado)")
_usuario = spark.sql("SELECT current_user()").collect()[0][0]
USER_SLUG = re.sub(r"[^a-z0-9]+", "_", _usuario.split("@")[0].lower()).strip("_")
CATALOGO = dbutils.widgets.get("catalogo")
SCHEMA = f"agentes_saude_{USER_SLUG}"
AGENT_DIR = f"/Volumes/{CATALOGO}/{SCHEMA}/assets"
AGENT_MODULE = f"agente_saude_{USER_SLUG}"
AGENT_FILE = f"{AGENT_DIR}/{AGENT_MODULE}.py"

assert os.path.exists(AGENT_FILE), (
    f"Arquivo do agente não encontrado em {AGENT_FILE}. "
    "Rode o notebook 04 (seção 4) primeiro para gerar o agente."
)

if AGENT_DIR not in sys.path:
    sys.path.insert(0, AGENT_DIR)
AGENT = importlib.import_module(AGENT_MODULE).AGENT

pergunta = ("Sou o beneficiário BF000002. Uma ressonância minha foi negada. "
            "Por que isso aconteceu e quando posso tentar de novo?")
resp = AGENT.predict(ResponsesAgentRequest(input=[{"role": "user", "content": pergunta}]))
print(resp.output[-1].content)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Desafio 2 — Adicione uma nova ferramenta (TODO)
# MAGIC Crie uma UC Function `simular_reembolso(valor_pago DOUBLE, procedimento STRING)` que
# MAGIC retorna quanto seria reembolsado (mínimo entre valor pago e valor de referência) e
# MAGIC registre-a como tool do agente.
# MAGIC
# MAGIC Crie a função no **seu** schema `workshop_dev.agentes_saude_<seu_usuario>` (use o
# MAGIC `CATALOGO`/`SCHEMA` calculados acima numa f-string):
# MAGIC
# MAGIC ```python
# MAGIC # TODO: complete a função (execute via spark.sql com f-string)
# MAGIC spark.sql(f"""
# MAGIC CREATE OR REPLACE FUNCTION {CATALOGO}.{SCHEMA}.simular_reembolso(
# MAGIC     valor_pago DOUBLE, p_procedimento STRING)
# MAGIC RETURNS TABLE(...)
# MAGIC COMMENT 'Simula o reembolso: mínimo entre valor pago e valor de referência.'
# MAGIC RETURN
# MAGIC   SELECT ...
# MAGIC """)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Desafio 3 — Melhore o system prompt (TODO)
# MAGIC Edite `agente_saude.py` para o agente:
# MAGIC - Sempre confirmar o id do beneficiário antes de citar dados.
# MAGIC - Terminar com um próximo passo claro ("você pode reabrir a solicitação em ...").
# MAGIC
# MAGIC Depois, **re-avalie** com o notebook 06 e compare os scores antes/depois.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Desafio 4 (bônus) — Coloque uma interface
# MAGIC Use uma **Databricks App** (Streamlit/Gradio) ou o **Review App** gerado pelo deploy
# MAGIC para conversar com o agente. Compartilhe com um colega para coletar feedback.
# MAGIC
# MAGIC Dica: o endpoint do agente aceita mensagens no formato ResponsesAgent; use o
# MAGIC `mlflow.deployments` client como nos notebooks anteriores.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Critérios de sucesso
# MAGIC - [ ] O agente explica a negativa usando **dados reais** do beneficiário.
# MAGIC - [ ] Cita a **regra de carência** correta (documento).
# MAGIC - [ ] Resposta em português, cordial, com próximo passo.
# MAGIC - [ ] Nova ferramenta de reembolso funcionando.
# MAGIC - [ ] Avaliação re-rodada mostrando melhora.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Encerramento
# MAGIC Você percorreu o ciclo completo de agentes no Databricks:
# MAGIC
# MAGIC | Capability | Notebook | Quando usar |
# MAGIC |-----------|----------|-------------|
# MAGIC | Genie Space | 02 | Self-service de dados (NL→SQL) |
# MAGIC | Knowledge Assistant | 03 | Q&A sobre documentos (RAG no-code) |
# MAGIC | Agente com código | 04 | Controle total, tools, RAG custom |
# MAGIC | Multi-Agent Supervisor | 05 | Orquestrar vários agentes |
# MAGIC | Avaliação + governança | 06 | Qualidade e produção |
# MAGIC
# MAGIC **Próximos passos**: leve para seus próprios dados, adicione mais tools, conecte a
# MAGIC uma Databricks App e configure monitors de produção. 🎉

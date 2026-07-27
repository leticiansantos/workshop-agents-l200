# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Agent Bricks — Knowledge Assistant (RAG sobre coberturas)
# MAGIC
# MAGIC O **Knowledge Assistant (KA)** é um agente de perguntas e respostas sobre seus
# MAGIC **documentos**. Ele cuida de ingestão, chunking, embeddings, indexação e citação
# MAGIC das fontes — sem você montar o pipeline de RAG na mão.
# MAGIC
# MAGIC Vamos criar um KA sobre os documentos de **cobertura, carência e reembolso** que
# MAGIC geramos no notebook 01.
# MAGIC
# MAGIC > Use o KA quando o conhecimento está em texto (manuais, políticas, PDFs).

# COMMAND ----------

# ── Isolamento por usuário (ambiente compartilhado) ────────────────────────────
import re

dbutils.widgets.text("catalogo", "workshop_dev", "Catálogo (compartilhado)")
CATALOGO = dbutils.widgets.get("catalogo")
_usuario = spark.sql("SELECT current_user()").collect()[0][0]
USER_SLUG = re.sub(r"[^a-z0-9]+", "_", _usuario.split("@")[0].lower()).strip("_")
SCHEMA = f"agentes_saude_{USER_SLUG}"
VOL_PATH = f"/Volumes/{CATALOGO}/{SCHEMA}/documentos"

print(f"Seu volume de documentos: {VOL_PATH}")
print(f"Nome sugerido do KA.....: Assistente de Coberturas — {USER_SLUG}")
display(dbutils.fs.ls(VOL_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Criar o Knowledge Assistant (UI)
# MAGIC
# MAGIC ⚠️ **Ambiente compartilhado**: use um nome único (com seu usuário) e aponte para o
# MAGIC **seu** volume (impresso na célula acima).
# MAGIC
# MAGIC No menu lateral: **Agents → Knowledge Assistant → Create**.
# MAGIC
# MAGIC 1. **Nome**: `Assistente de Coberturas — <seu_usuario>`
# MAGIC 2. **Descrição** (importante — o MAS usa isso para roteamento):
# MAGIC    ```
# MAGIC    Responde dúvidas sobre regras de cobertura, carência, reembolso, exclusões e
# MAGIC    rol de procedimentos do plano de saúde, com base nos manuais e políticas oficiais.
# MAGIC    ```
# MAGIC 3. **Fonte de conhecimento**: aponte para o **seu** volume
# MAGIC    `/Volumes/workshop_dev/agentes_saude_<seu_usuario>/documentos` (ou faça upload dos `.md`).
# MAGIC 4. Aguarde a indexação (ingestão + embeddings automáticos).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Instruções do assistente
# MAGIC No campo de instruções do KA, oriente o comportamento:
# MAGIC
# MAGIC ```
# MAGIC Você é um assistente de atendimento de uma operadora de plano de saúde.
# MAGIC Responda de forma clara e cordial, sempre citando a regra ou o documento de origem.
# MAGIC Se a informação não estiver nos documentos, diga que o beneficiário deve contatar
# MAGIC a central de atendimento. Nunca invente prazos ou valores.
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Teste na UI
# MAGIC Perguntas para experimentar:
# MAGIC - "Qual a carência para ressonância magnética?"
# MAGIC - "Posso usar hospital fora da rede?"
# MAGIC - "Como funciona o reembolso e qual o prazo?"
# MAGIC - "Meu plano é Ambulatorial. Ele cobre internação?"
# MAGIC - "O que é coparticipação?"
# MAGIC
# MAGIC Observe as **citações** — o KA aponta de qual documento veio a resposta.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Consultar o KA por API
# MAGIC Ao criar o KA, ele fica exposto como um **serving endpoint**. Copie o nome do
# MAGIC endpoint (em **Serving**) e consulte-o programaticamente.

# COMMAND ----------

dbutils.widgets.text("ka_endpoint", "", "Nome do endpoint do KA")
KA_ENDPOINT = dbutils.widgets.get("ka_endpoint")
assert KA_ENDPOINT, "Informe o nome do serving endpoint do Knowledge Assistant."

from mlflow.deployments import get_deploy_client
client = get_deploy_client("databricks")

# O Knowledge Assistant é servido como um ResponsesAgent — o payload usa a chave
# "input" (formato Responses), não "messages" (formato chat).
resposta = client.predict(
    endpoint=KA_ENDPOINT,
    inputs={
        "input": [
            {"role": "user", "content": "Qual a carência para cirurgias e internações?"}
        ]
    },
)
import json
print(json.dumps(resposta, indent=2, ensure_ascii=False)[:2000])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Discussão — KA vs. RAG na mão
# MAGIC
# MAGIC | Aspecto | Knowledge Assistant | RAG com código (notebook 04) |
# MAGIC |---------|---------------------|-------------------------------|
# MAGIC | Setup | UI, minutos | Você controla o pipeline |
# MAGIC | Chunking/embeddings | Automático | Você escolhe estratégia |
# MAGIC | Customização | Média | Total |
# MAGIC | Quando usar | Q&A sobre docs, rápido | Lógica custom, tools, controle fino |
# MAGIC
# MAGIC Guarde o **nome do endpoint do KA** — vamos usá-lo como ferramenta no MAS (notebook 05).
# MAGIC
# MAGIC Próximo: **`04_agente_codigo`** — construir um agente com código e tools.

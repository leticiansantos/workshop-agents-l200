# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup do ambiente
# MAGIC
# MAGIC **Workshop: Agentes no Databricks — domínio de Saúde**
# MAGIC
# MAGIC Este notebook prepara o ambiente compartilhado por todos os demais:
# MAGIC - Cria o **catálogo**, **schema** e **volume** no Unity Catalog.
# MAGIC - Instala as bibliotecas necessárias.
# MAGIC - Verifica acesso às Foundation Model APIs.
# MAGIC
# MAGIC > Requisitos: Unity Catalog + Serverless habilitados e permissão para criar catálogo/schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parâmetros do workshop — isolamento por usuário
# MAGIC
# MAGIC ⚠️ **Ambiente compartilhado.** Para evitar que participantes sobrescrevam os dados,
# MAGIC funções, índices e deploys uns dos outros, **cada pessoa usa um schema próprio**
# MAGIC derivado do seu usuário: `agentes_saude_<seu_usuario>`.
# MAGIC
# MAGIC - **Compartilhado por todos**: o catálogo e o endpoint de Vector Search (recursos
# MAGIC   caros que suportam multiusuário).
# MAGIC - **Isolado por pessoa**: schema, tabelas, volume, UC Functions, índice VS, modelo
# MAGIC   registrado, endpoint de deploy e experimento MLflow.
# MAGIC
# MAGIC > Este mesmo bloco de identificação se repete no início de todos os notebooks —
# MAGIC > ele recalcula o seu schema de forma determinística a partir do seu login.

# COMMAND ----------

import re

dbutils.widgets.text("catalogo", "workshop_dev", "Catálogo (compartilhado)")
CATALOGO = dbutils.widgets.get("catalogo")

# Slug determinístico a partir do login (ex.: "ana.souza@empresa.com" -> "ana_souza").
_usuario = spark.sql("SELECT current_user()").collect()[0][0]
USER_SLUG = re.sub(r"[^a-z0-9]+", "_", _usuario.split("@")[0].lower()).strip("_")

SCHEMA = f"agentes_saude_{USER_SLUG}"          # schema exclusivo do participante
VOLUME = "documentos"

# Endpoint de LLM usado no workshop (Foundation Model API). Compartilhado — só leitura.
# Troque para o modelo mais capaz disponível no seu workspace.
LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

print(f"Usuário.........: {_usuario}")
print(f"Catálogo........: {CATALOGO}  (compartilhado)")
print(f"Schema..........: {SCHEMA}  (exclusivo seu)")
print(f"Volume..........: /Volumes/{CATALOGO}/{SCHEMA}/{VOLUME}")
print(f"LLM endpoint....: {LLM_ENDPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Criação de schema e volume
# MAGIC O **catálogo** `workshop_dev` já existe (compartilhado, provisionado pelo facilitador).
# MAGIC Cada participante cria apenas o **seu** schema e volume dentro dele.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMA}.{VOLUME}")

spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print("✅ Schema e volume prontos.")
display(spark.sql(f"SHOW VOLUMES IN {CATALOGO}.{SCHEMA}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificação de acesso às Foundation Model APIs
# MAGIC Um "ping" no endpoint de LLM para garantir que o workspace consegue servir modelos.

# COMMAND ----------

from mlflow.deployments import get_deploy_client

# CATALOGO/SCHEMA/LLM_ENDPOINT já foram definidos na seção 1 (sem restartPython, seguem válidos).
client = get_deploy_client("databricks")
resposta = client.predict(
    endpoint=LLM_ENDPOINT,
    inputs={
        "messages": [
            {"role": "user", "content": "Responda apenas com: pronto para o workshop!"}
        ],
        "max_tokens": 20,
    },
)
print(resposta["choices"][0]["message"]["content"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Próximo passo
# MAGIC Ambiente pronto. Siga para **`01_dados_sinteticos`** para gerar os dados de saúde.

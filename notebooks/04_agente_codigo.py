# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Agente com código — ResponsesAgent + tools + deploy
# MAGIC
# MAGIC Agora saímos do no-code e construímos um agente **com código**, para ter controle
# MAGIC total. Vamos:
# MAGIC 1. Criar **UC Functions** (ferramentas SQL/Python governadas).
# MAGIC 2. Criar um índice de **Vector Search** sobre os documentos de cobertura.
# MAGIC 3. Escrever um **ResponsesAgent** (MLflow) que usa essas tools.
# MAGIC 4. **Logar** e **deployar** o agente em Model Serving com `databricks-agents`.
# MAGIC
# MAGIC > Esse é o padrão de produção para agentes customizados no Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Bibliotecas
# MAGIC Cada notebook roda numa sessão própria — instalamos aqui as libs de Vector Search,
# MAGIC agentes e LangChain usadas ao longo do notebook.

# COMMAND ----------

# MAGIC %pip install -U -qqq databricks-vectorsearch databricks-agents databricks-langchain langchain-core "mlflow[databricks]>=3.1"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# ── Isolamento por usuário (ambiente compartilhado) ────────────────────────────
# O endpoint de Vector Search é COMPARTILHADO (recurso caro, multiusuário). Todo o
# resto — índice, funções, modelo e deploy — fica no seu schema/nome exclusivo.
import re

dbutils.widgets.text("catalogo", "workshop_agentes", "Catálogo (compartilhado)")
CATALOGO = dbutils.widgets.get("catalogo")
_usuario = spark.sql("SELECT current_user()").collect()[0][0]
USER_SLUG = re.sub(r"[^a-z0-9]+", "_", _usuario.split("@")[0].lower()).strip("_")
SCHEMA = f"saude_{USER_SLUG}"

LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"
VS_ENDPOINT = "workshop_vs_endpoint"                    # COMPARTILHADO entre todos
VS_INDEX = f"{CATALOGO}.{SCHEMA}.doc_cobertura_index"   # exclusivo (dentro do seu schema)

spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print(f"Schema exclusivo: {CATALOGO}.{SCHEMA}")
print(f"VS endpoint (compartilhado): {VS_ENDPOINT}")
print(f"VS index (exclusivo)........: {VS_INDEX}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ferramentas como UC Functions
# MAGIC UC Functions são governadas (permissões, lineage) e podem ser chamadas por qualquer
# MAGIC agente. Criamos duas: consultar sinistros de um beneficiário e verificar carência.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOGO}.{SCHEMA}.sinistros_do_beneficiario(p_beneficiario_id STRING)
RETURNS TABLE(sinistro_id STRING, data_evento DATE, procedimento STRING,
              valor_solicitado DOUBLE, valor_aprovado DOUBLE, status STRING, motivo_negativa STRING)
COMMENT 'Retorna o histórico de sinistros de um beneficiário pelo seu id (ex: BF000123).'
RETURN
  SELECT s.sinistro_id, s.data_evento, pr.descricao AS procedimento,
         s.valor_solicitado, s.valor_aprovado, s.status, s.motivo_negativa
  FROM {CATALOGO}.{SCHEMA}.sinistros s
  JOIN {CATALOGO}.{SCHEMA}.procedimentos pr ON s.procedimento_id = pr.procedimento_id
  WHERE s.beneficiario_id = p_beneficiario_id
  ORDER BY s.data_evento DESC
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOGO}.{SCHEMA}.carencia_procedimento(p_descricao STRING)
RETURNS TABLE(procedimento STRING, categoria STRING, valor_referencia DOUBLE, carencia_dias INT)
COMMENT 'Retorna a carência (em dias) e o valor de referência de um procedimento pela descrição parcial.'
RETURN
  SELECT descricao AS procedimento, categoria, valor_referencia, carencia_dias
  FROM {CATALOGO}.{SCHEMA}.procedimentos
  WHERE lower(descricao) LIKE '%' || lower(p_descricao) || '%'
""")

print("✅ UC Functions criadas")
display(spark.sql(f"SELECT * FROM {CATALOGO}.{SCHEMA}.sinistros_do_beneficiario('BF000001')"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Vector Search sobre os documentos de cobertura
# MAGIC Primeiro materializamos os documentos do volume numa tabela com Change Data Feed,
# MAGIC depois criamos o índice gerenciado (embeddings automáticos).

# COMMAND ----------

import os, glob
VOL_PATH = f"/Volumes/{CATALOGO}/{SCHEMA}/documentos"

linhas = []
for caminho in glob.glob(f"{VOL_PATH}/*.md"):
    with open(caminho, "r", encoding="utf-8") as f:
        conteudo = f.read()
    linhas.append((os.path.basename(caminho), conteudo))

from pyspark.sql import Row
df_docs = spark.createDataFrame([Row(id=i, arquivo=n, texto=c) for i, (n, c) in enumerate(linhas)])
(df_docs.write.mode("overwrite")
   .option("delta.enableChangeDataFeed", "true")
   .saveAsTable(f"{CATALOGO}.{SCHEMA}.doc_cobertura"))

print(f"✅ {df_docs.count()} documentos materializados em doc_cobertura")

# COMMAND ----------

import re
from databricks.vector_search.client import VectorSearchClient

# Recalcula a identidade caso a célula seja executada isoladamente (evita NameError).
CATALOGO = dbutils.widgets.get("catalogo")
_usuario = spark.sql("SELECT current_user()").collect()[0][0]
USER_SLUG = re.sub(r"[^a-z0-9]+", "_", _usuario.split("@")[0].lower()).strip("_")
SCHEMA = f"saude_{USER_SLUG}"
VS_ENDPOINT = "workshop_vs_endpoint"                    # COMPARTILHADO entre todos
VS_INDEX = f"{CATALOGO}.{SCHEMA}.doc_cobertura_index"   # exclusivo (seu schema)

vsc = VectorSearchClient(disable_notice=True)

# Endpoint COMPARTILHADO: normalmente já existe (criado por outro participante).
# Só tratamos o caso "já existe"; outros erros devem aparecer.
endpoints_existentes = [e["name"] for e in (vsc.list_endpoints().get("endpoints") or [])]
if VS_ENDPOINT not in endpoints_existentes:
    vsc.create_endpoint(name=VS_ENDPOINT, endpoint_type="STANDARD")
    print(f"Endpoint '{VS_ENDPOINT}' criado; aguardando ficar online...")
else:
    print(f"Endpoint '{VS_ENDPOINT}' já existe (compartilhado) — reutilizando.")

vsc.wait_for_endpoint(VS_ENDPOINT, verbose=True)

# COMMAND ----------

# Índice gerenciado: o Databricks calcula os embeddings automaticamente.
# O índice vive no SEU schema, então é exclusivo. Criamos se ainda não existir.
indices_existentes = [
    i["name"] for i in (vsc.list_indexes(VS_ENDPOINT).get("vector_indexes") or [])
]
if VS_INDEX not in indices_existentes:
    vsc.create_delta_sync_index(
        endpoint_name=VS_ENDPOINT,
        index_name=VS_INDEX,
        source_table_name=f"{CATALOGO}.{SCHEMA}.doc_cobertura",
        pipeline_type="TRIGGERED",
        primary_key="id",
        embedding_source_column="texto",
        embedding_model_endpoint_name="databricks-gte-large-en",
    )
    print(f"Índice '{VS_INDEX}' criado.")
else:
    print(f"Índice '{VS_INDEX}' já existe — reutilizando.")

vsc.get_index(VS_ENDPOINT, VS_INDEX).wait_until_ready(verbose=True)
print("✅ Índice de Vector Search pronto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Teste rápido de recuperação

# COMMAND ----------

# Reconstrói o client caso a célula seja executada isoladamente (disable_notice
# silencia o aviso informativo de autenticação por token de notebook).
from databricks.vector_search.client import VectorSearchClient
vsc = VectorSearchClient(disable_notice=True)

idx = vsc.get_index(VS_ENDPOINT, VS_INDEX)
res = idx.similarity_search(
    query_text="qual a carência para ressonância magnética?",
    columns=["arquivo", "texto"],
    num_results=2,
)
print("Documentos mais relevantes:")
for linha in res["result"]["data_array"]:
    print("→", linha[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. O agente: `ResponsesAgent` com tools
# MAGIC Escrevemos o agente num arquivo Python para logá-lo com `code-based logging` do MLflow.
# MAGIC
# MAGIC ⚠️ **Ambiente compartilhado**: em vez de `%%writefile` (nome de arquivo fixo, que
# MAGIC colidiria no driver compartilhado), **geramos o arquivo por usuário** — caminho e
# MAGIC nome de módulo únicos — e injetamos o **seu** catálogo/schema no código. Assim o
# MAGIC agente de cada pessoa aponta para o próprio índice e as próprias funções.
# MAGIC
# MAGIC O arquivo é gravado num **volume do Unity Catalog** (`assets`), que é persistente
# MAGIC (sobrevive a reinício de cluster) e acessível de outras sessões — os notebooks 06 e
# MAGIC 07 importam o agente daí, sem duplicar o código. Usamos um volume separado do
# MAGIC `documentos` para o `.py` não ser indexado pelo Vector Search / Knowledge Assistant.

# COMMAND ----------

import os

# Volume dedicado ao código do agente (persistente, exclusivo do seu schema).
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMA}.assets")
AGENT_DIR = f"/Volumes/{CATALOGO}/{SCHEMA}/assets"
os.makedirs(AGENT_DIR, exist_ok=True)
AGENT_MODULE = f"agente_saude_{USER_SLUG}"
AGENT_FILE = f"{AGENT_DIR}/{AGENT_MODULE}.py"

# O código do agente. CATALOGO/SCHEMA/endpoints são injetados via f-string ({{}} = chave
# literal que sobra no arquivo final).
codigo_agente = f'''
import mlflow
from databricks_langchain import ChatDatabricks, VectorSearchRetrieverTool, UCFunctionToolkit
from mlflow.entities import SpanType
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import ResponsesAgentRequest, ResponsesAgentResponse

mlflow.langchain.autolog()

# Injetados na geração — exclusivos deste participante.
CATALOGO = "{CATALOGO}"
SCHEMA = "{SCHEMA}"
LLM_ENDPOINT = "{LLM_ENDPOINT}"
VS_INDEX = "{VS_INDEX}"

SYSTEM_PROMPT = """Você é o assistente de atendimento de uma operadora de plano de saúde.
Ajude beneficiários e atendentes a entender sinistros, coberturas e carências.
- Use a ferramenta de busca em documentos para perguntas sobre regras/cobertura/carência.
- Use as funções de sinistros/carência para dados específicos de um beneficiário.
- Cite a fonte quando usar documentos. Nunca invente valores ou prazos.
- Responda em português, de forma clara e cordial."""

toolkit = UCFunctionToolkit(function_names=[
    f"{{CATALOGO}}.{{SCHEMA}}.sinistros_do_beneficiario",
    f"{{CATALOGO}}.{{SCHEMA}}.carencia_procedimento",
])
retriever_tool = VectorSearchRetrieverTool(
    index_name=VS_INDEX,
    num_results=3,
    tool_name="buscar_documentos_cobertura",
    tool_description="Busca trechos dos manuais de cobertura, carência e reembolso do plano.",
)
TOOLS = toolkit.tools + [retriever_tool]


class AgenteSaude(ResponsesAgent):
    def __init__(self):
        self.llm = ChatDatabricks(endpoint=LLM_ENDPOINT).bind_tools(TOOLS)
        self.tools_by_name = {{t.name: t for t in TOOLS}}

    @mlflow.trace(span_type=SpanType.AGENT)
    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        from langchain_core.messages import SystemMessage, HumanMessage, AIMessage, ToolMessage
        mensagens = [SystemMessage(content=SYSTEM_PROMPT)]
        for m in request.input:
            if m.role == "user":
                mensagens.append(HumanMessage(content=m.content))
            elif m.role == "assistant":
                mensagens.append(AIMessage(content=m.content))

        # Loop agêntico: chama o LLM, executa tools, repete até resposta final.
        for _ in range(6):
            ai = self.llm.invoke(mensagens)
            mensagens.append(ai)
            if not ai.tool_calls:
                return ResponsesAgentResponse(
                    output=[self.create_text_output_item(ai.content, id="final")]
                )
            for tc in ai.tool_calls:
                tool = self.tools_by_name[tc["name"]]
                resultado = tool.invoke(tc["args"])
                mensagens.append(ToolMessage(content=str(resultado), tool_call_id=tc["id"]))

        return ResponsesAgentResponse(
            output=[self.create_text_output_item(
                "Não consegui concluir com as ferramentas disponíveis.", id="final")]
        )


from mlflow.models import set_model
AGENT = AgenteSaude()
set_model(AGENT)
'''

with open(AGENT_FILE, "w", encoding="utf-8") as f:
    f.write(codigo_agente)

print(f"✅ Agente gerado em: {AGENT_FILE}")
print(f"   (módulo: {AGENT_MODULE}, aponta para {CATALOGO}.{SCHEMA})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Testar o agente localmente

# COMMAND ----------

# Recalculamos a identidade do participante e localizamos o arquivo do agente gerado
# (exclusivo seu) — assim a célula funciona mesmo se executada isoladamente.
import re, sys, importlib

CATALOGO = dbutils.widgets.get("catalogo")
_usuario = spark.sql("SELECT current_user()").collect()[0][0]
USER_SLUG = re.sub(r"[^a-z0-9]+", "_", _usuario.split("@")[0].lower()).strip("_")
SCHEMA = f"saude_{USER_SLUG}"
VS_INDEX = f"{CATALOGO}.{SCHEMA}.doc_cobertura_index"

AGENT_DIR = f"/Volumes/{CATALOGO}/{SCHEMA}/assets"   # volume persistente do UC
AGENT_MODULE = f"agente_saude_{USER_SLUG}"
AGENT_FILE = f"{AGENT_DIR}/{AGENT_MODULE}.py"

# Torna o módulo exclusivo importável.
if AGENT_DIR not in sys.path:
    sys.path.insert(0, AGENT_DIR)
agente_mod = importlib.import_module(AGENT_MODULE)
AGENT = agente_mod.AGENT

from mlflow.types.responses import ResponsesAgentRequest
req = ResponsesAgentRequest(input=[
    {"role": "user", "content": "Qual a carência para ressonância magnética e o que diz o manual?"}
])
resp = AGENT.predict(req)
print(resp.output[0].content[0]["text"] if isinstance(resp.output[0].content, list)
      else resp.output[0].content)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Logar o agente com MLflow (code-based logging)
# MAGIC Declaramos os **recursos** (LLM, índice, UC Functions) para o Databricks provisionar
# MAGIC as credenciais automaticamente no deploy.

# COMMAND ----------

import mlflow
from mlflow.models.resources import (
    DatabricksServingEndpoint, DatabricksVectorSearchIndex, DatabricksFunction,
)

LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

# Experimento MLflow exclusivo do participante. O nome-folha inclui o USER_SLUG para ser
# único mesmo se os notebooks rodarem como job/service principal (current_user() igual
# para todos) ou o experimento cair numa pasta compartilhada.
try:
    mlflow.set_experiment(f"/Users/{_usuario}/workshop_agentes_saude_{USER_SLUG}")
except Exception:
    mlflow.set_experiment(f"/Shared/workshop_agentes/workshop_agentes_saude_{USER_SLUG}")

recursos = [
    DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT),
    DatabricksServingEndpoint(endpoint_name="databricks-gte-large-en"),
    DatabricksVectorSearchIndex(index_name=VS_INDEX),
    DatabricksFunction(function_name=f"{CATALOGO}.{SCHEMA}.sinistros_do_beneficiario"),
    DatabricksFunction(function_name=f"{CATALOGO}.{SCHEMA}.carencia_procedimento"),
]

with mlflow.start_run(run_name=f"agente_saude_{USER_SLUG}"):
    info = mlflow.pyfunc.log_model(
        name="agente_saude",
        python_model=AGENT_FILE,          # arquivo exclusivo gerado na seção 4
        resources=recursos,
        pip_requirements=[
            "mlflow", "databricks-langchain", "databricks-vectorsearch", "langchain-core",
        ],
    )
print("✅ Modelo logado:", info.model_uri)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Registrar no Unity Catalog e fazer deploy
# MAGIC O nome do modelo inclui o **seu** schema, então o endpoint de deploy gerado também
# MAGIC é exclusivo — sem colisão com os demais participantes.

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")
NOME_MODELO = f"{CATALOGO}.{SCHEMA}.agente_saude"   # schema exclusivo → nome exclusivo
versao = mlflow.register_model(model_uri=info.model_uri, name=NOME_MODELO)
print("Versão registrada:", versao.version)

# COMMAND ----------

from databricks import agents

deployment = agents.deploy(
    model_name=NOME_MODELO,
    model_version=versao.version,
    scale_to_zero=True,
)
print("✅ Deploy iniciado.")
print("Endpoint:", deployment.endpoint_name)
print("Review App:", deployment.query_endpoint)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Discussão
# MAGIC - **ResponsesAgent** é a interface recomendada do MLflow 3 para agentes.
# MAGIC - Tools governadas (UC Functions) + retriever (Vector Search) = padrão reutilizável.
# MAGIC - `agents.deploy` cria o endpoint, o **Review App** e liga a **inference table**
# MAGIC   (essencial para o notebook 06 de avaliação/monitoramento).
# MAGIC
# MAGIC Próximo: **`05_multi_agent`** — orquestrar Genie + KA + este agente num supervisor.

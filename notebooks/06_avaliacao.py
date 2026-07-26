# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Avaliação e governança — MLflow tracing + evaluation
# MAGIC
# MAGIC Agente em produção precisa de **qualidade mensurável** e **observabilidade**. Aqui:
# MAGIC 1. **Tracing** — inspecionar cada passo do agente.
# MAGIC 2. **Dataset de avaliação** — perguntas + respostas esperadas.
# MAGIC 3. **Scorers** — juízes automáticos (Correctness, Guidelines, Safety, RelevanceToQuery).
# MAGIC 4. **`mlflow.genai.evaluate`** — roda a avaliação e compara versões.
# MAGIC 5. **Monitoramento em produção** e **governança** com UC.
# MAGIC
# MAGIC > "Se você não mede, não melhora." Este bloco é o diferencial de produção.

# COMMAND ----------

# ── Isolamento por usuário (ambiente compartilhado) ────────────────────────────
import re

dbutils.widgets.text("catalogo", "workshop_agentes", "Catálogo (compartilhado)")
dbutils.widgets.text("agente_endpoint", "", "Endpoint do seu agente")
CATALOGO = dbutils.widgets.get("catalogo")
_usuario = spark.sql("SELECT current_user()").collect()[0][0]
USER_SLUG = re.sub(r"[^a-z0-9]+", "_", _usuario.split("@")[0].lower()).strip("_")
SCHEMA = f"saude_{USER_SLUG}"
AGENTE_ENDPOINT = dbutils.widgets.get("agente_endpoint")

# COMMAND ----------

# MAGIC %pip install -U -qqq "mlflow[databricks]>=3.1" databricks-agents databricks-langchain databricks-vectorsearch langchain-core
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Tracing — o que o agente fez?
# MAGIC Carregamos o agente do notebook 04 e observamos o trace: chamadas de LLM,
# MAGIC ferramentas acionadas, latência por passo.

# COMMAND ----------

import os, re, sys, importlib
import mlflow
from mlflow.types.responses import ResponsesAgentRequest

# O restartPython limpou o estado — recalcula a identidade e importa o SEU agente.
_usuario = spark.sql("SELECT current_user()").collect()[0][0]
USER_SLUG = re.sub(r"[^a-z0-9]+", "_", _usuario.split("@")[0].lower()).strip("_")

CATALOGO = dbutils.widgets.get("catalogo")
SCHEMA = f"saude_{USER_SLUG}"

# O agente foi gerado no notebook 04 e gravado num volume persistente do UC — importamos
# de lá (sobrevive a reinício de cluster, acessível entre sessões).
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

# Experimento exclusivo do participante (NÃO usar /Shared — colidiria entre pessoas).
mlflow.set_experiment(f"/Users/{_usuario}/workshop_agentes_avaliacao")

req = ResponsesAgentRequest(input=[
    {"role": "user", "content": "O beneficiário BF000001 teve algum sinistro negado? Por quê?"}
])
resp = AGENT.predict(req)
print(resp.output[-1].content)

# COMMAND ----------

# MAGIC %md
# MAGIC Abra a aba **Traces** do experimento (ou o painel lateral do MLflow) e navegue
# MAGIC pela árvore: `AGENT → LLM → tool calls`. Cada span traz inputs, outputs e latência.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Dataset de avaliação
# MAGIC Curamos um pequeno conjunto de perguntas com expectativas. Em produção, você
# MAGIC constrói datasets a partir de traces reais e feedback de especialistas.

# COMMAND ----------

eval_data = [
    {
        "inputs": {"pergunta": "Qual a carência para ressonância magnética?"},
        "expectations": {"expected_facts": ["180 dias"]},
    },
    {
        "inputs": {"pergunta": "Posso usar um hospital fora da rede credenciada?"},
        "expectations": {"expected_facts": ["apenas em urgência ou emergência",
                                            "reembolso mediante comprovantes"]},
    },
    {
        "inputs": {"pergunta": "Meu plano é Ambulatorial. Ele cobre internação?"},
        "expectations": {"expected_facts": ["Ambulatorial não cobre internação"]},
    },
    {
        "inputs": {"pergunta": "Qual a carência para parto?"},
        "expectations": {"expected_facts": ["300 dias"]},
    },
    {
        "inputs": {"pergunta": "Como funciona o reembolso?"},
        "expectations": {"expected_facts": ["tabela de referência da operadora",
                                            "solicitar em até 60 dias"]},
    },
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Função a avaliar
# MAGIC Um wrapper que chama o agente com a pergunta. Assim conseguimos avaliar tanto o
# MAGIC objeto local quanto o endpoint deployado.

# COMMAND ----------

def responder(pergunta: str) -> str:
    req = ResponsesAgentRequest(input=[{"role": "user", "content": pergunta}])
    r = AGENT.predict(req)
    ultimo = r.output[-1].content
    if isinstance(ultimo, list):
        return " ".join(c.get("text", "") for c in ultimo)
    return str(ultimo)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Scorers (juízes automáticos)
# MAGIC Usamos scorers prontos do MLflow — LLM-as-a-judge — e um scorer de **diretriz**
# MAGIC específica de negócio (não inventar prazos, sempre em português).

# COMMAND ----------

from mlflow.genai.scorers import Correctness, RelevanceToQuery, Safety, Guidelines

diretriz_negocio = Guidelines(
    name="tom_e_seguranca",
    guidelines=(
        "A resposta deve estar em português, ser cordial e NÃO inventar prazos ou valores "
        "que não estejam fundamentados. Se não souber, deve orientar a procurar a central "
        "de atendimento."
    ),
)

scorers = [
    Correctness(),          # confere contra expected_facts
    RelevanceToQuery(),     # a resposta é relevante à pergunta?
    Safety(),               # conteúdo seguro?
    diretriz_negocio,       # diretriz custom de negócio
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Rodar a avaliação

# COMMAND ----------

import mlflow

resultados = mlflow.genai.evaluate(
    data=eval_data,
    predict_fn=lambda pergunta: responder(pergunta),
    scorers=scorers,
)

print("✅ Avaliação concluída. Veja os resultados na UI do MLflow (aba Evaluations).")
resultados.metrics

# COMMAND ----------

# MAGIC %md
# MAGIC Compare os **scores por pergunta**, identifique falhas (ex.: alucinou um prazo),
# MAGIC ajuste o system prompt/tools no notebook 04 e **re-rode** — você terá duas *runs*
# MAGIC comparáveis lado a lado no MLflow.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Monitoramento em produção
# MAGIC Como o agente foi deployado com `agents.deploy` (notebook 04), as requisições ficam
# MAGIC registradas numa **inference table**. Podemos rodar scorers continuamente sobre o
# MAGIC tráfego real com **monitors**.

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC from databricks.agents.monitoring import create_external_monitor, AssessmentsSuiteConfig
# MAGIC
# MAGIC # Exemplo: monitor que amostra 20% do tráfego e aplica os scorers em produção.
# MAGIC monitor = create_external_monitor(
# MAGIC     catalog_name=CATALOGO,
# MAGIC     schema_name=SCHEMA,
# MAGIC     assessments_config=AssessmentsSuiteConfig(
# MAGIC         sample=0.2,
# MAGIC         assessments=[Safety(), RelevanceToQuery(), diretriz_negocio],
# MAGIC     ),
# MAGIC )
# MAGIC ```
# MAGIC O monitor gera um dashboard de qualidade ao longo do tempo — latência, custo,
# MAGIC scores — direto sobre a inference table governada pelo Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Governança (Unity Catalog)
# MAGIC Tudo que construímos é **governado**:
# MAGIC - **Modelo do agente** registrado em `catalog.schema.agente_saude` (versões, lineage).
# MAGIC - **UC Functions** com permissões (`GRANT EXECUTE`).
# MAGIC - **Vector Search index** e tabelas com controle de acesso por linha/coluna.
# MAGIC - **Inference tables** para auditoria de cada interação.
# MAGIC
# MAGIC Exemplo de concessão de acesso à ferramenta:

# COMMAND ----------

# Conceder execução da UC Function a um grupo (ajuste o nome do grupo).
# spark.sql(f"GRANT EXECUTE ON FUNCTION {CATALOGO}.{SCHEMA}.sinistros_do_beneficiario TO `atendimento`")
print("Lineage e permissões visíveis no Catalog Explorer → workshop_agentes.saude")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Resumo
# MAGIC - **Tracing** para depurar; **evaluation** para medir; **monitors** para produção.
# MAGIC - Scorers prontos + diretrizes custom cobrem qualidade, segurança e regras de negócio.
# MAGIC - Governança fim-a-fim pelo Unity Catalog.
# MAGIC
# MAGIC Próximo: **`07_capstone`** — junte tudo num agente de atendimento ao beneficiário.

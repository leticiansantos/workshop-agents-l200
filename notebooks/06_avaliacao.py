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

dbutils.widgets.text("catalogo", "workshop_dev", "Catálogo (compartilhado)")
dbutils.widgets.text("agente_endpoint", "", "Endpoint do seu agente")
CATALOGO = dbutils.widgets.get("catalogo")
_usuario = spark.sql("SELECT current_user()").collect()[0][0]
USER_SLUG = re.sub(r"[^a-z0-9]+", "_", _usuario.split("@")[0].lower()).strip("_")
SCHEMA = f"agentes_saude_{USER_SLUG}"
AGENTE_ENDPOINT = dbutils.widgets.get("agente_endpoint")

# COMMAND ----------

# MAGIC %pip install -U -qqq "mlflow[databricks]>=3.3" databricks-agents databricks-langchain databricks-vectorsearch langchain-core
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
SCHEMA = f"agentes_saude_{USER_SLUG}"

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

# Garante o rastreamento na SESSÃO do notebook (não só dentro do módulo do agente):
# tracking no Databricks + autolog do LangChain. Sem isto, dependendo da versão, os
# spans do agente podem não ser capturados nesta sessão.
mlflow.set_tracking_uri("databricks")
mlflow.langchain.autolog()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definir o experimento
# MAGIC ⚠️ **Ambiente compartilhado**: o nome-folha do experimento inclui o `USER_SLUG`, então
# MAGIC é único mesmo que os notebooks rodem como job/service principal (`current_user()` igual
# MAGIC para todos) ou que o experimento caia numa pasta compartilhada. Ancoramos na home do
# MAGIC usuário e caímos para `/Shared` se a home não for gravável.

# COMMAND ----------

EXP_LEAF = f"workshop_agentes_avaliacao_{USER_SLUG}"

def _definir_experimento(nome_folha):
    """Tenta criar/definir o experimento na home do usuário; se falhar, usa /Shared."""
    for base in (f"/Users/{_usuario}", "/Shared/workshop_agentes"):
        caminho = f"{base}/{nome_folha}"
        try:
            mlflow.set_experiment(caminho)
            return caminho
        except Exception:
            continue
    raise RuntimeError("Não foi possível criar o experimento em nenhum caminho.")

EXP_ATIVO = _definir_experimento(EXP_LEAF)
print(f"Experimento ativo: {EXP_ATIVO}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gerar o primeiro trace
# MAGIC Agora sim chamamos o agente — este é o trace que aparece na aba **Traces**.

# COMMAND ----------

req = ResponsesAgentRequest(input=[
    {"role": "user", "content": "O beneficiário BF000001 teve algum sinistro negado? Por quê?"}
])
resp = AGENT.predict(req)
print(resp.output[-1].content)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificar que o trace foi gravado
# MAGIC Confirmamos em qual experimento os traces caíram, quantos existem e o link direto.

# COMMAND ----------

# Os traces são exportados de forma assíncrona — força o flush antes de contar.
try:
    mlflow.flush_trace_async_logging()
except Exception:
    pass

exp_ativo = mlflow.get_experiment(mlflow.tracking.fluent._get_experiment_id())
traces = mlflow.search_traces(
    locations=[exp_ativo.experiment_id], max_results=10, return_type="list"
)
print(f"Experimento ativo: {exp_ativo.name}")
print(f"  experiment_id...: {exp_ativo.experiment_id}")
print(f"  traces gravados.: {len(traces)}")

_host = spark.conf.get("spark.databricks.workspaceUrl", None)
if _host:
    print(f"\nAbra: https://{_host}/ml/experiments/{exp_ativo.experiment_id}?compareRunsMode=TRACES")
print("\n⚠️ Confira que a UI está aberta NESTE experiment_id (não em outro).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Dataset de avaliação
# MAGIC Curamos um pequeno conjunto de perguntas com expectativas e o **persistimos no Unity
# MAGIC Catalog** como um *evaluation dataset*. Assim ele aparece na aba **Datasets** da UI
# MAGIC do experimento, fica versionado e reutilizável (não só uma lista em memória).

# COMMAND ----------

registros = [
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

# Cria um dataset gerenciado no UC, no SEU schema, VINCULADO ao experimento ativo.
# É o experiment_id que faz o dataset aparecer na aba Datasets do experimento.
#
# ⚠️ Um dataset criado ANTES sem experiment_id fica desvinculado e não aparece na aba.
# Por isso, se já existir, dropamos e recriamos vinculado (idempotente).
import mlflow.genai.datasets

DATASET_TABLE = f"{CATALOGO}.{SCHEMA}.eval_dataset_atendimento"
EXP_ID = mlflow.get_experiment_by_name(EXP_ATIVO).experiment_id

# Remove versão anterior (desvinculada), se houver.
try:
    mlflow.genai.datasets.delete_dataset(DATASET_TABLE)
    print(f"Dataset anterior removido: {DATASET_TABLE}")
except Exception:
    pass  # não existia — ok

eval_dataset = mlflow.genai.datasets.create_dataset(
    uc_table_name=DATASET_TABLE,
    experiment_id=EXP_ID,   # vincula ao experimento → aparece na aba Datasets
)
print(f"Dataset criado e vinculado ao experimento {EXP_ID}")

eval_dataset.merge_records(registros)
print(f"✅ Dataset com {len(eval_dataset.to_df())} registros — veja em Experiment → Datasets.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Função a avaliar
# MAGIC Um wrapper que chama o agente com a pergunta. Assim conseguimos avaliar tanto o
# MAGIC objeto local quanto o endpoint deployado.
# MAGIC
# MAGIC ⚠️ Para que a avaliação **gere um trace por linha** (visível na aba Traces), duas
# MAGIC coisas precisam estar ativas: o **autolog** na sessão e o `predict_fn` **rastreado**
# MAGIC com `@mlflow.trace`. Sem isso, o `evaluate` roda mas não registra traces.

# COMMAND ----------

# Ativa o rastreamento na sessão do notebook (não só dentro do módulo do agente).
mlflow.langchain.autolog()

@mlflow.trace(name="responder")
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

# MAGIC %md
# MAGIC ### 🧩 EXERCÍCIO 3 — Scorer de diretriz de negócio
# MAGIC Crie um scorer `Guidelines` que codifique a regra de atendimento da operadora. É um
# MAGIC **juiz LLM** que avalia se a resposta do agente segue a diretriz em texto livre.
# MAGIC
# MAGIC **Como preencher (use o Genie — ícone de lâmpada 💡 na célula):**
# MAGIC > "Crie um scorer `Guidelines` do MLflow chamado `diretriz_negocio`, com `name`
# MAGIC > 'tom_e_seguranca' e uma `guidelines` (string) dizendo que a resposta deve estar em
# MAGIC > português, ser cordial, NÃO inventar prazos ou valores sem fundamento, e que se não
# MAGIC > souber deve orientar a procurar a central de atendimento."
# MAGIC
# MAGIC 💡 A `guidelines` é lida por um LLM juiz — escreva a regra de forma clara e verificável.

# COMMAND ----------

from mlflow.genai.scorers import Correctness, RelevanceToQuery, Safety, Guidelines

# 🧩 TODO: crie o scorer diretriz_negocio (Guidelines) descrito no exercício acima.
diretriz_negocio = None  # substitua por Guidelines(name=..., guidelines=...)

# COMMAND ----------

scorers = [
    Correctness(),          # confere contra expected_facts
    RelevanceToQuery(),     # a resposta é relevante à pergunta?
    Safety(),               # conteúdo seguro?
    diretriz_negocio,       # diretriz custom de negócio (do exercício)
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Registrar os judges no experimento (aba Judges)
# MAGIC `.register()` grava o scorer no experimento — é isso que faz o juiz aparecer na aba
# MAGIC **Judges** da UI e permite reutilizá-lo/monitorá-lo depois. Fazemos isso para a nossa
# MAGIC diretriz de negócio e para o Safety.

# COMMAND ----------

for judge in [diretriz_negocio, Safety()]:
    try:
        judge.register(name=judge.name)
        print(f"✅ Judge registrado: {judge.name}")
    except Exception as e:
        print(f"Judge '{judge.name}' já registrado ou não pôde registrar: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Rodar a avaliação

# COMMAND ----------

import mlflow

# Passamos o DATASET PERSISTIDO (não a lista em memória) — assim a run de avaliação
# fica ligada ao dataset do UC, visível na aba Datasets.
resultados = mlflow.genai.evaluate(
    data=eval_dataset,
    predict_fn=responder,   # recebe os inputs desempacotados (kwarg 'pergunta')
    scorers=scorers,
)

print("✅ Avaliação concluída.")
print("   • Resultados por pergunta → aba Evaluations do experimento")
print("   • Dataset usado ...........→ aba Datasets")
print("   • Juízes registrados ......→ aba Judges")
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
print("Lineage e permissões visíveis no Catalog Explorer → workshop_dev.agentes_saude")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Resumo
# MAGIC - **Tracing** para depurar; **evaluation** para medir; **monitors** para produção.
# MAGIC - Scorers prontos + diretrizes custom cobrem qualidade, segurança e regras de negócio.
# MAGIC - Governança fim-a-fim pelo Unity Catalog.
# MAGIC
# MAGIC Próximo: **`07_capstone`** — junte tudo num agente de atendimento ao beneficiário.

# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Agent Bricks — Genie Space (linguagem natural → SQL)
# MAGIC
# MAGIC O **Genie** é um agente de dados que responde perguntas de negócio em linguagem
# MAGIC natural, gerando e executando SQL sobre suas tabelas do Unity Catalog.
# MAGIC
# MAGIC Neste bloco:
# MAGIC 1. Criamos um **Genie Space** sobre as tabelas de saúde (via UI).
# MAGIC 2. Damos boas **instruções** e exemplos para melhorar a qualidade.
# MAGIC 3. Consultamos o Genie **programaticamente** pela Conversation API.
# MAGIC
# MAGIC > Genie é a forma mais rápida de dar acesso self-service aos dados — sem escrever SQL.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Criar o Genie Space (UI)
# MAGIC
# MAGIC ⚠️ **Ambiente compartilhado**: dê um nome único ao seu Space (inclua seu usuário) e
# MAGIC aponte para o **seu** schema `workshop_agentes.saude_<seu_usuario>`. Rode a célula
# MAGIC abaixo para descobrir o nome exato do seu schema e o título sugerido.
# MAGIC
# MAGIC No menu lateral: **Genie → New** e configure:
# MAGIC
# MAGIC 1. **Título**: `Sinistros de Saúde — <seu_usuario>`
# MAGIC 2. **Tabelas**: adicione do **seu** schema (veja a célula abaixo):
# MAGIC    - `sinistros`, `beneficiarios`, `planos`, `hospitais`, `procedimentos`
# MAGIC 3. **SQL Warehouse**: selecione um warehouse Serverless.

# COMMAND ----------

import re
dbutils.widgets.text("catalogo", "workshop_agentes", "Catálogo (compartilhado)")
CATALOGO = dbutils.widgets.get("catalogo")
_usuario = spark.sql("SELECT current_user()").collect()[0][0]
USER_SLUG = re.sub(r"[^a-z0-9]+", "_", _usuario.split("@")[0].lower()).strip("_")
SCHEMA = f"saude_{USER_SLUG}"
print(f"Seu schema para o Genie...: {CATALOGO}.{SCHEMA}")
print(f"Título sugerido do Space..: Sinistros de Saúde — {USER_SLUG}")
print("\nTabelas para adicionar:")
for t in ["sinistros", "beneficiarios", "planos", "hospitais", "procedimentos"]:
    print(f"  {CATALOGO}.{SCHEMA}.{t}")

# COMMAND ----------

# MAGIC %md
# MAGIC (continuação da configuração do Space)
# MAGIC 4. **Instruções gerais** (cole no campo *Instructions*):
# MAGIC
# MAGIC ```
# MAGIC Você é um analista de uma operadora de plano de saúde. As tabelas descrevem
# MAGIC beneficiários, planos, hospitais, procedimentos e sinistros (eventos de uso).
# MAGIC - "Sinistro negado" = sinistros.status = 'Negado'.
# MAGIC - "Taxa de negativa" = negados / total de sinistros.
# MAGIC - Valores monetários estão em reais (R$).
# MAGIC - Sempre que citar um beneficiário, use o nome, não o id.
# MAGIC - Ao agrupar por plano, use planos.nome_plano.
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Exemplos de perguntas (SQL curados)
# MAGIC No Genie, adicione **Example SQL queries** para ancorar o modelo. Sugestões:
# MAGIC
# MAGIC **P: "Qual a taxa de negativa de sinistros por plano?"**
# MAGIC ```sql
# MAGIC SELECT p.nome_plano,
# MAGIC        COUNT(*) AS total,
# MAGIC        SUM(CASE WHEN s.status = 'Negado' THEN 1 ELSE 0 END) AS negados,
# MAGIC        ROUND(100.0 * SUM(CASE WHEN s.status = 'Negado' THEN 1 ELSE 0 END) / COUNT(*), 1) AS taxa_negativa_pct
# MAGIC FROM sinistros s
# MAGIC JOIN beneficiarios b ON s.beneficiario_id = b.beneficiario_id
# MAGIC JOIN planos p ON b.plano_id = p.plano_id
# MAGIC GROUP BY p.nome_plano
# MAGIC ORDER BY taxa_negativa_pct DESC;
# MAGIC ```
# MAGIC
# MAGIC **P: "Quais os motivos de negativa mais comuns?"**
# MAGIC ```sql
# MAGIC SELECT motivo_negativa, COUNT(*) AS qtd
# MAGIC FROM sinistros
# MAGIC WHERE status = 'Negado'
# MAGIC GROUP BY motivo_negativa
# MAGIC ORDER BY qtd DESC;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Teste na UI
# MAGIC Faça perguntas no chat do Genie, como:
# MAGIC - "Quantos sinistros foram negados no último ano?"
# MAGIC - "Qual hospital tem o maior valor aprovado total?"
# MAGIC - "Quais procedimentos mais geram negativa por carência?"
# MAGIC - "Ticket médio de valor aprovado por segmentação de plano"
# MAGIC
# MAGIC Observe o SQL gerado e ajuste as instruções conforme necessário.
# MAGIC **Copie o Space ID** da URL (`.../genie/rooms/<SPACE_ID>`) para a próxima etapa.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Consultar o Genie por API (Conversation API)
# MAGIC Aqui integramos o Genie de forma programática — é assim que ele entra num app
# MAGIC ou num agente supervisor (notebook 05).

# COMMAND ----------

dbutils.widgets.text("genie_space_id", "", "Genie Space ID")
GENIE_SPACE_ID = dbutils.widgets.get("genie_space_id")

assert GENIE_SPACE_ID, "Cole o Space ID do Genie no widget acima antes de continuar."

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

def perguntar_genie(pergunta: str, space_id: str = GENIE_SPACE_ID):
    """Inicia uma conversa no Genie e retorna a resposta e o SQL gerado."""
    conversa = w.genie.start_conversation_and_wait(space_id, pergunta)

    texto, sql, dados = None, None, None
    for att in (conversa.attachments or []):
        if att.text:
            texto = att.text.content
        if att.query:
            sql = att.query.query
            # Busca o resultado tabular da query executada.
            resultado = w.genie.get_message_query_result(
                space_id, conversa.conversation_id, conversa.id
            )
            dados = resultado.statement_response
    return texto, sql, dados

texto, sql, dados = perguntar_genie(
    "Qual a taxa de negativa de sinistros por plano de saúde?"
)
print("Resposta:", texto)
print("\nSQL gerado:\n", sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Perguntas de acompanhamento (contexto de conversa)
# MAGIC O Genie mantém contexto — dá para fazer follow-ups.

# COMMAND ----------

# Helper: extrai o texto de resposta procurando entre TODOS os attachments.
# (o primeiro attachment pode ser uma query/SQL, cujo .text é None)
def texto_da_conversa(msg) -> str:
    for att in (msg.attachments or []):
        if att.text and att.text.content:
            return att.text.content
    return "sem texto"

# Exemplo de follow-up usando a mesma conversa:
conversa = w.genie.start_conversation_and_wait(
    GENIE_SPACE_ID, "Quais são os 5 hospitais com maior valor aprovado?"
)
print(texto_da_conversa(conversa))

follow = w.genie.create_message_and_wait(
    GENIE_SPACE_ID, conversa.conversation_id,
    "E desses, quais são credenciados?"
)
print("\nFollow-up:")
for att in (follow.attachments or []):
    if att.text and att.text.content:
        print(att.text.content)
    if att.query:
        print("SQL:", att.query.query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Discussão
# MAGIC - **Vantagens**: zero código para o usuário final, governado por UC, respeita permissões.
# MAGIC - **Boas práticas**: comentários de tabela/coluna, exemplos de SQL, instruções de negócio.
# MAGIC - Guarde o `GENIE_SPACE_ID` — vamos reutilizá-lo como **ferramenta** no Multi-Agent
# MAGIC   Supervisor (notebook 05).
# MAGIC
# MAGIC Próximo: **`03_knowledge_assistant`** — Q&A sobre regras de cobertura (RAG).

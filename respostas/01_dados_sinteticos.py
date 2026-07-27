# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Geração de dados sintéticos de saúde
# MAGIC
# MAGIC Geramos os dados **estruturados** (tabelas Delta) e **não estruturados**
# MAGIC (documentos no volume) que alimentam todos os agentes do workshop.
# MAGIC
# MAGIC Domínio: operadora de plano de saúde — beneficiários, planos, hospitais,
# MAGIC procedimentos e sinistros. Ver `dados/README.md` para o dicionário de dados.
# MAGIC
# MAGIC > **Tudo sintético.** Nenhum dado real de paciente é usado.

# COMMAND ----------

# MAGIC %pip install faker -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# ── Isolamento por usuário (ambiente compartilhado) ────────────────────────────
# Cada participante grava no seu próprio schema saude_<usuario>. Ver notebook 00.
import re

dbutils.widgets.text("catalogo", "workshop_agentes", "Catálogo (compartilhado)")
CATALOGO = dbutils.widgets.get("catalogo")

_usuario = spark.sql("SELECT current_user()").collect()[0][0]
USER_SLUG = re.sub(r"[^a-z0-9]+", "_", _usuario.split("@")[0].lower()).strip("_")
SCHEMA = f"saude_{USER_SLUG}"
VOLUME = "documentos"
VOL_PATH = f"/Volumes/{CATALOGO}/{SCHEMA}/{VOLUME}"
print(f"Gravando em: {CATALOGO}.{SCHEMA}  (usuário: {_usuario})")

# Garante que o schema/volume do participante existem (idempotente).
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMA}.{VOLUME}")
spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros de volume
# MAGIC Quantidade de linhas a gerar — o suficiente para demonstrar sem custo alto.

# COMMAND ----------

N_BENEFICIARIOS = 5000
N_SINISTROS = 40000
print(f"Vamos gerar {N_BENEFICIARIOS:,} beneficiários e {N_SINISTROS:,} sinistros.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Tabelas de referência: planos, hospitais e procedimentos

# COMMAND ----------

from pyspark.sql import Row

planos = [
    Row(plano_id="PL001", nome_plano="Essencial Ambulatorial", segmentacao="Ambulatorial",
        abrangencia="Municipal", acomodacao="Enfermaria", mensalidade_base=189.90, coparticipacao=True),
    Row(plano_id="PL002", nome_plano="Vida Hospitalar", segmentacao="Hospitalar",
        abrangencia="Estadual", acomodacao="Enfermaria", mensalidade_base=349.90, coparticipacao=False),
    Row(plano_id="PL003", nome_plano="Família Plus", segmentacao="Hospitalar+Obstetrícia",
        abrangencia="Estadual", acomodacao="Apartamento", mensalidade_base=589.90, coparticipacao=False),
    Row(plano_id="PL004", nome_plano="Premium Nacional", segmentacao="Referência",
        abrangencia="Nacional", acomodacao="Apartamento", mensalidade_base=899.90, coparticipacao=False),
    Row(plano_id="PL005", nome_plano="Corporativo Smart", segmentacao="Hospitalar",
        abrangencia="Nacional", acomodacao="Enfermaria", mensalidade_base=429.90, coparticipacao=True),
]
spark.createDataFrame(planos).write.mode("overwrite").saveAsTable("planos")

hospitais = [
    Row(hospital_id="HP001", nome_hospital="Hospital Santa Clara", cidade="São Paulo", uf="SP", tipo="Geral", credenciado=True),
    Row(hospital_id="HP002", nome_hospital="Instituto do Coração Vida", cidade="São Paulo", uf="SP", tipo="Especializado", credenciado=True),
    Row(hospital_id="HP003", nome_hospital="Maternidade Aurora", cidade="Campinas", uf="SP", tipo="Maternidade", credenciado=True),
    Row(hospital_id="HP004", nome_hospital="Pronto-Socorro Central", cidade="Rio de Janeiro", uf="RJ", tipo="Pronto-socorro", credenciado=True),
    Row(hospital_id="HP005", nome_hospital="Hospital Bem Estar", cidade="Belo Horizonte", uf="MG", tipo="Geral", credenciado=True),
    Row(hospital_id="HP006", nome_hospital="Clínica Não Credenciada X", cidade="Curitiba", uf="PR", tipo="Geral", credenciado=False),
]
spark.createDataFrame(hospitais).write.mode("overwrite").saveAsTable("hospitais")

procedimentos = [
    Row(procedimento_id="PR001", descricao="Consulta médica eletiva", categoria="Consulta", valor_referencia=180.00, carencia_dias=30),
    Row(procedimento_id="PR002", descricao="Hemograma completo", categoria="Exame", valor_referencia=45.00, carencia_dias=30),
    Row(procedimento_id="PR003", descricao="Ressonância magnética", categoria="Exame", valor_referencia=1200.00, carencia_dias=180),
    Row(procedimento_id="PR004", descricao="Cirurgia de apendicite", categoria="Cirurgia", valor_referencia=8500.00, carencia_dias=180),
    Row(procedimento_id="PR005", descricao="Parto normal", categoria="Internação", valor_referencia=6500.00, carencia_dias=300),
    Row(procedimento_id="PR006", descricao="Sessão de fisioterapia", categoria="Terapia", valor_referencia=90.00, carencia_dias=60),
    Row(procedimento_id="PR007", descricao="Internação clínica (diária)", categoria="Internação", valor_referencia=2200.00, carencia_dias=180),
    Row(procedimento_id="PR008", descricao="Tomografia computadorizada", categoria="Exame", valor_referencia=850.00, carencia_dias=180),
]
spark.createDataFrame(procedimentos).write.mode("overwrite").saveAsTable("procedimentos")

print("✅ Tabelas de referência criadas: planos, hospitais, procedimentos")
display(spark.table("planos"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Beneficiários (pacientes)
# MAGIC Usamos Faker com locale `pt_BR` para nomes/cidades realistas. Geramos localmente
# MAGIC e paralelizamos em Spark.

# COMMAND ----------

import random
from datetime import date, timedelta
from faker import Faker
from pyspark.sql import Row

fake = Faker("pt_BR")
Faker.seed(42)
random.seed(42)

plano_ids = [r.plano_id for r in spark.table("planos").select("plano_id").collect()]

def gerar_beneficiario(i):
    nascimento = fake.date_of_birth(minimum_age=0, maximum_age=90)
    adesao = fake.date_between(start_date="-6y", end_date="today")
    return Row(
        beneficiario_id=f"BF{i:06d}",
        nome=fake.name(),
        data_nascimento=nascimento,
        sexo=random.choice(["M", "F"]),
        cidade=fake.city(),
        uf=fake.estado_sigla(),
        plano_id=random.choice(plano_ids),
        data_adesao=adesao,
        ativo=random.random() > 0.08,
    )

beneficiarios = [gerar_beneficiario(i) for i in range(1, N_BENEFICIARIOS + 1)]
(spark.createDataFrame(beneficiarios)
      .write.mode("overwrite").saveAsTable("beneficiarios"))

print(f"✅ {N_BENEFICIARIOS} beneficiários gerados")
display(spark.table("beneficiarios").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Sinistros (eventos de uso)
# MAGIC Cada sinistro liga beneficiário × hospital × procedimento, com valores e status.
# MAGIC Injetamos negativas realistas (carência, fora do rol, hospital não credenciado)
# MAGIC para dar o que analisar e explicar aos agentes.

# COMMAND ----------

import random
from pyspark.sql import Row

benef_ids = [r.beneficiario_id for r in spark.table("beneficiarios").select("beneficiario_id").collect()]
hosp = spark.table("hospitais").collect()
proc = spark.table("procedimentos").collect()

motivos_negativa = [
    "Carência não cumprida",
    "Procedimento fora do rol",
    "Hospital não credenciado",
    "Documentação insuficiente",
    "Limite de utilização excedido",
]

def gerar_sinistro(i):
    b = random.choice(benef_ids)
    h = random.choice(hosp)
    p = random.choice(proc)
    data_evento = fake.date_between(start_date="-2y", end_date="today")
    valor_solicitado = round(float(p.valor_referencia) * random.uniform(0.8, 1.4), 2)

    # Regras sintéticas de negativa.
    status = "Aprovado"
    motivo = None
    r = random.random()
    if not h.credenciado and r < 0.9:
        status, motivo = "Negado", "Hospital não credenciado"
    elif r < 0.12:
        status, motivo = "Negado", random.choice(motivos_negativa)
    elif r < 0.20:
        status, motivo = "Em análise", None

    valor_aprovado = valor_solicitado if status == "Aprovado" else 0.0
    return Row(
        sinistro_id=f"SN{i:07d}",
        beneficiario_id=b,
        hospital_id=h.hospital_id,
        procedimento_id=p.procedimento_id,
        data_evento=data_evento,
        valor_solicitado=valor_solicitado,
        valor_aprovado=valor_aprovado,
        status=status,
        motivo_negativa=motivo,
    )

# Geramos em lote para não estourar memória do driver.
LOTE = 10000
from pyspark.sql.types import (StructType, StructField, StringType, DateType, DoubleType)

schema_sin = StructType([
    StructField("sinistro_id", StringType()),
    StructField("beneficiario_id", StringType()),
    StructField("hospital_id", StringType()),
    StructField("procedimento_id", StringType()),
    StructField("data_evento", DateType()),
    StructField("valor_solicitado", DoubleType()),
    StructField("valor_aprovado", DoubleType()),
    StructField("status", StringType()),
    StructField("motivo_negativa", StringType()),
])

primeiro = True
for inicio in range(1, N_SINISTROS + 1, LOTE):
    fim = min(inicio + LOTE, N_SINISTROS + 1)
    linhas = [gerar_sinistro(i) for i in range(inicio, fim)]
    df = spark.createDataFrame(linhas, schema=schema_sin)
    (df.write.mode("overwrite" if primeiro else "append").saveAsTable("sinistros"))
    primeiro = False

print(f"✅ {N_SINISTROS} sinistros gerados")
display(spark.sql("SELECT status, COUNT(*) AS qtd FROM sinistros GROUP BY status"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Comentários de tabela e coluna (importantes para o Genie!)
# MAGIC O Genie usa metadados para gerar SQL melhor. Documentamos as tabelas.

# COMMAND ----------

comentarios = {
    "beneficiarios": "Beneficiários (pacientes) dos planos de saúde. Um por pessoa.",
    "planos": "Planos de saúde comercializados pela operadora, com segmentação e mensalidade.",
    "hospitais": "Rede de hospitais; coluna credenciado indica se pertence à rede.",
    "procedimentos": "Catálogo de procedimentos médicos com valor de referência e carência em dias.",
    "sinistros": "Eventos de uso do plano (autorizações). status pode ser Aprovado, Negado ou Em análise.",
}
for tabela, texto in comentarios.items():
    spark.sql(f"COMMENT ON TABLE {tabela} IS '{texto}'")

# Chaves primárias/estrangeiras ajudam o Genie a fazer joins.
# PKs no Unity Catalog exigem colunas NOT NULL. DataFrames criados a partir de Row
# inferem tudo como nullable, então marcamos as colunas-chave como NOT NULL primeiro.
colunas_chave = {
    "beneficiarios": "beneficiario_id",
    "planos": "plano_id",
    "hospitais": "hospital_id",
    "procedimentos": "procedimento_id",
    "sinistros": "sinistro_id",
}
for tabela, coluna in colunas_chave.items():
    spark.sql(f"ALTER TABLE {tabela} ALTER COLUMN {coluna} SET NOT NULL")

spark.sql("ALTER TABLE beneficiarios ADD CONSTRAINT pk_benef PRIMARY KEY (beneficiario_id)")
spark.sql("ALTER TABLE planos ADD CONSTRAINT pk_plano PRIMARY KEY (plano_id)")
spark.sql("ALTER TABLE hospitais ADD CONSTRAINT pk_hosp PRIMARY KEY (hospital_id)")
spark.sql("ALTER TABLE procedimentos ADD CONSTRAINT pk_proc PRIMARY KEY (procedimento_id)")
spark.sql("ALTER TABLE sinistros ADD CONSTRAINT pk_sin PRIMARY KEY (sinistro_id)")
spark.sql("ALTER TABLE sinistros ADD CONSTRAINT fk_sin_benef FOREIGN KEY (beneficiario_id) REFERENCES beneficiarios")
spark.sql("ALTER TABLE sinistros ADD CONSTRAINT fk_sin_hosp FOREIGN KEY (hospital_id) REFERENCES hospitais")
spark.sql("ALTER TABLE sinistros ADD CONSTRAINT fk_sin_proc FOREIGN KEY (procedimento_id) REFERENCES procedimentos")

print("✅ Comentários e constraints aplicados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Documentos não estruturados (para RAG / Knowledge Assistant)
# MAGIC Escrevemos manuais e políticas em Markdown no volume. O Knowledge Assistant
# MAGIC (notebook 03) e o Vector Search (notebook 04) vão indexar esses documentos.

# COMMAND ----------

import os
os.makedirs(VOL_PATH, exist_ok=True)

documentos = {
"manual_cobertura.md": """# Manual de Cobertura — Operadora Saúde Vida

## Princípios gerais
A cobertura segue a segmentação contratada no plano. Procedimentos fora da segmentação
não são cobertos. Consulte sempre o rol de procedimentos vigente.

## Segmentações
- **Ambulatorial**: consultas e exames; NÃO cobre internação.
- **Hospitalar**: internações e cirurgias; NÃO cobre obstetrícia.
- **Hospitalar + Obstetrícia**: inclui parto e pré-natal.
- **Referência**: cobertura mais ampla, inclui apartamento e abrangência nacional.

## Exclusões comuns
- Procedimentos estéticos sem indicação clínica.
- Tratamentos experimentais.
- Atendimento em hospital NÃO credenciado (exceto urgência/emergência comprovada).

## Acomodação
Planos com acomodação em Enfermaria não cobrem quarto particular (Apartamento),
salvo upgrade com coparticipação do beneficiário.
""",

"regras_carencia.md": """# Regras de Carência

Carência é o prazo mínimo, a partir da adesão, para uso de determinado procedimento.

| Tipo de procedimento | Carência |
|----------------------|----------|
| Consultas e exames simples | 30 dias |
| Fisioterapia e terapias | 60 dias |
| Exames de imagem complexos (RM, TC) | 180 dias |
| Cirurgias e internações | 180 dias |
| Parto | 300 dias |
| Urgência e emergência | 24 horas |

Casos de urgência e emergência têm carência reduzida de 24 horas, conforme legislação.
Doenças e lesões preexistentes podem ter cobertura parcial temporária de até 24 meses.
""",

"politica_reembolso.md": """# Política de Reembolso

Aplica-se a planos com livre escolha ou atendimento fora da rede em urgência.

## Como solicitar
1. Reúna nota fiscal, relatório médico e comprovante de pagamento.
2. Abra a solicitação no app ou portal do beneficiário em até 60 dias.
3. O reembolso segue a tabela de referência da operadora, não o valor pago.

## Prazos
- Análise: até 30 dias corridos.
- Pagamento após aprovação: até 10 dias úteis.

## Observações
O reembolso nunca ultrapassa o valor de referência do procedimento. Diferenças
são de responsabilidade do beneficiário.
""",

"rol_procedimentos.md": """# Rol de Procedimentos Cobertos

O rol lista os procedimentos com cobertura obrigatória. Exemplos cobertos:
- Consulta médica eletiva (PR001)
- Hemograma completo (PR002)
- Ressonância magnética (PR003) — mediante carência de 180 dias
- Cirurgia de apendicite (PR004)
- Parto normal (PR005) — apenas planos com Obstetrícia
- Sessão de fisioterapia (PR006)
- Internação clínica (PR007)
- Tomografia computadorizada (PR008)

Procedimentos não listados no rol vigente podem ser negados como "fora do rol".
""",

"faq_beneficiario.md": """# FAQ do Beneficiário

**Meu exame foi negado por carência. O que fazer?**
Verifique a data de adesão e a carência do procedimento. Após cumprir o prazo,
reabra a solicitação.

**Posso usar hospital fora da rede?**
Apenas em urgência/emergência. Nesses casos, guarde os comprovantes para reembolso.

**Como funciona a coparticipação?**
Em planos com coparticipação, você paga um percentual por evento (consulta, exame),
além da mensalidade.

**Quanto tempo para autorizar uma cirurgia?**
Procedimentos eletivos são analisados em até 21 dias úteis; urgências, imediatamente.
""",
}

for nome, conteudo in documentos.items():
    with open(f"{VOL_PATH}/{nome}", "w", encoding="utf-8") as f:
        f.write(conteudo)

print(f"✅ {len(documentos)} documentos escritos em {VOL_PATH}")
display(dbutils.fs.ls(VOL_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Resumo
# MAGIC Dados prontos! Temos:
# MAGIC - 5 tabelas Delta estruturadas com comentários e constraints.
# MAGIC - 5 documentos de política/cobertura no volume.
# MAGIC
# MAGIC Próximo: **`02_genie_space`** para perguntar em linguagem natural sobre os sinistros.

# COMMAND ----------

for t in ["beneficiarios", "planos", "hospitais", "procedimentos", "sinistros"]:
    print(f"{t:15s}: {spark.table(t).count():>7,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Conhecendo os dados (consulta analítica) — SOLUÇÃO
# MAGIC Taxa de negativa por plano de saúde.

# COMMAND ----------

df = spark.sql("""
    SELECT p.nome_plano,
           COUNT(*) AS total_sinistros,
           SUM(CASE WHEN s.status = 'Negado' THEN 1 ELSE 0 END) AS negados,
           ROUND(100.0 * SUM(CASE WHEN s.status = 'Negado' THEN 1 ELSE 0 END) / COUNT(*), 1)
               AS taxa_negativa_pct
    FROM sinistros s
    JOIN beneficiarios b ON s.beneficiario_id = b.beneficiario_id
    JOIN planos p ON b.plano_id = p.plano_id
    GROUP BY p.nome_plano
    ORDER BY taxa_negativa_pct DESC
""")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Desafios extras — SOLUÇÃO

# COMMAND ----------

# Top 5 procedimentos por valor total aprovado.
display(spark.sql("""
    SELECT pr.descricao, ROUND(SUM(s.valor_aprovado), 2) AS valor_aprovado_total
    FROM sinistros s
    JOIN procedimentos pr ON s.procedimento_id = pr.procedimento_id
    GROUP BY pr.descricao
    ORDER BY valor_aprovado_total DESC
    LIMIT 5
"""))

# Distribuição por status.
display(spark.sql("""
    SELECT status, COUNT(*) AS qtd
    FROM sinistros GROUP BY status ORDER BY qtd DESC
"""))

# Motivos de negativa mais comuns.
display(spark.sql("""
    SELECT motivo_negativa, COUNT(*) AS qtd
    FROM sinistros WHERE status = 'Negado'
    GROUP BY motivo_negativa ORDER BY qtd DESC
"""))

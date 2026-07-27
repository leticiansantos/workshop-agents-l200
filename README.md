# Workshop: Agentes no Databricks (L200) — Dia inteiro

Workshop hands-on de dia inteiro (~6h) para **data engineers e cientistas de dados**,
demonstrando as capabilities de agentes de IA no Databricks sobre um domínio de **saúde**
(beneficiários, hospitais, planos de saúde, coberturas e sinistros).

Todo o material está em **português** e usa **dados 100% sintéticos**.

---

## O que você vai construir

Ao longo do dia, você constrói progressivamente uma solução de atendimento para uma
operadora de plano de saúde:

1. **Genie Space** — responde perguntas de negócio em linguagem natural sobre sinistros.
2. **Knowledge Assistant** — responde dúvidas sobre regras de cobertura (RAG sobre documentos).
3. **Agente com código (SDK)** — um `ResponsesAgent` com tools (UC Functions + Vector Search)
   deployado em Model Serving.
4. **Multi-Agent Supervisor (MAS)** — orquestra os agentes anteriores.
5. **Avaliação e governança** — MLflow tracing, evaluation, scorers e monitoramento.
6. **Capstone** — um agente de atendimento ao beneficiário juntando tudo.

---

## ⚠️ Ambiente compartilhado — isolamento por usuário

Este workshop foi preparado para rodar num **workspace compartilhado**, com vários
participantes executando os mesmos notebooks ao mesmo tempo. Para evitar que uns
sobrescrevam os dados/assets dos outros:

- **Cada participante trabalha em um schema próprio**: `saude_<seu_usuario>`, derivado
  automaticamente de `current_user()` no início de cada notebook (nada a configurar).
- Ficam **isolados por pessoa**: schema, tabelas, volume de documentos, UC Functions,
  índice de Vector Search, modelo registrado no UC, endpoint de deploy e experimento MLflow.
- Ficam **compartilhados** (recursos caros, seguros para multiusuário): o **catálogo**
  `workshop_agentes` e o **endpoint de Vector Search** `workshop_vs_endpoint`.
- Os assets de UI (Genie Space, Knowledge Assistant, Multi-Agent Supervisor) devem ser
  criados com **o seu nome no título** — os notebooks imprimem o nome sugerido.
- O arquivo do agente com código é gerado em `/tmp/workshop_agentes/<seu_usuario>/` com
  nome de módulo único, evitando colisão no driver compartilhado.

> Resultado: dá para 30 pessoas rodarem `00`→`07` simultaneamente sem pisar umas nas outras.

---

## Pré-requisitos

- Workspace Databricks com **Unity Catalog** e **Serverless** habilitados.
- Permissão para criar catálogo/schema (ou um schema já provisionado).
- Acesso a **Foundation Model APIs** (ex.: `databricks-meta-llama-3-3-70b-instruct` /
  `databricks-claude-sonnet`).
- **Agent Bricks** habilitado no workspace (para Genie, KA e MAS).
- Familiaridade com Python e SQL.

---

## Agenda do dia

### MANHÃ (~3h) — Fundamentos e no-code/low-code

| Bloco | Duração | Tema | Notebook |
|:-----:|:-------:|------|----------|
| 0 | 20 min | Boas-vindas, panorama de agentes e arquitetura no Databricks | slides |
| 1 | 30 min | Setup do ambiente + geração de dados sintéticos de saúde | `00`, `01` |
| 2 | 40 min | Agent Bricks: **Genie Space** — NL → SQL sobre sinistros | `02` |
| ☕ | 15 min | Café | — |
| 3 | 45 min | Agent Bricks: **Knowledge Assistant** — Q&A sobre coberturas | `03` |
| 4 | 20 min | Discussão: quando usar no-code vs. código | slides |

### 🍽️ ALMOÇO (~60 min)

### TARDE (~3h) — Código, orquestração e produção

| Bloco | Duração | Tema | Notebook |
|:-----:|:-------:|------|----------|
| 5 | 60 min | **Agente com código (SDK)**: tools/UC Functions, RAG, deploy | `04` |
| ☕ | 15 min | Café | — |
| 6 | 30 min | **Multi-Agent Supervisor** orquestrando os agentes | `05` |
| 7 | 40 min | **Avaliação** (MLflow), tracing, scorers e monitoramento | `06` |
| 8 | 30 min | **Capstone**: agente de atendimento ao beneficiário | `07` |
| 9 | 15 min | Encerramento, governança e próximos passos | slides |

---

## Estrutura do repositório

```
workshop-agentes-saude/
├── README.md                     ← este guia do facilitador
├── slides/
│   └── workshop-agentes.html     ← deck de apresentação (abrir no navegador)
├── notebooks/
│   ├── 00_setup.py               ← catálogo, schema, volume, verificações
│   ├── 01_dados_sinteticos.py    ← geração dos dados de saúde
│   ├── 02_genie_space.py         ← Genie Space (NL → SQL)
│   ├── 03_knowledge_assistant.py ← Knowledge Assistant (RAG)
│   ├── 04_agente_codigo.py       ← ResponsesAgent + tools + deploy
│   ├── 05_multi_agent.py         ← Multi-Agent Supervisor
│   ├── 06_avaliacao.py           ← MLflow tracing + evaluation
│   └── 07_capstone.py            ← projeto guiado final
├── respostas/                    ← notebooks COMPLETOS (gabarito, sem lacunas)
│   ├── 00_setup.py … 07_capstone.py
│   └── README.md
├── dados/
│   └── README.md                 ← dicionário de dados do domínio de saúde
└── tasks/                        ← notas de planejamento do facilitador
```

---

## 🧩 Exercícios com o Databricks Assistant

Os notebooks da pasta `notebooks/` têm **lacunas** marcadas com **`🧩 EXERCÍCIO`**. Cada
lacuna traz:
- o **objetivo** do trecho a completar,
- um **prompt sugerido** para o **Databricks Assistant** (o ícone **✨** que aparece no canto
  de cada célula do notebook),
- **dicas** sobre armadilhas comuns.

**Como resolver cada exercício:**
1. Leia a instrução `🧩 EXERCÍCIO` acima da célula.
2. Clique no **✨ Assistant** na célula com o `🧩 TODO` e cole/adapte o prompt sugerido.
3. Revise o código gerado, rode a célula e confira o resultado.
4. Travou? A solução funcional está no notebook correspondente em **`respostas/`**.

> Os exercícios ficam nos pontos **conceitualmente centrais** de cada notebook (regras de
> negócio, UC Functions, scorers, roteamento do supervisor). A infraestrutura (setup,
> isolamento por usuário, deploy) já vem pronta para não travar a turma.

Onde está cada exercício:

| Notebook | Exercício |
|----------|-----------|
| `01` | Regras de negativa dos sinistros (lógica de negócio) |
| `02` | Consultar o Genie por código (Conversation API) |
| `03` | Consultar o Knowledge Assistant por API (payload `input`) |
| `04` | Criar a UC Function `carencia_procedimento` (tool do agente) |
| `05` | Roteador do Multi-Agent Supervisor |
| `06` | Scorer `Guidelines` de diretriz de negócio |
| `07` | Capstone — desafios abertos (estender o agente) |

---

## Como importar os notebooks

Os arquivos `.py` estão no formato **Databricks source** (`# Databricks notebook source`)
e importam diretamente como notebooks:

1. No workspace: **Workspace → Import → File** e selecione cada `.py`, ou
2. Faça upload da pasta inteira via **Repos** / Git, ou
3. Use a CLI:
   ```bash
   databricks workspace import-dir notebooks /Workspace/Users/<voce>/workshop-agentes
   ```

Execute os notebooks **na ordem** (`00 → 07`). Todos herdam as variáveis de catálogo/schema
definidas no `00_setup.py`.

Para a turma: distribua a pasta **`notebooks/`** (com exercícios). Mantenha **`respostas/`**
como gabarito do facilitador (ou libere ao final).

---

## Dicas para o facilitador

- **Rode o `00` e `01` antes do workshop** se quiser economizar tempo — a geração de dados
  leva alguns minutos. Alternativamente, deixe os participantes rodarem para experimentarem
  o fluxo.
- Os blocos de Agent Bricks (`02`, `03`, `05`) têm passos de **UI** — reserve tempo para
  telas. Os notebooks trazem o passo a passo e a alternativa via API/SDK.
- Mantenha um **workspace de referência** com tudo já construído, caso alguém trave.
- O bloco de **avaliação** (`06`) é o grande diferencial "produção" — não corte se atrasar;
  prefira encurtar o capstone.

---

## Créditos e segurança

- Dados sintéticos gerados com Faker/Spark; qualquer semelhança com dados reais é coincidência.
- Não insira PII/dados reais de pacientes em nenhum momento.

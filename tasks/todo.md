# Workshop: Agentes no Databricks (L200) — DIA INTEIRO (~6h)

## Objetivo
Workshop hands-on de dia inteiro (~3h manhã + ~3h tarde) para data engineers/cientistas
mostrando as capabilities de agentes no Databricks, usando um domínio de **saúde**
(pacientes, hospitais, planos de saúde, coberturas, sinistros). Material 100% em português.

## Público
Data engineers / cientistas de dados (sabem Python e SQL).

## Entregáveis
- [x] Estrutura de pastas do workshop
- [x] Guia do facilitador (README.md) com agenda minuto a minuto
- [x] Deck de slides (HTML self-contained)
- [x] Notebook 00 — Setup e catálogo
- [x] Notebook 01 — Geração de dados sintéticos de saúde
- [x] Notebook 02 — Agent Bricks: Genie Space (NL → SQL)
- [x] Notebook 03 — Agent Bricks: Knowledge Assistant (RAG sobre coberturas)
- [x] Notebook 04 — Agente com código: ResponsesAgent + UC Functions + Vector Search
- [x] Notebook 05 — Multi-Agent Supervisor (MAS)
- [x] Notebook 06 — Avaliação e governança: MLflow tracing + evaluation
- [x] Notebook 07 — Projeto guiado (capstone): agente de atendimento ao beneficiário
- [x] Guia de dados sintéticos (schema)

## Agenda dia inteiro (~360 min de conteúdo + intervalos)

### MANHÃ (~3h) — Fundamentos e no-code/low-code
| Bloco | Min | Tema |
|-------|-----|------|
| 0 | 20 | Boas-vindas, panorama de agentes e arquitetura no Databricks |
| 1 | 30 | Setup do ambiente + geração de dados sintéticos de saúde |
| 2 | 40 | Agent Bricks: Genie Space — NL → SQL sobre sinistros |
| — | 15 | Café |
| 3 | 45 | Agent Bricks: Knowledge Assistant — Q&A sobre regras de cobertura |
| 4 | 20 | Discussão: quando usar no-code vs código |

### ALMOÇO (~60 min)

### TARDE (~3h) — Código, orquestração e produção
| Bloco | Min | Tema |
|-------|-----|------|
| 5 | 60 | Agente com código (SDK): tools/UC Functions, RAG, deploy em Model Serving |
| — | 15 | Café |
| 6 | 30 | Multi-Agent Supervisor orquestrando os agentes |
| 7 | 40 | Avaliação (MLflow), tracing, scorers e monitoramento em produção |
| 8 | 30 | Capstone: agente de atendimento ao beneficiário (hands-on livre) |
| 9 | 15 | Encerramento, governança, próximos passos |

## Revisão
Workshop completo entregue:
- Guia do facilitador (README) com agenda de dia inteiro minuto a minuto.
- Deck de 13 slides HTML (navegação por teclado/botões).
- 8 notebooks em formato Databricks source (`00`→`07`), executáveis em ordem.
- Dicionário de dados do domínio de saúde.
- Cobertura das capabilities pedidas: Agent Bricks (Genie, KA, MAS), agente com
  código (ResponsesAgent + UC Functions + Vector Search + deploy) e avaliação/governança
  (MLflow tracing, evaluate, scorers, monitors, UC).

Pendências para o facilitador validar no workspace real:
- Nomes exatos de endpoints de Foundation Models (Llama/GTE) podem variar por workspace.
- APIs do Agent Bricks (KA/MAS) têm passos de UI; SDK evolui — confirmar antes do evento.
- Rodar `00`/`01` previamente para economizar tempo.

## Lições
Ver tasks/lessons.md.

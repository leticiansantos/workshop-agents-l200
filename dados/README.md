# Dados sintéticos — Domínio de Saúde

Todos os dados usados no workshop são **sintéticos** e gerados no Notebook `01`.
Nenhum dado real de paciente é utilizado. O domínio simula uma operadora de plano de
saúde brasileira.

## Catálogo e schema (ambiente compartilhado)
Para evitar conflitos entre participantes rodando ao mesmo tempo, **cada pessoa usa um
schema próprio** derivado do seu login:

- Catálogo (compartilhado): `workshop_agentes`
- Schema (exclusivo): `saude_<seu_usuario>` — ex.: `saude_ana_souza`
- Volume: `/Volumes/workshop_agentes/saude_<seu_usuario>/documentos`

O slug do usuário é calculado deterministicamente a partir de `current_user()` no início
de cada notebook. Tabelas, funções, índice de Vector Search, modelo e deploy ficam todos
dentro desse schema — logo, isolados por pessoa. **Apenas** o catálogo e o *endpoint* de
Vector Search (`workshop_vs_endpoint`) são compartilhados.

## Tabelas (dados estruturados)

### `beneficiarios`
Beneficiários (pacientes) dos planos de saúde.
| coluna | tipo | descrição |
|--------|------|-----------|
| beneficiario_id | STRING | ID único do beneficiário |
| nome | STRING | Nome completo (sintético) |
| data_nascimento | DATE | Data de nascimento |
| sexo | STRING | M / F |
| cidade | STRING | Cidade de residência |
| uf | STRING | Unidade federativa |
| plano_id | STRING | FK para `planos` |
| data_adesao | DATE | Data de entrada no plano |
| ativo | BOOLEAN | Se o beneficiário está ativo |

### `planos`
Planos de saúde comercializados.
| coluna | tipo | descrição |
|--------|------|-----------|
| plano_id | STRING | ID único do plano |
| nome_plano | STRING | Nome comercial |
| segmentacao | STRING | Ambulatorial, Hospitalar, Hospitalar+Obstetrícia, Referência |
| abrangencia | STRING | Municipal, Estadual, Nacional |
| acomodacao | STRING | Enfermaria, Apartamento |
| mensalidade_base | DECIMAL(10,2) | Mensalidade base |
| coparticipacao | BOOLEAN | Se há coparticipação |

### `hospitais`
Rede credenciada.
| coluna | tipo | descrição |
|--------|------|-----------|
| hospital_id | STRING | ID único |
| nome_hospital | STRING | Nome |
| cidade | STRING | Cidade |
| uf | STRING | UF |
| tipo | STRING | Geral, Especializado, Maternidade, Pronto-socorro |
| credenciado | BOOLEAN | Se está na rede credenciada |

### `procedimentos`
Catálogo de procedimentos (inspirado no rol ANS, sintético).
| coluna | tipo | descrição |
|--------|------|-----------|
| procedimento_id | STRING | Código do procedimento |
| descricao | STRING | Descrição |
| categoria | STRING | Consulta, Exame, Cirurgia, Terapia, Internação |
| valor_referencia | DECIMAL(10,2) | Valor de referência |
| carencia_dias | INT | Carência em dias |

### `sinistros`
Sinistros / autorizações (eventos de uso do plano).
| coluna | tipo | descrição |
|--------|------|-----------|
| sinistro_id | STRING | ID único |
| beneficiario_id | STRING | FK para `beneficiarios` |
| hospital_id | STRING | FK para `hospitais` |
| procedimento_id | STRING | FK para `procedimentos` |
| data_evento | DATE | Data do evento |
| valor_solicitado | DECIMAL(10,2) | Valor solicitado |
| valor_aprovado | DECIMAL(10,2) | Valor aprovado |
| status | STRING | Aprovado, Negado, Em análise |
| motivo_negativa | STRING | Motivo quando negado (carência, fora do rol, etc.) |

## Documentos não estruturados (para RAG / Knowledge Assistant)
Gerados como Markdown/PDF no volume `documentos/`:
- `manual_cobertura.md` — regras gerais de cobertura e exclusões
- `regras_carencia.md` — prazos de carência por tipo de procedimento
- `politica_reembolso.md` — como funciona o reembolso
- `rol_procedimentos.md` — descrição do rol de procedimentos cobertos
- `faq_beneficiario.md` — perguntas frequentes

## Relacionamentos
```
planos 1───N beneficiarios 1───N sinistros N───1 procedimentos
                                     │
                                     └──N───1 hospitais
```

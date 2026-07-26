# Lições — Workshop Agentes Databricks

- Workshop começou como 2h30 e virou **dia inteiro** (~3h manhã + ~3h tarde). Sempre
  confirmar duração/formato antes de dimensionar a agenda.
- Domínio definido pelo usuário: **saúde** (beneficiários, hospitais, planos, coberturas,
  sinistros). Ancorar todos os notebooks nesse dicionário de dados único (`dados/README.md`).
- Material 100% em português; identificadores técnicos em inglês (padrão do perfil global).
- **Ambiente compartilhado**: isolar por usuário via `current_user()` → schema
  `saude_<slug>`. Pontos que causariam conflito e foram tratados:
  - schema/tabelas/volume/UC Functions/índice VS/modelo/deploy → todos dentro do schema do usuário;
  - `%%writefile agente_saude.py` (nome fixo no driver) → trocado por geração de arquivo
    por usuário em `/tmp/workshop_agentes/<slug>/` com módulo único e schema injetado;
  - experimento MLflow `/Shared/...` → trocado por `/Users/<usuario>/...`;
  - após `dbutils.library.restartPython()` é preciso RECALCULAR o slug (estado é perdido);
  - catálogo e endpoint de Vector Search permanecem compartilhados (caros, multiusuário seguro).

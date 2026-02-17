1. **experience_global_rank**
   - Ranking global de jogadores baseado em `level` e `experience`.
   - Ordena por `level` desc, `experience` desc e `name` asc.
   - Atualizada incrementalmente a cada execução, com `updated_at` e `snapshot_date`.

2. **skills_global_rank**
   - Ranking global de skills por categoria (`category`) de cada jogador.
   - Ordena por `skill_level` desc e `name` asc dentro de cada categoria.
   - Particionada por `snapshot_date`, permitindo histórico diário.

3. **world_summary**
   - Resumo de jogadores por `world`, `world_type` e `vocation`.
   - Calcula `players_count` (total de jogadores distintos).
   - Atualizada com timestamp `updated_at`.

4. **experience_progression**
   - Evolução de `level` e `experience` de cada jogador ao longo do tempo.
   - Calcula:
     - `previous_level` e `previous_experience`
     - `level_gain` e `experience_gain`
     - `days_between_updates`
     - `avg_xp_per_day`
   - Permite análises de progressão histórica e comparativa de jogadores.

5. **skills_progression**
   - Evolução das skills de cada jogador (`skill_level`) ao longo do tempo.
   - Calcula:
     - `skill_before` e `skill_after`
     - `skill_gain`
     - `days_between_updates`
     - `avg_skill_per_day`
   - Permite análise detalhada de progressão por skill e jogador.

#### Características da camada Gold:

- **Agregações e rankings prontos**:
  - Rankings globais de experiência e skills.
  - Resumos por world e vocação.
  - Evolução temporal de níveis e skills de cada jogador.

- **Versionamento e histórico completo**:
  - Todas as tabelas são Iceberg + Nessie, com time travel e `current_timestamp` em cada registro.
  - Permite consultar estados antigos, comparações entre rodadas e auditoria.

- **Atualização incremental**:
  - Apenas registros novos ou modificados são inseridos.
  - Registros antigos são preservados com histórico ou marcados como `is_current = false`.

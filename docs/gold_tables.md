1. **experience_global_rank**
   - Ranking global de jogadores baseado em `level` e `experience`.
   - Ordena por `level` desc, `experience` desc e `name` asc.
   - Atualizada incrementalmente a cada execução, com `updated_at` e `snapshot_date`.
   - 
Exemplo de tabela `experience_global_rank`:

| Rank | Name                | Vocation       | World     | Level | Experience       | WorldType | UpdatedAt           |
|------|-------------------|----------------|-----------|-------|----------------|-----------|-------------------|
| 1    | Khaos Poderoso     | Master Sorcerer | Rasteibra | 2515  | 264,738,322,692 | Open PvP  | 2026-02-10 12:00  |
| 2    | Goa Luccas         | Master Sorcerer | Inabra    | 2357  | 217,738,829,108 | Open PvP  | 2026-02-10 12:00  |
| 3    | Syriz              | Master Sorcerer | Thyria    | 2189  | 174,396,658,081 | Open PvP  | 2026-02-10 12:00  |
| 4    | Dany Ellmagnifico  | Master Sorcerer | Inabra    | 2160  | 167,580,849,914 | Open PvP  | 2026-02-10 12:00  |
| 5    | Zonatto Bombinhams | Master Sorcerer | Honbra    | 2132  | 161,212,779,898 | Open PvP  | 2026-02-10 12:00  |


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

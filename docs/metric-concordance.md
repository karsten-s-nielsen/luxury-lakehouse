# Cross-Layer Metric Concordance

Maps domain metrics across layers: bronze columns, dbt gold columns, code variables, UI labels, and glossary keys. Ensures consistent naming across the platform.

## Core Metrics

| Metric | Bronze Column | dbt Gold Column | Code Variable | UI Label | Glossary Key |
|--------|---------------|-----------------|---------------|----------|--------------|
| Expected Goals | `shot_statsbomb_xg` | `statsbomb_xg` | `xg` | "xG" | `xG (Expected Goals)` |
| Expected Threat | `xt_value` | `xt_value` | `xt` | "xT" | `xT (Expected Threat)` |
| Off-Ball xT | `off_ball_xt` | `total_off_ball_xt`, `avg_off_ball_xt` | `off_ball_xt` | "Off-Ball xT" | `Off-Ball xT` |
| VAEP Value | `vaep_value` | `vaep_value` | `vaep` | "VAEP" | `VAEP` |
| DEFCON Credits | `defcon_value` | `total_pressure` | `defcon` | "DEFCON" | `DEFCON` |
| PAUSA Score | `pausa_score` | `avg_pausa`, `median_pausa` | `pausa` | "PAUSA" | `PAUSA` |
| OBSO (actual) | `actual_obso` | `actual_obso` | `obso` | "OBSO" | `OBSO` |
| Cosine Distance | n/a (computed at query time) | n/a (pgvector `<=>` operator) | `distance` | "Cosine Distance" | `Cosine Distance` |

## Derived / Aggregated Metrics

| Metric | dbt Gold Column | UI Label | Glossary Key |
|--------|-----------------|----------|--------------|
| VAEP per 90 | `vaep_per_90` | "VAEP/90" | `VAEP/90` |
| Offensive VAEP/90 | `off_vaep_per_90` | "Off. VAEP/90" | `Off. VAEP/90` |
| Defensive VAEP/90 | `def_vaep_per_90` | "Def. VAEP/90" | `Def. VAEP/90` |
| DEFCON per 90 | `defcon_per_90` | "DEFCON/90" | `DEFCON/90` |
| Goals per 90 | `goals_per_90` | "Goals/90" | `Goals/90` |
| Passes per 90 | `passes_per_90` | "Passes/90" | `Passes/90` |
| Pass Completion | `pass_pct` | "Pass %" | `Pass %` |
| xG Over-performance | `goals - total_xg` | "xG Over-performance" | `xG Over-performance` |
| Passes with Value | `passes_with_value` | "Passes with Value" | `Passes with Value` |

## DEFCON Credit Categories

| Category | dbt Gold Column | UI Label | Glossary Key |
|----------|-----------------|----------|--------------|
| Intercept | `intercept_credit` | "Intercept" | `Intercept` |
| Concede | `concede_credit` | "Concede" | `Concede` |
| Disturb | `disturb_credit` | "Disturb" | `Disturb` |
| Deter | `deter_credit` | "Deter" | `Deter` |

## PAUSA Sub-Components

| Component | Bronze Column | dbt Gold Column | UI Label | Glossary Key |
|-----------|---------------|-----------------|----------|--------------|
| Temporal Judgment | `temporal_judgment` | `temporal_judgment` | "Temporal Judgment" | `Temporal Judgment` |
| Spatial Selection | `spatial_selection` | `spatial_selection` | "Spatial Selection" | `Spatial Selection` |

## Embedding Vectors

| Vector | Storage | Dimension | UI Label | Glossary Key |
|--------|---------|-----------|----------|--------------|
| Behavioral | `behavioral_vector` (pgvector) | 128d | "Behavioral Vector" | `Behavioral Vector` |
| Statistical | `statistical_vector` (pgvector) | 13d | "Statistical Vector" | `Statistical Vector` |

## Team Shape Metrics

| Metric | dbt Gold Column | UI Label | Glossary Key |
|--------|-----------------|----------|--------------|
| Convex Hull Area | `hull_area` | "Convex Hull" | `Convex Hull` |
| Stretch Index | `stretch_index` | "Stretch Index" | `Stretch Index` |
| Team Length | `team_length` | "Team Length" | `Team Length` |
| Team Width | `team_width` | "Team Width" | `Team Width` |
| Defensive Line Height | `defensive_line_height` | "Defensive Line Height" | `Defensive Line Height` |
| Inter-Line Gaps | `inter_line_gaps` | "Inter-Line Gaps" | `Inter-Line Gaps` |

## Match & Event Metrics

| Metric | dbt Gold Column | UI Label | Glossary Key |
|--------|-----------------|----------|--------------|
| Brier Score | n/a (computed in app) | "Brier Score" | `Brier Score` |
| Pitch Control | n/a (computed live) | "Pitch Control" | `Pitch Control` |
| Line-Breaking Pass | `is_line_breaking` | "Line-Breaking Pass" | `Line-Breaking Pass` |
| Progressive Pass | `is_progressive` | "Progressive Pass" | `Progressive Pass` |
| PPDA | n/a (computed in app) | "PPDA" | `PPDA` |
| Most Active Zone | n/a (computed in app) | "Most Active Zone" | `Most Active Zone` |
| Percentile Rank | n/a (computed in app) | "Percentile Rank" | `Percentile Rank` |
| SPADL | n/a (format) | "SPADL" | `SPADL` |

## Formation Detection

| Metric | dbt Gold Column | UI Label | Glossary Key |
|--------|-----------------|----------|--------------|
| EFPI | `formation_label` | "EFPI" | `EFPI` |
| Shape Graph | n/a (graph structure) | "Shape Graph" | `Shape Graph` |

## Workflow Operations

| Concept | UI Label | Glossary Key |
|---------|----------|--------------|
| Cost Tier | "Cost Tier" | `Cost Tier` |
| Freshness SLA | "Freshness SLA" | `Freshness SLA` |
| Trigger | "Trigger" | `Trigger` |
| Workflow Card | "Workflow Card" | `Workflow Card` |
| Workflow Status | "Workflow Status" | `Workflow Status` |

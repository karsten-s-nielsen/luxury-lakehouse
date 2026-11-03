# Pining fixtures (recorded public-tier answers)

These files are answers of the pining-for-the-data API's **public tier** (documented public token), recorded on
2026-10-27. They hold public data only: no private entry, id or value is in this folder (spec §23.4). Owner-tier
entries in the tests are synthetic (`tests/support/piningkit.py`: invented ids and names on the observed field set).

| File | Source | Change |
|---|---|---|
| `idsse_matches_public.json` | `GET /v1/idsse/matches` | none (the 7 public IDSSE entries) |
| `skillcorner_matches_public.json` | `GET /v1/skillcorner/matches` | none (the 20 public SkillCorner entries) |
| `skillcorner_players_public.json` | `GET /v1/skillcorner/players` | 14 of the 307 public records: the first 12 and 2 without `dob` |

The content behind these entries is public: IDSSE (CC BY 4.0, Bassek et al. 2025; the pining files are
byte-identical to HF `pysport/idsse-data`) and SkillCorner Open Data (MIT; the pining `match.json` and tracking files
are byte-identical to GitHub `SkillCorner/opendata`, while its dynamic events and phases of play are an older version
of the opendata files, measured 2026-10-27).

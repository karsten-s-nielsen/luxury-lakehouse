# Truth set (spec §15.4, §15.5)

Ids only: one CSV per entity type (`players.csv`, `teams.csv`, `competitions.csv`, `matches.csv`), with the columns
`provider_a, native_id_a, provider_b, native_id_b, label, labelled_by, labelled_at, evidence_ref`. The files hold no
names, so the licence rule of §23.4 holds.

- `label` is `match` or `non_match`.
- `labelled_by` is the owner (the `truthset_owner` setting). `validate_truthset` refuses any other labeller.
- `evidence_ref` points to the evidence of the label, for example `decision:<id>` for a hand-checked queue decision.
  A link accepted as recommended never enters the truth set (§15.6).

The files are empty until plan Task 34 (Gate G-E): the owner labels the candidate pairs after an independent
investigation, and the rows from the owner's hand-checked decisions are added after the owner confirms that list.
`tests/live/test_resolution_quality.py` skips with a visible reason until then.

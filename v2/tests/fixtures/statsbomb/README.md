# StatsBomb fixtures (synthetic)

Every value in these files is invented. The StatsBomb open-data licence is `NOASSERTION` (the repository holds a
`LICENSE.pdf`), so the repository holds no real StatsBomb content (spec §23.4).

- **Shape.** The field paths come from a sample of the open data read on 2026-10-27: competitions, the match lists of
  the three default seasons (9/281, 43/3, 43/106), and the events, lineups and 360 frames of 3 matches per season. The
  events file holds one synthetic event per event-type object that the sample showed.
- **Ids and names.** The default competition and season ids and names (1. Bundesliga 2023/2024, FIFA World Cup 2018
  and 2022) are facts that `defaults.yaml` uses. All match, team, player and person ids and names are invented.
- **Proof of completeness.** These files prove only that the parser maps the observed shape. The live tier
  (`tests/live/test_statsbomb_live.py`, marker `live`) is the proof on real data (spec §12.2).
- **Commercial shape (pining owner tier).** `commercial_events_9100001.json`, `commercial_lineups_9100001.json`,
  `commercial_three_sixty_9100001.json` and `commercial_metadata_9100001.json` are the open-data fixtures with the
  fields that the pining owner tier's commercial deliveries (provenance `original`) add, on the field paths, value
  kinds and text forms measured on 2026-10-27 (36 matches; only paths, kinds and counts were read, never a value).
  Every added value is invented. `events_9100001.json` also gained `goalkeeper.success_out`, an open-data field
  that the 291 redistributed open-data matches showed.

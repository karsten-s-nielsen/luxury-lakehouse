# Gradient Sports fixtures (synthetic)

Every value in these files is invented. Gradient Sports (PFF FC) data has no written licence, so the repository holds
no real GS content (spec §23.4).

- **Shape.** The field paths come from the public Drive folder `1_a_q1e9CXeEPJ3GdCv_3-rNO3gPqacfa`, read on
  2026-10-27: the metadata and roster files of all 64 matches, the current root event files (`Event Data/<id>.json`,
  spec §29.15) of matches 3812, 3859 and 10517, and the first 1 MB (11,663 frames) of the same three tracking files.
  Each file here has exactly the field paths of that sample: `tracking_9001.jsonl` holds 5 frame records, including a
  second record of frame 101 (GS writes a frame once per possession event) and a frame without a ball.
- **Ids and names.** Match 9001, teams 9101 and 9102, players 8001 to 8004, competition 77 ("Invented Cup") and
  season 2030 are invented.
- **Values.** Coordinates use 3 decimals, as the source does (millimetres, spec §29.15); `periodElapsedTime` uses 6
  decimals (microseconds).
- **Proof of completeness.** These files prove only that the parser maps the observed shape. The live tier
  (`tests/live/test_gradientsports_live.py`, marker `live`; events and tracking also `heavy`) is the proof on the real
  data (spec §12.2).

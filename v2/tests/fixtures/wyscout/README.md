# Wyscout fixtures (real slices, CC BY 4.0)

These files are small slices of the Wyscout public dataset: Pappalardo, L. and Massucco, E. (2019), "Soccer match
event dataset", figshare collection 4415000 (<https://figshare.com/collections/Soccer_match_event_dataset/4415000>),
described in Pappalardo, L. et al. "A public data set of spatio-temporal match events in soccer competitions",
Scientific Data 6, 236 (2019), doi:10.1038/s41597-019-0247-7. The licence is CC BY 4.0
(<https://creativecommons.org/licenses/by/4.0/>). The files were read on 2026-10-27. Changes from the source files are
listed below. All values are unchanged.

| File | Source file (figshare article) | Change |
|---|---|---|
| `competitions.json` | `competitions.json` (7765316, v4) | none (byte-identical) |
| `teams.json` | `teams.json` (7765310, v3) | none (byte-identical) |
| `eventid2name.csv` | `eventid2name.csv` (11743836, v1) | line endings CRLF to LF |
| `tags2name.csv` | `tags2name.csv` (11743818, v1) | line endings CRLF to LF |
| `matches_World_Cup_slice.json` | `matches_World_Cup.json` in `matches.zip` (7770422, v1) | 2 matches: the final 2058017 and the quarter-final 2058012 (extra time and penalties) |
| `matches_cover.json` | `matches_England.json` in `matches.zip` | 2 matches that add a path or kind the World Cup matches lack: 2500039 (team 1628 writes `"substitutions": "null"`) and 2499985 (no referees) |
| `events_World_Cup_slice.json` | `events_World_Cup.json` in `events.zip` (7770599, v1) | 121 events of the two World Cup matches: the first 60 of each, plus a greedy cover of every path and value kind of the World Cup and European Championship events (for example `subEventId` written as `""`) |
| `players_slice.json` | `players.json` (7765196, v3) | 138 players: every player that the fixture matches and events name, plus a cover of every path and value kind (for example `currentNationalTeamId` written as the text `"null"`) |
| `coaches_slice.json` | `coaches.json` (8082650, v1) | 6 coaches: the coaches of the fixture matches, plus a cover of every path and value kind |
| `figshare_collection.json` | `https://api.figshare.com/v2/collections/4415000/articles` | the recorded answer, trimmed to `id`, `title` and `url` |
| `figshare_articles.json` | `https://api.figshare.com/v2/articles/<id>` for the 10 articles with data | the recorded answers, trimmed to `id`, `title`, `version` and the file fields the adapter reads |

How the slices were made:

- The JSON slices were selected with `json.loads` and written with `json.dumps`, so the spacing differs from the
  source, and every value is the same. The players, teams and matches files escape non-ASCII letters twice
  (`"I. Rakiti\\u0107"`), so the decoded text holds a literal `ć`; `json.dumps` writes it back the same way, and
  bronze keeps it as delivered.
- The covers come from a greedy pass: a record is kept when it adds a field path or a value kind (for example a string
  where the others hold an integer) that the kept records do not hold yet. The contract test
  `tests/contract/test_wyscout_inventory.py` checks that the fixtures hold exactly the fields of the full files.
- The adapter test builds `matches.zip` and `events.zip` from these slices, so the recorded md5 and size of the two zip
  files are replaced by those of the built files.

Facts the tests use:

- Event positions are percentages of the pitch (0 to 100) from the view of the team that performs the event: x is the
  nearness to the opponent's goal and y the nearness to the right side of the field (data paper, "Data Records >
  Events").
- The formations write their counts as text (`"0"`). `goals` holds the text `"null"` in 39,700 of the 74,098 lineup
  and bench entries of the whole dataset, and is never missing or a JSON null.
- The published `referees.json` (article 8082665) stops inside its 627th object (its md5 matches figshare's), so it is
  not read (owner decision, spec §29.15).

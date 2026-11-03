# IDSSE fixtures (real slices, CC BY 4.0)

These files are small slices of the IDSSE dataset: Bassek, M. et al. "An integrated dataset of spatiotemporal and
event data in elite soccer", Scientific Data 12, 195 (2025), doi:10.1038/s41597-025-04505-y. The data owner is the
Deutsche Fußball Liga (DFL); Sportec Solutions collected it. The licence is CC BY 4.0
(<https://creativecommons.org/licenses/by/4.0/>). The files come from the mirror HF `pysport/idsse-data` (original
host: figshare, doi:10.6084/m9.figshare.28196177). They were read on 2026-10-27. Changes from the source files are
listed below. All attribute values are unchanged.

| File | Source file | Change |
|---|---|---|
| `matchinformation_<match>.xml` (7) | `DFL_02_01_matchinformation_DFL-COM-<c>_DFL-MAT-<match>.xml` | none (byte-identical) |
| `events_J03WPY_slice.xml` | `DFL_03_02_events_raw_DFL-COM-000002_DFL-MAT-J03WPY.xml` | the first 40 events, plus a greedy cover of every element and attribute path (69 of 1504 events) |
| `events_<match>_cover.xml` (6) | the events file of each other match | only the events that add a path the other fixtures do not hold |
| `positions_J03WPY_slice.xml` | `DFL_04_03_positions_raw_observed_DFL-COM-000002_DFL-MAT-J03WPY.xml` | the header and every FrameSet, with the first 50 frames of each |
| `hf_tree.json` | `https://huggingface.co/api/datasets/pysport/idsse-data/tree/main?recursive=true` | none (the recorded listing) |

How the slices were made:

- **Positions.** A streaming byte scan of the 370 MB file (constant memory, no XML tree). It keeps every line outside
  a FrameSet and the first 50 `<Frame>` lines of each FrameSet. The slice has the same structure as the file: all 55
  FrameSets (53 players and the ball in two game sections). Substitutes' FrameSets start late, so the ball has no frame
  at some of their frame numbers in the slice.
- **Events.** Parsed with `defusedxml`; the kept `Event` elements were written back with ElementTree. The line breaks
  between elements are LF, as in the source.

Facts the tests use, measured on all 7 matches (`tests/unit/test_idsse_parser.py::test_coordinate_system_idsse`):

- Events: `X-Position` runs from 0 to the pitch length. In each match, the shots of the team in `KickOff@TeamLeft`
  have a mean X of 88.5 to 93.1, and those of the other team 10.8 to 17.9. So +x points to the goal that
  `KickOff@TeamRight` defends. A team that attacks towards +x takes its `Side="left"`
  throw-ins at Y = 68, so +y is on its left hand. The origin is the bottom-left corner of that view.
- Positions: X runs from -52.5 to 52.5 (centre origin). At the first kick-off frame, the `TeamLeft` players have
  X < 0, and the left back (`LV`) of the team that attacks towards +x has Y > 0. So the axes point the same ways as in
  the events.

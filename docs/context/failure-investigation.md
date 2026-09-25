# Context — Failure Investigation & Discipline

On-demand detail for the Failure Investigation rules in `AGENTS.md`. These are process disciplines; the war-stories below are why each exists.

## Failure Investigation Protocol

- **Three-strikes rule**: Once might be a coincidence, twice is suspicious, three times is a pattern. After the FIRST unexpected failure or hang, investigate the root cause — do not retry the same operation.
- **Investigate before retrying**: When an infrastructure operation fails (warehouse timeout, deploy hang, API error), check service state and logs FIRST. A 2-minute REST API call beats a 14-minute blind retry.
- **Never disappear into long-running commands**: Any command that may take >30 seconds MUST use `run_in_background: true` so the user sees responses while it runs. Poll the output file every 15-30 seconds and report progress. A spinning timer with no text is not feedback — the user must see what is happening.
- **Report findings before fixes**: Present the diagnosis (with evidence) to the user before proposing or implementing a fix. The user decides the approach.
- **Proactively flag patterns**: When the same symptom appears twice, explicitly tell the user "this is a pattern that needs investigation, not another attempt."

## Investigation Discipline

- **Answer the specific questions first**: When given specific investigation questions, answer THOSE questions directly before exploring anything else. Do not go on tangents.
- **"I don't know yet" is acceptable — speculation is not**: If the evidence is insufficient, say so and describe what you need to check next. Never fill gaps with theories presented as findings.
- **Reproduce at the exact conditions**: If you cannot reproduce a reported bug, fixing your reproduction setup is the priority — not theorizing about why it might happen. Wrong viewport, missing data, or wrong interaction sequence means the investigation is incomplete, not that the bug is a mystery.
- **Never declare a root cause without evidence**: Saying "this is a framework bug" or "this is a CSS issue" requires concrete evidence showing the exact mechanism. Without it, say "I haven't found the root cause yet."

"""RawHtml builders for the GK Analytics insight views — ★ BIG STORY callout + honest-secondary strip.

Mirrors state/match_summary_render.py: self-contained <style> + RawHtml (rendered via the
registered content provider). Pure string assembly — no ids reach the output (display names only).
"""

from __future__ import annotations

from services.gk_insight import Verdict

from state.workflows_dag import RawHtml

_STYLE = (
    "<style>"
    ".gka-story{background:rgba(255,215,0,0.08);border:1px solid rgba(255,215,0,0.25);"
    "border-left:3px solid #d9a300;border-radius:6px;padding:13px 16px;font-family:system-ui,sans-serif;}"
    ".gka-story-label{color:#d9a300;font-size:11px;text-transform:uppercase;letter-spacing:.6px;margin-bottom:5px;}"
    ".gka-story-verdict{font-size:15px;font-weight:650;color:#f0a23c;margin-bottom:4px;}"
    ".gka-story-body{font-size:13px;line-height:1.5;color:#dfe7f0;}"
    ".gka-sec{display:grid;grid-template-columns:1fr 1fr;gap:12px;font-family:system-ui,sans-serif;}"
    ".gka-sbox{background:#10161f;border:1px dashed #2c3a4a;border-radius:8px;padding:10px 13px;}"
    ".gka-sk{font-size:10px;letter-spacing:.05em;color:#5d7290;text-transform:uppercase;}"
    ".gka-sv{font-size:17px;font-weight:700;margin:3px 0 2px;color:#c9d1d9;}"
    ".gka-ss{font-size:11px;color:#8b9bb0;}"
    ".gka-low{display:inline-block;font-size:9px;padding:1px 6px;border-radius:8px;margin-left:6px;"
    "background:#2a2030;color:#e8b34a;border:1px solid #6b5320;}"
    "</style>"
)


def render_honest_secondary_html(
    *, ghost_dev: str, ghost_n: str, goals_prevented: str, gp_note: str, low_sample: bool
) -> RawHtml:
    """Shot-facing honest-secondary strip: ghost-positioning deviation + goals-prevented as two
    dashed boxes (thin sample — value ± band, never ranked). The "low sample" badge is driven by the
    mart's low_sample flag (NOT hardcoded). Matches defensive-v4."""
    low_badge = '<span class="gka-low">low sample</span>' if low_sample else ""
    return RawHtml(
        _STYLE
        + '<div class="gka-sec">'
        + '<div class="gka-sbox"><div class="gka-sk">Ghost-positioning deviation</div>'
        + f'<div class="gka-sv">{ghost_dev}</div><div class="gka-ss">{ghost_n}</div></div>'
        + f'<div class="gka-sbox"><div class="gka-sk">Goals prevented{low_badge}</div>'
        + f'<div class="gka-sv">{goals_prevented}</div><div class="gka-ss">{gp_note}</div></div>'
        + "</div>"
    )


def render_big_story_html(verdict: Verdict, *, body: str) -> RawHtml:
    """★ BIG STORY callout: the verdict phrase + a plain-language body (descriptive; any system-fit
    read is an explicit hypothesis, never asserted)."""
    return RawHtml(
        _STYLE
        + '<div class="gka-story">'
        + '<div class="gka-story-label">★ Big story</div>'
        + f'<div class="gka-story-verdict">{verdict.phrase}</div>'
        + f'<div class="gka-story-body">{body}</div>'
        + "</div>"
    )

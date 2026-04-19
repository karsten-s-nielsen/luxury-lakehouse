"""Taipy app — thin orchestrator. Imports state + pages, runs gui."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from admin_api import build_admin_blueprint
from extensions.ll_ext import LlExtLibrary
from flask import Flask
from page_template import PageEntry, build_nav
from pages.action_values import page_config as action_values_config
from pages.action_values import page_md as action_values_page
from pages.conversion_funnel import page_config as funnel_config
from pages.conversion_funnel import page_md as funnel_page
from pages.defensive_valuation import page_config as defensive_impact_config
from pages.defensive_valuation import page_md as defensive_impact_page
from pages.goalkeeper import page_config as goalkeeper_config
from pages.goalkeeper import page_md as goalkeeper_page
from pages.heat_map import page_config as heat_map_config
from pages.heat_map import page_md as heat_map_page
from pages.match_summary import page_config as match_summary_config
from pages.match_summary import page_md as match_summary_page
from pages.movement_analysis import page_config as movement_config
from pages.movement_analysis import page_md as movement_page
from pages.pass_map import page_config as pass_map_config
from pages.pass_map import page_md as pass_map_page
from pages.pass_network import page_config as pass_network_config
from pages.pass_network import page_md as pass_network_page
from pages.pass_timing import page_config as pass_timing_config
from pages.pass_timing import page_md as pass_timing_page
from pages.pitch_control import page_config as pitch_control_config
from pages.pitch_control import page_md as pitch_control_page
from pages.player_radar import page_config as player_radar_config
from pages.player_radar import page_md as player_radar_page
from pages.player_similarity import page_config as player_similarity_config
from pages.player_similarity import page_md as player_similarity_page

# --- Page layouts ---
from pages.shot_map import page_config as shot_map_config
from pages.shot_map import page_md as shot_map_page
from pages.tactical_positions import page_config as tactical_positions_config
from pages.tactical_positions import page_md as tactical_positions_page
from pages.team_shape import page_config as team_shape_config
from pages.team_shape import page_md as team_shape_page
from pages.workflows import page_config as workflows_config
from pages.workflows import page_md as workflows_page
from state.action_values import *  # noqa: F403
from state.conversion_funnel import *  # noqa: F403
from state.defensive_valuation import *  # noqa: F403
from state.goalkeeper import *  # noqa: F403
from state.heat_map import *  # noqa: F403
from state.match_summary import *  # noqa: F403
from state.movement_analysis import *  # noqa: F403
from state.pass_map import *  # noqa: F403
from state.pass_network import *  # noqa: F403
from state.pass_timing import *  # noqa: F403
from state.pitch_control import *  # noqa: F403
from state.player_radar import *  # noqa: F403
from state.player_similarity import *  # noqa: F403

# --- State imports (star import required for Taipy module-level binding) ---
from state.shared import *  # noqa: F403
from state.shot_map import *  # noqa: F403
from state.tactical_positions import *  # noqa: F403
from state.team_shape import *  # noqa: F403
from state.workflows import *  # noqa: F403
from state.workflows import RawHtml
from taipy.gui import Gui
from template import build_root_page


class _JsonFormatter(logging.Formatter):
    """Structured JSON log formatter for container environments."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0]:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry)


_handler = logging.StreamHandler()
_handler.setFormatter(_JsonFormatter())
logging.basicConfig(level=logging.INFO, handlers=[_handler])


# --- RawHtml content provider ---
# Taipy's <|{var}|text|raw|> escapes HTML. Register a content provider so
# <|part|content={var}|> renders RawHtml objects as actual HTML.
def _raw_html_provider(content: RawHtml) -> str:
    """Taipy content provider: renders RawHtml as actual HTML."""
    return content.html


Gui.register_content_provider(RawHtml, _raw_html_provider)

# --- Page registry (ordered) ---
# List order = nav display order. Section headers appear in order of first occurrence.
# The on_navigate callback receives route keys as page_name.
PAGE_REGISTRY: list[PageEntry] = [
    # Match Analysis (alphabetical)
    PageEntry("Conversion-Funnel", funnel_config, funnel_page),
    PageEntry("Heat-Map", heat_map_config, heat_map_page),
    PageEntry("Match-Summary", match_summary_config, match_summary_page),
    PageEntry("Pass-Map", pass_map_config, pass_map_page),
    PageEntry("Pass-Network", pass_network_config, pass_network_page),
    PageEntry("Shot-Map", shot_map_config, shot_map_page),
    # Player Analysis
    PageEntry("Goalkeeper-Analytics", goalkeeper_config, goalkeeper_page),
    PageEntry("Player-Comparison", player_radar_config, player_radar_page),
    PageEntry("Player-Impact", action_values_config, action_values_page),
    PageEntry("Player-Similarity", player_similarity_config, player_similarity_page),
    # Advanced (Defensive Impact last per user preference)
    PageEntry("Movement-Pressing", movement_config, movement_page),
    PageEntry("Pass-Timing", pass_timing_config, pass_timing_page),
    PageEntry("Pitch-Control", pitch_control_config, pitch_control_page),
    PageEntry("Tactical-Positions", tactical_positions_config, tactical_positions_page),
    PageEntry("Team-Shape", team_shape_config, team_shape_page),
    PageEntry("Defensive-Impact", defensive_impact_config, defensive_impact_page),
    # Operations
    PageEntry("AI-ML-Workflows", workflows_config, workflows_page),
]

# Generate nav and root page
_nav_md = build_nav(PAGE_REGISTRY)
root_page = build_root_page(_nav_md)

# Health check page — returns HTTP 200 when Taipy server is alive.
# DB pool connectivity is verified in the background via health_check module.
_HEALTH_PAGE = "<|part|class_name=health-check|OK|>"

# Build Taipy pages dict — Heat-Map must be the first non-root entry
# (Taipy defaults to the first content page on initial load)
pages: dict[str, str] = {"/": root_page}
pages.update({entry.route: entry.markdown for entry in PAGE_REGISTRY})
pages["health"] = _HEALTH_PAGE

if __name__ == "__main__":
    import health_check
    from config import validate_databricks_credentials

    validate_databricks_credentials()
    health_check.start()

    flask_app = Flask("luxury-lakehouse-taipy")
    flask_app.register_blueprint(build_admin_blueprint())

    # Lightbox — click any content image to expand in a full-viewport overlay.
    # Injected via after_request because Taipy markdown strips <script> tags.
    # Accessible: role=dialog, aria-modal, Escape closes, focus trap, figcaption
    # cloned from the page scope marker (.ll-page-scope / [data-role=page-scope])
    # rendered as a prominent header ABOVE the image. Gallery navigation: the
    # user can step through every chart on the page via Left/Right arrow keys
    # or the on-screen prev/next buttons (hidden when only one chart is
    # present). The gallery wraps at both ends.
    _LIGHTBOX_SCRIPT = """<script>
(function(){
  let _previouslyFocused = null;
  let _overlay = null;
  let _overlayImg = null;
  let _gallery = [];
  let _idx = -1;

  function gatherGallery() {
    return Array.from(document.querySelectorAll('.ll-content-row img'));
  }

  function showAt(index) {
    if (!_overlay || !_overlayImg || _gallery.length === 0) return;
    const n = _gallery.length;
    _idx = ((index % n) + n) % n;
    const target = _gallery[_idx];
    _overlayImg.src = target.src;
    _overlayImg.alt = target.alt || '';
  }

  function goNext() { showAt(_idx + 1); }
  function goPrev() { showAt(_idx - 1); }

  function trapFocus(e) {
    if (!_overlay) return;
    if (e.key === 'Escape') { e.preventDefault(); closeOverlay(); return; }
    if (e.key === 'ArrowRight') { e.preventDefault(); goNext(); return; }
    if (e.key === 'ArrowLeft') { e.preventDefault(); goPrev(); return; }
    if (e.key !== 'Tab') return;
    const focusables = _overlay.querySelectorAll('button, [tabindex="0"]');
    if (!focusables.length) return;
    const first = focusables[0];
    const last = focusables[focusables.length - 1];
    if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
    else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
  }

  function closeOverlay() {
    if (!_overlay) return;
    document.removeEventListener('keydown', trapFocus);
    _overlay.remove();
    _overlay = null;
    _overlayImg = null;
    _gallery = [];
    _idx = -1;
    if (_previouslyFocused) { _previouslyFocused.focus(); _previouslyFocused = null; }
  }

  function openOverlay(img) {
    _previouslyFocused = document.activeElement;
    _gallery = gatherGallery();
    _idx = _gallery.indexOf(img);
    if (_idx < 0) _idx = 0;

    const scopeEl = document.querySelector('.ll-page-scope, [data-role="page-scope"]');
    const scopeHtml = scopeEl ? scopeEl.innerHTML : '';

    _overlay = document.createElement('div');
    _overlay.className = 'll-lightbox-overlay';
    _overlay.setAttribute('role', 'dialog');
    _overlay.setAttribute('aria-modal', 'true');
    _overlay.setAttribute('aria-label', 'Enlarged chart view');

    const closeBtn = document.createElement('button');
    closeBtn.className = 'll-lightbox-close';
    closeBtn.setAttribute('aria-label', 'Close (Escape)');
    closeBtn.textContent = '\\u00d7';
    closeBtn.addEventListener('click', closeOverlay);

    const fig = document.createElement('figure');

    // Scope caption FIRST in DOM so it sits above the image inside the
    // flex column. The CSS `.ll-lightbox-caption` handles the prominent
    // bar styling (left-accent, uppercase labels, bold values).
    if (scopeHtml) {
      const caption = document.createElement('figcaption');
      caption.className = 'll-lightbox-caption';
      caption.innerHTML = scopeHtml;
      fig.appendChild(caption);
    }

    _overlayImg = document.createElement('img');
    _overlayImg.src = img.src;
    _overlayImg.alt = img.alt || '';
    fig.appendChild(_overlayImg);

    _overlay.appendChild(closeBtn);
    _overlay.appendChild(fig);

    // On-screen prev/next buttons for gallery navigation. Only render when
    // there is more than one chart to step through.
    if (_gallery.length > 1) {
      const prev = document.createElement('button');
      prev.className = 'll-lightbox-nav ll-lightbox-nav-prev';
      prev.setAttribute('aria-label', 'Previous chart (Left arrow)');
      prev.textContent = '\\u2039';
      prev.addEventListener('click', function(e){ e.stopPropagation(); goPrev(); });
      _overlay.appendChild(prev);

      const next = document.createElement('button');
      next.className = 'll-lightbox-nav ll-lightbox-nav-next';
      next.setAttribute('aria-label', 'Next chart (Right arrow)');
      next.textContent = '\\u203a';
      next.addEventListener('click', function(e){ e.stopPropagation(); goNext(); });
      _overlay.appendChild(next);
    }

    _overlay.addEventListener('click', function(e) {
      if (e.target === _overlay) closeOverlay();
    });

    document.body.appendChild(_overlay);
    document.addEventListener('keydown', trapFocus);
    closeBtn.focus();
  }

  document.addEventListener('click', function(e) {
    const img = e.target;
    if (img.tagName !== 'IMG') return;
    if (!img.closest('.ll-content-row')) return;
    if (_overlay) return;
    e.stopPropagation();
    openOverlay(img);
  });

  document.addEventListener('keydown', function(e) {
    if (e.key !== 'Enter') return;
    const el = document.activeElement;
    if (el && el.tagName === 'IMG' && el.closest('.ll-content-row')) {
      e.preventDefault();
      openOverlay(el);
    }
  });

  const observer = new MutationObserver(function() {
    document.querySelectorAll('.ll-content-row img:not([tabindex])').forEach(function(img) {
      img.setAttribute('tabindex', '0');
    });
  });
  observer.observe(document.body, { childList: true, subtree: true });
})();
</script>"""

    @flask_app.after_request
    def _inject_lightbox(response):  # type: ignore[no-untyped-def]
        if response.content_type and "text/html" in response.content_type:
            html = response.get_data(as_text=True)
            if "</body>" in html and "ll-lightbox-overlay" not in html:
                response.set_data(html.replace("</body>", _LIGHTBOX_SCRIPT + "</body>"))
        return response

    gui = Gui(pages=pages, css_file="style_v2.css", flask=flask_app)
    gui.add_library(LlExtLibrary())
    # Taipy 4.1 stubs miss on_init/on_navigate kwargs; both are valid at runtime.
    gui.run(
        host="0.0.0.0",
        port=7860,
        title="(Right! Luxury!) Lakehouse",
        dark_mode=True,
        use_reloader=False,
        on_init=on_init,  # pyright: ignore[reportCallIssue]
        on_navigate=on_navigate,  # pyright: ignore[reportCallIssue]
        stylekit={
            "color_primary": "#f59e0b",
            "color_secondary": "#d97706",
            "color_paper": "#1a1a2e",
            "color_background": "#0e1117",
            "color_background_dark": "#0a0d12",
            "color_background_light": "#1a1d23",
            "font_family": "Source Sans Pro, -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif",
        },
    )

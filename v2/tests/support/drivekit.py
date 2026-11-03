"""Synthetic Google Drive pages for the Gradient Sports adapter tests (plan Task 18).

The markup mirrors Drive's public `embeddedfolderview` page and its virus-scan confirm page as read on 2026-10-27.
Every id, name and date here is invented: GS data has no licence, so the repository holds no real GS content (§23.4).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

__all__ = ["DriveItem", "confirm_page", "folder_page"]


@dataclass(frozen=True)
class DriveItem:
    item_id: str
    title: str
    modified: str  # as Drive shows it, for example "2/2/24"
    folder: bool = False
    google_doc: bool = False


def _href(item: DriveItem) -> str:
    if item.folder:
        return f"https://drive.google.com/drive/folders/{item.item_id}"
    if item.google_doc:
        return f"https://docs.google.com/document/d/{item.item_id}/edit?usp=drive_web"
    return f"https://drive.google.com/file/d/{item.item_id}/view?usp=drive_web"


def folder_page(title: str, items: Sequence[DriveItem]) -> bytes:
    entries = "".join(
        f'<div class="flip-entry" id="entry-{item.item_id}" tabindex="0" role="link"><div class="flip-entry-info">'
        f'<a href="{_href(item)}" target="_blank"><div class="flip-entry-visual"><div class="flip-entry-visual-card">'
        '<div class="flip-entry-icon"><img src="https://drive-thirdparty.googleusercontent.com/128/type/x" alt=""/>'
        '</div></div></div><div class="flip-entry-list-icon"><img src="https://drive-thirdparty.googleusercontent.com/'
        f'16/type/x" alt=""/></div><div class="flip-entry-title">{item.title}</div></a></div>'
        f'<div class="flip-entry-last-modified"><div>{item.modified}</div>'
        '<div class="flip-entry-last-writer">Unknown user</div></div></div>'
        for item in items
    )
    return (
        f'<!DOCTYPE html><html><head><title>{title}</title><meta http-equiv="content-type" '
        'content="text/html; charset=utf-8"/></head><body><div class="flip-view"><div class="flip-entries">'
        f"{entries}</div></div></body></html>"
    ).encode()


def confirm_page(file_id: str, *, uuid: str = "00000000-0000-4000-8000-000000000000") -> bytes:
    return (
        '<!DOCTYPE html><html><head><title>Google Drive - Virus scan warning</title><meta http-equiv="content-type" '
        'content="text/html; charset=utf-8"/></head><body><div class="uc-main"><div id="uc-text">'
        '<p class="uc-warning-caption">Google Drive can\'t scan this file for viruses.</p><p class="uc-warning-'
        'subcaption"><span class="uc-name-size"><a href="/open?id=x">match.jsonl.bz2</a> (46M)</span> is too large '
        'for Google to scan for viruses.</p><form id="download-form" action="https://drive.usercontent.google.com/'
        'download" method="get"><input type="submit" id="uc-download-link" class="goog-inline-block jfk-button '
        f'jfk-button-action" value="Download anyway"/><input type="hidden" name="id" value="{file_id}">'
        '<input type="hidden" name="export" value="download"><input type="hidden" name="confirm" value="t">'
        f'<input type="hidden" name="uuid" value="{uuid}"></form></div></div></body></html>'
    ).encode()

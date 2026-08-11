"""Which bronze tables are part of the documented contract, and which are not.

Pure core — no I/O beyond reading repo files, no SDK. Imported by the bronze-coverage tests
and by ``sync_bronze_sources_yml.py``, so classifier and generator cannot disagree (the same
fixer-equals-checker property ``scripts/_tf_env_pins.py`` gives the Terraform env pins).

WHY THIS EXISTS
---------------
Enumerating a provider's bronze tables by name prefix picks up things that are not part of the
contract — migration backups, scratch copies. Documenting them would demand hundreds of columns
of ``sources.yml`` for throwaway artifacts; silently dropping them re-creates the gap that let
ADR-064's ``visibility``/``access_tier`` sit undocumented on Gradient Sports, because a table
nothing watches is a table that drifts.

So neither filter nor ignore: **partition**. Every live bronze table in a provider's namespace
is either CONTRACT (documented in that provider's ``sources.yml``, governed by the coverage
gate) or NON-CONTRACT (listed below **with a reason**). A new table is in neither set, which
fails the completeness assertion and forces a conscious decision.

This mirrors ``ingestion.access_tier_backfill.ALL_ACCESS_TIER_TABLES``, whose three-way
partition exists for the same reason and carries the same comment: *"a NEW access_tier table
forces a conscious backfill/defer decision and can never be silently dropped."*

CONTRACT IS DERIVED, NOT DECLARED
---------------------------------
The contract set is parsed from ``sources.yml`` rather than duplicated here. A hand-maintained
copy would be a second source of truth to drift; the only hand-maintained data in this module is
``NON_CONTRACT_TABLES``, and every entry needs a reason.
"""

from __future__ import annotations

import dataclasses
import pathlib

import yaml

PROVIDERS: tuple[str, ...] = (
    "statsbomb",
    "wyscout",
    "idsse",
    "metrica",
    "skillcorner",
    "gradientsports",
)


@dataclasses.dataclass(frozen=True)
class NonContract:
    """A bronze table deliberately outside the documented contract.

    Fields mirror ``.pip-audit-ignores.yml``'s schema, which exists for the same reason: an
    exception with a justification but no condition to revisit it becomes permanent by silence.
    ``review_trigger`` is what makes the exception revisitable rather than forgotten.

    ``TEMPORARY`` adds the one thing a dependency cap does not need — a **deadline**. A cap waits
    on an upstream event that may never arrive; a migration backup is temporally scoped by its
    own reason, so its expiry is a date, and ``test_no_temporary_exclusion_is_overdue`` fires
    when it passes.
    """

    reason: str
    classification: str  # TEMPORARY | PERMANENT
    recorded: str  # ISO date the exclusion was first justified
    review_trigger: str


TEMPORARY = "TEMPORARY"
PERMANENT = "PERMANENT"

# Days a TEMPORARY exclusion may stand before the gate demands a decision. 180 puts the four
# LL2 backups at 2026-10-26 — a real deadline, not a rubber stamp, and far enough out that this
# ships green rather than landing the suite red on a decision only an operator can take.
TEMPORARY_EXCLUSION_MAX_AGE_DAYS = 180

_LL2 = (
    "LL2 Path B migration backup (docs/superpowers/plans/2026-04-29-pr-ll2-path-b-close-out.md). "
    "Verified 2026-08-10: no dbt model, no sources.yml entry, no Terraform reference, no code "
    "reader — the only mention in the repo is that plan. Documenting it would add ~247 columns "
    "of sources.yml for a table nothing consumes."
)
_LL2_TRIGGER = (
    "Operator confirming the LL2 Path B close-out is settled and the backup can be DROPPED. "
    "Classified, NOT endorsed — without this trigger the exclusion would be permanent by silence."
)

# Live bronze tables in a provider's namespace that are deliberately OUTSIDE the contract.
NON_CONTRACT_TABLES: dict[str, NonContract] = {
    t: NonContract(reason=_LL2, classification=TEMPORARY, recorded="2026-04-29", review_trigger=_LL2_TRIGGER)
    for t in (
        "idsse_events_pre_close_out_backup",
        "idsse_events_pre_ll2_backfill",
        "metrica_events_pre_close_out_backup",
        "metrica_events_pre_ll2_backfill",
    )
}

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


def sources_yml_path(provider: str) -> pathlib.Path:
    """Path to a provider's dbt sources file."""
    return _REPO_ROOT / "dbt_project" / "models" / "staging" / provider / f"_{provider}__sources.yml"


def contract_tables(provider: str) -> frozenset[str]:
    """Tables documented in ``provider``'s sources.yml — the set the coverage gate governs."""
    doc = yaml.safe_load(sources_yml_path(provider).read_text(encoding="utf-8"))
    return frozenset(t["name"] for t in doc["sources"][0].get("tables", []))


def namespace_tables(provider: str, live_tables: frozenset[str]) -> frozenset[str]:
    """Live bronze tables belonging to ``provider``.

    Prefix match UNION the documented set — because membership is not purely nominal:
    ``elastic_sync_results`` is an IDSSE table that does not start with ``idsse``. Taking only
    the prefix would drop it; taking only the documented set would make a NEW undocumented
    table invisible, which is the gap this module exists to close.
    """
    return frozenset(t for t in live_tables if t.startswith(provider)) | (contract_tables(provider) & live_tables)


def classify(provider: str, live_tables: frozenset[str]) -> tuple[frozenset[str], frozenset[str], frozenset[str]]:
    """Partition ``provider``'s live namespace into (contract, non_contract, UNCLASSIFIED).

    A non-empty third element is the failure signal: a bronze table exists that is neither
    documented nor deliberately excluded, and somebody must decide which it is.
    """
    ns = namespace_tables(provider, live_tables)
    contract = contract_tables(provider) & ns
    non_contract = frozenset(NON_CONTRACT_TABLES) & ns
    return contract, non_contract, ns - contract - non_contract

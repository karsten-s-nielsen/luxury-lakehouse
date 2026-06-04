"""submit_ac1_oneshot exposes the ghost-GK backend + remote-task-timeout escape hatch."""

from pathlib import Path


def test_oneshot_exposes_backend_and_timeout():
    src = Path("scripts/submit_ac1_oneshot.py").read_text(encoding="utf-8")
    assert "--ghost-gk-backend" in src
    assert "--timeout-seconds" in src
    # backend flows into the compute wheel params; timeout into the submitted task
    assert 'wheel_params += ["--ghost-gk-backend"' in src
    assert "timeout_seconds=args.timeout_seconds" in src

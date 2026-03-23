"""Workflow execution exceptions."""


class WorkflowSkippedError(Exception):
    """Raised when a workflow's skip guard determines all items are already processed."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


class WorkflowFailedError(Exception):
    """Raised when a workflow fails during execution."""


class WorkflowTimeoutError(Exception):
    """Raised when a workflow exceeds its configured timeout."""

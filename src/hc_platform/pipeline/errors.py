"""Pipeline control-flow errors."""

from __future__ import annotations


class SimulatedFailure(RuntimeError):
    """Raised when a run is asked to stop after a stage so recovery can be demonstrated."""


class ReconciliationError(RuntimeError):
    def __init__(self, failed: list[dict[str, str]]):
        self.failed = failed
        super().__init__(f"{len(failed)} reconciliation checks failed")

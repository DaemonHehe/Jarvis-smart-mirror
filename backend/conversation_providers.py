"""Conversation provider contracts shared by cloud and local implementations."""

from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Protocol


EmitEvent = Callable[[dict[str, Any]], Awaitable[None]]
KnowledgeSearch = Callable[[str, int], Awaitable[list[dict[str, Any]]]]


@dataclass
class ConversationResult:
    """Summary of a completed provider session."""

    provider: str
    turns: list[tuple[str, str]] = field(default_factory=list)
    ended_by: str = "idle"


class ConversationProvider(Protocol):
    """Provider interface used by the backend orchestrator."""

    name: str

    async def run(self) -> ConversationResult:
        """Run a conversation until the provider reaches a terminal boundary."""


class LocalConversationProvider:
    """Adapter that preserves the existing local pipeline implementation."""

    name = "local"

    def __init__(self, run_local: Callable[[], Awaitable[None]]):
        self._run_local = run_local

    async def run(self) -> ConversationResult:
        await self._run_local()
        return ConversationResult(provider=self.name, ended_by="turn-complete")

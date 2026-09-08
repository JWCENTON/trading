"""Process-local entry authority shared by all strategy runtimes."""

from __future__ import annotations

import os
from collections.abc import Mapping


EXIT_ONLY_ENV = "WALTRADE_EXIT_ONLY"
EXIT_ONLY_BLOCK_REASON = "EXIT_ONLY_ENTRY_BLOCKED"
_TRUE = frozenset({"1", "true", "yes", "on"})


def exit_only_active(environment: Mapping[str, str] | None = None) -> bool:
    source = os.environ if environment is None else environment
    return str(source.get(EXIT_ONLY_ENV, "0")).strip().lower() in _TRUE


def entry_authority(
    *, is_exit: bool, environment: Mapping[str, str] | None = None
) -> tuple[bool, str]:
    """EXIT_ONLY is fail-closed for entries and never blocks an exit."""
    if is_exit:
        return True, "EXIT_AUTHORITY_PRESERVED"
    if exit_only_active(environment):
        return False, EXIT_ONLY_BLOCK_REASON
    return True, "ENTRY_AUTHORITY_AVAILABLE"

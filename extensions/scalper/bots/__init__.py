"""Measurement bots: declared observation contracts, forward outcomes, stats."""

from .contracts import BOTS, active_specs, spec_by_id
from .observer import BotObservatory

__all__ = ["BOTS", "BotObservatory", "active_specs", "spec_by_id"]

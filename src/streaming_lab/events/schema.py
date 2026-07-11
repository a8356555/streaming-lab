"""Event domain: append-only e-commerce orders.

Two immutable event types. A cancellation is a NEW row referencing the same
``order_id`` and amount, never an UPDATE (see SPEC seam-as-append-only, §3.2 C).
GMV is the algebraic sum: created contributes +amount, cancelled -amount.

Money is carried as a string on the wire and parsed to Decimal at both ends so
the "exactly equal" correctness assertions are meaningful (no float drift).
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

ORDER_CREATED = "order_created"
ORDER_CANCELLED = "order_cancelled"
EVENT_TYPES = (ORDER_CREATED, ORDER_CANCELLED)


@dataclass(frozen=True)
class OrderEvent:
    event_id: str
    order_id: str
    event_type: str
    amount: Decimal
    user_id: int
    event_time_ms: int

    def gmv_delta(self) -> Decimal:
        """Signed contribution to GMV."""
        if self.event_type == ORDER_CREATED:
            return self.amount
        if self.event_type == ORDER_CANCELLED:
            return -self.amount
        raise ValueError(f"unknown event_type: {self.event_type}")

    def to_wire(self) -> dict[str, Any]:
        # amount as string; ClickHouse JSONEachRow parses "123.45" into Decimal(18,2).
        return {
            "event_id": self.event_id,
            "order_id": self.order_id,
            "event_type": self.event_type,
            "amount": f"{self.amount:.2f}",
            "user_id": self.user_id,
            "event_time_ms": self.event_time_ms,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_wire(), separators=(",", ":"))

    @classmethod
    def from_wire(cls, d: dict[str, Any]) -> "OrderEvent":
        et = d["event_type"]
        if et not in EVENT_TYPES:
            raise ValueError(f"unknown event_type: {et}")
        return cls(
            event_id=str(d["event_id"]),
            order_id=str(d["order_id"]),
            event_type=et,
            amount=Decimal(str(d["amount"])),
            user_id=int(d["user_id"]),
            event_time_ms=int(d["event_time_ms"]),
        )

    @classmethod
    def from_json(cls, line: str) -> "OrderEvent":
        return cls.from_wire(json.loads(line))

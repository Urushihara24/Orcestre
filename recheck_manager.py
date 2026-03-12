from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Iterable, Literal

GroupMode = Literal["sender", "nickname"]


@dataclass(slots=True, frozen=True)
class RecheckPair:
    account_id: int
    target_id: int
    nickname: str


class RecheckQueuePlanner:
    """
    In-memory planner for recheck ordering.

    - mode="sender": group by account_id (sender), optionally shuffle items inside sender.
    - mode="nickname": group by nickname, optionally shuffle nickname groups.
    """

    def __init__(
        self,
        mode: GroupMode = "sender",
        shuffle_groups: bool = False,
        shuffle_inside_group: bool = True,
        seed: int | None = None,
    ) -> None:
        self.mode: GroupMode = mode if mode in {"sender", "nickname"} else "sender"
        self.shuffle_groups = bool(shuffle_groups)
        self.shuffle_inside_group = bool(shuffle_inside_group)
        self._random = random.Random(seed)

        self._group_order: list[str] = []
        self._group_items: dict[str, list[RecheckPair]] = {}

    def _group_key(self, pair: RecheckPair) -> str:
        if self.mode == "sender":
            return f"sender:{int(pair.account_id)}"
        return f"nickname:{str(pair.nickname or '').strip().lower()}"

    def build(self, pairs: Iterable[RecheckPair]) -> None:
        self._group_order.clear()
        self._group_items.clear()
        seen: set[tuple[int, int]] = set()

        for pair in pairs:
            key_pair = (int(pair.account_id), int(pair.target_id))
            if key_pair in seen:
                continue
            seen.add(key_pair)

            gk = self._group_key(pair)
            if gk not in self._group_items:
                self._group_items[gk] = []
                self._group_order.append(gk)
            self._group_items[gk].append(pair)

        if self.shuffle_groups and len(self._group_order) > 1:
            self._random.shuffle(self._group_order)

        if self.shuffle_inside_group:
            for gk in self._group_order:
                items = self._group_items.get(gk, [])
                if len(items) > 1:
                    self._random.shuffle(items)

    def pop_next(self) -> RecheckPair | None:
        while self._group_order:
            gk = self._group_order[0]
            items = self._group_items.get(gk, [])
            if not items:
                self._group_items.pop(gk, None)
                self._group_order.pop(0)
                continue

            pair = items.pop(0)
            if not items:
                self._group_items.pop(gk, None)
                self._group_order.pop(0)
            return pair
        return None

    def pop_many(self, limit: int) -> list[RecheckPair]:
        out: list[RecheckPair] = []
        need = max(0, int(limit))
        while len(out) < need:
            nxt = self.pop_next()
            if nxt is None:
                break
            out.append(nxt)
        return out

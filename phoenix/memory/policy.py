from __future__ import annotations
from typing import Dict, Any, List, Tuple
import random

class ContextualBandit:
    def __init__(self, exploration: float = 0.15):
        self.epsilon = exploration
        self.arms: Dict[str, Dict[str, Dict[str, float]]] = {}
        # structure: arms[family][op] = {"trials": n, "wins": m, "avg_gain": g}

    def _arm_stats(self, family: str, op: str) -> Dict[str,float]:
        fam = self.arms.setdefault(family, {})
        return fam.setdefault(op, {"trials":0.0, "wins":0.0, "avg_gain":0.0})

    def choose(self, family: str, ops: List[str]) -> List[str]:
        if random.random() < self.epsilon:
            random.shuffle(ops)
            return ops
        # exploit first: sort by empirical win-rate then avg gain
        scored = []
        for op in ops:
            s = self._arm_stats(family, op)
            wr = (s["wins"]/s["trials"]) if s["trials"]>0 else 0.0
            scored.append((op, wr, s["avg_gain"]))
        scored.sort(key=lambda x: (x[1], x[2]), reverse=True)
        return [op for op,_,_ in scored]

    def update(self, family: str, op: str, gain: float, guard_ok: bool, threshold: float) -> None:
        s = self._arm_stats(family, op)
        s["trials"] += 1.0
        win = guard_ok and (gain >= threshold)
        if win: s["wins"] += 1.0
        # incremental avg
        s["avg_gain"] = ((s["avg_gain"] * (s["trials"]-1)) + gain) / s["trials"]

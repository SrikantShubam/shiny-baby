from __future__ import annotations
from typing import Dict, Any, List, Tuple

# registry of lightweight transforms; return (new_table, gain_estimate, guard_ok)
# NOTE: in Phase-1A we keep these conservative and mostly no-op unless preconditions are met.

def _copy_table(t: Dict[str,Any]) -> Dict[str,Any]:
    return {
        **t,
        "headers": list(t.get("headers", []) or []),
        "data": [list(r) for r in (t.get("data", []) or [])]
    }

def _guard_min_columns(tbl: Dict[str,Any], min_cols: int = 2) -> bool:
    data = tbl.get("data",[]) or []
    cols = max((len(r) for r in data), default=len(tbl.get("headers",[]) or []))
    return cols >= min_cols

def op_semantic_promote(tbl: Dict[str,Any], params: Dict[str,Any]) -> Tuple[Dict[str,Any], float, bool]:
    # if front-page contact slab: promote first row to header if it has multiple fields
    t = _copy_table(tbl)
    data: List[List[str]] = t.get("data", []) or []
    if not data: return t, 0.0, True
    first = data[0]
    non_empty = [c for c in first if (c or "").strip()]
    if len(non_empty) >= max(3, len(t.get("headers",[]))):  # conservative
        t["headers"] = [c.strip() for c in first]
        t["data"] = data[1:]
        return t, 0.12, _guard_min_columns(t, 2)
    return t, 0.0, True

def op_period_compose(tbl: Dict[str,Any], params: Dict[str,Any]) -> Tuple[Dict[str,Any], float, bool]:
    # stub: if headers contain scattered period markers, keep as-is but give tiny gain as placeholder
    # (real composition lives in surgeon.period; here we only signal willingness)
    return _copy_table(tbl), 0.02, True

def op_row_label_guard(tbl: Dict[str,Any], params: Dict[str,Any]) -> Tuple[Dict[str,Any], float, bool]:
    # ensure we didn't mis-elevate row stubs to headers (no actual change; signals a guard pass)
    return _copy_table(tbl), 0.01, True

REGISTRY = {
    "semantic_promote": op_semantic_promote,
    "period_compose": op_period_compose,
    "row_label_guard": op_row_label_guard,
}

FAMILY_DEFAULTS = {
    "contact_slab": ["semantic_promote","row_label_guard"],
    "period_grid": ["period_compose","row_label_guard"],
    "ledger_stub": ["row_label_guard"],
    "micro_tables": ["semantic_promote"],
    "generic": ["row_label_guard"]
}

def candidates_for_family(family: str) -> List[str]:
    return FAMILY_DEFAULTS.get(family, ["row_label_guard"])

def apply(op: str, tbl: Dict[str,Any], params: Dict[str,Any]) -> Tuple[Dict[str,Any], float, bool]:
    fn = REGISTRY.get(op)
    if not fn: return tbl, 0.0, True
    return fn(tbl, params or {})

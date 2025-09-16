# # #!/usr/bin/env python3
# # # -*- coding: utf-8 -*-
# # """
# # Phoenix Memory Layer (MVP)
# # - JSONL store of patterns: {id, signature, recipe, stats}
# # - Signature: tokens from headers (+period hints) + col count
# # - Similarity: Jaccard over token sets (no extra deps)

# # CLI:
# #   Apply:  python phoenix/memory/memory_layer.py apply \
# #             --patterns out/patterns/patterns.jsonl \
# #             --in out/preproc/sah.preproc_v6_2.json \
# #             --out out/preproc/sah.preproc_v6_2.enhanced.json
# #   Update: python phoenix/memory/memory_layer.py update \
# #             --patterns out/patterns/patterns.jsonl \
# #             --signature_json '{"tokens":["revenue","__has_year__"],"cols":5}' --ok
# # """
# # import argparse, json, re, hashlib
# # from pathlib import Path
# # from typing import Dict, Any, List, Tuple

# # MONTHS = ("jan","feb","mar","apr","may","jun","jul","aug","sep","sept","oct","nov","dec")
# # YEAR_RE = re.compile(r"(?:19|20)\d{2}")

# # def normalize(s: str) -> str:
# #     return re.sub(r"\s+", " ", (s or "").strip().lower())

# # def header_signature(headers: List[str]) -> List[str]:
# #     toks = []
# #     for h in headers or []:
# #         t = normalize(h)
# #         if not t: continue
# #         parts = re.split(r"[^a-z0-9%]+", t)
# #         parts = [p for p in parts if p]
# #         toks.extend(parts)
# #         if any(m in t for m in MONTHS): toks.append("__has_month__")
# #         if YEAR_RE.search(t): toks.append("__has_year__")
# #     return toks

# # def table_signature(table: Dict[str, Any]) -> Dict[str, Any]:
# #     headers = table.get("headers", []) or []
# #     cols = max(len(headers), max((len(r) for r in table.get("data",[]) or []), default=0))
# #     toks = sorted(set(header_signature(headers)))
# #     return {"tokens": toks, "cols": cols}

# # def jaccard(a: List[str], b: List[str]) -> float:
# #     A, B = set(a), set(b)
# #     if not A and not B: return 1.0
# #     return len(A & B) / max(1, len(A | B))

# # def load_patterns(path: Path) -> List[Dict[str, Any]]:
# #     if not path.exists(): return []
# #     out = []
# #     for line in path.read_text(encoding="utf-8").splitlines():
# #         line = line.strip()
# #         if not line: continue
# #         try: out.append(json.loads(line))
# #         except Exception: continue
# #     return out

# # def save_patterns(path: Path, patterns: List[Dict[str, Any]]) -> None:
# #     with path.open("w", encoding="utf-8") as f:
# #         for p in patterns: f.write(json.dumps(p, ensure_ascii=False) + "\n")

# # def best_match(patterns, sig_tokens, cols, thresh=0.8) -> Tuple[float, Dict[str, Any]]:
# #     best = (0.0, None)
# #     for p in patterns:
# #         if abs(p.get("signature",{}).get("cols", 0) - cols) > 2:  # basic guard
# #             continue
# #         sim = jaccard(sig_tokens, p.get("signature",{}).get("tokens", []))
# #         if sim > best[0]: best = (sim, p)
# #     if best[0] >= thresh and best[1] is not None:
# #         return best
# #     return (0.0, None)

# # def apply_recipes(patterns_path: Path, in_preproc: Path, out_path: Path, thresh=0.8) -> None:
# #     patterns = load_patterns(patterns_path)
# #     data = json.loads(in_preproc.read_text(encoding="utf-8"))
# #     tables = data.get("processed", []) or []
# #     applied = 0
# #     for t in tables:
# #         sig = table_signature(t)
# #         sim, patt = best_match(patterns, sig["tokens"], sig["cols"], thresh=thresh)
# #         if patt and patt.get("recipe"):
# #             recipe = patt["recipe"]
# #             headers = t.get("headers", []) or []
# #             # remap
# #             for i, h in enumerate(headers):
# #                 nh = recipe.get("header_remap", {}).get(h.lower())
# #                 if nh: headers[i] = nh
# #             # drops
# #             drop = set(recipe.get("drop_columns_by_index", []))
# #             if drop:
# #                 keep = [j for j in range(len(headers)) if j not in drop]
# #                 t["headers"] = [headers[j] for j in keep]
# #                 t["data"] = [[row[j] for j in keep] for row in (t.get("data",[]) or [])]
# #             t.setdefault("memory_applied", []).append({"pattern_id": patt.get("id"), "similarity": round(sim,3)})
# #             applied += 1
# #     data["memory_applied"] = applied
# #     out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

# # def update_pattern(patterns_path: Path, signature: Dict[str, Any], ok: bool) -> None:
# #     patterns = load_patterns(patterns_path)
# #     for p in patterns:
# #         if p.get("signature",{}) == signature:
# #             s = p.setdefault("stats", {"success":0,"failure":0})
# #             s["success" if ok else "failure"] += 1
# #             save_patterns(patterns_path, patterns); return
# #     patt = {"id": f"sig:{hashlib.md5(json.dumps(signature, sort_keys=True).encode()).hexdigest()[:10]}",
# #             "signature": signature, "recipe": {}, "stats": {"success": 1 if ok else 0, "failure": 0 if ok else 1}}
# #     patterns.append(patt); save_patterns(patterns_path, patterns)

# # def cli():
# #     ap = argparse.ArgumentParser()
# #     sub = ap.add_subparsers(dest="cmd", required=True)
# #     ap_apply = sub.add_parser("apply")
# #     ap_apply.add_argument("--patterns", required=True)
# #     ap_apply.add_argument("--in", dest="infile", required=True)
# #     ap_apply.add_argument("--out", dest="outfile", required=True)
# #     ap_apply.add_argument("--thresh", type=float, default=0.8)

# #     ap_upd = sub.add_parser("update")
# #     ap_upd.add_argument("--patterns", required=True)
# #     ap_upd.add_argument("--signature_json", required=True)
# #     ap_upd.add_argument("--ok", action="store_true")
# #     ap_upd.add_argument("--fail", action="store_true")

# #     args = ap.parse_args()
# #     if args.cmd == "apply":
# #         apply_recipes(Path(args.patterns), Path(args.infile), Path(args.outfile), thresh=args.thresh)
# #         print(json.dumps({"applied": True, "patterns": args.patterns, "in": args.infile, "out": args.outfile}, ensure_ascii=False))
# #     elif args.cmd == "update":
# #         sig = json.loads(args.signature_json)
# #         update_pattern(Path(args.patterns), sig, ok=bool(args.ok and not args.fail))
# #         print(json.dumps({"updated": True, "ok": bool(args.ok and not args.fail)}, ensure_ascii=False))

# # if __name__ == "__main__":
# #     cli()































# from __future__ import annotations
# from typing import Dict, Any, List
# import json, os, pathlib, time

# from .schemas import Signature, Pattern, RecipeRecord, LearningEvent, serialize_event
# from . import matcher, recipes, policy as policy_mod

# # lazy singletons
# _BANDIT = None
# _PATTERNS: List[Dict[str,Any]] = []
# _CFG: Dict[str,Any] = {}
# _EVENTS_PATH: pathlib.Path = pathlib.Path("out/review/learning_events.jsonl")
# _PATTERNS_PATH: pathlib.Path = pathlib.Path("out/patterns/patterns.jsonl")

# def _ensure_dirs(path: pathlib.Path) -> None:
#     path.parent.mkdir(parents=True, exist_ok=True)
#     if not path.exists():
#         path.write_text("", encoding="utf-8")

# def _load_config(config: Any) -> Dict[str,Any]:
#     global _CFG, _EVENTS_PATH, _PATTERNS_PATH
#     if isinstance(config, dict):
#         _CFG = config.get("learning", config)
#     else:
#         _CFG = {}
#     # defaults
#     _CFG.setdefault("enabled", False)
#     _CFG.setdefault("exploration_rate", 0.15)
#     _CFG.setdefault("min_gain", 0.08)
#     _CFG.setdefault("candidate_limit", 6)
#     _CFG.setdefault("guards_required", ["G1","G2","G4"])
#     _CFG.setdefault("events_path", "out/review/learning_events.jsonl")
#     _CFG.setdefault("patterns_path", "out/patterns/patterns.jsonl")

#     _EVENTS_PATH = pathlib.Path(_CFG["events_path"])
#     _PATTERNS_PATH = pathlib.Path(_CFG["patterns_path"])
#     _ensure_dirs(_EVENTS_PATH)
#     _ensure_dirs(_PATTERNS_PATH)
#     return _CFG

# def _load_patterns() -> None:
#     global _PATTERNS
#     _PATTERNS = []
#     try:
#         with _PATTERNS_PATH.open("r", encoding="utf-8") as f:
#             for line in f:
#                 line=line.strip()
#                 if not line: continue
#                 _PATTERNS.append(json.loads(line))
#     except FileNotFoundError:
#         pass
#     except Exception:
#         # patterns file corrupt? treat as empty
#         _PATTERNS = []

# def _bandit() -> policy_mod.ContextualBandit:
#     global _BANDIT
#     if _BANDIT is None:
#         _BANDIT = policy_mod.ContextualBandit(exploration=_CFG.get("exploration_rate", 0.15))
#     return _BANDIT

# def _objective(tbl: Dict[str,Any]) -> Dict[str,float]:
#     # simple objective: tabularity + header completeness + period hint presence
#     data = tbl.get("data", []) or []
#     headers = tbl.get("headers", []) or []
#     cols = max((len(r) for r in data), default=len(headers))
#     if cols <= 1: tab = 0.0
#     else:
#         numerics = []
#         for j in range(cols):
#             vals = [r[j] for r in data if j < len(r) and (r[j] or "").strip()]
#             if not vals: numerics.append(0.0); continue
#             nums = sum(1 for v in vals if any(ch.isdigit() for ch in str(v)))
#             numerics.append(nums/len(vals))
#         purity = sum(max(p, 1-p) for p in numerics)/len(numerics) if numerics else 0.0
#         strong = sum(1 for p in numerics if p>0.7)
#         width = 1.0 if (cols>=2 and strong>=1) else 0.0
#         tab = 0.7*purity + 0.3*width
#     head = sum(1 for h in headers if (h or "").strip())
#     head_norm = min(1.0, head/max(1, cols))
#     period_hint = 1.0 if any(k in " ".join(headers).lower() for k in ("fy","year","mar","jun","sep","dec","as on","ended")) else 0.0
#     return {"tabularity": round(tab,3), "header": round(head_norm,3), "period": period_hint}

# def _guards_ok(tbl: Dict[str,Any]) -> bool:
#     # minimal guards: at least 2 columns and headers length not worse than data width
#     data = tbl.get("data", []) or []
#     cols = max((len(r) for r in data), default=len(tbl.get("headers",[])))
#     if cols < 2: return False
#     if len(tbl.get("headers",[])) > cols*2: return False
#     return True

# def apply(*, table_dict: Dict[str,Any], dossier_name: str, config: Dict[str,Any] | None) -> (Dict[str,Any], Dict[str,Any] | None):
#     """
#     Main entry point called by the Surgeon. Safe: returns original table on any error or if learning disabled.
#     """
#     try:
#         _load_config(config or {})
#         if not _CFG.get("enabled", False):
#             return table_dict, None

#         if not _PATTERNS:
#             _load_patterns()

#         sig = matcher.compute_signature(table_dict)
#         families = [fam for fam,_ in matcher.match_patterns(sig, _PATTERNS, topk=3)]
#         # augment with heuristic hints
#         for h in matcher.family_hints(sig):
#             if h not in families: families.append(h)

#         # assemble candidate ops
#         cand_ops: List[str] = []
#         for fam in families:
#             for op in recipes.candidates_for_family(fam):
#                 if op not in cand_ops:
#                     cand_ops.append(op)
#         cand_ops = cand_ops[: int(_CFG.get("candidate_limit", 6))] or ["row_label_guard"]

#         # score baseline
#         pre = _objective(table_dict)

#         # rank by policy
#         ranked = _bandit().choose(families[0], cand_ops)

#         best_tbl = table_dict
#         best_gain = 0.0
#         best_op = "baseline"
#         best_guard = True

#         for op in ranked:
#             new_tbl, gain_hint, guard_ok = recipes.apply(op, best_tbl, {})
#             post = _objective(new_tbl)
#             gain = (post["tabularity"] - pre["tabularity"]) + 0.25*(post["header"] - pre["header"]) + 0.1*(post["period"] - pre["period"])
#             # prefer real gain over hint
#             eff_gain = max(gain, gain_hint)
#             if guard_ok and eff_gain > best_gain:
#                 best_tbl = new_tbl
#                 best_gain = eff_gain
#                 best_op = op
#                 best_guard = guard_ok

#         # accept if min_gain met and guard passes
#         accepted = (best_op != "baseline") and best_guard and (best_gain >= float(_CFG.get("min_gain", 0.08)))
#         chosen = best_op if accepted else "baseline"
#         final_tbl = best_tbl if accepted else table_dict

#         # update policy
#         _bandit().update(families[0], chosen if chosen!="baseline" else ranked[0], best_gain, True, float(_CFG.get("min_gain", 0.08)))

#         # log event
#         evt = LearningEvent(
#             timestamp=time.time(),
#             dossier=dossier_name,
#             sig_id=sig.id(),
#             family_candidates=families[:3],
#             recipes_tried=[{"op": op} for op in ranked],
#             chosen_recipe=chosen,
#             pre_metrics=pre,
#             post_metrics=_objective(final_tbl),
#             guard_ok=bool(best_guard),
#             outcome=("win" if accepted else ("neutral" if chosen=="baseline" else "fail"))
#         )
#         with _EVENTS_PATH.open("a", encoding="utf-8") as f:
#             f.write(serialize_event(evt) + "\n")

#         return final_tbl, {"chosen_recipe": chosen, "guard_ok": bool(best_guard)}
#     except Exception:
#         return table_dict, None
















#new changes memory layer D
# phoenix/memory/memory_layer.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import json, re, random, time, os
from pathlib import Path
from typing import Any, Dict, List, Tuple

EMAIL_RE  = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
URL_RE    = re.compile(r"(?:(?:https?://)?(?:www\.)?[A-Za-z0-9.-]+\.[A-Za-z]{2,}(?:/[^\s]*)?)", re.I)
PHONE_RE  = re.compile(r"(?:\+?\d[\d\s\-]{7,}\d)")
KV_RE     = re.compile(r"[A-Za-z][A-Za-z\s]{1,30}:")
MONTH_TOK = ("jan","feb","mar","apr","may","jun","jul","aug","sep","sept","oct","nov","dec")
YEAR_RE   = re.compile(r"(?:19|20)\d{2}")
LEX_TOKS  = (
  "registrar","rta","brlm","book running lead manager","lead manager","merchant banker",
  "contact person","company secretary","compliance officer","investor relations",
  "email","e-mail","website","tel","telephone","phone","fax","price band","isin","cin","pan","sebi"
)

def _norm(x: Any) -> str:
    return re.sub(r"\s+"," ",("" if x is None else str(x)).strip())

def _flat_text(table: Dict[str,Any], rows:int=12) -> str:
    ds = table.get("data",[]) or []
    head = ds[:rows]
    return " ".join(" ".join(r) for r in head)

def _tabularity_proxy(rows: List[List[str]]) -> float:
    if not rows: return 0.0
    cols = max((len(r) for r in rows), default=0)
    if cols <= 1: return 0.0
    col_scores=[]
    for j in range(cols):
        vals=[_norm(r[j]) for r in rows if j<len(r)]
        vals=[v for v in vals if v]
        if not vals: col_scores.append(0.0); continue
        nums=sum(bool(re.fullmatch(r"[0-9,.\-()%₹]+",v)) for v in vals)
        frac=max(nums/len(vals), 1-nums/len(vals))
        col_scores.append(frac)
    purity=sum(col_scores)/len(col_scores)
    strong=sum(1 for f in col_scores if f>0.7)
    width=1.0 if (cols>=2 and strong>=1) else 0.0
    return round(0.7*purity+0.3*width,3)

def _signature(table: Dict[str,Any]) -> Dict[str,Any]:
    txt = (_flat_text(table) + " " + " ".join(table.get("headers",[]) or [])).lower()
    data = table.get("data",[]) or []
    sig = {
        "table_type": table.get("table_type"),
        "page_number": table.get("page_number"),
        "tabularity_proxy": _tabularity_proxy(data),
        "email_count": len(EMAIL_RE.findall(txt)),
        "url_count": len(URL_RE.findall(txt)),
        "phone_count": len(PHONE_RE.findall(txt)),
        "kv_label_count": len(KV_RE.findall(txt)),
        "lex_hits": sum(1 for t in LEX_TOKS if t in txt),
        "non_empty_ratio": (sum(1 for r in data for c in r if (c or "").strip()) / max(1,sum(len(r) for r in data))) if data else 0.0,
        "has_period_tokens": (any(m in txt for m in MONTH_TOK) or bool(YEAR_RE.search(txt))),
        "col_count": max((len(r) for r in data), default=0)
    }
    # band: weak heuristic; still useful for policy context
    pn = sig["page_number"] or 9999
    sig["page_band"] = "front" if pn<=2 else ("mid" if pn<=6 else "back")
    sig["anchor_count"] = int(sig["email_count"]>0) + int(sig["url_count"]>0) + int(sig["phone_count"]>0)
    return sig

def _load_patterns(path: Path) -> List[Dict[str,Any]]:
    if not path.exists(): return []
    lines = path.read_text(encoding="utf-8").splitlines()
    out=[]
    for ln in lines:
        ln=ln.strip()
        if not ln: continue
        try: out.append(json.loads(ln))
        except Exception: continue
    return out

def _apply_ops(table: Dict[str,Any], recipe: Dict[str,Any]) -> Dict[str,Any]:
    ops = recipe.get("ops",[])
    data = table.get("data",[]) or []
    headers = table.get("headers",[]) or []

    for op in ops:
        kind = op.get("op")
        if kind == "singlecol_compose":
            # Ensure single text column with a stable header
            headers = ["Contact Block"]
            data = [[_norm(" ".join(r))] for r in data if any((c or "").strip() for c in r)]
        elif kind == "kv_pair_extractor":
            # Pull key:value pairs into columns (best-effort)
            txt = _flat_text({"data":data})
            pairs = re.findall(r"([A-Za-z][A-Za-z\s]{1,30}):\s*([^\n;|]+)", txt)
            if pairs:
                # normalize labels and collapse to one row (conservative)
                labels = []
                values = []
                for k,v in pairs:
                    lab = re.sub(r"\s+"," ",k).strip().title()
                    if lab not in labels:
                        labels.append(lab)
                        values.append(_norm(v))
                headers = labels or headers or ["Contact Block"]
                data = [values] if values else data
        elif kind == "promote_front_fields":
            # If we see Email/Website/Tel in text, surface canonical labels as headers
            txt = _flat_text({"data":data})
            labs = []
            for pat,name in [
                (r"email|e-?mail", "Email"),
                (r"website|web\s*site|url", "Website"),
                (r"tel|telephone|phone|fax", "Telephone"),
                (r"registrar(\s+to\s+the)?\s+(issue|offer)", "Registrar"),
                (r"(lead\s+manager|merchant\s+banker|brlm)", "Lead Manager"),
                (r"(contact\s+person|compliance\s+officer|company\s+secretary)", "Contact")
            ]:
                if re.search(pat, txt, re.I): labs.append(name)
            if labs:
                # Keep data as-is but rename headers to reflect intent
                n = max(1, len(data[0]) if data else 1)
                headers = (labs + [f"Field_{i}" for i in range(max(0, n-len(labs)))])[:n]
        elif kind == "drop_empty_cols":
            thr = float(op.get("threshold",0.30))
            if data:
                keep=[]
                for j in range(len(data[0])):
                    non_empty=sum(1 for r in data if (j<len(r) and (r[j] or "").strip() not in {"","-"}))
                    if non_empty/len(data) >= thr:
                        keep.append(j)
                if keep:
                    data = [[(r[j] if j<len(r) else "") for j in keep] for r in data]
                    headers = [(headers[j] if j<len(headers) else f"Column_{k}") for k,j in enumerate(keep)]
        # else: ignore unknown ops
    out = dict(table)
    out["headers"] = headers
    out["data"] = data
    return out

def _score_candidate(sig: Dict[str,Any], kind:str) -> float:
    # Lightweight scoring that the bandit can bias with exploration.
    base = 0.0
    if kind == "FRONT_CONTACT_SLIM":
        base = 0.35*sig["anchor_count"] + 0.25*sig["lex_hits"] + 0.15*(sig["page_band"]=="front") + 0.15*(sig["kv_label_count"]>=1)
    elif kind == "FRONT_CONTACT_KV":
        base = 0.30*sig["anchor_count"] + 0.35*(sig["kv_label_count"]>=3) + 0.15*(sig["page_band"] in {"front","mid"}) + 0.10*(sig["tabularity_proxy"]<0.35)
    return float(base)

def _write_event(path: Path, evt: Dict[str,Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(evt, ensure_ascii=False) + "\n")

def apply(table_dict: Dict[str,Any], dossier_name: str, config: Dict[str,Any], stage: str="pre_gate") -> Tuple[Dict[str,Any], Dict[str,Any]]:
    """
    Returns (possibly modified table, decision dict).
    decision = {action, family, accept_override(bool), reward_proxy(float)}
    """
    rng = random.Random((hash(dossier_name) ^ int(time.time()) ) & 0xFFFFFFFF)
    patterns_path = Path(config.get("patterns_path","out/patterns/patterns.jsonl"))
    events_path   = Path(config.get("events_path","out/review/learning_events.jsonl"))
    eps           = float(config.get("exploration_rate", 0.15))
    enabled       = bool(config.get("enabled", True))

    sig = _signature(table_dict)
    table_dict = dict(table_dict)
    table_dict["_signature"] = sig  # stash for lineage

    if not enabled:
        return table_dict, {"action":"none","family":None,"accept_override":False,"reward_proxy":0.0}

    # Candidate families (from patterns store; fall back to hardcoded two)
    patterns = _load_patterns(patterns_path)
    families: List[str] = []
    for p in patterns:
        fam = (p.get("family") or p.get("id") or "").upper()
        if fam.startswith("FRONT_CONTACT_SLIM") or fam.startswith("FRONT_CONTACT_KV"):
            families.append(fam.split(".")[0])
    if not families:
        families = ["FRONT_CONTACT_SLIM","FRONT_CONTACT_KV"]

    # Pick action via epsilon-greedy on simple feature-based score
    explore = rng.random() < eps
    scored = [(fam, _score_candidate(sig, fam)) for fam in families]
    scored.sort(key=lambda x: x[1], reverse=True)
    chosen_fam = (rng.choice(families) if explore else (scored[0][0] if scored else "FRONT_CONTACT_SLIM"))

    # Define ops per family (recipe template)
    recipes = {
      "FRONT_CONTACT_SLIM": {"ops":[{"op":"singlecol_compose"},{"op":"promote_front_fields"}]},
      "FRONT_CONTACT_KV":   {"ops":[{"op":"singlecol_compose"},{"op":"kv_pair_extractor"},{"op":"promote_front_fields"}]}
    }

    before = {
        "headers": table_dict.get("headers",[]),
        "data_len": len(table_dict.get("data",[]) or []),
        "non_empty_ratio": sig["non_empty_ratio"],
        "tabularity_proxy": sig["tabularity_proxy"]
    }

    modified = _apply_ops(table_dict, recipes[chosen_fam])
    # Proxy reward: delta in anchor density & header salience proxy
    flat_before = _flat_text(table_dict)
    flat_after  = _flat_text(modified)

    anchor_before = int(bool(EMAIL_RE.search(flat_before))) + int(bool(URL_RE.search(flat_before))) + int(bool(PHONE_RE.search(flat_before)))
    anchor_after  = int(bool(EMAIL_RE.search(flat_after)))  + int(bool(URL_RE.search(flat_after)))  + int(bool(PHONE_RE.search(flat_after)))
    kv_before = len(KV_RE.findall(flat_before))
    kv_after  = len(KV_RE.findall(flat_after))

    reward_proxy = 0.6*max(0, anchor_after - anchor_before) + 0.4*max(0, kv_after - kv_before)

    # Policy can optionally override acceptance for FRONT_PAGE, low-tabularity but high anchors
    accept_override = False
    if stage=="pre_gate" and (table_dict.get("table_type")=="FRONT_PAGE"):
        if sig["tabularity_proxy"] < 0.35 and (sig["anchor_count"]>=2 or sig["kv_label_count"]>=3):
            accept_override = True

    event = {
        "ts": int(time.time()),
        "dossier": dossier_name,
        "page": table_dict.get("page_number"),
        "table_index": table_dict.get("table_index"),
        "family_candidates": [chosen_fam],
        "action": "apply_recipe",
        "stage": stage,
        "signature": sig,
        "before": before,
        "after": {
            "headers": modified.get("headers",[]),
            "data_len": len(modified.get("data",[]) or []),
            "flat_len": len(flat_after)
        },
        "reward_proxy": round(float(reward_proxy),3),
        "accept_override": accept_override
    }
    try:
        _write_event(events_path, event)
    except Exception:
        pass

    decision = {"action":"apply_recipe","family":chosen_fam,"accept_override":accept_override,"reward_proxy":reward_proxy}
    return modified, decision

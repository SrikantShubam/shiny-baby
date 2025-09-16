from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Any, Optional
import hashlib, json, time

@dataclass
class Signature:
    page_no: int
    table_idx: int
    cols: int
    rows: int
    num_frac_per_col: List[float] = field(default_factory=list)
    alpha_frac_per_col: List[float] = field(default_factory=list)
    header_ngrams: List[str] = field(default_factory=list)
    contact_cues: int = 0
    period_cues: int = 0
    row_label_density: float = 0.0
    linebreak_entropy: float = 0.0
    proto_grid_score: float = 0.0
    minhash: List[int] = field(default_factory=list)
    source_extractor: str = ""
    ocr_flag: bool = False

    def id(self) -> str:
        s = json.dumps({
            "p": self.page_no, "i": self.table_idx, "c": self.cols, "r": self.rows,
            "nf": [round(x,3) for x in self.num_frac_per_col[:8]],
            "af": [round(x,3) for x in self.alpha_frac_per_col[:8]],
            "h": self.header_ngrams[:8],
            "cc": self.contact_cues, "pc": self.period_cues,
            "rl": round(self.row_label_density,3),
            "le": round(self.linebreak_entropy,3),
            "pg": round(self.proto_grid_score,3),
            "mh": self.minhash[:8],
            "se": self.source_extractor, "oc": self.ocr_flag
        }, sort_keys=True, ensure_ascii=False)
        return hashlib.md5(s.encode("utf-8")).hexdigest()

@dataclass
class RecipeRecord:
    op: str
    params: Dict[str, Any] = field(default_factory=dict)
    preconditions: Dict[str, Any] = field(default_factory=dict)
    win_rate: float = 0.0
    trials: int = 0
    avg_gain: float = 0.0
    guard_failures: int = 0
    scope: str = "quarantined"  # quarantined|canary|global
    last_success: float = 0.0

@dataclass
class Pattern:
    family: str
    signature_sketch: Dict[str, Any]  # compact fields used for matching
    recipes: List[RecipeRecord]

@dataclass
class LearningEvent:
    timestamp: float
    dossier: str
    sig_id: str
    family_candidates: List[str]
    recipes_tried: List[Dict[str, Any]]
    chosen_recipe: str
    pre_metrics: Dict[str, float]
    post_metrics: Dict[str, float]
    guard_ok: bool
    outcome: str   # win|neutral|fail

def serialize_event(evt: LearningEvent) -> str:
    return json.dumps(asdict(evt), ensure_ascii=False)

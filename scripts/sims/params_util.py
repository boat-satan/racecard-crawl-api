# scripts/sims/params_util.py
import os, json, tomllib

def load_param_file(path: str) -> dict:
    if not path:
        return {}
    p = os.path.expanduser(path)
    if not os.path.isfile(p):
        raise FileNotFoundError(p)
    ext = os.path.splitext(p)[1].lower()
    if ext == ".json":
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    if ext == ".toml":
        with open(p, "rb") as f:
            return tomllib.load(f)
    raise ValueError(f"Unsupported params file extension: {ext} (use .json or .toml)")

def parse_set_overrides(expr: str) -> dict:
    """
    expr 例: "b_dt=17,cK=1.05,base_wake=0.15"
    右辺は float として解釈できれば数値、ダメなら文字列のまま。
    """
    out = {}
    if not expr:
        return out
    parts = [p.strip() for p in expr.split(",") if p.strip()]
    for kv in parts:
        if "=" not in kv:
            continue
        k, v = kv.split("=", 1)
        k = k.strip()
        v = v.strip()
        try:
            if v.lower() in ("true","false"):
                out[k] = (v.lower()=="true")
            else:
                out[k] = float(v) if ("." in v or "e" in v.lower()) else int(v)
        except Exception:
            out[k] = v
    return out

def apply_overrides_to_class(cls, over: dict):
    for k, v in over.items():
        if hasattr(cls, k):
            setattr(cls, k, v)

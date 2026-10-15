"""
Your defense. Implement register(ctx) and a handler per event type.
See ../README.md for the full interface + toolkit reference, and
../RULES.md before you start.

Strategy
--------
* Exactly one metered call per event. A second call never adds information
  (tools are deterministic), and skipping is a bad trade: one credit over
  budget costs 20/budget points, while an event's expected catch is worth
  ~P(fault) * 50/n_faulty points -- several times more.
* Scoring weighs a missed fault ~4-5x heavier than a false alarm
  (0.5/n_faulty vs 0.3/n_clean), so thresholds sit at 2-2.5 sigma instead of
  the published 3-sigma baselines wherever the sigma can be recovered.
* ctx.baseline values are mean +/- 3 sigma of the clean stream. Two-sided
  bounds give mean and sigma directly. For one-sided bounds (only the max is
  published) the clean mean is estimated online as the running median of what
  we have seen (robust to the minority of faulty events), and
  sigma = (max - median) / 3.
* Structural facts (upstream edges, downstream count, payload columns) are
  learned from the stream itself as majority behaviour.
"""
from collections import Counter

from api import Verdict

# A miss costs ~4-5x a false alarm, so one-sided Gaussian metrics alert at
# 2 sigma (clean exceedance ~2.3%) rather than the published 3.
K_SIGMA = 2.0
WARMUP = 3           # observations needed before trusting the running median
CFG = {
    # two-sided baselines: sigma multipliers per side. Faults push volume and
    # amounts *up* (spikes / shifts), so the upper side is tighter.
    "row_hi": 2.5, "row_lo": 3.0, "amt_hi": 2.5, "amt_lo": 3.0,
    # one-sided, roughly Gaussian: sigma multiplier around the running median
    "stale_k": K_SIGMA, "fresh_k": K_SIGMA, "dur_k": K_SIGMA, "dur_lo_k": 3.0, "age_k": K_SIGMA,
    # skewed / absolute-valued metrics: fraction of the published max.
    # |mean_shift_sigma| on clean batches does reach ~0.5 (above the published
    # 0.41), while even "subtle" skew sits at 2+ sigma, so leave headroom.
    "null_frac": 0.9, "feat_frac": 1.5, "cent_frac": 0.9,
    # no baseline at all: robust z on running median/MAD
    "std_z": 4.0,
}


def register(ctx):
    ctx.on("data_batch", check_data_batch)
    ctx.on("contract_checkpoint", check_contract_checkpoint)
    ctx.on("lineage_run", check_lineage_run)
    ctx.on("feature_materialization", check_feature_materialization)
    ctx.on("embedding_batch", check_embedding_batch)


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------

def _median(values):
    # `statistics` is off the sandbox import allowlist (it pulls in `numbers`)
    xs = sorted(values)
    n = len(xs)
    mid = n // 2
    return xs[mid] if n % 2 else (xs[mid - 1] + xs[mid]) / 2.0


def _history(ctx, name):
    return ctx.state.setdefault("hist", {}).setdefault(name, [])


def _upper_threshold(ctx, name, value, bmax, k=K_SIGMA):
    """Threshold for a one-sided metric whose baseline only publishes mean+3sd.
    Records `value` into the history *after* computing the threshold."""
    hist = _history(ctx, name)
    thr = bmax
    if bmax is not None and len(hist) >= WARMUP:
        med = _median(hist)
        if med < bmax:
            sigma = (bmax - med) / 3.0
            thr = med + k * sigma
    hist.append(value)
    return thr


def _robust_z(ctx, name, value, min_n=8):
    """z-score of `value` against the running median/MAD of a metric with no
    published baseline. Returns 0 until enough history exists."""
    hist = _history(ctx, name)
    z = 0.0
    if len(hist) >= min_n:
        med = _median(hist)
        mad = _median(abs(h - med) for h in hist) * 1.4826
        if mad > 0:
            z = (value - med) / mad
    hist.append(value)
    return z


def _two_sided(value, bmin, bmax, k_lo=3.0, k_hi=3.0):
    """Two-sided check using the baseline's own mean/sigma."""
    if bmin is None or bmax is None:
        return False, 0.0
    mean = (bmin + bmax) / 2.0
    sigma = (bmax - bmin) / 6.0
    if sigma <= 0:
        return False, 0.0
    z = (value - mean) / sigma
    return z > k_hi or z < -k_lo, z


def _num(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def _verdict(reasons, pillar):
    if reasons:
        return Verdict(alert=True, confidence=min(1.0, 0.6 + 0.2 * len(reasons)),
                       reason="; ".join(reasons), pillar=pillar)
    return Verdict(alert=False, confidence=0.8, reason="within bounds", pillar=pillar)


def _structural_drift(ctx, name, items):
    """Track a set-valued property; return the majority items missing now."""
    seen = ctx.state.setdefault("sets", {}).setdefault(name, {"n": 0, "c": Counter()})
    missing = []
    if seen["n"] >= 3:
        expected = {k for k, c in seen["c"].items() if c * 2 > seen["n"]}
        missing = sorted(expected - set(items))
    seen["n"] += 1
    seen["c"].update(set(items))
    return missing


# --------------------------------------------------------------------------
# checks pillar
# --------------------------------------------------------------------------

def check_data_batch(payload, ctx):
    b = ctx.baseline
    reasons = []

    cols = payload.get("columns")
    if isinstance(cols, list):
        missing = _structural_drift(ctx, "columns:" + str(payload.get("table")), cols)
        if missing:
            reasons.append(f"columns missing {missing}")

    r = ctx.tools.batch_profile(payload["batch_id"])
    if not isinstance(r, dict) or "error" in r:
        return _verdict(reasons, "checks")

    rows = r.get("row_count")
    if _num(rows):
        bad, z = _two_sided(rows, b.get("row_count_min"), b.get("row_count_max"), CFG["row_lo"], CFG["row_hi"])
        if bad:
            reasons.append(f"row_count {rows} (z={z:.1f})")

    mean_amt = r.get("mean_amount")
    if _num(mean_amt):
        bad, z = _two_sided(mean_amt, b.get("mean_amount_min"), b.get("mean_amount_max"), CFG["amt_lo"], CFG["amt_hi"])
        if bad:
            reasons.append(f"mean_amount {mean_amt} (z={z:.1f})")

    nulls = (r.get("null_rate") or {}).get("customer_id")
    if _num(nulls) and _num(b.get("null_rate_max")) and nulls > b["null_rate_max"] * CFG["null_frac"]:
        reasons.append(f"null_rate {nulls}")

    stale = r.get("staleness_min")
    if _num(stale):
        thr = _upper_threshold(ctx, "staleness", stale, b.get("staleness_min_max"), CFG["stale_k"])
        if thr is not None and stale > thr:
            reasons.append(f"staleness {stale} > {thr:.2f}")

    std_amt = r.get("std_amount")
    if _num(std_amt):
        z = _robust_z(ctx, "std_amount", std_amt)
        if abs(z) > CFG["std_z"]:
            reasons.append(f"std_amount {std_amt} (robust z={z:.1f})")

    return _verdict(reasons, "checks")


# --------------------------------------------------------------------------
# contracts pillar
# --------------------------------------------------------------------------

def check_contract_checkpoint(payload, ctx):
    b = ctx.baseline
    reasons = []
    r = ctx.tools.contract_diff(payload["contract_id"], payload["checkpoint_batch_id"])
    if not isinstance(r, dict) or "error" in r:
        return _verdict(reasons, "contracts")

    violations = r.get("violations") or []
    if violations:
        reasons.append(f"violations {violations}")

    delay = r.get("freshness_delay_min")
    if _num(delay):
        thr = _upper_threshold(ctx, "freshness_delay", delay, b.get("freshness_delay_max_min"), CFG["fresh_k"])
        sla = (payload.get("declared_sla") or {}).get("freshness_min")
        if _num(sla) and delay > sla:
            reasons.append(f"freshness {delay} breaks declared SLA {sla}")
        elif thr is not None and delay > thr:
            reasons.append(f"freshness {delay} > {thr:.2f}")

    return _verdict(reasons, "contracts")


# --------------------------------------------------------------------------
# lineage pillar
# --------------------------------------------------------------------------

def check_lineage_run(payload, ctx):
    b = ctx.baseline
    reasons = []
    r = ctx.tools.lineage_graph_slice(payload["run_id"])
    if not isinstance(r, dict) or "error" in r:
        return _verdict(reasons, "lineage")

    job = str(payload.get("job"))
    upstream = r.get("actual_upstream")
    if isinstance(upstream, list):
        missing = _structural_drift(ctx, "upstream:" + job, upstream)
        if missing:
            reasons.append(f"missing upstream {missing}")
        declared = [i.get("name") for i in payload.get("inputs") or [] if isinstance(i, dict)]
        undeclared = [d for d in declared if d and d not in upstream]
        if undeclared:
            reasons.append(f"declared inputs absent from lineage {undeclared}")

    down = r.get("actual_downstream_count")
    if _num(down):
        hist = _history(ctx, "downstream:" + job)
        expected = _median(hist) if len(hist) >= 3 else 1
        hist.append(down)
        if down <= 0 or down < expected:
            reasons.append(f"downstream_count {down} (expected {expected:g})")

    dur = r.get("duration_ms")
    if _num(dur):
        bmax = b.get("lineage_duration_ms_max")
        hist = _history(ctx, "duration")
        med = _median(hist) if len(hist) >= WARMUP else None
        thr = _upper_threshold(ctx, "duration", dur, bmax, CFG["dur_k"])
        if thr is not None and dur > thr:
            reasons.append(f"duration {dur} > {thr:.0f}")
        elif med is not None and _num(bmax) and med < bmax:
            # abnormally fast runs (e.g. a skipped transform) at 3 sigma
            low = med - CFG["dur_lo_k"] * (bmax - med) / 3.0
            if dur < low:
                reasons.append(f"duration {dur} < {low:.0f}")

    return _verdict(reasons, "lineage")


# --------------------------------------------------------------------------
# ai_infra pillar
# --------------------------------------------------------------------------

def check_feature_materialization(payload, ctx):
    b = ctx.baseline
    reasons = []
    r = ctx.tools.feature_drift(payload["feature_view"], payload["batch_id"])
    if not isinstance(r, dict) or "error" in r:
        return _verdict(reasons, "ai_infra")

    shift = r.get("mean_shift_sigma")
    if not _num(shift) and all(_num(r.get(k)) for k in ("serve_mean", "train_mean", "train_std")) \
            and r["train_std"] > 0:
        shift = abs(r["serve_mean"] - r["train_mean"]) / r["train_std"]
    # |shift| is half-normal, not Gaussian, so scale the published max
    # instead of using the median-based helper (see CFG["feat_frac"]).
    bmax = b.get("feature_mean_shift_sigma_max")
    if _num(shift) and _num(bmax) and shift > bmax * CFG["feat_frac"]:
        reasons.append(f"mean_shift_sigma {shift} > {bmax * CFG['feat_frac']:.3f}")

    return _verdict(reasons, "ai_infra")


def check_embedding_batch(payload, ctx):
    b = ctx.baseline
    reasons = []
    r = ctx.tools.embedding_drift(payload["corpus"], payload["chunk_batch_id"])
    if not isinstance(r, dict) or "error" in r:
        return _verdict(reasons, "ai_infra")

    shift = r.get("centroid_shift")
    bmax = b.get("embedding_centroid_shift_max")
    # right-skewed positive metric: scaled published max, not the Gaussian helper
    if _num(shift) and _num(bmax) and shift > bmax * CFG["cent_frac"]:
        reasons.append(f"centroid_shift {shift} > {bmax * CFG['cent_frac']:.4f}")

    age = r.get("avg_doc_age_days")
    if _num(age):
        thr = _upper_threshold(ctx, "doc_age:" + str(payload.get("corpus")), age,
                               b.get("corpus_avg_doc_age_days_max"), CFG["age_k"])
        if thr is not None and age > thr:
            reasons.append(f"avg_doc_age_days {age} > {thr:.1f}")

    return _verdict(reasons, "ai_infra")

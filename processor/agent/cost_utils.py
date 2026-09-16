"""
cost_utils.py -- token-cost estimation for the LLM calls made in nodes.py.

Why this exists: task 3 asks for a per-PR token-cost line in USD and CAD.
Two numbers drive that estimate (Gemini $/1M tokens, and the USD->CAD rate)
and both of them change over time independently of any code change here, so
neither is hardcoded in this file -- they're read from config/pricing.json,
with an in-code fallback only for the case where that file is missing.

This module knows nothing about LangGraph state or PRs. It only converts a
model name + token counts into a cost. nodes.py calls extract_usage() right
after each LLM call; pr.py calls summarize_run_cost() once, at PR-body build
time, on whatever usage dicts nodes.py has already put into state.
"""
import os
import json

PRICING_CONFIG_PATH = os.environ.get("PRICING_CONFIG_PATH", "config/pricing.json")

# Fallback only -- used if config/pricing.json is missing or malformed, so a
# broken config file degrades to an approximate estimate instead of crashing
# a live run. Keep this roughly in sync with the real file; don't treat it
# as the source of truth for pricing.
_DEFAULT_PRICING = {
    "gemini-2.5-flash": {"input_per_million_usd": 0.30, "output_per_million_usd": 2.50},
    "usd_to_cad_rate": 1.39,
}

_PRICING_CACHE = None


def _load_pricing() -> dict:
    global _PRICING_CACHE
    if _PRICING_CACHE is not None:
        return _PRICING_CACHE
    try:
        with open(PRICING_CONFIG_PATH, "r") as f:
            _PRICING_CACHE = json.load(f)
        print(f"Loaded pricing config from {PRICING_CONFIG_PATH}")
    except Exception as e:
        print(f"WARNING: could not load {PRICING_CONFIG_PATH} ({e!r}); using built-in default pricing")
        _PRICING_CACHE = _DEFAULT_PRICING
    return _PRICING_CACHE


def extract_usage(response) -> dict:
    """Pulls token counts off a langchain-google-genai AIMessage. Returns
    zeros (never None) so downstream summing never has to null-check."""
    usage = getattr(response, "usage_metadata", None) or {}
    return {
        "input_tokens": usage.get("input_tokens", 0) or 0,
        "output_tokens": usage.get("output_tokens", 0) or 0,
    }


def estimate_cost_usd(model_name: str, input_tokens: int, output_tokens: int) -> float:
    pricing = _load_pricing()
    rates = pricing.get(model_name, _DEFAULT_PRICING["gemini-2.5-flash"])
    return (
        input_tokens / 1_000_000 * rates["input_per_million_usd"]
        + output_tokens / 1_000_000 * rates["output_per_million_usd"]
    )


def usd_to_cad(amount_usd: float) -> float:
    pricing = _load_pricing()
    rate = pricing.get("usd_to_cad_rate", _DEFAULT_PRICING["usd_to_cad_rate"])
    return amount_usd * rate


def summarize_run_cost(model_name: str, usages: list) -> dict:
    """usages: list of usage dicts from extract_usage(), one per LLM call
    made during this run (currently: root-cause analysis + fix generation).
    Missing/empty entries are treated as zero rather than skipped, so a
    partially-failed run still reports a (possibly partial) cost instead of
    silently omitting the line."""
    total_in = sum((u or {}).get("input_tokens", 0) for u in usages)
    total_out = sum((u or {}).get("output_tokens", 0) for u in usages)
    cost_usd = estimate_cost_usd(model_name, total_in, total_out)
    cost_cad = usd_to_cad(cost_usd)
    return {
        "input_tokens": total_in,
        "output_tokens": total_out,
        "cost_usd": round(cost_usd, 5),
        "cost_cad": round(cost_cad, 5),
    }
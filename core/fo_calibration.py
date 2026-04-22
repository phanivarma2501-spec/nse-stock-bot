"""Platt-scaling calibration for F&O trade confidence.
Copied from phani-market-v2/core/calibration.py with the settings.PLATT_SCALE
reference swapped for the nse-stock-bot Pydantic Settings object.
"""
import math


def calibrate(raw_probability: float, platt_scale: float = 0.85) -> float:
    """Single-pass Platt scaling. Compresses extreme probabilities toward base rate."""
    if raw_probability is None:
        return None
    p = max(0.01, min(0.99, raw_probability))
    log_odds = math.log(p / (1 - p))
    scaled_log_odds = log_odds * platt_scale
    calibrated = 1 / (1 + math.exp(-scaled_log_odds))
    return max(0.01, min(0.99, calibrated))


def calculate_brier_score(predicted: float, actual: float) -> float:
    """Brier score for a resolved prediction. Lower is better."""
    return (predicted - actual) ** 2

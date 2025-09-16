def confidence(features: dict, weights: dict) -> float:
    # features: {"cv_score":0..1, "grid_consistency":..., "numeric_density":..., "period_detected":0/1, "header_vocab_match":0..1}
    return sum(weights[k] * float(features.get(k, 0.0)) for k in weights)

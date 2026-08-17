"""Statistical transformations shared by SACZ index calculations."""

from math import exp


def logistic_probability(score: float) -> float:
    """Convert a linear model score into a probability without overflow."""
    if score >= 0:
        return 1.0 / (1.0 + exp(-score))

    exponential_score = exp(score)
    return exponential_score / (1.0 + exponential_score)

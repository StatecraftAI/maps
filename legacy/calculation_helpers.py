"""
Calculation Helpers - Common Business Logic Patterns

This module extracts common calculation patterns from processing files
to eliminate duplication and provide consistent calculation logic.

Focuses on reusable calculation patterns, not a complex formula engine.
"""

from typing import List, Optional, Union

import numpy as np
import pandas as pd
from loguru import logger


def calculate_percentage(
    numerator: Union[pd.Series, float, int],
    denominator: Union[pd.Series, float, int],
    handle_zero: bool = True,
    round_digits: int = 1,
) -> Union[pd.Series, float]:
    """
    Safe percentage calculation with zero handling.

    Args:
        numerator: Values to divide (can be Series or scalar)
        denominator: Values to divide by (can be Series or scalar)
        handle_zero: Whether to handle division by zero (returns 0)
        round_digits: Number of decimal places to round to

    Returns:
        Percentage values (numerator/denominator * 100)
    """
    if handle_zero:
        # Handle division by zero
        if isinstance(denominator, pd.Series):
            result = np.where(denominator != 0, (numerator / denominator) * 100, 0)
        else:
            result = (numerator / denominator) * 100 if denominator != 0 else 0
    else:
        result = (numerator / denominator) * 100

    # Round the result
    if isinstance(result, pd.Series):
        return result.round(round_digits)
    else:
        return round(result, round_digits)


def calculate_vote_margin(
    votes_series: pd.DataFrame, candidate_cols: List[str], margin_type: str = "absolute"
) -> pd.Series:
    """
    Calculate vote margins between candidates.

    Args:
        votes_series: DataFrame with vote columns
        candidate_cols: List of candidate vote column names
        margin_type: "absolute" for raw vote difference, "percentage" for margin as % of total

    Returns:
        Series with vote margins
    """
    if len(candidate_cols) < 2:
        logger.warning("Need at least 2 candidates to calculate margin")
        return pd.Series(0, index=votes_series.index)

    # Get the top 2 vote getters for each row
    candidate_votes = votes_series[candidate_cols].fillna(0)

    # Sort candidates by votes for each row and get top 2
    sorted_votes = candidate_votes.apply(lambda row: row.sort_values(ascending=False), axis=1)
    first_place = sorted_votes.iloc[:, 0]
    second_place = sorted_votes.iloc[:, 1] if len(candidate_cols) > 1 else 0

    margin = first_place - second_place

    if margin_type == "percentage" and "votes_total" in votes_series.columns:
        total_votes = votes_series["votes_total"].fillna(1)  # Avoid division by zero
        margin = calculate_percentage(margin, total_votes, handle_zero=True)

    return margin


def calculate_competitiveness(
    margin_pct: Union[pd.Series, float],
    thresholds: Optional[List[float]] = None,
    labels: Optional[List[str]] = None,
) -> Union[pd.Series, str]:
    """
    Categorize elections by competitiveness based on margin thresholds.

    Args:
        margin_pct: Margin percentages
        thresholds: Threshold values for categorization (default: [5, 15])
        labels: Labels for categories (default: ["Close", "Clear", "Safe"])

    Returns:
        Competitiveness categories
    """
    if thresholds is None:
        thresholds = [5, 15]
    if labels is None:
        labels = ["Close", "Clear", "Safe"]

    if len(labels) != len(thresholds) + 1:
        raise ValueError("Number of labels must be one more than number of thresholds")

    # Use absolute margin for competitiveness
    abs_margin = np.abs(margin_pct) if hasattr(margin_pct, "__iter__") else abs(margin_pct)

    if isinstance(margin_pct, pd.Series):
        result = pd.cut(
            abs_margin, bins=[0] + thresholds + [float("inf")], labels=labels, include_lowest=True
        )
        return result.astype(str)
    else:
        for i, threshold in enumerate(thresholds):
            if abs_margin <= threshold:
                return labels[i]
        return labels[-1]


def categorize_by_thresholds(
    values: Union[pd.Series, float],
    thresholds: List[float],
    labels: List[str],
    include_lowest: bool = True,
) -> Union[pd.Series, str]:
    """
    General threshold-based categorization.

    Args:
        values: Values to categorize
        thresholds: Threshold values
        labels: Labels for categories
        include_lowest: Whether to include the lowest threshold

    Returns:
        Categories based on thresholds
    """
    if len(labels) != len(thresholds) + 1:
        raise ValueError("Number of labels must be one more than number of thresholds")

    bins = [-float("inf")] + thresholds + [float("inf")]

    if isinstance(values, pd.Series):
        result = pd.cut(values, bins=bins, labels=labels, include_lowest=include_lowest)
        return result.astype(str)
    else:
        for i, threshold in enumerate(thresholds):
            if values <= threshold:
                return labels[i]
        return labels[-1]


def calculate_turnout_rate(
    votes_cast: Union[pd.Series, float],
    registered_voters: Union[pd.Series, float],
    round_digits: int = 1,
) -> Union[pd.Series, float]:
    """
    Calculate voter turnout rate.

    Args:
        votes_cast: Number of votes cast
        registered_voters: Number of registered voters
        round_digits: Number of decimal places

    Returns:
        Turnout rate as percentage
    """
    return calculate_percentage(
        votes_cast, registered_voters, handle_zero=True, round_digits=round_digits
    )


def calculate_vote_share(
    candidate_votes: Union[pd.Series, float],
    total_votes: Union[pd.Series, float],
    round_digits: int = 1,
) -> Union[pd.Series, float]:
    """
    Calculate candidate vote share.

    Args:
        candidate_votes: Votes for specific candidate
        total_votes: Total votes cast
        round_digits: Number of decimal places

    Returns:
        Vote share as percentage
    """
    return calculate_percentage(
        candidate_votes, total_votes, handle_zero=True, round_digits=round_digits
    )


def find_leading_candidate(votes_df: pd.DataFrame, candidate_cols: List[str]) -> pd.Series:
    """
    Find the leading candidate in each row.

    Args:
        votes_df: DataFrame with vote columns
        candidate_cols: List of candidate vote column names

    Returns:
        Series with leading candidate names
    """
    candidate_votes = votes_df[candidate_cols].fillna(0)

    # Find the column with the maximum value for each row
    leading_candidate = candidate_votes.idxmax(axis=1)

    # Clean up column names (remove "votes_" prefix if present)
    leading_candidate = leading_candidate.str.replace("votes_", "").str.title()

    return leading_candidate


def calculate_density(
    count: Union[pd.Series, float],
    area: Union[pd.Series, float],
    area_unit: str = "km2",
    round_digits: int = 1,
) -> Union[pd.Series, float]:
    """
    Calculate density (count per unit area).

    Args:
        count: Count values (e.g., population, voters)
        area: Area values
        area_unit: Unit of area for labeling
        round_digits: Number of decimal places

    Returns:
        Density values
    """
    if isinstance(area, pd.Series):
        # Handle division by zero
        density = np.where(area > 0, count / area, 0)
        if isinstance(density, pd.Series):
            return density.round(round_digits)
        else:
            return round(density, round_digits)
    else:
        density = count / area if area > 0 else 0
        return round(density, round_digits)


def safe_divide(
    numerator: Union[pd.Series, float], denominator: Union[pd.Series, float], fill_value: float = 0
) -> Union[pd.Series, float]:
    """
    Safe division with configurable fill value for division by zero.

    Args:
        numerator: Values to divide
        denominator: Values to divide by
        fill_value: Value to use when denominator is zero

    Returns:
        Division result with safe handling
    """
    if isinstance(denominator, pd.Series):
        return np.where(denominator != 0, numerator / denominator, fill_value)
    else:
        return numerator / denominator if denominator != 0 else fill_value


def calculate_contribution_percentage(
    part_values: Union[pd.Series, float],
    total_value: Union[pd.Series, float],
    round_digits: int = 1,
) -> Union[pd.Series, float]:
    """
    Calculate what percentage each part contributes to the total.

    Args:
        part_values: Individual part values
        total_value: Total value (sum of all parts)
        round_digits: Number of decimal places

    Returns:
        Contribution percentages
    """
    return calculate_percentage(
        part_values, total_value, handle_zero=True, round_digits=round_digits
    )


def normalize_to_range(
    values: Union[pd.Series, List[float]], min_val: float = 0, max_val: float = 100
) -> Union[pd.Series, List[float]]:
    """
    Normalize values to a specific range.

    Args:
        values: Values to normalize
        min_val: Minimum value of output range
        max_val: Maximum value of output range

    Returns:
        Normalized values
    """
    if isinstance(values, pd.Series):
        values_min = values.min()
        values_max = values.max()

        if values_max == values_min:
            return pd.Series(min_val, index=values.index)

        normalized = (values - values_min) / (values_max - values_min)
        return normalized * (max_val - min_val) + min_val
    else:
        values_min = min(values)
        values_max = max(values)

        if values_max == values_min:
            return [min_val] * len(values)

        return [
            ((v - values_min) / (values_max - values_min)) * (max_val - min_val) + min_val
            for v in values
        ]


# Election-specific helper functions


def calculate_election_metrics(
    votes_df: pd.DataFrame, candidate_cols: List[str], total_col: str = "votes_total"
) -> pd.DataFrame:
    """
    Calculate comprehensive election metrics for a DataFrame.

    Args:
        votes_df: DataFrame with vote data
        candidate_cols: List of candidate vote columns
        total_col: Name of total votes column

    Returns:
        DataFrame with added election metrics
    """
    result_df = votes_df.copy()

    # Calculate vote margins
    result_df["vote_margin"] = calculate_vote_margin(
        votes_df, candidate_cols, margin_type="absolute"
    )

    # Calculate margin percentage
    if total_col in votes_df.columns:
        result_df["margin_pct"] = calculate_percentage(
            result_df["vote_margin"], result_df[total_col], handle_zero=True
        )

        # Calculate competitiveness
        result_df["competitiveness"] = calculate_competitiveness(result_df["margin_pct"])

    # Find leading candidate
    result_df["leading_candidate"] = find_leading_candidate(votes_df, candidate_cols)

    # Calculate vote shares for each candidate
    if total_col in votes_df.columns:
        for col in candidate_cols:
            share_col = col.replace("votes_", "vote_pct_")
            result_df[share_col] = calculate_vote_share(result_df[col], result_df[total_col])

    return result_df


# Example usage and testing
if __name__ == "__main__":
    logger.info("Testing calculation helpers...")

    # Test percentage calculation
    test_pct = calculate_percentage(25, 100)
    logger.info(f"Percentage test: 25/100 = {test_pct}%")

    # Test competitiveness
    test_comp = calculate_competitiveness(3.5)
    logger.info(f"Competitiveness test: 3.5% margin = {test_comp}")

    logger.info("Calculation helpers ready for use!")

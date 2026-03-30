"""
Rule-based data quality engine for Silver layer validation.

Applies declarative quality rules and routes failures to quarantine
tables. Works alongside the entropy framework — rules catch presence
problems, entropy catches distribution problems.

Author: Anthony Johnson | EthereaLogic LLC
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from entropy_governed_medallion.contracts import QualityRuleResult


@dataclass
class QualityRule:
    """Declarative quality rule definition."""
    name: str
    column: str
    check: Callable[[DataFrame, str], DataFrame]  # returns failing rows
    action: str = "QUARANTINE"  # QUARANTINE | REJECT | LOG


class QualityRuleEngine:
    """
    Apply quality rules to a Silver DataFrame and separate
    passing records from quarantined failures.
    """

    def __init__(self, rules: Optional[List[QualityRule]] = None):
        self.rules = rules or []

    def add_rule(self, rule: QualityRule) -> None:
        self.rules.append(rule)

    def evaluate(self, df: DataFrame) -> tuple[DataFrame, DataFrame, List[QualityRuleResult]]:
        """
        Apply all rules. Returns (clean_df, quarantine_df, results).

        Clean DF contains only records that passed ALL rules.
        Quarantine DF contains records that failed ANY rule, with
        a failure_reason column indicating which rule(s) failed.
        """
        total = df.count()
        quarantine_frames = []
        results = []

        remaining = df
        for rule in self.rules:
            failing = rule.check(remaining, rule.column)
            n_fail = failing.count()

            if n_fail > 0:
                tagged = failing.withColumn("failure_reason", F.lit(rule.name))
                quarantine_frames.append(tagged)
                # Remove failing rows from remaining
                remaining = remaining.subtract(failing)

            results.append(QualityRuleResult(
                rule_name=rule.name,
                column_name=rule.column,
                passed=n_fail == 0,
                records_checked=total,
                records_failed=n_fail,
                failure_ratio=round(n_fail / total, 4) if total > 0 else 0.0,
                action_taken=rule.action if n_fail > 0 else "NONE",
            ))

        if quarantine_frames:
            quarantine_df = quarantine_frames[0]
            for extra in quarantine_frames[1:]:
                quarantine_df = quarantine_df.unionByName(extra, allowMissingColumns=True)
        else:
            quarantine_df = df.limit(0).withColumn("failure_reason", F.lit(""))

        return remaining, quarantine_df, results


# --- Pre-built rule checks ---

def check_not_null(df: DataFrame, column: str) -> DataFrame:
    """Return rows where column is null."""
    return df.filter(F.col(column).isNull())


def check_positive(df: DataFrame, column: str) -> DataFrame:
    """Return rows where column is not positive."""
    return df.filter((F.col(column).isNull()) | (F.col(column) <= 0))


def check_valid_date_range(df: DataFrame, column: str) -> DataFrame:
    """Return rows where date is in the future or before 2009."""
    return df.filter(
        (F.col(column).isNull())
        | (F.col(column) > F.current_date())
        | (F.col(column) < F.lit("2009-01-01"))
    )


def check_no_duplicates(df: DataFrame, column: str) -> DataFrame:
    """Return duplicate rows based on column (keeps first occurrence)."""
    from pyspark.sql.window import Window
    w = Window.partitionBy(column).orderBy(F.monotonically_increasing_id())
    ranked = df.withColumn("_dup_rank", F.row_number().over(w))
    dupes = ranked.filter(F.col("_dup_rank") > 1).drop("_dup_rank")
    return dupes

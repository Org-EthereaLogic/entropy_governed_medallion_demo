"""
Bronze target materialization seam.

Plan/execute separation, CREATE TABLE AS SELECT with CDF enabled,
deterministic operations.

Author: Anthony Johnson | EthereaLogic LLC
"""

from __future__ import annotations

from entropy_governed_medallion.contracts import (
    MaterializationOperation,
    MaterializationPlan,
    MaterializationResult,
    SourceTableRef,
    TargetTableRef,
)


class BronzeMaterializationSeam:
    """Plan or execute managed-target creation for Bronze tables."""

    def plan(
        self, *, source: SourceTableRef, target: TargetTableRef
    ) -> MaterializationPlan:
        operations = (
            MaterializationOperation(
                name="ensure_target_schema",
                sql=f"CREATE SCHEMA IF NOT EXISTS {target.catalog}.{target.schema}",
            ),
            MaterializationOperation(
                name="create_managed_target_with_cdf",
                sql=(
                    f"CREATE TABLE IF NOT EXISTS {target.fully_qualified_name} "
                    f"USING DELTA "
                    f"TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true') AS "
                    f"SELECT *, "
                    f"  current_timestamp() AS ingest_ts, "
                    f"  '{source.schema}' AS source_system, "
                    f"  '{source.fully_qualified_name}' AS source_file_path "
                    f"FROM {source.fully_qualified_name}"
                ),
            ),
        )
        return MaterializationPlan(
            source=source,
            target=target,
            write_disposition="create_if_absent",
            operations=operations,
        )

    def execute(
        self,
        *,
        plan: MaterializationPlan,
        gateway,
        dry_run: bool,
    ) -> MaterializationResult:
        target_exists_before = gateway.target_table_exists(plan.target)
        if dry_run:
            return MaterializationResult(
                plan=plan,
                executed=False,
                target_exists_before=target_exists_before,
                target_exists_after=target_exists_before,
                statements_attempted=len(plan.operations),
                statements_executed=0,
            )

        statements_executed = 0
        for operation in plan.operations:
            gateway.execute_sql(operation.sql)
            statements_executed += 1

        target_exists_after = gateway.target_table_exists(plan.target)
        return MaterializationResult(
            plan=plan,
            executed=True,
            target_exists_before=target_exists_before,
            target_exists_after=target_exists_after,
            statements_attempted=len(plan.operations),
            statements_executed=statements_executed,
        )

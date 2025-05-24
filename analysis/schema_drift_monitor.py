#!/usr/bin/env python3
"""
Schema Drift Monitoring System for Election Analysis Pipeline

This module provides comprehensive monitoring capabilities for detecting and tracking
schema drift across data processing runs. It includes historical tracking, alerting,
and detailed analysis reports.
"""

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import geopandas as gpd
from loguru import logger


@dataclass
class SchemaSnapshot:
    """Represents a snapshot of the data schema at a specific point in time."""

    timestamp: str
    total_fields: int
    field_names: List[str]
    field_types: Dict[str, str]
    field_categories: Dict[str, List[str]]
    sample_values: Dict[str, Any]
    schema_hash: str
    data_source: str
    record_count: int
    null_counts: Dict[str, int]
    unique_counts: Dict[str, int]


@dataclass
class SchemaDriftAlert:
    """Represents a schema drift alert with severity and details."""

    timestamp: str
    severity: str  # 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'
    alert_type: str
    title: str
    description: str
    affected_fields: List[str]
    impact_assessment: str
    recommended_actions: List[str]


class SchemaDriftMonitor:
    """
    Advanced schema drift monitoring system with historical tracking and alerting.
    """

    def __init__(self, monitoring_dir: str = "schema_monitoring"):
        self.monitoring_dir = Path(monitoring_dir)
        self.monitoring_dir.mkdir(exist_ok=True)

        # File paths for persistent storage
        self.snapshots_file = self.monitoring_dir / "schema_snapshots.json"
        self.alerts_file = self.monitoring_dir / "schema_alerts.json"
        self.config_file = self.monitoring_dir / "monitor_config.json"

        # Load configuration
        self.config = self._load_config()

        # Initialize storage files if they don't exist
        self._initialize_storage()

        logger.info(f"Schema drift monitor initialized with storage at: {self.monitoring_dir}")

    def _load_config(self) -> Dict[str, Any]:
        """Load monitoring configuration with sensible defaults."""
        default_config = {
            "alert_thresholds": {
                "new_fields_critical": 10,
                "new_fields_high": 5,
                "new_fields_medium": 2,
                "removed_fields_critical": 5,
                "removed_fields_high": 2,
                "removed_fields_medium": 1,
                "type_changes_critical": 3,
                "type_changes_high": 1,
                "null_rate_change_threshold": 0.2,
                "record_count_change_threshold": 0.1,
            },
            "retention_days": 90,
            "auto_cleanup": True,
            "enable_email_alerts": False,
            "enable_slack_alerts": False,
            "monitoring_enabled": True,
        }

        if self.config_file.exists():
            try:
                with open(self.config_file, "r") as f:
                    config = json.load(f)
                # Merge with defaults for any missing keys
                for key, value in default_config.items():
                    if key not in config:
                        config[key] = value
                return config
            except Exception as e:
                logger.warning(f"Failed to load config, using defaults: {e}")

        # Save default config
        with open(self.config_file, "w") as f:
            json.dump(default_config, f, indent=2)

        return default_config

    def _initialize_storage(self) -> None:
        """Initialize storage files if they don't exist."""
        if not self.snapshots_file.exists():
            with open(self.snapshots_file, "w") as f:
                json.dump([], f)

        if not self.alerts_file.exists():
            with open(self.alerts_file, "w") as f:
                json.dump([], f)

    def _categorize_fields(self, gdf: gpd.GeoDataFrame) -> Dict[str, List[str]]:
        """Categorize fields by type and purpose."""
        categories = {
            "identifiers": [],
            "vote_counts": [],
            "vote_percentages": [],
            "registration_counts": [],
            "registration_percentages": [],
            "geographic_districts": [],
            "candidate_metadata": [],
            "boolean_flags": [],
            "calculated_metrics": [],
            "shape_metadata": [],
            "other": [],
        }

        for col in gdf.columns:
            if col == "geometry":
                continue

            if col in ["precinct", "Precinct", "base_precinct", "record_type"]:
                categories["identifiers"].append(col)
            elif col.startswith("votes_"):
                categories["vote_counts"].append(col)
            elif col.startswith("vote_pct_"):
                categories["vote_percentages"].append(col)
            elif col in [
                "TOTAL",
                "DEM",
                "REP",
                "NAV",
                "OTH",
                "IND",
                "LBT",
                "WFP",
                "WTP",
                "CON",
                "NLB",
                "PGP",
                "PRO",
            ]:
                categories["registration_counts"].append(col)
            elif col.startswith("reg_pct_"):
                categories["registration_percentages"].append(col)
            elif col in [
                "OR_House",
                "OR_Senate",
                "USCongress",
                "CITY",
                "SchoolDist",
                "FIRE_DIST",
                "TRAN_DIST",
                "WaterDist",
                "SewerDist",
                "PUD",
                "ESD",
                "METRO",
                "Mult_Comm",
                "CommColleg",
                "CoP_Dist",
                "Soil_Water",
                "UFSWQD",
                "Unincorp",
            ]:
                categories["geographic_districts"].append(col)
            elif col.startswith("candidate_"):
                categories["candidate_metadata"].append(col)
            elif col.startswith("is_") or col.startswith("has_"):
                categories["boolean_flags"].append(col)
            elif col in [
                "margin_pct",
                "total_votes",
                "pps_vote_share",
                "precinct_size",
                "competitiveness_score",
                "engagement_rate",
                "swing_potential",
            ]:
                categories["calculated_metrics"].append(col)
            elif col in ["Shape_Area", "Shape_Leng"]:
                categories["shape_metadata"].append(col)
            else:
                categories["other"].append(col)

        return categories

    def _calculate_schema_hash(self, field_names: List[str], field_types: Dict[str, str]) -> str:
        """Calculate a hash of the schema for quick comparison."""
        schema_string = json.dumps(
            {
                "fields": sorted(field_names),
                "types": {k: v for k, v in sorted(field_types.items())},
            },
            sort_keys=True,
        )
        return hashlib.sha256(schema_string.encode()).hexdigest()[:16]

    def capture_schema_snapshot(
        self, gdf: gpd.GeoDataFrame, data_source: str = "election_data"
    ) -> SchemaSnapshot:
        """Capture a complete snapshot of the current schema."""
        logger.info(f"Capturing schema snapshot for {data_source}")

        # Basic field information
        field_names = [col for col in gdf.columns if col != "geometry"]
        field_types = {col: str(gdf[col].dtype) for col in field_names}
        field_categories = self._categorize_fields(gdf)

        # Sample values (first non-null value for each field)
        sample_values = {}
        for col in field_names:
            non_null_values = gdf[col].dropna()
            if len(non_null_values) > 0:
                sample_values[col] = str(non_null_values.iloc[0])
            else:
                sample_values[col] = None

        # Null and unique counts
        null_counts = {col: int(gdf[col].isnull().sum()) for col in field_names}
        unique_counts = {}
        for col in field_names:
            try:
                unique_counts[col] = int(gdf[col].nunique())
            except Exception:
                unique_counts[col] = -1  # Indicates couldn't calculate

        # Create snapshot
        snapshot = SchemaSnapshot(
            timestamp=datetime.now().isoformat(),
            total_fields=len(field_names),
            field_names=field_names,
            field_types=field_types,
            field_categories=field_categories,
            sample_values=sample_values,
            schema_hash=self._calculate_schema_hash(field_names, field_types),
            data_source=data_source,
            record_count=len(gdf),
            null_counts=null_counts,
            unique_counts=unique_counts,
        )

        # Save snapshot
        self._save_snapshot(snapshot)

        logger.info(
            f"Schema snapshot captured: {snapshot.total_fields} fields, hash: {snapshot.schema_hash}"
        )
        return snapshot

    def _save_snapshot(self, snapshot: SchemaSnapshot) -> None:
        """Save a schema snapshot to persistent storage."""
        try:
            # Load existing snapshots
            with open(self.snapshots_file, "r") as f:
                snapshots = json.load(f)

            # Add new snapshot
            snapshots.append(asdict(snapshot))

            # Clean up old snapshots if auto_cleanup is enabled
            if self.config.get("auto_cleanup", True):
                cutoff_date = datetime.now() - timedelta(days=self.config.get("retention_days", 90))
                snapshots = [
                    s for s in snapshots if datetime.fromisoformat(s["timestamp"]) > cutoff_date
                ]

            # Save updated snapshots
            with open(self.snapshots_file, "w") as f:
                json.dump(snapshots, f, indent=2)

        except Exception as e:
            logger.error(f"Failed to save schema snapshot: {e}")

    def _save_alert(self, alert: SchemaDriftAlert) -> None:
        """Save an alert to persistent storage."""
        try:
            # Load existing alerts
            with open(self.alerts_file, "r") as f:
                alerts = json.load(f)

            # Add new alert
            alerts.append(asdict(alert))

            # Clean up old alerts if auto_cleanup is enabled
            if self.config.get("auto_cleanup", True):
                cutoff_date = datetime.now() - timedelta(days=self.config.get("retention_days", 90))
                alerts = [a for a in alerts if datetime.fromisoformat(a["timestamp"]) > cutoff_date]

            # Save updated alerts
            with open(self.alerts_file, "w") as f:
                json.dump(alerts, f, indent=2)

        except Exception as e:
            logger.error(f"Failed to save alert: {e}")

    def analyze_schema_drift(self, current_snapshot: SchemaSnapshot) -> List[SchemaDriftAlert]:
        """Analyze schema drift by comparing with previous snapshots."""
        alerts = []

        # Load previous snapshots
        try:
            with open(self.snapshots_file, "r") as f:
                snapshots_data = json.load(f)
        except Exception:
            logger.warning("No previous snapshots found for comparison")
            return alerts

        if len(snapshots_data) < 2:
            logger.info("Insufficient snapshots for drift analysis")
            return alerts

        # Get the previous snapshot
        previous_data = snapshots_data[-2]  # Second to last (last is current)
        previous_fields = set(previous_data["field_names"])
        current_fields = set(current_snapshot.field_names)

        # Analyze field additions
        new_fields = current_fields - previous_fields
        if new_fields:
            severity = self._determine_severity_new_fields(len(new_fields))
            alert = SchemaDriftAlert(
                timestamp=datetime.now().isoformat(),
                severity=severity,
                alert_type="NEW_FIELDS",
                title=f"{len(new_fields)} New Fields Detected",
                description=f"New fields have been added to the data schema: {', '.join(sorted(new_fields))}",
                affected_fields=list(new_fields),
                impact_assessment=self._assess_new_fields_impact(new_fields, current_snapshot),
                recommended_actions=self._recommend_new_fields_actions(new_fields),
            )
            alerts.append(alert)

        # Analyze field removals
        removed_fields = previous_fields - current_fields
        if removed_fields:
            severity = self._determine_severity_removed_fields(len(removed_fields))
            alert = SchemaDriftAlert(
                timestamp=datetime.now().isoformat(),
                severity=severity,
                alert_type="REMOVED_FIELDS",
                title=f"{len(removed_fields)} Fields Removed",
                description=f"Fields have been removed from the data schema: {', '.join(sorted(removed_fields))}",
                affected_fields=list(removed_fields),
                impact_assessment=self._assess_removed_fields_impact(removed_fields),
                recommended_actions=self._recommend_removed_fields_actions(removed_fields),
            )
            alerts.append(alert)

        # Analyze type changes
        type_changes = self._find_type_changes(
            previous_data["field_types"], current_snapshot.field_types
        )
        if type_changes:
            severity = self._determine_severity_type_changes(len(type_changes))
            alert = SchemaDriftAlert(
                timestamp=datetime.now().isoformat(),
                severity=severity,
                alert_type="TYPE_CHANGES",
                title=f"{len(type_changes)} Field Type Changes",
                description=f"Data types have changed for fields: {', '.join(type_changes.keys())}",
                affected_fields=list(type_changes.keys()),
                impact_assessment=self._assess_type_changes_impact(type_changes),
                recommended_actions=self._recommend_type_changes_actions(type_changes),
            )
            alerts.append(alert)

        # Analyze data quality changes
        quality_alerts = self._analyze_data_quality_changes(previous_data, current_snapshot)
        alerts.extend(quality_alerts)

        # Save all alerts
        for alert in alerts:
            self._save_alert(alert)
            logger.warning(f"Schema drift alert ({alert.severity}): {alert.title}")

        return alerts

    def _determine_severity_new_fields(self, count: int) -> str:
        """Determine severity based on number of new fields."""
        thresholds = self.config["alert_thresholds"]
        if count >= thresholds["new_fields_critical"]:
            return "CRITICAL"
        elif count >= thresholds["new_fields_high"]:
            return "HIGH"
        elif count >= thresholds["new_fields_medium"]:
            return "MEDIUM"
        else:
            return "LOW"

    def _determine_severity_removed_fields(self, count: int) -> str:
        """Determine severity based on number of removed fields."""
        thresholds = self.config["alert_thresholds"]
        if count >= thresholds["removed_fields_critical"]:
            return "CRITICAL"
        elif count >= thresholds["removed_fields_high"]:
            return "HIGH"
        elif count >= thresholds["removed_fields_medium"]:
            return "MEDIUM"
        else:
            return "LOW"

    def _determine_severity_type_changes(self, count: int) -> str:
        """Determine severity based on number of type changes."""
        thresholds = self.config["alert_thresholds"]
        if count >= thresholds["type_changes_critical"]:
            return "CRITICAL"
        elif count >= thresholds["type_changes_high"]:
            return "HIGH"
        else:
            return "MEDIUM"

    def _find_type_changes(
        self, previous_types: Dict[str, str], current_types: Dict[str, str]
    ) -> Dict[str, Tuple[str, str]]:
        """Find fields where data types have changed."""
        changes = {}
        for field in set(previous_types.keys()) & set(current_types.keys()):
            if previous_types[field] != current_types[field]:
                changes[field] = (previous_types[field], current_types[field])
        return changes

    def _assess_new_fields_impact(self, new_fields: Set[str], snapshot: SchemaSnapshot) -> str:
        """Assess the impact of new fields."""
        categories = snapshot.field_categories
        impact_parts = []

        # Categorize new fields
        for category, fields in categories.items():
            new_in_category = [f for f in new_fields if f in fields]
            if new_in_category:
                impact_parts.append(
                    f"{len(new_in_category)} new {category.replace('_', ' ')} fields"
                )

        if not impact_parts:
            return "New fields don't fit existing categories - may indicate major schema change"

        return f"Impact: {', '.join(impact_parts)}. May require field registry updates and documentation."

    def _assess_removed_fields_impact(self, removed_fields: Set[str]) -> str:
        """Assess the impact of removed fields."""
        critical_patterns = ["votes_", "reg_pct_", "is_pps_precinct", "precinct"]
        critical_removed = [
            f for f in removed_fields if any(f.startswith(p) for p in critical_patterns)
        ]

        if critical_removed:
            return f"CRITICAL: Removed fields include critical data: {', '.join(critical_removed)}. This may break downstream analysis."
        else:
            return f"Moderate impact: {len(removed_fields)} fields removed. Check if any downstream code depends on these fields."

    def _assess_type_changes_impact(self, type_changes: Dict[str, Tuple[str, str]]) -> str:
        """Assess the impact of type changes."""
        critical_changes = []
        for field, (old_type, new_type) in type_changes.items():
            if "object" in old_type and "int" in new_type:
                critical_changes.append(f"{field}: string→numeric")
            elif "int" in old_type and "object" in new_type:
                critical_changes.append(f"{field}: numeric→string")

        if critical_changes:
            return f"Type changes may break analysis: {', '.join(critical_changes)}"
        else:
            return f"Minor type changes detected for {len(type_changes)} fields"

    def _recommend_new_fields_actions(self, new_fields: Set[str]) -> List[str]:
        """Recommend actions for new fields."""
        actions = [
            "Review new fields and determine if they need documentation",
            "Update field registry with explanations for important fields",
            "Check if new fields affect existing analysis or visualizations",
        ]

        if len(new_fields) > 5:
            actions.append(
                "Consider implementing auto-registration patterns for bulk field additions"
            )

        return actions

    def _recommend_removed_fields_actions(self, removed_fields: Set[str]) -> List[str]:
        """Recommend actions for removed fields."""
        return [
            "Review code for references to removed fields",
            "Update field registry to remove obsolete definitions",
            "Check if removed fields were used in critical calculations",
            "Consider graceful degradation for missing fields",
        ]

    def _recommend_type_changes_actions(
        self, type_changes: Dict[str, Tuple[str, str]]
    ) -> List[str]:
        """Recommend actions for type changes."""
        return [
            "Review data cleaning and type conversion logic",
            "Test existing calculations with new data types",
            "Update validation rules if necessary",
            "Consider adding explicit type conversion in preprocessing",
        ]

    def _analyze_data_quality_changes(
        self, previous_data: Dict, current_snapshot: SchemaSnapshot
    ) -> List[SchemaDriftAlert]:
        """Analyze data quality changes between snapshots."""
        alerts = []

        # Check for significant changes in null rates
        for field in set(previous_data["null_counts"].keys()) & set(
            current_snapshot.null_counts.keys()
        ):
            prev_null_rate = previous_data["null_counts"][field] / previous_data["record_count"]
            curr_null_rate = current_snapshot.null_counts[field] / current_snapshot.record_count

            null_rate_change = abs(curr_null_rate - prev_null_rate)
            if null_rate_change > self.config["alert_thresholds"]["null_rate_change_threshold"]:
                alert = SchemaDriftAlert(
                    timestamp=datetime.now().isoformat(),
                    severity="MEDIUM",
                    alert_type="DATA_QUALITY",
                    title=f"Significant Null Rate Change: {field}",
                    description=f"Null rate changed from {prev_null_rate:.1%} to {curr_null_rate:.1%}",
                    affected_fields=[field],
                    impact_assessment="Data quality change may indicate upstream issues or different data collection",
                    recommended_actions=[
                        "Investigate source of null rate change",
                        "Review data validation rules",
                    ],
                )
                alerts.append(alert)

        # Check for significant record count changes
        record_count_change = (
            abs(current_snapshot.record_count - previous_data["record_count"])
            / previous_data["record_count"]
        )
        if record_count_change > self.config["alert_thresholds"]["record_count_change_threshold"]:
            severity = "HIGH" if record_count_change > 0.5 else "MEDIUM"
            alert = SchemaDriftAlert(
                timestamp=datetime.now().isoformat(),
                severity=severity,
                alert_type="RECORD_COUNT",
                title="Significant Record Count Change",
                description=f"Record count changed from {previous_data['record_count']} to {current_snapshot.record_count} ({record_count_change:.1%} change)",
                affected_fields=[],
                impact_assessment="Large changes in record count may indicate data source changes or processing issues",
                recommended_actions=[
                    "Verify data source completeness",
                    "Check for filtering or processing changes",
                ],
            )
            alerts.append(alert)

        return alerts

    def generate_drift_report(self, days_back: int = 30) -> str:
        """Generate a comprehensive schema drift report."""
        logger.info(f"Generating schema drift report for last {days_back} days")

        # Load snapshots and alerts
        try:
            with open(self.snapshots_file, "r") as f:
                snapshots = json.load(f)
            with open(self.alerts_file, "r") as f:
                alerts = json.load(f)
        except Exception as e:
            logger.error(f"Failed to load monitoring data: {e}")
            return "Error: Could not load monitoring data"

        # Filter by date range
        cutoff_date = datetime.now() - timedelta(days=days_back)
        recent_snapshots = [
            s for s in snapshots if datetime.fromisoformat(s["timestamp"]) > cutoff_date
        ]
        recent_alerts = [a for a in alerts if datetime.fromisoformat(a["timestamp"]) > cutoff_date]

        # Generate report
        report_lines = [
            "# Schema Drift Monitoring Report",
            "",
            f"**Report Period:** {cutoff_date.strftime('%Y-%m-%d')} to {datetime.now().strftime('%Y-%m-%d')} ({days_back} days)",
            f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "## Executive Summary",
            "",
            f"- **Schema Snapshots Captured:** {len(recent_snapshots)}",
            f"- **Drift Alerts Generated:** {len(recent_alerts)}",
            f"- **Current Schema Hash:** {recent_snapshots[-1]['schema_hash'] if recent_snapshots else 'N/A'}",
            f"- **Current Field Count:** {recent_snapshots[-1]['total_fields'] if recent_snapshots else 'N/A'}",
            "",
        ]

        # Alert summary
        if recent_alerts:
            alert_counts = {}
            for alert in recent_alerts:
                severity = alert["severity"]
                alert_counts[severity] = alert_counts.get(severity, 0) + 1

            report_lines.extend(
                ["### Alert Summary", "", "| Severity | Count |", "|----------|-------|"]
            )

            for severity in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
                count = alert_counts.get(severity, 0)
                report_lines.append(f"| {severity} | {count} |")

            report_lines.append("")

        # Recent alerts detail
        if recent_alerts:
            report_lines.extend(["## Recent Alerts", ""])

            for alert in sorted(recent_alerts, key=lambda x: x["timestamp"], reverse=True)[:10]:
                timestamp = datetime.fromisoformat(alert["timestamp"]).strftime("%Y-%m-%d %H:%M")
                report_lines.extend(
                    [
                        f"### {alert['severity']}: {alert['title']}",
                        "",
                        f"- **Time:** {timestamp}",
                        f"- **Type:** {alert['alert_type']}",
                        f"- **Description:** {alert['description']}",
                        f"- **Impact:** {alert['impact_assessment']}",
                        "",
                        "**Recommended Actions:**",
                    ]
                )

                for action in alert["recommended_actions"]:
                    report_lines.append(f"- {action}")

                report_lines.append("")

        # Schema evolution
        if len(recent_snapshots) > 1:
            first_snapshot = recent_snapshots[0]
            last_snapshot = recent_snapshots[-1]

            field_growth = last_snapshot["total_fields"] - first_snapshot["total_fields"]
            record_growth = last_snapshot["record_count"] - first_snapshot["record_count"]

            report_lines.extend(
                [
                    "## Schema Evolution",
                    "",
                    f"- **Field Count Change:** {field_growth:+d} fields",
                    f"- **Record Count Change:** {record_growth:+d} records",
                    f"- **Schema Stability:** {'Stable' if field_growth == 0 else 'Evolving'}",
                    "",
                ]
            )

        # Field category analysis
        if recent_snapshots:
            latest_snapshot = recent_snapshots[-1]
            categories = latest_snapshot["field_categories"]

            report_lines.extend(
                [
                    "## Current Field Distribution",
                    "",
                    "| Category | Count |",
                    "|----------|-------|",
                ]
            )

            for category, fields in categories.items():
                if fields:  # Only show non-empty categories
                    report_lines.append(f"| {category.replace('_', ' ').title()} | {len(fields)} |")

            report_lines.append("")

        # Recommendations
        report_lines.extend(
            [
                "## Recommendations",
                "",
                f"Based on the analysis of schema drift over the past {days_back} days:",
                "",
            ]
        )

        if len(recent_alerts) == 0:
            report_lines.append(
                "✅ **Schema Stable:** No significant drift detected. Continue monitoring."
            )
        elif len([a for a in recent_alerts if a["severity"] in ["CRITICAL", "HIGH"]]) > 0:
            report_lines.extend(
                [
                    "⚠️ **Action Required:** Critical or high-severity alerts detected.",
                    "- Review and address high-priority alerts immediately",
                    "- Investigate upstream data source changes",
                    "- Update field registry and documentation as needed",
                ]
            )
        else:
            report_lines.extend(
                [
                    "📊 **Monitor Closely:** Low to medium drift detected.",
                    "- Continue monitoring for trends",
                    "- Plan field registry updates for next maintenance window",
                ]
            )

        return "\n".join(report_lines)

    def get_alert_summary(
        self, severity_filter: Optional[str] = None, days_back: int = 7
    ) -> Dict[str, Any]:
        """Get a summary of recent alerts for API/dashboard use."""
        try:
            with open(self.alerts_file, "r") as f:
                alerts = json.load(f)
        except Exception:
            return {"error": "Could not load alerts data"}

        # Filter by date and severity
        cutoff_date = datetime.now() - timedelta(days=days_back)
        filtered_alerts = [
            a
            for a in alerts
            if datetime.fromisoformat(a["timestamp"]) > cutoff_date
            and (severity_filter is None or a["severity"] == severity_filter)
        ]

        # Calculate summary stats
        alert_counts = {}
        type_counts = {}

        for alert in filtered_alerts:
            severity = alert["severity"]
            alert_type = alert["alert_type"]

            alert_counts[severity] = alert_counts.get(severity, 0) + 1
            type_counts[alert_type] = type_counts.get(alert_type, 0) + 1

        return {
            "total_alerts": len(filtered_alerts),
            "severity_breakdown": alert_counts,
            "type_breakdown": type_counts,
            "most_recent": filtered_alerts[-1] if filtered_alerts else None,
            "period_days": days_back,
        }


def monitor_schema_drift(
    gdf: gpd.GeoDataFrame, data_source: str = "election_data"
) -> Dict[str, Any]:
    """
    Convenience function to monitor schema drift for a GeoDataFrame.

    Args:
        gdf: GeoDataFrame to monitor
        data_source: Identifier for the data source

    Returns:
        Dictionary with monitoring results and any alerts
    """
    monitor = SchemaDriftMonitor()

    # Capture current snapshot
    snapshot = monitor.capture_schema_snapshot(gdf, data_source)

    # Analyze drift
    alerts = monitor.analyze_schema_drift(snapshot)

    # Get alert summary
    alert_summary = monitor.get_alert_summary(days_back=1)

    return {
        "snapshot": asdict(snapshot),
        "alerts": [asdict(alert) for alert in alerts],
        "alert_summary": alert_summary,
        "monitoring_status": "active",
    }


if __name__ == "__main__":
    # Demo: Generate a monitoring report
    monitor = SchemaDriftMonitor()
    report = monitor.generate_drift_report(days_back=30)

    report_file = Path("schema_monitoring/drift_report.md")
    with open(report_file, "w") as f:
        f.write(report)

    logger.info(f"📊 Schema drift report generated: {report_file}")
    logger.info("" + "=" * 60)
    logger.info(report)

#!/usr/bin/env python3
"""
Command-line interface for Schema Drift Monitoring

This CLI provides easy access to schema monitoring capabilities including:
- Generating reports
- Viewing alerts
- Configuring monitoring settings
- Running analysis on data files
"""

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add the analysis directory to the path for imports
sys.path.append(str(Path(__file__).parent))

try:
    import geopandas as gpd
    import pandas as pd
    from schema_drift_monitor import SchemaDriftMonitor, monitor_schema_drift

    DEPENDENCIES_AVAILABLE = True
except ImportError as e:
    print(f"❌ Missing dependencies: {e}")
    print("💡 Make sure you have geopandas, pandas, and loguru installed")
    DEPENDENCIES_AVAILABLE = False


def generate_report(args: argparse.Namespace) -> None:
    """Generate a schema drift report."""
    if not DEPENDENCIES_AVAILABLE:
        print("❌ Cannot generate report - missing dependencies")
        return

    print(f"📊 Generating schema drift report for last {args.days} days...")

    monitor = SchemaDriftMonitor()
    report = monitor.generate_drift_report(days_back=args.days)

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            f.write(report)
        print(f"✅ Report saved to: {output_path}")
    else:
        print("\n" + "=" * 60)
        print(report)


def view_alerts(args: argparse.Namespace) -> None:
    """View recent schema drift alerts."""
    if not DEPENDENCIES_AVAILABLE:
        print("❌ Cannot view alerts - missing dependencies")
        return

    monitor = SchemaDriftMonitor()
    alert_summary = monitor.get_alert_summary(severity_filter=args.severity, days_back=args.days)

    if "error" in alert_summary:
        print(f"❌ Error: {alert_summary['error']}")
        return

    print(f"🚨 Alert Summary (Last {args.days} days)")
    print("=" * 50)
    print(f"Total Alerts: {alert_summary['total_alerts']}")

    if alert_summary["severity_breakdown"]:
        print("\nBy Severity:")
        for severity, count in alert_summary["severity_breakdown"].items():
            emoji = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🟢"}.get(severity, "ℹ️")
            print(f"  {emoji} {severity}: {count}")

    if alert_summary["type_breakdown"]:
        print("\nBy Type:")
        for alert_type, count in alert_summary["type_breakdown"].items():
            print(f"  • {alert_type.replace('_', ' ').title()}: {count}")

    if alert_summary["most_recent"]:
        recent = alert_summary["most_recent"]
        timestamp = datetime.fromisoformat(recent["timestamp"]).strftime("%Y-%m-%d %H:%M")
        print("\nMost Recent Alert:")
        print(f"  🕒 {timestamp}")
        print(f"  📋 {recent['severity']}: {recent['title']}")
        print(f"  📝 {recent['description']}")


def analyze_file(args: argparse.Namespace) -> None:
    """Analyze a data file for schema drift."""
    if not DEPENDENCIES_AVAILABLE:
        print("❌ Cannot analyze file - missing dependencies")
        return

    file_path = Path(args.file)

    if not file_path.exists():
        print(f"❌ File not found: {file_path}")
        return

    print(f"🔍 Analyzing schema for: {file_path}")

    try:
        # Determine file type and load data
        if file_path.suffix.lower() == ".csv":
            df = pd.read_csv(file_path, dtype=str)
            # Create a simple geometry column for GeoDataFrame
            from shapely.geometry import Point

            geometry = [Point(0, 0) for _ in range(len(df))]
            gdf = gpd.GeoDataFrame(df, geometry=geometry)
        elif file_path.suffix.lower() in [".geojson", ".json"]:
            gdf = gpd.read_file(file_path)
        else:
            print(f"❌ Unsupported file type: {file_path.suffix}")
            return

        print(f"  📊 Loaded {len(gdf)} records with {len(gdf.columns)} fields")

        # Run schema drift analysis
        data_source = args.source or file_path.stem
        results = monitor_schema_drift(gdf, data_source)

        # Display results
        snapshot = results["snapshot"]
        alerts = results["alerts"]

        print("\n📸 Schema Analysis Results:")
        print(f"  • Total Fields: {snapshot['total_fields']}")
        print(f"  • Schema Hash: {snapshot['schema_hash']}")
        print(f"  • Record Count: {snapshot['record_count']}")

        # Show field categories
        categories = snapshot["field_categories"]
        print("\n📋 Field Categories:")
        for category, fields in categories.items():
            if fields:
                print(f"  • {category.replace('_', ' ').title()}: {len(fields)} fields")

        # Show alerts if any
        if alerts:
            print(f"\n🚨 Drift Alerts Generated: {len(alerts)}")
            for alert in alerts:
                severity_emoji = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🟢"}
                emoji = severity_emoji.get(alert["severity"], "ℹ️")
                print(f"  {emoji} {alert['severity']}: {alert['title']}")
                print(f"     {alert['description']}")
        else:
            print("\n✅ No schema drift alerts - structure is stable")

    except Exception as e:
        print(f"❌ Error analyzing file: {e}")


def configure_monitoring(args: argparse.Namespace) -> None:
    """Configure monitoring settings."""
    if not DEPENDENCIES_AVAILABLE:
        print("❌ Cannot configure monitoring - missing dependencies")
        return

    monitor = SchemaDriftMonitor()
    config_file = monitor.config_file

    if args.show:
        print("📋 Current Monitoring Configuration:")
        print(f"Configuration file: {config_file}")
        print("\n" + json.dumps(monitor.config, indent=2))
        return

    # Update configuration
    if args.set:
        key, value = args.set.split("=", 1)

        # Try to parse value as JSON for complex types
        try:
            parsed_value = json.loads(value)
        except json.JSONDecodeError:
            # Treat as string
            parsed_value = value

        # Update configuration
        keys = key.split(".")
        config = monitor.config

        # Navigate to the nested key
        current = config
        for k in keys[:-1]:
            if k not in current:
                current[k] = {}
            current = current[k]

        current[keys[-1]] = parsed_value

        # Save updated configuration
        with open(config_file, "w") as f:
            json.dump(config, f, indent=2)

        print(f"✅ Updated configuration: {key} = {parsed_value}")
        print(f"💾 Saved to: {config_file}")


def status(args: argparse.Namespace) -> None:
    """Show monitoring system status."""
    if not DEPENDENCIES_AVAILABLE:
        print("❌ Dependencies not available")
        return

    monitor = SchemaDriftMonitor()

    # Check for recent activity
    snapshots_file = monitor.snapshots_file
    alerts_file = monitor.alerts_file

    print("📊 Schema Drift Monitoring Status")
    print("=" * 50)
    print(f"Monitoring Directory: {monitor.monitoring_dir}")
    print(f"Configuration: {monitor.config_file}")

    # Snapshots status
    if snapshots_file.exists():
        with open(snapshots_file, "r") as f:
            snapshots = json.load(f)
        if snapshots:
            latest_snapshot = snapshots[-1]
            timestamp = datetime.fromisoformat(latest_snapshot["timestamp"])
            age = datetime.now() - timestamp
            print("\n📸 Latest Snapshot:")
            print(f"  • Timestamp: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"  • Age: {age.days} days, {age.seconds // 3600} hours ago")
            print(f"  • Total Snapshots: {len(snapshots)}")
            print(f"  • Schema Hash: {latest_snapshot['schema_hash']}")
            print(f"  • Field Count: {latest_snapshot['total_fields']}")
        else:
            print("\n📸 No snapshots found")
    else:
        print("\n📸 Snapshots file not found")

    # Alerts status
    if alerts_file.exists():
        with open(alerts_file, "r") as f:
            alerts = json.load(f)

        recent_alerts = [
            a
            for a in alerts
            if datetime.fromisoformat(a["timestamp"]) > datetime.now() - timedelta(days=7)
        ]

        print("\n🚨 Recent Alerts (7 days):")
        if recent_alerts:
            alert_counts = {}
            for alert in recent_alerts:
                severity = alert["severity"]
                alert_counts[severity] = alert_counts.get(severity, 0) + 1

            for severity in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
                count = alert_counts.get(severity, 0)
                if count > 0:
                    emoji = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🟢"}[severity]
                    print(f"  {emoji} {severity}: {count}")
        else:
            print("  ✅ No recent alerts")
    else:
        print("\n🚨 Alerts file not found")

    # Configuration status
    print("\n⚙️ Configuration:")
    print(
        f"  • Monitoring Enabled: {'✅' if monitor.config.get('monitoring_enabled', True) else '❌'}"
    )
    print(f"  • Auto Cleanup: {'✅' if monitor.config.get('auto_cleanup', True) else '❌'}")
    print(f"  • Retention Days: {monitor.config.get('retention_days', 90)}")

    thresholds = monitor.config.get("alert_thresholds", {})
    print(f"  • New Fields Alert Threshold: {thresholds.get('new_fields_medium', 2)}")
    print(f"  • Removed Fields Alert Threshold: {thresholds.get('removed_fields_medium', 1)}")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Schema Drift Monitoring CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s report --days 7 --output report.md
  %(prog)s alerts --severity HIGH --days 3
  %(prog)s analyze data/enriched_results.csv --source "election_data"
  %(prog)s config --show
  %(prog)s config --set "alert_thresholds.new_fields_medium=5"
  %(prog)s status
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Report command
    report_parser = subparsers.add_parser("report", help="Generate drift report")
    report_parser.add_argument(
        "--days", type=int, default=30, help="Number of days to include in report (default: 30)"
    )
    report_parser.add_argument(
        "--output", type=str, help="Output file path (default: print to console)"
    )
    report_parser.set_defaults(func=generate_report)

    # Alerts command
    alerts_parser = subparsers.add_parser("alerts", help="View recent alerts")
    alerts_parser.add_argument(
        "--severity",
        type=str,
        choices=["CRITICAL", "HIGH", "MEDIUM", "LOW"],
        help="Filter by severity level",
    )
    alerts_parser.add_argument(
        "--days", type=int, default=7, help="Number of days to look back (default: 7)"
    )
    alerts_parser.set_defaults(func=view_alerts)

    # Analyze command
    analyze_parser = subparsers.add_parser("analyze", help="Analyze a data file")
    analyze_parser.add_argument("file", type=str, help="Path to data file (CSV or GeoJSON)")
    analyze_parser.add_argument(
        "--source", type=str, help="Data source identifier (default: filename)"
    )
    analyze_parser.set_defaults(func=analyze_file)

    # Config command
    config_parser = subparsers.add_parser("config", help="Configure monitoring settings")
    config_group = config_parser.add_mutually_exclusive_group(required=True)
    config_group.add_argument("--show", action="store_true", help="Show current configuration")
    config_group.add_argument(
        "--set",
        type=str,
        metavar="KEY=VALUE",
        help="Set configuration value (e.g., alert_thresholds.new_fields_medium=5)",
    )
    config_parser.set_defaults(func=configure_monitoring)

    # Status command
    status_parser = subparsers.add_parser("status", help="Show monitoring system status")
    status_parser.set_defaults(func=status)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    args.func(args)


if __name__ == "__main__":
    main()

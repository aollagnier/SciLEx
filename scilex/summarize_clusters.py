"""Generate summary reports for detected communities.

Reads cluster CSV(s) from graph analysis output and produces
a Markdown report with statistics and Mermaid mindmap.

Usage:
    scilex-summarize [--input PATH]

Output is written to:
    {output_dir}/{collect_name}/graph_analysis/summary_report.md
"""

import argparse
import logging
import os
import sys

import pandas as pd

from scilex.config_defaults import DEFAULT_OUTPUT_DIR
from scilex.constants import normalize_path_component
from scilex.crawlers.utils import load_all_configs
from scilex.logging_config import setup_logging
from scilex.summarize.report import generate_report
from scilex.summarize.stats import compute_cluster_stats

setup_logging()
logger = logging.getLogger(__name__)


def load_config() -> dict:
    """Load scilex.config.yml."""
    configs = load_all_configs({"main_config": "scilex.config.yml"})
    return configs["main_config"]


def main():
    """Entry point for cluster summarization."""
    parser = argparse.ArgumentParser(
        description="Generate summary reports for detected communities"
    )
    parser.add_argument(
        "--input",
        default=None,
        help="Path to clusters CSV (overrides auto-detection)",
    )
    args = parser.parse_args()

    try:
        config = load_config()

        if "collect_name" not in config:
            raise ValueError("collect_name not specified in scilex.config.yml")

        output_dir = config.get("output_dir", DEFAULT_OUTPUT_DIR)
        collect_name = normalize_path_component(config["collect_name"])
        collect_dir = os.path.join(output_dir, collect_name)
        analysis_dir = os.path.join(collect_dir, "graph_analysis")

        # Find clusters CSV
        if args.input:
            csv_path = args.input
        else:
            # Auto-detect: prefer cocitation, fall back to coupling
            candidates = [
                os.path.join(analysis_dir, "clusters_cocitation.csv"),
                os.path.join(analysis_dir, "clusters_coupling.csv"),
            ]
            csv_path = next(
                (p for p in candidates if os.path.exists(p)),
                None,
            )
            if csv_path is None:
                raise FileNotFoundError(
                    f"No clusters CSV found in {analysis_dir}. "
                    "Run scilex-analyze first."
                )

        logger.info(f"Input: {csv_path}")

        # Load and analyze
        df = pd.read_csv(csv_path, dtype=str)
        # Ensure numeric columns
        if "cluster_id" in df.columns:
            df["cluster_id"] = (
                pd.to_numeric(df["cluster_id"], errors="coerce").fillna(-1).astype(int)
            )
        if "pagerank" in df.columns:
            df["pagerank"] = pd.to_numeric(df["pagerank"], errors="coerce").fillna(0.0)

        stats = compute_cluster_stats(df)

        if not stats:
            print("\nNo clusters found in the data.")
            return

        # Generate report
        report_path = os.path.join(analysis_dir, "summary_report.md")
        generate_report(stats, report_path, collect_name=collect_name)

        print(f"\nSummary report: {report_path}")

    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error during summarization: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

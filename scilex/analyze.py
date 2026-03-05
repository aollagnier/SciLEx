"""Analyze citation networks: community detection, centrality, and export.

Reads citation caches produced by ``scilex-enrich-citations`` and builds
co-citation and/or bibliographic coupling graphs.  Detects communities
using Louvain and computes PageRank centrality.

Usage:
    scilex-analyze [--graph-type cocitation|coupling|both]
                   [--resolution FLOAT] [--min-weight INT]
                   [--format gexf|graphml]

Output is written to:
    {output_dir}/{collect_name}/graph_analysis/
"""

import argparse
import logging
import os
import sys

import pandas as pd

from scilex.config_defaults import (
    DEFAULT_AGGREGATED_FILENAME,
    DEFAULT_GRAPH_FORMAT,
    DEFAULT_GRAPH_MIN_WEIGHT,
    DEFAULT_GRAPH_TYPE,
    DEFAULT_LOUVAIN_RESOLUTION,
    DEFAULT_OUTPUT_DIR,
)
from scilex.constants import is_valid, normalize_path_component
from scilex.crawlers.utils import load_all_configs
from scilex.graph_analysis.community import compute_centrality, detect_communities
from scilex.graph_analysis.export import export_clusters_csv, export_graph
from scilex.graph_analysis.graphs import (
    build_bibliographic_coupling_graph,
    build_cocitation_graph,
)
from scilex.graph_analysis.loader import load_citation_caches
from scilex.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def load_config() -> dict:
    """Load scilex.config.yml."""
    configs = load_all_configs({"main_config": "scilex.config.yml"})
    return configs["main_config"]


def _load_corpus_dois(collect_dir: str, config: dict) -> tuple[pd.DataFrame, set[str]]:
    """Load aggregated CSV and extract the set of valid corpus DOIs.

    Returns:
        Tuple of (DataFrame, set of DOI strings).
    """
    filename = config.get("aggregated_filename", DEFAULT_AGGREGATED_FILENAME)
    csv_path = os.path.join(collect_dir, filename)

    if not os.path.exists(csv_path):
        raise FileNotFoundError(
            f"Aggregated CSV not found: {csv_path}. Run scilex-aggregate first."
        )

    # Auto-detect separator
    with open(csv_path, encoding="utf-8") as f:
        first_line = f.readline()
    sep = "\t" if "\t" in first_line else ","

    df = pd.read_csv(csv_path, sep=sep, dtype=str)
    dois = {str(d).strip() for d in df["DOI"] if is_valid(d) and str(d).strip()}

    logger.info(f"Loaded {len(df)} papers, {len(dois)} with valid DOIs")
    return df, dois


def main():
    """Entry point for graph analysis."""
    parser = argparse.ArgumentParser(
        description="Analyze citation networks: communities and centrality"
    )
    parser.add_argument(
        "--graph-type",
        choices=["cocitation", "coupling", "both"],
        default=DEFAULT_GRAPH_TYPE,
        help=f"Type of graph to build (default: {DEFAULT_GRAPH_TYPE})",
    )
    parser.add_argument(
        "--resolution",
        type=float,
        default=DEFAULT_LOUVAIN_RESOLUTION,
        help=f"Louvain resolution — higher = more clusters (default: {DEFAULT_LOUVAIN_RESOLUTION})",
    )
    parser.add_argument(
        "--min-weight",
        type=int,
        default=DEFAULT_GRAPH_MIN_WEIGHT,
        help=f"Minimum edge weight to keep (default: {DEFAULT_GRAPH_MIN_WEIGHT})",
    )
    parser.add_argument(
        "--format",
        choices=["gexf", "graphml"],
        default=DEFAULT_GRAPH_FORMAT,
        help=f"Graph export format (default: {DEFAULT_GRAPH_FORMAT})",
    )
    args = parser.parse_args()

    try:
        config = load_config()

        if "collect_name" not in config:
            raise ValueError("collect_name not specified in scilex.config.yml")

        output_dir = config.get("output_dir", DEFAULT_OUTPUT_DIR)
        collect_name = normalize_path_component(config["collect_name"])
        collect_dir = os.path.join(output_dir, collect_name)

        # Output subdirectory
        analysis_dir = os.path.join(collect_dir, "graph_analysis")
        os.makedirs(analysis_dir, exist_ok=True)

        # Load data
        df, corpus_dois = _load_corpus_dois(collect_dir, config)
        references, citers = load_citation_caches(collect_dir)

        graph_type = args.graph_type
        fmt = args.format

        # Build graph(s) and run analysis
        if graph_type in ("cocitation", "both"):
            _analyze_graph(
                graph=build_cocitation_graph(
                    references, citers, corpus_dois, min_weight=args.min_weight
                ),
                name="cocitation",
                df=df,
                analysis_dir=analysis_dir,
                resolution=args.resolution,
                fmt=fmt,
            )

        if graph_type in ("coupling", "both"):
            _analyze_graph(
                graph=build_bibliographic_coupling_graph(
                    references, corpus_dois, min_weight=args.min_weight
                ),
                name="coupling",
                df=df,
                analysis_dir=analysis_dir,
                resolution=args.resolution,
                fmt=fmt,
            )

        print(f"\nGraph analysis complete: {analysis_dir}")

    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error during graph analysis: {e}", exc_info=True)
        sys.exit(1)


def _analyze_graph(
    graph,
    name: str,
    df: pd.DataFrame,
    analysis_dir: str,
    resolution: float,
    fmt: str,
) -> None:
    """Run community detection + centrality on a graph and export results."""
    partition = detect_communities(graph, resolution=resolution)
    pagerank = compute_centrality(graph)

    # Export clusters CSV
    csv_path = os.path.join(analysis_dir, f"clusters_{name}.csv")
    export_clusters_csv(df, partition, pagerank, csv_path)

    # Export graph file
    graph_path = os.path.join(analysis_dir, f"{name}_graph.{fmt}")
    export_graph(graph, partition, pagerank, graph_path, fmt=fmt)


if __name__ == "__main__":
    main()

"""Load citation caches (JSON) produced by scilex-enrich-citations.

The caches are simple ``{doi: [list_of_dois]}`` mappings:

- ``citations_cache.json`` — outgoing references (what each paper cites)
- ``citers_cache.json``    — incoming citers (who cites each paper)
"""

import json
import logging
import os

logger = logging.getLogger(__name__)


def load_citation_caches(
    collect_dir: str,
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """Load reference and citer caches from the collection output directory.

    Args:
        collect_dir: Path to ``output/{collect_name}/``.

    Returns:
        Tuple of (references_map, citers_map) where each is
        ``{doi: [list_of_dois]}``.

    Raises:
        FileNotFoundError: If neither cache file exists.
    """
    ref_path = os.path.join(collect_dir, "citations_cache.json")
    citer_path = os.path.join(collect_dir, "citers_cache.json")

    if not os.path.exists(ref_path) and not os.path.exists(citer_path):
        raise FileNotFoundError(
            f"No citation caches found in {collect_dir}. "
            "Run scilex-enrich-citations first."
        )

    references = _load_json_cache(ref_path)
    citers = _load_json_cache(citer_path)

    logger.info(
        f"Loaded citation caches: {len(references)} references, {len(citers)} citers"
    )
    return references, citers


def _load_json_cache(path: str) -> dict[str, list[str]]:
    """Load a single JSON cache file, returning empty dict if absent."""
    if not os.path.exists(path):
        logger.warning(f"Cache file not found: {path}")
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.warning(f"Could not load cache {path}: {e}")
        return {}

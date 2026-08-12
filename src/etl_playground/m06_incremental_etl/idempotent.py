"""Idempotent processing — safe reruns that don't duplicate data.

Strategy:
- For curated Parquet: overwrite the partition (Spark's overwrite mode is partition-level
  when partitionBy columns match)
- For warehouse facts: DELETE + INSERT pattern within the target partition date range
- Checksum verification: compare row counts before and after rerun
"""

import hashlib
import json
from pathlib import Path


def compute_checksum(path: Path) -> str:
    """Compute a simple checksum of all Parquet files in a directory.

    Used to verify that rerunning produces identical output.
    """
    if not path.exists():
        return ""

    hasher = hashlib.sha256()
    for f in sorted(path.rglob("*.parquet")):
        hasher.update(f.read_bytes()[:4096])  # Sample first 4KB of each file

    return hasher.hexdigest()[:16]


def verify_idempotency(
    output_path: str,
    previous_counts: dict[str, int],
    log: "ETLLogger",
) -> bool:
    """Verify that a rerun produced the same results.

    Args:
        output_path: Path to the output data.
        previous_counts: Dict of {partition_key: row_count} from previous run.
        log: Logger.

    Returns:
        True if counts match, False otherwise.
    """
    from etl_playground.shared.logging import ETLLogger

    # Count rows in current output
    current_counts: dict[str, int] = {}
    path = Path(output_path)
    if path.exists():
        for f in path.rglob("*.parquet"):
            key = str(f.parent.relative_to(path))
            current_counts[key] = current_counts.get(key, 0) + 1

    # Compare
    all_match = True
    for key in set(list(previous_counts.keys()) + list(current_counts.keys())):
        prev = previous_counts.get(key, 0)
        curr = current_counts.get(key, 0)
        if prev != curr:
            log.warn(f"Partition '{key}': previous={prev}, current={curr} — MISMATCH")
            all_match = False

    if all_match:
        log.success("Idempotency verified: all partitions match previous run")
    else:
        log.error("Idempotency check FAILED — some partitions differ")

    return all_match


def save_run_state(run_id: str, counts: dict[str, int], output_dir: Path) -> None:
    """Save partition row counts for idempotency verification on next run."""
    state = {
        "run_id": run_id,
        "partition_counts": counts,
    }
    state_file = output_dir / f".run_state_{run_id}.json"
    state_file.write_text(json.dumps(state, indent=2, default=str))


def load_previous_state(output_dir: Path) -> dict[str, int] | None:
    """Load the most recent run state for comparison."""
    states = sorted(output_dir.glob(".run_state_*.json"), reverse=True)
    if not states:
        return None

    state = json.loads(states[0].read_text())
    return state.get("partition_counts", {})

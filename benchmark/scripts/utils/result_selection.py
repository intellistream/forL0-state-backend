"""Pure helpers for selecting scientifically comparable benchmark results."""

from __future__ import annotations

from typing import Any, Hashable, Mapping


BACKENDS = ('hashmap', 'forl0')


def wordcount_workload_identity(data: Mapping[str, Any]) -> tuple[Hashable, ...]:
    """Return fields that must match before two WordCount results are compared."""
    return (
        data.get('scenario') or data.get('_metadata', {}).get('test_name') or 'default',
        data.get('total_records'),
        data.get('parallelism'),
        data.get('repeat_runs'),
        data.get('repeat_policy'),
    )


def nexmark_workload_identity(
    scenario_name: str, query: str, query_data: Mapping[str, Any]
) -> tuple[Hashable, ...]:
    """Return the effective NexMark workload identity for backend pairing.

    Scenario names alone are insufficient: an operator may tune TPS or event
    proportions without renaming a scenario.  These fields describe the load
    actually submitted to NexMark and therefore form the comparison boundary.
    """
    return (
        scenario_name or 'default',
        query,
        query_data.get('configured_tps'),
        query_data.get('person_proportion'),
        query_data.get('auction_proportion'),
        query_data.get('bid_proportion'),
        query_data.get('bid_hot_ratio_auctions'),
        query_data.get('bid_hot_ratio_bidders'),
        query_data.get('auction_hot_ratio_sellers'),
        query_data.get('parallelism'),
        query_data.get('metric_tps_vertex'),
        query_data.get('metric_monitor_duration'),
    )


def scoped_nexmark_workload_identity(
    run_id: str | None,
    source_dir: str,
    scenario_name: str,
    query: str,
    query_data: Mapping[str, Any],
) -> tuple[Hashable, ...]:
    """Add campaign custody to a NexMark workload identity.

    A run ID authorizes pairing across isolated backend directories. Without
    one, only records preserved in the same directory may be compared.
    """
    return (run_id or source_dir,) + nexmark_workload_identity(
        scenario_name, query, query_data)


def newest_complete_pair(candidates: Mapping[Any, Mapping[str, dict]]) -> dict:
    """Choose the newest identity containing both backends, or return empty."""
    complete = []
    for identity, pair in candidates.items():
        if not all(pair.get(backend) for backend in BACKENDS):
            continue
        rank = max(
            str(pair[backend].get('_selection_rank', '')) for backend in BACKENDS
        )
        complete.append((rank, repr(identity), pair))
    if not complete:
        return {}
    return max(complete, key=lambda item: (item[0], item[1]))[2]

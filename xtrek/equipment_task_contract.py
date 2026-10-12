"""Aggregation source/task contracts; deliberately independent of report validation.

Missing, null or blank Quantity means no quantity limit for an aggregation phone
assignment. It is not numeric zero. This predicate applies to production sources,
not generic printer jobs using a separate `count` field and not scanned reports.
"""
from collections.abc import Mapping


def unlimited_aggregation_source(source):
    if not isinstance(source, Mapping):
        raise ValueError('Aggregation source must be an object')
    quantity = source.get('Quantity')
    return quantity is None or (isinstance(quantity, str) and not quantity.strip())

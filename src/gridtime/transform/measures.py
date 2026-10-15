"""One canonical vocabulary for both Elia feeds.

The near-real-time (ods175) and historical (ods165) datasets publish the same
balancing measures, but some fields are named differently. Every Silver row uses
the canonical name on the left.
"""

from dataclasses import dataclass

NRT = "ods175"          # near real-time, current hours, non-validated
HISTORICAL = "ods165"   # refreshed daily, ends at yesterday

STAGES = {NRT: "near_real_time", HISTORICAL: "historical"}


@dataclass(frozen=True)
class Measure:
    name: str
    unit: str
    label: str
    fields: dict[str, str]  # dataset_id -> source field name


def _same(field: str) -> dict[str, str]:
    return {NRT: field, HISTORICAL: field}


MEASURES: list[Measure] = [
    Measure("system_imbalance", "MW", "System imbalance", _same("systemimbalance")),
    Measure("ace", "MW", "Area control error", _same("ace")),
    Measure("price_up_marginal", "EUR/MWh", "Upward activation price",
            _same("marginalincrementalprice")),
    Measure("price_down_marginal", "EUR/MWh", "Downward activation price",
            _same("marginaldecrementalprice")),
    Measure("price_floor", "EUR/MWh", "Floor price", _same("floorprice")),
    Measure("price_cap", "EUR/MWh", "Cap price", _same("cap")),
    Measure("price_r2_up", "EUR/MWh", "aFRR (R2) upward price", _same("r2priceup")),
    Measure("price_r2_down", "EUR/MWh", "aFRR (R2) downward price", _same("r2pricedown")),
    # Renamed between feeds. Equivalence is an assumption, checked by the
    # revision statistics once both feeds overlap (see docs/decisions/002).
    Measure("price_mfrr_up", "EUR/MWh", "mFRR upward price",
            {NRT: "mfrrup", HISTORICAL: "upwardavailablemfrrprice"}),
    Measure("price_mfrr_down", "EUR/MWh", "mFRR downward price",
            {NRT: "mfrrdown", HISTORICAL: "downwardavailablemfrrprice"}),
    Measure("price_intertso_export", "EUR/MWh", "Inter-TSO export price",
            {NRT: "price_intertso_e", HISTORICAL: "intertsopriceexport"}),
    Measure("price_intertso_import", "EUR/MWh", "Inter-TSO import price",
            {NRT: "price_intertso_i", HISTORICAL: "intertsopriceimport"}),
]

BY_NAME = {m.name: m for m in MEASURES}


def mapping_rows() -> list[tuple[str, str, str, str]]:
    """(dataset_id, source_field, measure, unit) for every mapped field."""
    return [
        (dataset, field, m.name, m.unit)
        for m in MEASURES
        for dataset, field in m.fields.items()
    ]

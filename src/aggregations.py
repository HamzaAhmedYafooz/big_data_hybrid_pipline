"""
src/aggregations.py
===================
المرحلة الثانية: Aggregation Pipelines + Materialized View
وفق معايير المحاضرة السابعة (07_aggregation_query_optimization.ipynb)

المحتويات:
  1. Client-side Processing (Baseline)
  2. Aggregation Pipelines ($group, $sort, $limit, $match, $unwind)
  3. Explain ('executionStats') Benchmark
  4. Heavy Aggregation Pipeline
  5. Materialized View ($merge)
"""

from __future__ import annotations

import json
from collections import defaultdict
from pprint import pprint
from time import perf_counter

from pymongo import ASCENDING, MongoClient
from config.settings import (
    COLLECTION_VALIDATED,
    MONGO_DB_NAME,
    MONGO_URI,
)

COLLECTION_MATERIALIZED = "monthly_city_products"
AGGS_INDEXES = ["agg_idx_city", "agg_idx_city_date", "agg_idx_status"]

# Translation map for Arabic-Indic digits and decimal comma
ARABIC_NUMERALS_MAP = str.maketrans('٠١٢٣٤٥٦٧٨٩٫', '0123456789.')


def safe_float(val) -> float:
    """Safely converts any numeric value, string with Arabic digits, or comma to float."""
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        cleaned = val.translate(ARABIC_NUMERALS_MAP).replace(',', '').strip()
        try:
            return float(cleaned)
        except ValueError:
            return 0.0
    return 0.0


# =========================================================
# Helpers
# =========================================================

def _explain_find(db, collection_name: str, filter_query: dict) -> dict:
    return db.command(
        "explain",
        {"find": collection_name, "filter": filter_query},
        verbosity="executionStats",
    )


def _collect_stages(node) -> list:
    stages = []
    if isinstance(node, dict):
        if "stage" in node:
            stages.append(node["stage"])
        for value in node.values():
            stages.extend(_collect_stages(value))
    elif isinstance(node, list):
        for item in node:
            stages.extend(_collect_stages(item))
    return stages


def _explain_summary(explain_doc: dict) -> dict:
    stats = explain_doc.get("executionStats", {})
    planner = explain_doc.get("queryPlanner", {})
    winning_plan = planner.get("winningPlan", {})
    return {
        "stages": list(dict.fromkeys(_collect_stages(winning_plan))),
        "nReturned": stats.get("nReturned", 0),
        "executionTimeMillis": stats.get("executionTimeMillis", 0),
        "totalKeysExamined": stats.get("totalKeysExamined", 0),
        "totalDocsExamined": stats.get("totalDocsExamined", 0),
    }


# =========================================================
# 1. Baseline -- Client-side Processing
# =========================================================

def client_side_total_sales(orders) -> dict:
    print("\n" + "=" * 60)
    print("1. Baseline -- Client-side Total Sales")
    print("=" * 60)

    start = perf_counter()
    total = 0.0
    count = 0
    for doc in orders.find({}, {"_id": 0, "total_amount": 1}):
        total += safe_float(doc.get("total_amount", 0))
        count += 1
    elapsed = perf_counter() - start

    result = {
        "documents_moved_to_python": count,
        "total_sales": round(total, 2),
        "elapsed_seconds": round(elapsed, 3),
    }
    print(f"Documents moved to Python : {count:,}")
    print(f"Total sales               : {total:,.2f}")
    print(f"Elapsed                   : {elapsed:.3f} seconds")
    return result


# =========================================================
# 2. Aggregation Pipelines
# =========================================================

def agg_sales_by_city(orders) -> list:
    print("\n" + "=" * 60)
    print("2. Aggregation -- Sales by City ($group)")
    print("=" * 60)

    pipeline = [
        {
            "$group": {
                "_id": {"$ifNull": ["$city", "$customer.address.city"]},
                "total_sales": {
                    "$sum": {
                        "$convert": {
                            "input": "$total_amount",
                            "to": "double",
                            "onError": 0.0,
                            "onNull": 0.0
                        }
                    }
                },
                "orders_count": {"$sum": 1},
            }
        },
        {"$sort": {"total_sales": -1}}
    ]
    start = perf_counter()
    results = list(orders.aggregate(pipeline))
    elapsed = perf_counter() - start

    print(f"Rows returned to Python : {len(results)}")
    print(f"Elapsed                 : {elapsed:.3f} seconds")
    pprint(results[:5])
    return results


def agg_top5_cities(orders) -> list:
    print("\n" + "=" * 60)
    print("3. Aggregation -- Top 5 Cities ($sort + $limit)")
    print("=" * 60)

    pipeline = [
        {
            "$group": {
                "_id": {"$ifNull": ["$city", "$customer.address.city"]},
                "total_sales": {
                    "$sum": {
                        "$convert": {
                            "input": "$total_amount",
                            "to": "double",
                            "onError": 0.0,
                            "onNull": 0.0
                        }
                    }
                },
                "orders_count": {"$sum": 1},
            }
        },
        {"$sort": {"total_sales": -1}},
        {"$limit": 5},
    ]
    results = list(orders.aggregate(pipeline))
    pprint(results)
    return results


def agg_delivered_top_cities(orders) -> list:
    print("\n" + "=" * 60)
    print("4. Aggregation -- Delivered Orders Top Cities ($match first)")
    print("=" * 60)

    pipeline = [
        {"$match": {"status": {"$in": ["تم التسليم", "delivered", "مكتمل", "Confirmed", "Paid"]}}},
        {
            "$group": {
                "_id": {"$ifNull": ["$city", "$customer.address.city"]},
                "total_sales": {
                    "$sum": {
                        "$convert": {
                            "input": "$total_amount",
                            "to": "double",
                            "onError": 0.0,
                            "onNull": 0.0
                        }
                    }
                },
                "orders_count": {"$sum": 1},
            }
        },
        {"$sort": {"total_sales": -1}},
        {"$limit": 5},
    ]
    start = perf_counter()
    results = list(orders.aggregate(pipeline))
    elapsed = perf_counter() - start

    print(f"Elapsed : {elapsed:.3f} seconds")
    pprint(results)
    return results


def agg_top_products(orders) -> list:
    print("\n" + "=" * 60)
    print("5. Aggregation -- Top 5 Products")
    print("=" * 60)

    # Check if structured array 'items' exists
    has_items = orders.find_one({"items": {"$exists": True, "$type": "array"}}) is not None
    if has_items:
        pipeline = [
            {"$unwind": "$items"},
            {
                "$group": {
                    "_id": "$items.sku",
                    "product_name": {"$first": "$items.name"},
                    "units_sold": {"$sum": "$items.qty"},
                    "sales": {"$sum": "$items.total"},
                }
            },
            {"$sort": {"units_sold": -1}},
            {"$limit": 5},
        ]
        results = list(orders.aggregate(pipeline))
    else:
        # Parse items_json robustly
        stats = defaultdict(lambda: {"product_name": "", "units_sold": 0, "sales": 0.0})
        for doc in orders.find({}, {"items_json": 1}):
            raw = doc.get("items_json")
            if not raw:
                continue
            try:
                items_list = json.loads(raw) if isinstance(raw, str) else raw
                if not isinstance(items_list, list):
                    continue
                for it in items_list:
                    sku = it.get("sku", "UNKNOWN")
                    stats[sku]["product_name"] = it.get("name", sku)
                    stats[sku]["units_sold"] += int(it.get("qty", 1) or 1)
                    stats[sku]["sales"] += safe_float(it.get("total", 0.0))
            except Exception:
                continue

        sorted_items = sorted(stats.items(), key=lambda x: x[1]["units_sold"], reverse=True)[:5]
        results = [
            {
                "_id": sku,
                "product_name": data["product_name"],
                "units_sold": data["units_sold"],
                "sales": round(data["sales"], 2)
            }
            for sku, data in sorted_items
        ]

    pprint(results)
    return results


def agg_monthly_sales(orders) -> list:
    print("\n" + "=" * 60)
    print("6. Aggregation -- Monthly Sales ($set + $convert + $group)")
    print("=" * 60)

    pipeline = [
        {
            "$set": {
                "_order_date": {
                    "$convert": {
                        "input": "$order_date",
                        "to": "date",
                        "onError": None,
                        "onNull": None,
                    }
                }
            }
        },
        {"$match": {"_order_date": {"$ne": None}}},
        {
            "$group": {
                "_id": {
                    "year": {"$year": "$_order_date"},
                    "month": {"$month": "$_order_date"},
                },
                "total_sales": {
                    "$sum": {
                        "$convert": {
                            "input": "$total_amount",
                            "to": "double",
                            "onError": 0.0,
                            "onNull": 0.0
                        }
                    }
                },
                "orders_count": {"$sum": 1},
            }
        },
        {"$sort": {"_id.year": 1, "_id.month": 1}},
    ]
    results = list(orders.aggregate(pipeline))
    print(f"Months found : {len(results)}")
    pprint(results[:6])
    return results


def _safe_drop_index_for_keys(orders, keys):
    target_keys = list(keys)
    for idx in orders.list_indexes():
        if idx["name"] == "_id_":
            continue
        if list(idx["key"].items()) == target_keys or idx["name"] in ["agg_idx_city", "idx_city", "agg_idx_city_date", "idx_city_date", "agg_idx_status", "idx_status"]:
            try:
                orders.drop_index(idx["name"])
            except Exception:
                pass


def _safe_create_index(orders, keys, name):
    _safe_drop_index_for_keys(orders, keys)
    return orders.create_index(keys, name=name)


# =========================================================
# 3. explain("executionStats") -- COLLSCAN -> IXSCAN
# =========================================================

def run_index_benchmark(db, orders) -> dict:
    print("\n" + "=" * 60)
    print("7. Indexing Benchmark -- explain('executionStats')")
    print("=" * 60)

    # Detect city field in collection
    sample = orders.find_one({"city": {"$exists": True}})
    city_field = "city" if sample else "customer.address.city"

    CITY = "صنعاء"
    city_query = {city_field: CITY}
    city_date_query = {
        city_field: CITY,
        "order_date": {
            "$gte": "2026-02-01",
            "$lt": "2026-05-01",
        },
    }
    status_query = {"status": {"$in": ["تم التسليم", "delivered", "مكتمل"]}}

    # Drop existing custom indexes for clean baseline measurement
    all_potential_indexes = [
        "agg_idx_city", "idx_city",
        "agg_idx_city_date", "idx_city_date",
        "agg_idx_status", "idx_status", "idx_status_amount"
    ]
    existing = {idx["name"] for idx in orders.list_indexes()}
    for name in all_potential_indexes:
        if name in existing:
            try:
                orders.drop_index(name)
                print(f"Dropped for baseline: {name}")
            except Exception:
                pass

    # City: Before
    t0 = perf_counter()
    orders.count_documents(city_query)
    elapsed_before = perf_counter() - t0
    summary_before = _explain_summary(_explain_find(db, COLLECTION_VALIDATED, city_query))
    print(f"\n[Before agg_idx_city]  elapsed={elapsed_before:.4f}s")
    pprint(summary_before)

    # City: Create index + After
    _safe_create_index(orders, [(city_field, ASCENDING)], name="agg_idx_city")
    print("OK agg_idx_city created")
    t0 = perf_counter()
    orders.count_documents(city_query)
    elapsed_after = perf_counter() - t0
    summary_after = _explain_summary(_explain_find(db, COLLECTION_VALIDATED, city_query))
    print(f"[After agg_idx_city]   elapsed={elapsed_after:.4f}s")
    pprint(summary_after)

    # Compound (city + date): Before
    t0 = perf_counter()
    orders.count_documents(city_date_query)
    elapsed_cd_before = perf_counter() - t0
    summary_cd_before = _explain_summary(_explain_find(db, COLLECTION_VALIDATED, city_date_query))

    # Compound: Create + After
    _safe_create_index(
        orders,
        [(city_field, ASCENDING), ("order_date", ASCENDING)],
        name="agg_idx_city_date",
    )
    print("OK agg_idx_city_date created")
    t0 = perf_counter()
    orders.count_documents(city_date_query)
    elapsed_cd_after = perf_counter() - t0
    summary_cd_after = _explain_summary(_explain_find(db, COLLECTION_VALIDATED, city_date_query))

    # Status: Before
    t0 = perf_counter()
    orders.count_documents(status_query)
    elapsed_st_before = perf_counter() - t0
    summary_st_before = _explain_summary(_explain_find(db, COLLECTION_VALIDATED, status_query))

    # Status: Create + After
    _safe_create_index(orders, [("status", ASCENDING)], name="agg_idx_status")
    print("OK agg_idx_status created")
    t0 = perf_counter()
    orders.count_documents(status_query)
    elapsed_st_after = perf_counter() - t0
    summary_st_after = _explain_summary(_explain_find(db, COLLECTION_VALIDATED, status_query))

    performance = [
        {"query": "City",      "state": "Before Index",    "elapsed_seconds": elapsed_before,    **summary_before},
        {"query": "City",      "state": "After Index",     "elapsed_seconds": elapsed_after,     **summary_after},
        {"query": "City+Date", "state": "Before Compound", "elapsed_seconds": elapsed_cd_before, **summary_cd_before},
        {"query": "City+Date", "state": "After Compound",  "elapsed_seconds": elapsed_cd_after,  **summary_cd_after},
        {"query": "Status",    "state": "Before Index",    "elapsed_seconds": elapsed_st_before, **summary_st_before},
        {"query": "Status",    "state": "After Index",     "elapsed_seconds": elapsed_st_after,  **summary_st_after},
    ]

    print("\n-- Performance Comparison --")
    for row in performance:
        print(
            f"  {row['query']:12s}  {row['state']:22s}"
            f"  stages={row['stages']}"
            f"  time={row['elapsed_seconds']:.4f}s"
            f"  docsExamined={row['totalDocsExamined']}"
        )
    return {"performance": performance}


# =========================================================
# 4. Heavy Pipeline (month x city x sales)
# =========================================================

def _build_heavy_pipeline(orders) -> list:
    has_items = orders.find_one({"items": {"$exists": True, "$type": "array"}}) is not None

    if has_items:
        return [
            {
                "$set": {
                    "_order_date": {
                        "$convert": {
                            "input": "$order_date",
                            "to": "date",
                            "onError": None,
                            "onNull": None,
                        }
                    }
                }
            },
            {"$match": {"_order_date": {"$ne": None}}},
            {"$unwind": "$items"},
            {
                "$group": {
                    "_id": {
                        "year": {"$year": "$_order_date"},
                        "month": {"$month": "$_order_date"},
                        "city": {"$ifNull": ["$city", "$customer.address.city"]},
                        "sku": "$items.sku",
                        "product_name": "$items.name",
                    },
                    "units_sold": {"$sum": "$items.qty"},
                    "product_sales": {"$sum": "$items.total"},
                    "orders_count": {"$sum": 1},
                }
            },
            {"$sort": {"_id.year": 1, "_id.month": 1, "_id.city": 1, "product_sales": -1}},
            {
                "$group": {
                    "_id": {"year": "$_id.year", "month": "$_id.month", "city": "$_id.city"},
                    "city_total_sales": {"$sum": "$product_sales"},
                    "city_units_sold": {"$sum": "$units_sold"},
                    "products": {
                        "$push": {
                            "sku": "$_id.sku",
                            "name": "$_id.product_name",
                            "sales": "$product_sales",
                            "units": "$units_sold",
                            "orders": "$orders_count",
                        }
                    },
                }
            },
            {
                "$project": {
                    "_id": {
                        "$concat": [
                            {"$toString": "$_id.year"},
                            "_",
                            {"$toString": "$_id.month"},
                            "_",
                            {"$ifNull": [{"$toString": "$_id.city"}, "UNKNOWN"]}
                        ]
                    },
                    "year": "$_id.year",
                    "month": "$_id.month",
                    "city": "$_id.city",
                    "city_total_sales": {"$round": ["$city_total_sales", 2]},
                    "city_units_sold": 1,
                    "top_products": {"$slice": ["$products", 5]},
                }
            },
            {"$sort": {"year": 1, "month": 1, "city_total_sales": -1}},
        ]
    else:
        # Pipeline when items_json is string
        return [
            {
                "$set": {
                    "_order_date": {
                        "$convert": {
                            "input": "$order_date",
                            "to": "date",
                            "onError": None,
                            "onNull": None,
                        }
                    },
                    "_amount": {
                        "$convert": {
                            "input": "$total_amount",
                            "to": "double",
                            "onError": 0.0,
                            "onNull": 0.0
                        }
                    }
                }
            },
            {"$match": {"_order_date": {"$ne": None}}},
            {
                "$group": {
                    "_id": {
                        "year": {"$year": "$_order_date"},
                        "month": {"$month": "$_order_date"},
                        "city": {"$ifNull": ["$city", "$customer.address.city"]},
                    },
                    "city_total_sales": {"$sum": "$_amount"},
                    "orders_count": {"$sum": 1},
                }
            },
            {
                "$project": {
                    "_id": {
                        "$concat": [
                            {"$toString": "$_id.year"},
                            "_",
                            {"$toString": "$_id.month"},
                            "_",
                            {"$ifNull": [{"$toString": "$_id.city"}, "UNKNOWN"]}
                        ]
                    },
                    "year": "$_id.year",
                    "month": "$_id.month",
                    "city": "$_id.city",
                    "city_total_sales": {"$round": ["$city_total_sales", 2]},
                    "orders_count": 1,
                }
            },
            {"$sort": {"year": 1, "month": 1, "city_total_sales": -1}},
        ]


def run_heavy_aggregation(orders) -> list:
    print("\n" + "=" * 60)
    print("8. Heavy Aggregation -- month x city breakdown")
    print("=" * 60)

    pipeline = _build_heavy_pipeline(orders)
    start = perf_counter()
    results = list(orders.aggregate(pipeline, allowDiskUse=True))
    elapsed = perf_counter() - start

    print(f"Rows returned : {len(results)}")
    print(f"Elapsed       : {elapsed:.2f} seconds")
    if results:
        pprint(results[0])
    return results


# =========================================================
# 5. Materialized View -- $merge
# =========================================================

def materialize_monthly_city_products(orders, db) -> dict:
    print("\n" + "=" * 60)
    print("9. Materialized View -- $merge -> monthly_city_products")
    print("=" * 60)

    # Ensure target index exists for fast month/city lookups
    target_col = db[COLLECTION_MATERIALIZED]
    try:
        target_col.create_index(
            [("year", ASCENDING), ("month", ASCENDING), ("city", ASCENDING)],
            name="idx_mat_year_month_city"
        )
    except Exception:
        pass

    pipeline = _build_heavy_pipeline(orders)
    pipeline.append(
        {
            "$merge": {
                "into": COLLECTION_MATERIALIZED,
                "on": "_id",
                "whenMatched": "replace",
                "whenNotMatched": "insert",
            }
        }
    )

    start = perf_counter()
    list(orders.aggregate(pipeline, allowDiskUse=True))
    elapsed = perf_counter() - start

    count = db[COLLECTION_MATERIALIZED].count_documents({})
    result = {
        "collection": COLLECTION_MATERIALIZED,
        "documents_written": count,
        "elapsed_seconds": round(elapsed, 2),
    }

    print(f"Materialized View built in {elapsed:.2f}s")
    print(f"   Collection : {COLLECTION_MATERIALIZED}")
    print(f"   Documents  : {count:,}")

    sample = db[COLLECTION_MATERIALIZED].find_one({}, {"_id": 0})
    if sample:
        print("\nSample document:")
        pprint(sample)

    return result


# =========================================================
# Entry point
# =========================================================

def run_all(db) -> dict:
    orders = db[COLLECTION_VALIDATED]
    report = {}

    report["baseline_total_sales"]    = client_side_total_sales(orders)
    report["sales_by_city"]           = agg_sales_by_city(orders)
    report["top5_cities"]             = agg_top5_cities(orders)
    report["delivered_top_cities"]    = agg_delivered_top_cities(orders)
    report["top_products"]            = agg_top_products(orders)
    report["monthly_sales"]           = agg_monthly_sales(orders)
    report["index_benchmark"]         = run_index_benchmark(db, orders)
    report["heavy_aggregation_count"] = len(run_heavy_aggregation(orders))
    report["materialized_view"]       = materialize_monthly_city_products(orders, db)

    print("\n" + "=" * 60)
    print("Phase 2 complete -- all aggregations done successfully")
    print("=" * 60)
    return report


if __name__ == "__main__":
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    try:
        run_all(db)
    finally:
        client.close()

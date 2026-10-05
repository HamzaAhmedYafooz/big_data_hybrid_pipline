"""
وحدة الاستعلامات والفهارس وتحسين الأداء (Analytics Queries, Indexes & Explain Optimization)
وفق مواصفات مقرر البيانات الضخمة (جامعة الرازي) - المحاضرة 7 (07_aggregation_query_optimization.ipynb)
"""

import sys
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='backslashreplace')
        sys.stderr.reconfigure(encoding='utf-8', errors='backslashreplace')
    except Exception:
        pass

import time
from pprint import pprint
from pymongo import MongoClient, ASCENDING, DESCENDING
from config.settings import MONGO_URI, MONGO_DB_NAME, COLLECTION_VALIDATED

# ==============================================================================
# 1. تعريف الاستعلامات العملية الخمسة (5 Practical Business Queries)
# ==============================================================================

QUERIES_DEFINITION = {
    "customer_orders": {
        "title": "استعلام طلبات عميل محدد",
        "description": "البحث عن جميع الطلبات الخاصة بعميل معين للوصول السريع لملف العميل وسجل مشترياته.",
        "target_index": "idx_customer_id",
        "index_type": "Single Field Index",
        "filter": lambda customer_id="عميل-8000001": {"customer_id": customer_id},
        "sample_filter": {"customer_id": "عميل-8000001"}
    },
    "city_orders": {
        "title": "استعلام طلبات مدينة محددة",
        "description": "تصفية الطلبات حسب مدينة التوصيل لدراسة التوزيع الجغرافي واللوجستي.",
        "target_index": "idx_city",
        "index_type": "Single Field Index",
        "filter": lambda city="صنعاء": {"city": city},
        "sample_filter": {"city": "صنعاء"}
    },
    "city_date_range": {
        "title": "استعلام مركب: مدينة ونطاق تاريخي",
        "description": "البحث عن طلبات مدينة معينة خلال فترة زمنية محددة (استعلام عملي مركب لتتبع مبيعات الفروع).",
        "target_index": "idx_city_date",
        "index_type": "Compound Index (City + Order Date)",
        "filter": lambda city="صنعاء", start_date="2026-01-01", end_date="2026-07-01": {
            "city": city,
            "order_date": {"$gte": start_date, "$lt": end_date}
        },
        "sample_filter": {
            "city": "صنعاء",
            "order_date": {"$gte": "2026-01-01", "$lt": "2026-07-01"}
        }
    },
    "status_high_value": {
        "title": "استعلام الطلبات ذات القيمة العالية وحالة محددة",
        "description": "جلب الطلبات المكتملة أو المؤكدة التي تتجاوز قيمة مالية معينة لمتابعة كبار العملاء والسيولة.",
        "target_index": "idx_status_amount",
        "index_type": "Compound Index (Status + Total Amount)",
        "filter": lambda status="تم التسليم", min_amount=10000.0: {
            "status": status,
            "$or": [
                {"total_amount": {"$gte": float(min_amount)}},
                {"total_amount": {"$gte": str(float(min_amount))}}
            ]
        },
        "sample_filter": {
            "status": {"$in": ["تم التسليم", "مؤكد", "مكتمل"]},
            "$or": [
                {"total_amount": {"$gte": 10000.0}},
                {"total_amount": {"$gte": "10000.0"}}
            ]
        }
    },
    "payment_method_orders": {
        "title": "استعلام طلبات طريقة دفع محددة",
        "description": "استرجاع الطلبات حسب وسيلة الدفع (نقد عند الاستلام أو بطاقة) لمطابقة الحسابات المالية.",
        "target_index": "idx_payment_method",
        "index_type": "Single Field Index",
        "filter": lambda method="نقدًا عند التسليم": {"payment_method": method},
        "sample_filter": {"payment_method": {"$in": ["نقدًا عند التسليم", "نقد عند الاستلام"]}}
    }
}

# ==============================================================================
# 2. تعريف الفهارس المطلوبة (3 Indexes Minimum Including 1 Compound Index)
# ==============================================================================

INDEXES_DEFINITION = [
    {
        "name": "idx_customer_id",
        "keys": [("customer_id", ASCENDING)],
        "type": "Single Index",
        "reason": "تسريع عمليات البحث الفردية لخدمة العملاء من O(N) إلى O(log N) عبر الشجرة الثنائية."
    },
    {
        "name": "idx_city",
        "keys": [("city", ASCENDING)],
        "type": "Single Index",
        "reason": "تجنب مسح كامل الجدول (COLLSCAN) عند تصفية الطلبات الجغرافية في المدن الرئيسية."
    },
    {
        "name": "idx_city_date",
        "keys": [("city", ASCENDING), ("order_date", ASCENDING)],
        "type": "Compound Index",
        "reason": "فهرس مركب أساسي يخدم مبدأ Equality-Sort-Range، حيث يبدأ بالتطابق على المدينة ثم الفلترة أو الترتيب بالتاريخ."
    },
    {
        "name": "idx_status_amount",
        "keys": [("status", ASCENDING), ("total_amount", ASCENDING)],
        "type": "Compound Index (Bonus)",
        "reason": "فهرس مركب إضافي لتسريع تقارير السيولة والطلبات المنجزة ذات المبالغ الكبيرة."
    }
]

# ==============================================================================
# 3. دوال Explain وقياس الأداء بنفس أسلوب المحاضرة 7 (Cell 31)
# ==============================================================================

def explain_find(db, collection_name, filter_query):
    """
    تنفيذ أمر explain على مستوى قاعدة البيانات بأسلوب المحاضرة 7:
    db.command('explain', {'find': ..., 'filter': ...}, verbosity='executionStats')
    """
    return db.command(
        "explain",
        {
            "find": collection_name,
            "filter": filter_query,
        },
        verbosity="executionStats",
    )

def collect_stages(node):
    """
    Sequentially collect unique stages from the winning plan (winningPlan),
    such as: COLLSCAN, IXSCAN, or FETCH.
    """
    stages = []
    if isinstance(node, dict):
        if "stage" in node:
            stages.append(node["stage"])
        for value in node.values():
            stages.extend(collect_stages(value))
    elif isinstance(node, list):
        for item in node:
            stages.extend(collect_stages(item))
    return stages

def explain_summary(explain_doc):
    """
    Extract a precise performance summary:
    - stages
    - number of returned documents (nReturned)
    - execution time in milliseconds (executionTimeMillis)
    - number of index keys examined (totalKeysExamined)
    - number of documents examined from disk/memory (totalDocsExamined)
    """
    stats = explain_doc.get("executionStats", {})
    planner = explain_doc.get("queryPlanner", {})
    winning_plan = planner.get("winningPlan", {})

    return {
        "stages": list(dict.fromkeys(collect_stages(winning_plan))),
        "nReturned": stats.get("nReturned", 0),
        "executionTimeMillis": stats.get("executionTimeMillis", 0),
        "totalKeysExamined": stats.get("totalKeysExamined", 0),
        "totalDocsExamined": stats.get("totalDocsExamined", 0),
    }

# ==============================================================================
# 4. Index management (create and drop)
# ==============================================================================

def create_indexes(db, collection_name=COLLECTION_VALIDATED):
    """Create all indexes approved in the requirements document and lecture 7."""
    col = db[collection_name]
    created = []
    for idx in INDEXES_DEFINITION:
        try:
            # Drop any existing index with the same keys or name to prevent IndexOptionsConflict
            target_keys = list(idx["keys"])
            for existing in col.list_indexes():
                if existing["name"] != "_id_":
                    if existing["name"] == idx["name"] or list(existing["key"].items()) == target_keys:
                        try:
                            col.drop_index(existing["name"])
                        except Exception:
                            pass
            name = col.create_index(idx["keys"], name=idx["name"])
            created.append({"name": name, "keys": idx["keys"], "type": idx["type"]})
            print(f"✅ Index created successfully: {idx['name']} ({idx['type']})")
        except Exception as e:
            print(f"❌ Error creating index {idx['name']}: {e}")
    return created

def drop_indexes(db, collection_name=COLLECTION_VALIDATED):
    """Drop custom indexes to measure baseline performance without removing primary order_id index"""
    col = db[collection_name]
    dropped = []
    for idx in INDEXES_DEFINITION:
        name = idx["name"]
        try:
            col.drop_index(name)
            dropped.append(name)
            print(f"🗑️ Dropped index for baseline measurement: {name}")
        except Exception:
            pass
    return dropped

# ==============================================================================
# 5. تشغيل الاستعلامات وجلب البيانات
# ==============================================================================

def execute_query(db, query_name, custom_params=None, limit=100, collection_name=COLLECTION_VALIDATED):
    """
    Execute a specific named query and return results with execution time
    (Serves API route: GET /queries/{name})
    """
    if query_name not in QUERIES_DEFINITION:
        raise ValueError(f"Unknown query: {query_name}. Available: {list(QUERIES_DEFINITION.keys())}")

    q_def = QUERIES_DEFINITION[query_name]
    
    if not custom_params:
        filter_dict = dict(q_def["sample_filter"])
    else:
        filter_dict = {}
        if query_name == "customer_orders":
            cid = custom_params.get("customer_id")
            filter_dict["customer_id"] = cid if cid else "عميل-8000001"

        elif query_name == "city_orders":
            city = custom_params.get("city")
            if city and city not in ("all", "الكل", "all_cities"):
                filter_dict["city"] = city
            else:
                filter_dict["city"] = "صنعاء"

        elif query_name == "city_date_range":
            city = custom_params.get("city")
            if city and city not in ("all", "الكل", "all_cities"):
                filter_dict["city"] = city
            else:
                filter_dict["city"] = "صنعاء"
                
            if "order_date" in custom_params:
                filter_dict["order_date"] = custom_params["order_date"]
            elif "start_date" in custom_params or "end_date" in custom_params:
                d_q = {}
                if "start_date" in custom_params and custom_params["start_date"]:
                    d_q["$gte"] = custom_params["start_date"]
                if "end_date" in custom_params and custom_params["end_date"]:
                    d_q["$lt"] = custom_params["end_date"]
                if d_q:
                    filter_dict["order_date"] = d_q
                else:
                    filter_dict["order_date"] = {"$gte": "2026-01-01", "$lt": "2026-07-01"}
            else:
                filter_dict["order_date"] = {"$gte": "2026-01-01", "$lt": "2026-07-01"}

        elif query_name == "status_high_value":
            st = custom_params.get("status")
            if st and st not in ("all", "الكل"):
                if st == "مكتمل":
                    filter_dict["status"] = {"$in": ["تم التسليم", "مؤكد", "مكتمل"]}
                else:
                    filter_dict["status"] = st
            else:
                filter_dict["status"] = {"$in": ["تم التسليم", "مؤكد", "مكتمل"]}

            min_val = custom_params.get("min_amount")
            if min_val is None and "total_amount" in custom_params:
                ta = custom_params["total_amount"]
                if isinstance(ta, dict) and "$gte" in ta:
                    min_val = ta["$gte"]
            if min_val is None:
                min_val = 10000.0

            try:
                min_f = float(min_val)
            except (ValueError, TypeError):
                min_f = 10000.0

            filter_dict["$or"] = [
                {"total_amount": {"$gte": min_f}},
                {"total_amount": {"$gte": str(min_f)}}
            ]

        elif query_name == "payment_method_orders":
            pm = custom_params.get("payment_method")
            if pm and pm not in ("all", "الكل"):
                if "نقد" in pm:
                    filter_dict["payment_method"] = {"$in": ["نقدًا عند التسليم", "نقد عند الاستلام"]}
                else:
                    filter_dict["payment_method"] = pm
            else:
                filter_dict["payment_method"] = {"$in": ["نقدًا عند التسليم", "نقد عند الاستلام"]}
        else:
            filter_dict = {**q_def["sample_filter"], **custom_params}

    col = db[collection_name]
    start_t = time.perf_counter()
    cursor = col.find(filter_dict, {"_id": 0}).limit(limit)
    results = list(cursor)
    elapsed_sec = time.perf_counter() - start_t

    return {
        "query_name": query_name,
        "title": q_def["title"],
        "target_index": q_def.get("target_index"),
        "index_type": q_def.get("index_type"),
        "filter_applied": filter_dict,
        "count_returned": len(results),
        "execution_time_seconds": round(elapsed_sec, 4),
        "results": results
    }

# ==============================================================================
# 6. المقارنة القياسية قبل وبعد الفهارس (Explain Comparison Runner)
# ==============================================================================

def run_explain_benchmarks(db, collection_name=COLLECTION_VALIDATED):
    """
    Execute 3 core benchmark queries before and after creating indexes:
    1. Customer Orders (Single Index: idx_customer_id)
    2. City Orders (Single Index: idx_city)
    3. City + Date Range (Compound Index: idx_city_date)
    """
    test_queries = ["customer_orders", "city_orders", "city_date_range"]
    col = db[collection_name]

    print("\n" + "="*70)
    print("🔬 Running Explain ('executionStats') Benchmark (Before vs After Indexing)")
    print("="*70)

    # Step A: Drop indexes to measure baseline full table scan (COLLSCAN)
    print("\n[1/3] Dropping indexes to measure baseline (COLLSCAN)...")
    drop_indexes(db, collection_name)

    before_stats = {}
    for q_name in test_queries:
        filter_q = QUERIES_DEFINITION[q_name]["sample_filter"]
        exp_doc = explain_find(db, collection_name, filter_q)
        before_stats[q_name] = explain_summary(exp_doc)
        print(f"-> Tested [{q_name}] before index: stages={before_stats[q_name]['stages']} | docsExamined={before_stats[q_name]['totalDocsExamined']:,} | time={before_stats[q_name]['executionTimeMillis']}ms")

    # Step B: Create approved indexes
    print("\n[2/3] Creating approved indexes...")
    create_indexes(db, collection_name)

    # Step C: Re-measure after enabling indexes (IXSCAN)
    print("\n[3/3] Measuring performance with indexes enabled (IXSCAN)...")
    after_stats = {}
    comparison_report = []

    for q_name in test_queries:
        filter_q = QUERIES_DEFINITION[q_name]["sample_filter"]
        q_info = QUERIES_DEFINITION[q_name]
        exp_doc = explain_find(db, collection_name, filter_q)
        after_stats[q_name] = explain_summary(exp_doc)

        b_stat = before_stats[q_name]
        a_stat = after_stats[q_name]

        # Calculate reduction in docs examined
        b_docs = b_stat["totalDocsExamined"]
        a_docs = a_stat["totalDocsExamined"]
        docs_reduction = round(((b_docs - a_docs) / b_docs * 100), 2) if b_docs > 0 else 0

        row = {
            "query_name": q_name,
            "title": q_info["title"],
            "index_used": q_info["target_index"],
            "index_type": q_info["index_type"],
            "before": {
                "stages": b_stat["stages"],
                "docs_examined": b_docs,
                "keys_examined": b_stat["totalKeysExamined"],
                "time_ms": b_stat["executionTimeMillis"],
                "returned": b_stat["nReturned"]
            },
            "after": {
                "stages": a_stat["stages"],
                "docs_examined": a_docs,
                "keys_examined": a_stat["totalKeysExamined"],
                "time_ms": a_stat["executionTimeMillis"],
                "returned": a_stat["nReturned"]
            },
            "efficiency_gain": {
                "docs_scanned_reduction_pct": f"{docs_reduction}%",
                "stage_transition": f"{' -> '.join(b_stat['stages'])}  ==>  {' -> '.join(a_stat['stages'])}"
            }
        }
        comparison_report.append(row)

    print("\n" + "="*70)
    print("📊 Explain Results Summary (Performance Impact):")
    print("="*70)
    for rep in comparison_report:
        print(f"\n🔹 Query: {rep['title']} ({rep['query_name']})")
        print(f"   Index: {rep['index_used']} [{rep['index_type']}]")
        print(f"   Before Index: stages={rep['before']['stages']} | docs examined={rep['before']['docs_examined']:,} | time={rep['before']['time_ms']} ms")
        print(f"   After Index:  stages={rep['after']['stages']} | docs examined={rep['after']['docs_examined']:,} | time={rep['after']['time_ms']} ms")
        print(f"   Improvement:  Documents examined reduced by {rep['efficiency_gain']['docs_scanned_reduction_pct']}")

    return comparison_report

if __name__ == "__main__":
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    try:
        report = run_explain_benchmarks(db)
    finally:
        client.close()

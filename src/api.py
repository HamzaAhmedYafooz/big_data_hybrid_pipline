"""
src/api.py
==========
المرحلة الخامسة من المشروع النهائي: واجهة API الموحدة للتشغيل والاختبار
وفق المواصفات الرسمية المقررة لمقرر البيانات الضخمة (جامعة الرازي)

المسارات المطلوبة رسمياً لنظام التقييم التلقائي:
  - GET  /health
  - POST /ingest
  - POST /indexes
  - GET  /queries
  - GET  /queries/{name}
  - GET  /aggregations
  - GET  /aggregations/{name}
  - POST /refresh-mv
  - GET  /jobs
  - POST /jobs/{name}/run

التوثيق التفاعلي متاح تلقائياً عبر: /docs (Swagger UI)
"""

from __future__ import annotations

import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from pymongo import MongoClient

from config.settings import (
    COLLECTION_VALIDATED,
    HUGE_FILE_PATH,
    MONGO_DB_NAME,
    MONGO_URI,
    SMALL_SAMPLE_PATH,
)
from src.analytics_queries import (
    INDEXES_DEFINITION,
    QUERIES_DEFINITION,
    create_indexes,
    execute_query,
    run_explain_benchmarks,
)
from src.batch_loader import load_batch
from src.elt_pipeline import run_elt
from src.file_router import select_processing_engine
from src.materialized_views import (
    COLLECTION_MV_DAILY_SALES,
    COLLECTION_MV_TOP_PRODUCTS,
    get_daily_sales_view,
    get_top_products_view,
    refresh_all_views,
    refresh_daily_sales_summary,
    refresh_top_products_summary,
)
from src.metrics import generate_metrics
from src.mongo_setup import setup_mongodb
from src.scheduler_jobs import (
    REGISTERED_JOBS,
    BackgroundJobScheduler,
    execute_job,
    get_job_history,
    list_registered_jobs,
)
from src.spark_loader import load_spark


# ==============================================================================
# نماذج البيانات (Pydantic Models)
# ==============================================================================

class IngestRequest(BaseModel):
    file_path: Optional[str] = None
    sample_size: Optional[int] = None


class RefreshMVRequest(BaseModel):
    mode: Optional[str] = "incremental"  # incremental or full


# ==============================================================================
# إدارة دورة حياة التطبيق (Lifespan & Background Worker)
# ==============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    # بدء الاتصال بقاعدة البيانات
    app.state.client = MongoClient(MONGO_URI)
    app.state.db = app.state.client[MONGO_DB_NAME]
    print("✅ Connected to MongoDB successfully")

    # Start background job scheduler
    BackgroundJobScheduler.start()

    yield

    # Stop scheduler and close MongoDB client
    BackgroundJobScheduler.stop()
    app.state.client.close()
    print("🔒 Connection to MongoDB and Background Scheduler closed")


app = FastAPI(
    title="Unified Big Data Pipeline API",
    description="Unified API for execution and automated evaluation - Big Data Course (Al-Razi University)",
    version="2.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)


# ==============================================================================
# Helper Functions
# ==============================================================================

def get_db(req: Request):
    return req.app.state.db


import os
from fastapi.responses import FileResponse

# Mount static files directory (serves CSS, JS, images, and HTML assets)
_static_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static")
if os.path.isdir(_static_dir):
    app.mount("/static", StaticFiles(directory=_static_dir), name="static")

# ==============================================================================
# 1. Health Check & Root & Dashboard
# ==============================================================================

@app.get("/", tags=["1. System Health"])
def root():
    """
    Root endpoint: returns service status and links to documentation and dashboard.
    """
    return {
        "service": "Unified Big Data Pipeline API",
        "status": "online",
        "message": "Welcome to Al-Razi Big Data Pipeline API",
        "documentation": "/docs",
        "dashboard": "/dashboard",
        "health_check": "/health"
    }


@app.get("/dashboard", tags=["1. System Health"], response_class=FileResponse)
def get_dashboard():
    """
    Executive Analytics Dashboard: Real-time UI for API operations,
    Explain benchmarks, Aggregation reports, and Scheduled Jobs.
    """
    dashboard_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static", "dashboard.html")
    if os.path.exists(dashboard_path):
        return FileResponse(dashboard_path)
    raise HTTPException(status_code=404, detail="Dashboard file not found")


@app.get("/health", tags=["1. System Health"])
def get_health(req: Request):
    """
    GET /health
    Verify system status and MongoDB database connectivity.
    """
    db = get_db(req)
    db_status = "connected"
    try:
        db.command("ping")
    except Exception as e:
        db_status = f"disconnected: {e}"

    return {
        "status": "healthy" if db_status == "connected" else "unhealthy",
        "service": "Unified Big Data Pipeline API",
        "database": db_status,
        "timestamp": datetime.utcnow().isoformat(),
        "version": "2.0.0"
    }


# ==============================================================================
# 2. Ingest Pipeline
# ==============================================================================

@app.post("/ingest", tags=["2. Data Ingestion"])
def trigger_ingest(payload: Optional[IngestRequest] = None):
    """
    POST /ingest
    Uses the exact same ELT pipeline and ingestion gateway implemented in the midterm.
    """
    target_path = (payload.file_path if payload and payload.file_path else None) or SMALL_SAMPLE_PATH
    if not os.path.exists(target_path):
        alt_path = os.path.join("data", "01_student_test_small.csv")
        if os.path.exists(alt_path):
            target_path = alt_path

    engine = select_processing_engine(target_path)
    if not engine:
        raise HTTPException(status_code=404, detail=f"File not found: {target_path}")

    setup_mongodb()
    run_id = f"run_{uuid.uuid4().hex[:8]}"

    start_time_total = time.time()
    raw_loaded = 0
    if engine == "python_batch":
        raw_loaded = load_batch(run_id, target_path)
    elif engine == "pyspark":
        raw_loaded = load_spark(run_id, target_path)

    elt_counters = {}
    if raw_loaded > 0:
        elt_counters, _ = run_elt(run_id)

    total_seconds = time.time() - start_time_total
    metrics = generate_metrics(run_id, target_path, engine, raw_loaded, elt_counters, total_seconds)

    return {
        "status": "success",
        "run_id": run_id,
        "engine_used": engine,
        "raw_records_loaded": raw_loaded,
        "metrics": metrics
    }


# ==============================================================================
# 3. Indexing Management
# ==============================================================================

@app.post("/indexes", tags=["3. Indexing"])
def setup_indexes(req: Request):
    """
    POST /indexes
    Create and activate all required indexes (including compound index).
    """
    db = get_db(req)
    created = create_indexes(db)
    return {
        "status": "success",
        "message": f"Successfully configured and activated {len(created)} indexes.",
        "indexes_created": created
    }


@app.get("/indexes/explain", tags=["3. Indexing"])
def run_explain(req: Request):
    """
    GET /indexes/explain
    تنفيذ فحص Explain (executionStats) قبل وبعد الفهارس لبيان تحول المسار من COLLSCAN إلى IXSCAN.
    """
    db = get_db(req)
    report = run_explain_benchmarks(db)
    return {
        "status": "success",
        "benchmark_report": report
    }


# ==============================================================================
# 4. الاستعلامات (Practical Queries)
# ==============================================================================

@app.get("/queries", tags=["4. Practical Queries"])
def list_queries():
    """
    GET /queries
    عرض قائمة الاستعلامات العملية الخمسة المتاحة مع وصف كل منها والفهرس المخصص لها.
    """
    summary = []
    for k, v in QUERIES_DEFINITION.items():
        summary.append({
            "name": k,
            "title": v["title"],
            "description": v["description"],
            "target_index": v["target_index"],
            "index_type": v["index_type"],
            "sample_filter": v["sample_filter"]
        })
    return {"total_queries": len(summary), "queries": summary}


@app.get("/queries/{name}", tags=["4. Practical Queries"])
def run_query(
    name: str,
    req: Request,
    customer_id: Optional[str] = None,
    city: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    status: Optional[str] = None,
    payment_method: Optional[str] = None,
    min_amount: Optional[float] = None,
    limit: int = Query(50, ge=1, le=1000)
):
    """
    GET /queries/{name}
    تنفيذ استعلام عملي محدد بالاسم مع تمرير المعاملات الاختيارية.
    الأسماء المتاحة:
      - customer_orders
      - city_orders
      - city_date_range
      - status_high_value
      - payment_method_orders
    """
    db = get_db(req)

    custom_params = {}
    
    # Sanitize customer_id
    if customer_id and customer_id.strip() and customer_id.strip() not in ("null", "undefined"):
        custom_params["customer_id"] = customer_id.strip()
        
    # Sanitize city
    if city and city.strip() and city.strip() not in ("all", "الكل", "all_cities", "المدينة (اختياري)", "City (optional)", "null", "undefined"):
        custom_params["city"] = city.strip()
        
    # Sanitize status
    if status and status.strip() and status.strip() not in ("all", "الكل", "null", "undefined"):
        custom_params["status"] = status.strip()
        
    # Sanitize payment_method
    if payment_method and payment_method.strip() and payment_method.strip() not in ("all", "الكل", "null", "undefined"):
        custom_params["payment_method"] = payment_method.strip()
        
    # Sanitize date range
    clean_start = start_date.strip() if start_date and start_date.strip() not in ("null", "undefined") else None
    clean_end = end_date.strip() if end_date and end_date.strip() not in ("null", "undefined") else None
    if clean_start or clean_end:
        date_q = {}
        if clean_start:
            date_q["$gte"] = clean_start
        if clean_end:
            date_q["$lt"] = clean_end
        custom_params["order_date"] = date_q
        
    # Sanitize min_amount
    if min_amount is not None:
        custom_params["min_amount"] = min_amount
        custom_params["total_amount"] = {"$gte": min_amount}

    try:
        res = execute_query(db, name, custom_params=custom_params if custom_params else None, limit=limit)
        return res
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


# ==============================================================================
# 5. التجميعات والتقارير (Aggregation Reports)
# ==============================================================================

AGGREGATIONS_REGISTRY = {
    "sales_by_city": {
        "title": "المبيعات حسب المدينة",
        "description": "إجمالي المبيعات وعدد الطلبات مجمعة حسب المدينة ($group)",
        "pipeline": [
            {
                "$group": {
                    "_id": "$city",
                    "total_sales": {
                        "$sum": {
                            "$convert": {"input": "$total_amount", "to": "double", "onError": 0.0, "onNull": 0.0}
                        }
                    },
                    "orders_count": {"$sum": 1},
                    "avg_order_value": {
                        "$avg": {
                            "$convert": {"input": "$total_amount", "to": "double", "onError": 0.0, "onNull": 0.0}
                        }
                    }
                }
            },
            {"$sort": {"total_sales": -1}}
        ]
    },
    "top_products": {
        "title": "أفضل المنتجات مبيعاً",
        "description": "تفكيك السلة وعرض أفضل المنتجات حسب المبيعات والكمية",
        "pipeline": [
            # يمكن الاستعلام من العرض المادي مباشرة لسرعة خيالية
        ]
    },
    "top_customers": {
        "title": "أفضل العملاء (Top Customers)",
        "description": "تحديد أكثر العملاء إنفاقاً وعدد طلباتهم لتفعيل برامج الولاء",
        "pipeline": [
            {
                "$group": {
                    "_id": "$customer_id",
                    "customer_name": {"$first": "$customer_name"},
                    "customer_email": {"$first": "$customer_email"},
                    "total_spent": {
                        "$sum": {
                            "$convert": {"input": "$total_amount", "to": "double", "onError": 0.0, "onNull": 0.0}
                        }
                    },
                    "orders_count": {"$sum": 1}
                }
            },
            {"$sort": {"total_spent": -1}},
            {"$limit": 20}
        ]
    },
    "sales_by_period": {
        "title": "المبيعات حسب الفترة (يومياً / شهرياً)",
        "description": "تتبع الإيرادات التاريخية مع ترتيب زمني متصاعد",
        "pipeline": [
            {
                "$group": {
                    "_id": "$order_date",
                    "total_sales": {
                        "$sum": {
                            "$convert": {"input": "$total_amount", "to": "double", "onError": 0.0, "onNull": 0.0}
                        }
                    },
                    "orders_count": {"$sum": 1}
                }
            },
            {"$sort": {"_id": -1}},
            {"$limit": 30}
        ]
    },
    "orders_by_status": {
        "title": "توزيع الطلبات حسب الحالة (Orders by Status)",
        "description": "دراسة نسبة الطلبات المكتملة، قيد التوصيل، والملغية",
        "pipeline": [
            {
                "$group": {
                    "_id": "$status",
                    "orders_count": {"$sum": 1},
                    "total_value": {
                        "$sum": {
                            "$convert": {"input": "$total_amount", "to": "double", "onError": 0.0, "onNull": 0.0}
                        }
                    }
                }
            },
            {"$sort": {"orders_count": -1}}
        ]
    }
}


@app.get("/aggregations", tags=["5. Aggregations & Reports"])
def list_aggregations():
    """
    GET /aggregations
    عرض قائمة تقارير التجميعات الخمسة المتاحة في النظام.
    """
    reports = [
        {"name": k, "title": v["title"], "description": v["description"]}
        for k, v in AGGREGATIONS_REGISTRY.items()
    ]
    return {"total_reports": len(reports), "reports": reports}


@app.get("/aggregations/{name}", tags=["5. Aggregations & Reports"])
def get_aggregation(name: str, req: Request, limit: int = Query(50, ge=1, le=500)):
    """
    GET /aggregations/{name}
    تنفيذ تقرير تجميع محدد بالاسم وإرجاع البيانات الملخصة.
    التقارير المتاحة:
      - sales_by_city
      - top_products
      - top_customers
      - sales_by_period
      - orders_by_status
    """
    if name not in AGGREGATIONS_REGISTRY:
        raise HTTPException(
            status_code=404,
            detail=f"Report '{name}' not found. Available: {list(AGGREGATIONS_REGISTRY.keys())}"
        )

    db = get_db(req)

    # معالجة خاصة لتقرير top_products (قراءة سريعة من العرض المادي أو التجميع المباشر)
    if name == "top_products":
        results = get_top_products_view(db, limit=limit)
        return {
            "report_name": name,
            "title": AGGREGATIONS_REGISTRY[name]["title"],
            "source": "materialized_view:top_products_summary",
            "count": len(results),
            "results": results
        }

    pipeline = list(AGGREGATIONS_REGISTRY[name]["pipeline"])
    # إضافة أو تعديل $limit إذا وجد
    has_limit = any("$limit" in stage for stage in pipeline)
    if not has_limit:
        pipeline.append({"$limit": limit})

    start_t = time.perf_counter()
    results = list(db[COLLECTION_VALIDATED].aggregate(pipeline, allowDiskUse=True))
    elapsed = time.perf_counter() - start_t

    return {
        "report_name": name,
        "title": AGGREGATIONS_REGISTRY[name]["title"],
        "count": len(results),
        "execution_time_seconds": round(elapsed, 4),
        "results": results
    }


# ==============================================================================
# 6. العروض المادية (Materialized Views)
# ==============================================================================

@app.post("/refresh-mv", tags=["6. Materialized Views"])
def refresh_materialized_views_endpoint(req: Request, payload: Optional[RefreshMVRequest] = None):
    """
    POST /refresh-mv
    تحديث العروض المادية (daily_sales_summary و top_products_summary)
    باستخدام آلية التحديث التزايدي Incremental Refresh.
    """
    db = get_db(req)
    mode = payload.mode if payload and payload.mode else "incremental"
    result = refresh_all_views(db, mode=mode)
    return result


@app.get("/materialized/daily-sales", tags=["6. Materialized Views"])
def get_daily_sales_mv(req: Request, limit: int = Query(30, ge=1, le=365)):
    """استرجاع بيانات ملخص المبيعات اليومية من العرض المادي."""
    db = get_db(req)
    return {
        "view": COLLECTION_MV_DAILY_SALES,
        "results": get_daily_sales_view(db, limit=limit)
    }


@app.get("/materialized/top-products", tags=["6. Materialized Views"])
def get_top_products_mv(req: Request, limit: int = Query(20, ge=1, le=100)):
    """استرجاع أفضل المنتجات من العرض المادي."""
    db = get_db(req)
    return {
        "view": COLLECTION_MV_TOP_PRODUCTS,
        "results": get_top_products_view(db, limit=limit)
    }


# ==============================================================================
# 7. المهام المجدولة (Scheduled Jobs)
# ==============================================================================

@app.get("/jobs", tags=["7. Scheduled Jobs"])
def get_jobs_list(req: Request):
    """
    GET /jobs
    عرض قائمة المهام المجدولة في النظام مع سجلات التشغيل الأخيرة.
    """
    db = get_db(req)
    jobs = list_registered_jobs(db)
    recent_history = get_job_history(db, limit=10)
    return {
        "registered_jobs": jobs,
        "recent_execution_history": recent_history
    }


@app.post("/jobs/{name}/run", tags=["7. Scheduled Jobs"])
def trigger_job(name: str, req: Request):
    """
    POST /jobs/{name}/run
    تشغيل يدوي فوري لمهمة محددة وتوثيق النتيجة في logs.
    المهام المتاحة:
      - refresh_materialized_views
      - generate_periodic_report
    """
    db = get_db(req)
    try:
        result = execute_job(name, db=db, triggered_by="api_manual_trigger")
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

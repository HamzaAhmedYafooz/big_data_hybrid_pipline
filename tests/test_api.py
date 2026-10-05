"""
tests/test_api.py
=================
اختبارات المرحلة الثالثة: FastAPI Routes

يستخدم TestClient من fastapi.testclient لاختبار الـ Routes
بدون تشغيل سيرفر حقيقي، مع mock لـ MongoDB.
"""

import pytest
import sys
import os
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi.testclient import TestClient
from src.api import app


# =========================================================
# fixture: mock MongoDB
# =========================================================

@pytest.fixture
def mock_db():
    """Mock للـ db بدون اتصال MongoDB حقيقي."""
    db = MagicMock()

    # orders_validated mock
    orders = MagicMock()
    orders.count_documents.return_value = 1000
    orders.distinct.return_value = ["صنعاء", "عدن", "تعز"]
    orders.aggregate.return_value = iter([
        {"_id": "صنعاء", "total_sales": 500000.0, "orders_count": 200},
        {"_id": "عدن",   "total_sales": 400000.0, "orders_count": 180},
    ])
    orders.find.return_value = MagicMock(
        __iter__=lambda s: iter([
            {"order_id": "ORD-1", "status": "تم التسليم", "total_amount": 5000.0}
        ])
    )

    # monthly_city_products mock
    mat = MagicMock()
    mat.count_documents.return_value = 5
    mat.find.return_value = MagicMock(
        sort=lambda *a: MagicMock(
            limit=lambda n: iter([
                {"year": 2025, "month": 1, "city": "صنعاء", "city_total_sales": 100000.0}
            ])
        )
    )

    db.__getitem__.side_effect = lambda name: orders if "validated" in name else mat
    return db


@pytest.fixture
def client(mock_db):
    """TestClient مع mock db مُحقون في app.state."""
    app.state.db = mock_db
    app.state.client = MagicMock()
    with TestClient(app, raise_server_exceptions=True) as c:
        yield c


# =========================================================
# 1. Health Check
# =========================================================

def test_root_returns_ok(client):
    """GET / يجب ان يرجع status ok."""
    resp = client.get("/")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert "message" in data


def test_root_has_docs_link(client):
    """GET / يجب ان يحتوي على رابط /docs."""
    resp = client.get("/")
    assert "/docs" in resp.json()["docs"]


# =========================================================
# 2. /stats
# =========================================================

def test_stats_returns_200(client):
    """GET /stats يجب ان يرجع 200."""
    resp = client.get("/stats")
    assert resp.status_code == 200


def test_stats_has_required_keys(client):
    """GET /stats يجب ان يحتوي على المفاتيح المطلوبة."""
    resp = client.get("/stats")
    data = resp.json()
    assert "total_orders" in data
    assert "orders_by_status" in data
    assert "unique_cities" in data


def test_stats_total_orders_is_int(client):
    """total_orders يجب ان يكون رقما صحيحا."""
    resp = client.get("/stats")
    assert isinstance(resp.json()["total_orders"], int)


# =========================================================
# 3. /top-cities
# =========================================================

def test_top_cities_returns_200(client):
    """GET /top-cities يجب ان يرجع 200."""
    resp = client.get("/top-cities")
    assert resp.status_code == 200


def test_top_cities_has_cities_key(client):
    """الاستجابة يجب ان تحتوي على مفتاح cities."""
    resp = client.get("/top-cities")
    assert "cities" in resp.json()


def test_top_cities_limit_param(client):
    """يجب ان يقبل الـ limit parameter."""
    resp = client.get("/top-cities?limit=3")
    assert resp.status_code == 200
    assert resp.json()["limit"] == 3


def test_top_cities_invalid_limit(client):
    """limit=0 يجب ان يرجع 422 (validation error)."""
    resp = client.get("/top-cities?limit=0")
    assert resp.status_code == 422


# =========================================================
# 4. /top-products
# =========================================================

def test_top_products_returns_200(client):
    """GET /top-products يجب ان يرجع 200."""
    resp = client.get("/top-products")
    assert resp.status_code == 200


def test_top_products_has_products_key(client):
    """الاستجابة يجب ان تحتوي على مفتاح products."""
    resp = client.get("/top-products")
    assert "products" in resp.json()


# =========================================================
# 5. /monthly-sales
# =========================================================

def test_monthly_sales_returns_200(client):
    """GET /monthly-sales يجب ان يرجع 200."""
    resp = client.get("/monthly-sales")
    assert resp.status_code == 200


def test_monthly_sales_has_data_key(client):
    """الاستجابة يجب ان تحتوي على مفتاح data."""
    resp = client.get("/monthly-sales")
    assert "data" in resp.json()
    assert "months" in resp.json()


# =========================================================
# 6. /materialized
# =========================================================

def test_materialized_returns_200_when_data_exists(client):
    """GET /materialized يرجع 200 عند وجود بيانات."""
    resp = client.get("/materialized")
    assert resp.status_code == 200


def test_materialized_has_filters_key(client):
    """الاستجابة يجب ان تحتوي على مفتاح filters."""
    resp = client.get("/materialized")
    data = resp.json()
    assert "filters" in data
    assert "data" in data


def test_materialized_filter_by_year(client):
    """يقبل فلترة بالسنة."""
    resp = client.get("/materialized?year=2025")
    assert resp.status_code == 200
    assert resp.json()["filters"]["year"] == 2025


def test_materialized_filter_invalid_month(client):
    """month=13 يجب ان يرجع 422."""
    resp = client.get("/materialized?month=13")
    assert resp.status_code == 422


# =========================================================
# 7. /search/city/{city}
# =========================================================

def test_search_city_returns_200(client):
    """GET /search/city/صنعاء يجب ان يرجع 200 عند وجود نتائج."""
    resp = client.get("/search/city/صنعاء")
    assert resp.status_code == 200


def test_search_city_has_required_keys(client):
    """الاستجابة يجب ان تحتوي على city و orders."""
    resp = client.get("/search/city/صنعاء")
    data = resp.json()
    assert "city" in data
    assert "orders" in data
    assert "total_in_db" in data

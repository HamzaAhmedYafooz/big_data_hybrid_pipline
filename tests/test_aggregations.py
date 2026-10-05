"""
tests/test_aggregations.py
==========================
اختبارات المرحلة الثانية: Aggregation Pipelines

يختبر هذا الملف بنية الـ Pipelines (بدون MongoDB حقيقي)
وكذلك دوال الـ helpers المستقلة.
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.aggregations import (
    _collect_stages,
    _explain_summary,
    _build_heavy_pipeline,
    COLLECTION_MATERIALIZED,
    AGGS_INDEXES,
)


# =========================================================
# 1. اختبارات _collect_stages
# =========================================================

def test_collect_stages_simple():
    """يجب ان يستخرج stage واحد من قاموس بسيط."""
    node = {"stage": "COLLSCAN"}
    result = _collect_stages(node)
    assert "COLLSCAN" in result


def test_collect_stages_nested():
    """يجب ان يستخرج stages من هيكل متداخل."""
    node = {
        "stage": "FETCH",
        "inputStage": {
            "stage": "IXSCAN",
        },
    }
    result = _collect_stages(node)
    assert "FETCH" in result
    assert "IXSCAN" in result


def test_collect_stages_list():
    """يجب ان يعمل مع قوائم."""
    node = [{"stage": "SORT"}, {"stage": "GROUP"}]
    result = _collect_stages(node)
    assert "SORT" in result
    assert "GROUP" in result


def test_collect_stages_empty():
    """قاموس فارغ يرجع قائمة فارغة."""
    result = _collect_stages({})
    assert result == []


# =========================================================
# 2. اختبارات _explain_summary
# =========================================================

def test_explain_summary_extracts_fields():
    """يجب ان يستخرج الحقول الصحيحة من مخرجات explain."""
    mock_explain = {
        "executionStats": {
            "nReturned": 100,
            "executionTimeMillis": 25,
            "totalKeysExamined": 100,
            "totalDocsExamined": 100,
        },
        "queryPlanner": {
            "winningPlan": {
                "stage": "IXSCAN",
            }
        },
    }
    result = _explain_summary(mock_explain)

    assert result["nReturned"] == 100
    assert result["executionTimeMillis"] == 25
    assert result["totalKeysExamined"] == 100
    assert result["totalDocsExamined"] == 100
    assert "IXSCAN" in result["stages"]


def test_explain_summary_missing_fields():
    """يجب ان يتحمل explain ناقص البيانات بدون استثناء."""
    result = _explain_summary({})
    assert result["nReturned"] is None
    assert result["executionTimeMillis"] is None
    assert result["stages"] == []


# =========================================================
# 3. اختبارات بنية الـ Heavy Pipeline
# =========================================================

def test_heavy_pipeline_has_8_stages():
    """الـ Heavy Pipeline يجب ان يحتوي على 8 مراحل بالضبط."""
    pipeline = _build_heavy_pipeline()
    assert len(pipeline) == 8


def test_heavy_pipeline_first_stage_is_set():
    """المرحلة الاولى يجب ان تكون $set لتحويل التاريخ."""
    pipeline = _build_heavy_pipeline()
    assert "$set" in pipeline[0]
    assert "_order_date" in pipeline[0]["$set"]


def test_heavy_pipeline_second_stage_is_match():
    """المرحلة الثانية يجب ان تكون $match للفلترة."""
    pipeline = _build_heavy_pipeline()
    assert "$match" in pipeline[1]
    assert "status" in pipeline[1]["$match"]


def test_heavy_pipeline_third_stage_is_unwind():
    """المرحلة الثالثة يجب ان تكون $unwind لتفكيك items[]."""
    pipeline = _build_heavy_pipeline()
    assert "$unwind" in pipeline[2]
    assert pipeline[2]["$unwind"] == "$items"


def test_heavy_pipeline_last_stage_is_sort():
    """المرحلة الاخيرة يجب ان تكون $sort."""
    pipeline = _build_heavy_pipeline()
    assert "$sort" in pipeline[-1]


def test_heavy_pipeline_has_project_with_top_products():
    """Pipeline يجب ان يحتوي على $project مع $slice للحصول على top 5."""
    pipeline = _build_heavy_pipeline()
    project_stages = [s for s in pipeline if "$project" in s]
    assert len(project_stages) >= 1
    proj = project_stages[0]["$project"]
    assert "top_products" in proj
    assert "$slice" in proj["top_products"]


def test_heavy_pipeline_has_two_group_stages():
    """يجب ان يكون هناك تجميعان ($group مزدوج)."""
    pipeline = _build_heavy_pipeline()
    group_stages = [s for s in pipeline if "$group" in s]
    assert len(group_stages) == 2


# =========================================================
# 4. اختبارات الثوابت
# =========================================================

def test_materialized_collection_name():
    """اسم مجموعة الـ Materialized View يجب ان يكون صحيحا."""
    assert COLLECTION_MATERIALIZED == "monthly_city_products"


def test_aggs_indexes_count():
    """يجب ان يكون هناك 3 فهارس مخططة."""
    assert len(AGGS_INDEXES) == 3


def test_aggs_indexes_names():
    """اسماء الفهارس يجب ان تحتوي على المتوقع."""
    assert "agg_idx_city" in AGGS_INDEXES
    assert "agg_idx_city_date" in AGGS_INDEXES
    assert "agg_idx_status" in AGGS_INDEXES

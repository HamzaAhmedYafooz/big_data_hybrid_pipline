import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.quality_rules import apply_quality_rules

def test_valid_record_classification():
    raw = {
        "order_id": "ORD-100",
        "customer_id": "CUST-100",
        "customer_phone": "771234567",
        "customer_email": "valid.user@example.com",
        "order_date": "2026-10-01",
        "status": "Completed",
        "delivery_cost": "500",
        "payment_amount": "2500",
        "total_amount": "2500",
        "currency": "YER",
        "items_json": '[{"sku": "A100", "qty": 1, "total": 2000}]'
    }
    cleaned, status, corrections, errors = apply_quality_rules(raw)
    assert status == "valid"
    assert len(errors) == 0
    assert len(corrections) == 0

def test_missing_order_id_quarantine():
    raw = {
        "order_id": "",
        "customer_id": "CUST-101",
        "customer_phone": "771234567",
        "customer_email": "user@example.com",
        "order_date": "2026-10-01",
        "status": "Completed",
        "delivery_cost": "500",
        "payment_amount": "2500",
        "total_amount": "2500",
        "currency": "YER",
        "items_json": '[{"sku": "A", "qty": 1, "total": 2000}]'
    }
    cleaned, status, corrections, errors = apply_quality_rules(raw)
    assert status == "quarantined"
    assert "missing_order_id" in errors or "MISSING_ORDER_ID" in errors

def test_missing_customer_id_quarantine():
    raw = {
        "order_id": "ORD-102",
        "customer_id": "",
        "customer_phone": "771234567",
        "customer_email": "user@example.com",
        "order_date": "2026-10-01",
        "status": "Completed",
        "delivery_cost": "500",
        "payment_amount": "2500",
        "total_amount": "2500",
        "currency": "YER",
        "items_json": '[{"sku": "A", "qty": 1, "total": 2000}]'
    }
    cleaned, status, corrections, errors = apply_quality_rules(raw)
    assert status == "quarantined"
    assert "missing_customer_id" in errors or "MISSING_CUSTOMER_ID" in errors

def test_corrupted_items_json_quarantine():
    raw = {
        "order_id": "ORD-103",
        "customer_id": "CUST-103",
        "customer_phone": "771234567",
        "customer_email": "user@example.com",
        "order_date": "2026-10-01",
        "status": "Completed",
        "delivery_cost": "500",
        "payment_amount": "2500",
        "total_amount": "2500",
        "currency": "YER",
        "items_json": 'not a valid json {{{{[['
    }
    cleaned, status, corrections, errors = apply_quality_rules(raw)
    assert status == "quarantined"
    assert "corrupted_items_json" in errors or "CORRUPTED_ITEMS_JSON" in errors

def test_empty_items_quarantine():
    raw = {
        "order_id": "ORD-104",
        "customer_id": "CUST-104",
        "customer_phone": "771234567",
        "customer_email": "user@example.com",
        "order_date": "2026-10-01",
        "status": "Completed",
        "delivery_cost": "500",
        "payment_amount": "2500",
        "total_amount": "2500",
        "currency": "YER",
        "items_json": '[]'
    }
    cleaned, status, corrections, errors = apply_quality_rules(raw)
    assert status == "quarantined"
    assert "empty_items" in errors or "EMPTY_ITEMS" in errors

def test_invalid_phone_too_short_quarantine():
    raw = {
        "order_id": "ORD-105",
        "customer_id": "CUST-105",
        "customer_phone": "123",
        "customer_email": "user@example.com",
        "order_date": "2026-10-01",
        "status": "Completed",
        "delivery_cost": "500",
        "payment_amount": "2500",
        "total_amount": "2500",
        "currency": "YER",
        "items_json": '[{"sku": "A", "qty": 1, "total": 2000}]'
    }
    cleaned, status, corrections, errors = apply_quality_rules(raw)
    assert status == "quarantined"
    assert "invalid_phone_too_short" in errors

def test_email_missing_domain_quarantine():
    raw = {
        "order_id": "ORD-106",
        "customer_id": "CUST-106",
        "customer_phone": "771234567",
        "customer_email": "invalid_email_no_at",
        "order_date": "2026-10-01",
        "status": "Completed",
        "delivery_cost": "500",
        "payment_amount": "2500",
        "total_amount": "2500",
        "currency": "YER",
        "items_json": '[{"sku": "A", "qty": 1, "total": 2000}]'
    }
    cleaned, status, corrections, errors = apply_quality_rules(raw)
    assert status == "quarantined"
    assert "email_missing_domain" in errors

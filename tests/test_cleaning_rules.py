import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.quality_rules import apply_quality_rules

def test_arabic_digits_cleaning():
    raw = {
        "order_id": "ORD-1",
        "customer_id": "CUST-1",
        "customer_phone": "771234567",
        "customer_email": "user@example.com",
        "order_date": "2026-10-01",
        "status": "Completed",
        "delivery_cost": "٥٠٠",
        "payment_amount": "٢٠٠٠",
        "total_amount": "2500",
        "currency": "YER",
        "items_json": '[{"sku": "A", "qty": 1, "total": 2000}]'
    }
    cleaned, status, corrections, errors = apply_quality_rules(raw)
    assert status == "corrected"
    assert cleaned["delivery_cost"] == "500"
    assert cleaned["payment_amount"] == "2000"
    rule_codes = [c["rule_code"] for c in corrections]
    assert "arabic_digits_delivery_cost" in rule_codes
    assert "arabic_digits_payment_amount" in rule_codes

def test_thousands_separator_cleaning():
    raw = {
        "order_id": "ORD-2",
        "customer_id": "CUST-2",
        "customer_phone": "771234567",
        "customer_email": "user@example.com",
        "order_date": "2026-10-01",
        "status": "Completed",
        "delivery_cost": "500",
        "payment_amount": "12500",
        "total_amount": "12,500",
        "currency": "YER",
        "items_json": '[{"sku": "A", "qty": 1, "total": 12000}]'
    }
    cleaned, status, corrections, errors = apply_quality_rules(raw)
    assert status == "corrected"
    assert cleaned["total_amount"] == 12500.0 or cleaned["total_amount"] == "12500"

def test_email_double_at_cleaning():
    raw = {
        "order_id": "ORD-3",
        "customer_id": "CUST-3",
        "customer_phone": "771234567",
        "customer_email": "user@@example.com",
        "order_date": "2026-10-01",
        "status": "Completed",
        "delivery_cost": "500",
        "payment_amount": "2500",
        "total_amount": "2500",
        "currency": "YER",
        "items_json": '[{"sku": "A", "qty": 1, "total": 2000}]'
    }
    cleaned, status, corrections, errors = apply_quality_rules(raw)
    assert status == "corrected"
    assert cleaned["customer_email"] == "user@example.com"
    rule_codes = [c["rule_code"] for c in corrections]
    assert any("email" in r.lower() for r in rule_codes)

def test_phone_standardization():
    raw = {
        "order_id": "ORD-4",
        "customer_id": "CUST-4",
        "customer_phone": "+967 771234567",
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
    assert status == "corrected"
    assert cleaned["customer_phone"] == "771234567"

def test_date_standardization():
    raw = {
        "order_id": "ORD-5",
        "customer_id": "CUST-5",
        "customer_phone": "771234567",
        "customer_email": "user@example.com",
        "order_date": "31-01-2026 12:00:00",
        "status": "Completed",
        "delivery_cost": "500",
        "payment_amount": "2500",
        "total_amount": "2500",
        "currency": "YER",
        "items_json": '[{"sku": "A", "qty": 1, "total": 2000}]'
    }
    cleaned, status, corrections, errors = apply_quality_rules(raw)
    assert status == "corrected"
    assert cleaned["order_date"] == "2026-01-31"

def test_currency_standardization():
    raw = {
        "order_id": "ORD-6",
        "customer_id": "CUST-6",
        "customer_phone": "771234567",
        "customer_email": "user@example.com",
        "order_date": "2026-10-01",
        "status": "Completed",
        "delivery_cost": "500",
        "payment_amount": "2500",
        "total_amount": "2500",
        "currency": "ريال يمني",
        "items_json": '[{"sku": "A", "qty": 1, "total": 2000}]'
    }
    cleaned, status, corrections, errors = apply_quality_rules(raw)
    assert status == "corrected"
    assert cleaned["currency"] == "YER"

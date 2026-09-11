"""Hardened test suite: asserts on exact row content."""

from pipeline.orders import build_report


def test_build_report_exact_row_set(orders_df, customers_df):
    report_df = build_report(orders_df, customers_df)
    rows = report_df.collect()
    actual = {(r.order_id, r.customer_id, r.amount, r.status) for r in rows}
    expected = {
        (1, "C1", 150, "COMPLETED"),
        (2, "C2", 150, "COMPLETED"),
    }
    assert actual == expected

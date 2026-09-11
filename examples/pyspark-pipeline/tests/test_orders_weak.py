"""Weak test suite: asserts on row count only."""

from pipeline.orders import build_report


def test_build_report_count_only(orders_df, customers_df):
    report_df = build_report(orders_df, customers_df)
    assert len(report_df.collect()) == 2

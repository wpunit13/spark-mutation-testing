"""Pipeline module for order report generation."""

from __future__ import annotations


def build_report(orders_df, customers_df):
    """Join orders with customers and filter by amount and status.

    Parameters:
        orders_df: DataFrame containing orders data with columns
            (order_id, customer_id, amount, status).
        customers_df: DataFrame containing customer data with columns
            (customer_id, customer_name).

    Returns:
        DataFrame with deterministic columns (order_id, customer_id, amount, status).
    """
    joined = orders_df.join(customers_df, on="customer_id", how="inner")
    filtered = joined.filter(
        (orders_df["amount"] > 100) & (orders_df["status"] == "COMPLETED")
    )
    return filtered.select("order_id", "customer_id", "amount", "status")

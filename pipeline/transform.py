import pandas as pd
from config import FX_RATES
import hashlib

# FX conversion ----------------
def add_usd_columns(df, amount_columns):
    """
    Adds USD-converted columns while keeping original values.
    """
    for col in amount_columns:
        df[f"{col}_USD"] = df[col] * df["Currency"].map(FX_RATES)
    return df

# Sales ----------------
def transform_sales(df):
    df = df.copy()

    df["Date"] = pd.to_datetime(df["Date"])
    df["Quantity"] = df["Quantity"].astype(int)

    df = add_usd_columns(
        df,
        amount_columns=["UnitPrice", "TotalSales"]
    )

    # Use existing sales_id as unique key for idempotency
    # Ensure column exists
    if "sales_id" not in df.columns:
        df["sales_id"] = df.index.astype(str)  # fallback if sales_id not in CSV

    return df[['sales_id', 'Date', 'Quantity', 'UnitPrice', 'TotalSales', 'UnitPrice_USD', 'TotalSales_USD']]

# Financial --------------
def transform_financial(df):
    df = df.copy()

    df["Date"] = pd.to_datetime(df["Date"])

    df = add_usd_columns(
        df,
        amount_columns=["Revenue", "Expense", "Profit"]
    )

    # Use existing transaction_id as unique key
    if "transaction_id" not in df.columns:
        df["transaction_id"] = df.index.astype(str)

    return df[['transaction_id', 'Date', 'Revenue', 'Expense', 'Profit', 'Revenue_USD', 'Expense_USD', 'Profit_USD']]

# Attendance -------------
def transform_attendance(df):
    df = df.copy()

    df["Date"] = pd.to_datetime(df["Date"])

    # Generate deterministic attendance_id for idempotency
    def generate_attendance_key(row):
        # Combines staff_id + date + session_id
        key_str = f"{row['StaffID']}_{row['Date']}_{row['SessionID']}"
        return hashlib.md5(key_str.encode()).hexdigest()

    df['attendance_id'] = df.apply(generate_attendance_key, axis=1)

    return df[['attendance_id', 'StaffID', 'Date', 'SessionID', 'Status']]

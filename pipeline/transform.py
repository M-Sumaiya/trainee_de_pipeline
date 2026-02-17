import pandas as pd
from config import FX_RATES
import hashlib


# FX conversion
# -----------------------------
def add_usd_columns(df, amount_columns):
    """
    Adds USD-converted columns while keeping original values.
    """
    for col in amount_columns:
        df[f"{col}_USD"] = df[col] * df["Currency"].map(FX_RATES)
    return df


# Data Quality Checks
# -----------------------------
def check_sales_quality(df):
    if df.isnull().any().any():
        print("Warning: Null values detected in sales data")
    if (df["Quantity"] < 0).any():
        print("Warning: Negative quantities found in sales data")
    if (df["UnitPrice"] < 0).any() or (df["TotalSales"] < 0).any():
        print("Warning: Negative sales amounts detected")
    return df

def check_financial_quality(df):
    if df.isnull().any().any():
        print("Warning: Null values detected in financial data")
    if (df["Revenue"] < 0).any() or (df["Expense"] < 0).any() or (df["Profit"] < 0).any():
        print("Warning: Negative amounts detected in financial data")
    return df

def check_attendance_quality(df):
    if df.isnull().any().any():
        print("Warning: Null values detected in attendance data")
    valid_status = ["Present", "Absent", "Leave"]
    if not df['Status'].isin(valid_status).all():
        print("Warning: Some attendance statuses are invalid")
    return df


# Sales Transformation
# -----------------------------
def transform_sales(df):
    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"])
    df["Quantity"] = df["Quantity"].astype(int)

    df = add_usd_columns(df, amount_columns=["UnitPrice", "TotalSales"])

    # Ensure unique ID
    if "sales_id" not in df.columns:
        df["sales_id"] = df.index.astype(str)

    # Data quality check
    df = check_sales_quality(df)

    return df[['sales_id', 'Date', 'Quantity', 'UnitPrice', 'TotalSales', 'UnitPrice_USD', 'TotalSales_USD']]


# Financial Transformation
# -----------------------------
def transform_financial(df):
    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"])

    df = add_usd_columns(df, amount_columns=["Revenue", "Expense", "Profit"])

    # Ensure unique ID
    if "transaction_id" not in df.columns:
        df["transaction_id"] = df.index.astype(str)

    # Data quality check
    df = check_financial_quality(df)

    return df[['transaction_id', 'Date', 'Revenue', 'Expense', 'Profit', 'Revenue_USD', 'Expense_USD', 'Profit_USD']]


# Attendance Transformation
# -----------------------------
def transform_attendance(df):
    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"])

    # Generate deterministic attendance_id for idempotency
    def generate_attendance_key(row):
        key_str = f"{row['StaffID']}_{row['Date']}_{row['SessionID']}"
        return hashlib.md5(key_str.encode()).hexdigest()

    df['attendance_id'] = df.apply(generate_attendance_key, axis=1)

    # Data quality check
    df = check_attendance_quality(df)

    return df[['attendance_id', 'StaffID', 'Date', 'SessionID', 'Status']]

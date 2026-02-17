-- Main Financial table
CREATE TABLE IF NOT EXISTS `trainee-data-engineering.trainee_de_dataset.financial` (
    transaction_id STRING NOT NULL,
    Date DATE,
    Revenue FLOAT64,
    Expense FLOAT64,
    Profit FLOAT64,
    Revenue_USD FLOAT64,
    Expense_USD FLOAT64,
    Profit_USD FLOAT64,
    PRIMARY KEY(transaction_id)
);

-- Staging Financial table
CREATE TABLE IF NOT EXISTS `trainee-data-engineering.trainee_de_dataset.financial_staging` (
    transaction_id STRING,
    Date DATE,
    Revenue FLOAT64,
    Expense FLOAT64,
    Profit FLOAT64,
    Revenue_USD FLOAT64,
    Expense_USD FLOAT64,
    Profit_USD FLOAT64
);

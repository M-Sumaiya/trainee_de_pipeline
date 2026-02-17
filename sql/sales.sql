-- Main Sales table
CREATE TABLE IF NOT EXISTS `trainee-data-engineering.trainee_de_dataset.sales` (
    sales_id STRING NOT NULL,
    Date DATE,
    Quantity INT64,
    UnitPrice FLOAT64,
    TotalSales FLOAT64,
    UnitPrice_USD FLOAT64,
    TotalSales_USD FLOAT64,
    PRIMARY KEY(sales_id)
);

-- Staging Sales table
CREATE TABLE IF NOT EXISTS `trainee-data-engineering.trainee_de_dataset.sales_staging` (
    sales_id STRING,
    Date DATE,
    Quantity INT64,
    UnitPrice FLOAT64,
    TotalSales FLOAT64,
    UnitPrice_USD FLOAT64,
    TotalSales_USD FLOAT64
);

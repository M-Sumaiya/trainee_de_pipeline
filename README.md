# Trainee Data Engineer – ETL Pipeline Project

## 📌 Project Overview

This project implements a simple **ETL (Extract, Transform, Load) pipeline** as part of a **Trainee Data Engineer assessment**.

The pipeline:

* Generates three datasets
* Extracts data from CSV files
* Performs cleaning and transformations
* Converts all currency values to **USD**
* Loads the data into **Google BigQuery**
* Uses deterministic keys to support idempotent loads

The project is designed to be **clear, modular, and beginner-friendly**.

---

## 🗂️ Project Structure

```
.
├── data/                        # Generated CSV datasets
│   ├── attendance_dataset_3m.csv
│   ├── financial_dataset_3m.csv
│   └── sales_dataset_3m.csv
│
├── generators/                  # Dataset generator scripts
│   ├── attendance_dataset_3m.py
│   ├── financial_dataset_3m.py
│   └── sales_dataset_3m.py
│
├── notebooks/                   # Notebook to generate datasets
│   └── data_generator.ipynb
│
├── pipeline/                    # Core ETL pipeline
│   ├── __init__.py
│   ├── extract.py
│   ├── transform.py
│   └── load.py
│
├── sql/                         # BigQuery schema DDL
│   ├── attendance.sql
│   ├── dataset.sql
│   ├── financial.sql
│   └── sales.sql
│
├── config.py                    
├── run_pipeline.py              # Main pipeline script
├── requirements.txt             # Python dependencies
├── .gitignore                   # Ignored files
└── README.md                    # Project documentation
```

---

## ⚙️ Technologies Used

* Python
* Pandas
* Google BigQuery
* SQL

---

## 🔄 ETL Flow

### 1. Data Generation

Run the notebook:

```
notebooks/data_generator.ipynb
```

This:

* Executes scripts in the `generators/` folder
* Produces three CSV files
* Saves them in the `data/` directory

Generated datasets:

* Attendance
* Financial
* Sales

---

### 2. Extract

* CSV files are read from the `data/` folder.
* Files are processed in chunks for memory efficiency.

---

### 3. Transform

Transformations are handled in `pipeline/transform.py`.

#### Currency Conversion

* Uses exchange rates defined in `config.py` (`FX_RATES`)
* Adds new columns with `_USD` suffix
* Keeps original currency values

Example:

```
UnitPrice → UnitPrice_USD
TotalSales → TotalSales_USD
Revenue → Revenue_USD
```

---

#### Deterministic Keys (Idempotency)

Each dataset uses a unique key:

**Sales**

* Uses existing `sales_id`
* If missing, uses row index as fallback

**Financial**

* Uses existing `transaction_id`
* If missing, uses row index

**Attendance**

* Generates `attendance_id` using:

```
StaffID + Date + SessionID
```

* Hashed with MD5 to create a deterministic key

This ensures:

* Same records always produce the same IDs
* Safe repeated pipeline runs

---

### 4. Load

* Data is loaded into BigQuery using `google-cloud-bigquery`.
* Tables are defined using SQL files in the `sql/` folder.
* Data is loaded in chunks for efficiency.

---

## Idempotency Strategy

Idempotency is achieved through:

1. Deterministic primary keys:

   * `sales_id`
   * `transaction_id`
   * `attendance_id`

2. Safe load strategy in BigQuery:

   * Tables can be overwritten or merged
   * Prevents duplicate records

---

## ✅ Data Quality Checks

Basic checks during transformation:

* Required columns exist
* Currency values are numeric
* Dates are valid
* Deterministic keys are generated

---

## Requirements

### Python Version

Python 3.9 or higher.

### Install Dependencies

From the project root:

```bash
pip install -r requirements.txt
```

### requirements.txt

```
pandas>=1.5.0
google-cloud-bigquery>=3.10.0
pyarrow>=10.0.0
```

---

## BigQuery Setup

### Step 1: Create Google Cloud Resources

1. Create a Google Cloud project.
2. Enable the **BigQuery API**.
3. Create a dataset.
4. Create a service account.
5. Download the JSON key.

---

## BigQuery Authentication

### Windows (PowerShell)

```powershell
setx GOOGLE_APPLICATION_CREDENTIALS "path\to\key.json"
```

Example:

```powershell
setx GOOGLE_APPLICATION_CREDENTIALS "C:\Users\YourName\keys\key.json"
```

Restart the terminal after running the command.

---

### Mac/Linux

```bash
export GOOGLE_APPLICATION_CREDENTIALS="path/to/key.json"
```

---

## Configuration

Edit values in `run_pipeline.py` if needed:

```python
PROJECT_ID = "your-project-id"
DATASET = "your_dataset"
```

---

## How to Run the Pipeline

### Step 1: Generate Data

Run the notebook:

```
notebooks/data_generator.ipynb
```

---

### Step 2: Run the Pipeline

From the project root:

```bash
python run_pipeline.py
```

The pipeline will:

1. Extract CSV data
2. Transform datasets
3. Apply schemas
4. Load data into BigQuery

---

## BigQuery Output

Tables created:

* `sales`
* `financial`
* `attendance`

Each table follows the schema defined in the `sql/` folder.

---

## Deliverables Mapping

| Requirement         | Implementation                       |
| ------------------- | ------------------------------------ |
| Runnable pipeline   | `run_pipeline.py`                    |
| Schema DDL          | `sql/` folder                        |
| Idempotent loads    | Deterministic keys in `transform.py` |
| Data quality checks | `pipeline/transform.py`              |
| Documentation       | `README.md`                          |

---

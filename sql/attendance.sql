-- Main table
CREATE TABLE IF NOT EXISTS `trainee-data-engineering.trainee_de_dataset.attendance` (
    attendance_id STRING NOT NULL,
    staff_id STRING,
    date DATE,
    session_id STRING,
    status STRING,
    PRIMARY KEY(attendance_id)
);

-- Staging table
CREATE TABLE IF NOT EXISTS `trainee-data-engineering.trainee_de_dataset.attendance_staging` (
    attendance_id STRING,
    staff_id STRING,
    date DATE,
    session_id STRING,
    status STRING
);

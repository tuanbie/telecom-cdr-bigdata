CREATE DATABASE IF NOT EXISTS telecom_report;
USE telecom_report;

CREATE TABLE IF NOT EXISTS top_callers (
    caller_prefix_full VARCHAR(20),
    total_transactions BIGINT,
    total_calls BIGINT,
    run_date DATE DEFAULT (CURRENT_DATE)
);

CREATE TABLE IF NOT EXISTS top_stations (
    station_id VARCHAR(20),
    total_records BIGINT,
    total_duration_sec BIGINT,
    run_date DATE DEFAULT (CURRENT_DATE)
);

CREATE TABLE IF NOT EXISTS traffic_by_date (
    report_month VARCHAR(7),
    service_type VARCHAR(10),
    total_records BIGINT,
    total_call_minutes DOUBLE
);

CREATE TABLE IF NOT EXISTS revenue_by_service (
    report_year VARCHAR(4),
    service_type VARCHAR(10),
    total_revenue DOUBLE,
    total_records BIGINT
);

CREATE TABLE IF NOT EXISTS revenue_by_station (
    report_month VARCHAR(7),
    station_id VARCHAR(20),
    total_revenue DOUBLE,
    total_records BIGINT
);

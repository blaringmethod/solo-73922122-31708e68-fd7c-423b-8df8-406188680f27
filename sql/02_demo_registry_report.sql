-- ============================================================
-- Demo registry/report pair for the incremental migration API.
--
-- Strictly within the migration feature's supported shape:
--   * exactly two ordinary base tables, integer columns only;
--   * each table has a single-column integer primary key and no
--     other key constraints;
--   * one nullable, NOT DEFERRABLE, ON DELETE NO ACTION single-column
--     foreign key from the report table to the registry table;
--   * no other foreign keys, no CHECK/DEFAULT/trigger/unique index.
--
-- Zero data rows: the sample service generates every value.
-- ============================================================

CREATE TABLE IF NOT EXISTS dw_registry (
    reg_id     integer NOT NULL,
    region_code smallint NOT NULL,
    load_batch  bigint,
    CONSTRAINT dw_registry_pk PRIMARY KEY (reg_id)
);

CREATE TABLE IF NOT EXISTS dw_report (
    report_id  bigint NOT NULL,
    reg_id     integer,                       -- nullable FK column
    metric     integer NOT NULL,
    CONSTRAINT dw_report_pk PRIMARY KEY (report_id),
    CONSTRAINT dw_report_reg_fk FOREIGN KEY (reg_id)
        REFERENCES dw_registry (reg_id) MATCH SIMPLE
        ON DELETE NO ACTION ON UPDATE NO ACTION NOT DEFERRABLE
);

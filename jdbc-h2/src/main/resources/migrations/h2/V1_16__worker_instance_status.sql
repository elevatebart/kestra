/* ----------------------- workerInstance ----------------------- */
ALTER TABLE worker_instance ALTER COLUMN "status" VARCHAR(32) NOT NULL GENERATED ALWAYS AS (JQ_STRING("value",'.status'));
/* ----------------------- workerInstance ----------------------- */
ALTER TABLE `worker_instance` MODIFY COLUMN `status` VARCHAR(32) GENERATED ALWAYS AS (value ->> '$.status') STORED NOT NULL;
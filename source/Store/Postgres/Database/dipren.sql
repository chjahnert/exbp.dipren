
--
-- This script creates the database objects required by Dipren.
--


CREATE SCHEMA "dipren";

COMMENT ON SCHEMA "dipren" IS 'Defines the Dipren database objects';


CREATE TABLE "dipren"."jobs"
(
  "id" VARCHAR(256) NOT NULL,
  "created" TIMESTAMP NOT NULL,
  "updated" TIMESTAMP NOT NULL,
  "state" INTEGER NOT NULL,
  "error" TEXT NULL,
  
  CONSTRAINT "pk_jobs" PRIMARY KEY ("id")
);

COMMENT ON COLUMN "dipren"."jobs"."id" IS 'The unique identifier of the distributed processing job';
COMMENT ON COLUMN "dipren"."jobs"."created" IS 'A timestamp indicating when the job was created.';
COMMENT ON COLUMN "dipren"."jobs"."updated" IS 'A timestamp indicating when the job was last updated.';
COMMENT ON COLUMN "dipren"."jobs"."state" IS 'Indicates the state of the job such.';
COMMENT ON COLUMN "dipren"."jobs"."error" IS 'Describes the error that caused the job to fail.';


CREATE TABLE "dipren"."partitions"
(
  "id" CHAR(36) NOT NULL,
  "job_id" VARCHAR(256) NOT NULL,
  "created" TIMESTAMP NOT NULL,
  "updated" TIMESTAMP NOT NULL,
  "owner" VARCHAR(256) NULL,
  "first" TEXT NOT NULL,
  "last" TEXT NOT NULL,
  "is_inclusive" BOOLEAN NOT NULL,
  "position" TEXT NULL,
  "processed" BIGINT NOT NULL,
  "remaining" BIGINT NOT NULL,
  "is_completed" BOOLEAN NOT NULL,
  "is_split_requested" BOOLEAN NOT NULL,

  CONSTRAINT "pk_partitions" PRIMARY KEY ("id"),
  CONSTRAINT "fk_partitions_to_job" FOREIGN KEY ("job_id") REFERENCES "dipren"."jobs"("id") ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE INDEX "ix_partitions_by_state" ON "dipren"."partitions" ("job_id", "updated", "owner") WHERE ("is_completed" = FALSE);
CREATE INDEX "ux_partitions_owner" ON "dipren"."partitions" ("owner") WHERE ("owner" IS NOT NULL);

COMMENT ON COLUMN "dipren"."partitions"."id" IS 'The unique identifier of the partition.';
COMMENT ON COLUMN "dipren"."partitions"."job_id" IS 'The unique identifier of the job the partition belongs to.';
COMMENT ON COLUMN "dipren"."partitions"."created" IS 'A timestamp value indicating when the partition was created.';
COMMENT ON COLUMN "dipren"."partitions"."updated" IS 'A timestamp value indicating when the partition was last updated.';
COMMENT ON COLUMN "dipren"."partitions"."owner" IS 'The unique identifier of the processing node owning the partition.';
COMMENT ON COLUMN "dipren"."partitions"."first" IS 'The first key of the key range associated with the partition.';
COMMENT ON COLUMN "dipren"."partitions"."last" IS 'The last key of the key range associated with the partition.';
COMMENT ON COLUMN "dipren"."partitions"."is_inclusive" IS 'Indicates whether the last key is included in the key range.';
COMMENT ON COLUMN "dipren"."partitions"."position" IS 'The last key that was processed.';
COMMENT ON COLUMN "dipren"."partitions"."processed" IS 'The number of keys processed so far.';
COMMENT ON COLUMN "dipren"."partitions"."remaining" IS 'The estimated number of keys left to process.';
COMMENT ON COLUMN "dipren"."partitions"."is_completed" IS 'Indicates whether the entire partition has been processed.';
COMMENT ON COLUMN "dipren"."partitions"."is_split_requested" IS 'Indicates whether a split has been requested.';
COMMENT ON INDEX "dipren"."ix_partitions_by_state" IS 'Use when an idle processing node tries to acquire a partition.';
COMMENT ON INDEX "dipren"."ux_partitions_owner" IS 'Ensures that each processing node can only acquire a single partition.';
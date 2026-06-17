import { Job, Processor } from "bullmq";
import {
  clickhouseClient,
  getClickhouseEntityType,
  getCurrentSpan,
  getS3EventStorageClient,
  hasS3SlowdownFlag,
  IngestionEventType,
  isS3SlowDownError,
  logger,
  markProjectS3Slowdown,
  processEventBatch,
  QueueName,
  recordDistribution,
  recordHistogram,
  recordIncrement,
  redis,
  SecondaryIngestionQueue,
  TQueueJobTypes,
  traceException,
} from "@langfuse/shared/src/server";
import { prisma } from "@langfuse/shared/src/db";

import { env, v4WritesToEventsTable } from "../env";
import {
  IngestionService,
  type S3ProcessedFile,
} from "../services/IngestionService";
import { ClickhouseWriter, TableName } from "../services/ClickhouseWriter";
import { chunk } from "lodash";
import { randomUUID } from "crypto";

export const ingestionQueueProcessorBuilder = (
  enableRedirectToSecondaryQueue: boolean,
): Processor => {
  const projectIdsToRedirectToSecondaryQueue =
    env.LANGFUSE_SECONDARY_INGESTION_QUEUE_ENABLED_PROJECT_IDS?.split(",") ??
    [];

  return async (job: Job<TQueueJobTypes[QueueName.IngestionQueue]>) => {
    try {
      const span = getCurrentSpan();
      if (span) {
        span.setAttribute("messaging.bullmq.job.input.id", job.data.id);
        span.setAttribute(
          "messaging.bullmq.job.input.projectId",
          job.data.payload.authCheck.scope.projectId,
        );
        span.setAttribute(
          "messaging.bullmq.job.input.eventBodyId",
          job.data.payload.data.eventBodyId,
        );
        span.setAttribute(
          "messaging.bullmq.job.input.type",
          job.data.payload.data.type,
        );
        span.setAttribute(
          "messaging.bullmq.job.input.fileKey",
          job.data.payload.data.fileKey ?? "",
        );
      }

      // We write the new file into the ClickHouse event log to keep track for retention and deletions
      const clickhouseWriter = ClickhouseWriter.getInstance();

      if (
        env.LANGFUSE_ENABLE_BLOB_STORAGE_FILE_LOG === "true" &&
        job.data.payload.data.fileKey &&
        job.data.payload.data.fileKey
      ) {
        const fileName = `${job.data.payload.data.fileKey}.json`;
        clickhouseWriter.addToQueue(TableName.BlobStorageFileLog, {
          id: randomUUID(),
          project_id: job.data.payload.authCheck.scope.projectId,
          entity_type: getClickhouseEntityType(job.data.payload.data.type),
          entity_id: job.data.payload.data.eventBodyId,
          event_id: job.data.payload.data.fileKey,
          bucket_name: env.LANGFUSE_S3_EVENT_UPLOAD_BUCKET,
          bucket_path: `${env.LANGFUSE_S3_EVENT_UPLOAD_PREFIX}${job.data.payload.authCheck.scope.projectId}/${getClickhouseEntityType(job.data.payload.data.type)}/${job.data.payload.data.eventBodyId}/${fileName}`,
          created_at: new Date().getTime(),
          updated_at: new Date().getTime(),
          event_ts: new Date().getTime(),
          is_deleted: 0,
        });
      }

      // If fileKey was processed within the last minutes, i.e. has a match in redis, we skip processing.
      if (
        env.LANGFUSE_ENABLE_REDIS_SEEN_EVENT_CACHE === "true" &&
        redis &&
        job.data.payload.data.fileKey
      ) {
        const key = `langfuse:ingestion:recently-processed:${job.data.payload.authCheck.scope.projectId}:${job.data.payload.data.type}:${job.data.payload.data.eventBodyId}:${job.data.payload.data.fileKey}`;
        const exists = await redis.exists(key);
        if (exists) {
          recordIncrement("langfuse.ingestion.recently_processed_cache", 1, {
            type: job.data.payload.data.type,
            skipped: "true",
          });
          logger.debug(
            `Skipping ingestion event ${job.data.payload.data.fileKey} for project ${job.data.payload.authCheck.scope.projectId}`,
          );
          return;
        } else {
          recordIncrement("langfuse.ingestion.recently_processed_cache", 1, {
            type: job.data.payload.data.type,
            skipped: "false",
          });
        }
      }

      // Check if project should be redirected to secondary queue
      const projectId = job.data.payload.authCheck.scope.projectId;
      const shouldRedirectEnv =
        projectIdsToRedirectToSecondaryQueue.includes(projectId);
      const shouldRedirectSlowdown = await hasS3SlowdownFlag(projectId);

      if (
        enableRedirectToSecondaryQueue &&
        (shouldRedirectEnv || shouldRedirectSlowdown)
      ) {
        logger.debug(
          `Redirecting ingestion event to secondary queue for project ${projectId}`,
          {
            reason: shouldRedirectSlowdown ? "s3_slowdown_flag" : "env_config",
          },
        );
        const shardingKey = `${projectId}-${job.data.payload.data.eventBodyId}`;
        const secondaryQueue = SecondaryIngestionQueue.getInstance({
          shardingKey,
        });
        if (secondaryQueue) {
          await secondaryQueue.add(QueueName.IngestionSecondaryQueue, job.data);
          // If we don't redirect, we continue with the ingestion. Otherwise, we finish here.
          return;
        }
      }

      const s3Client = getS3EventStorageClient(
        env.LANGFUSE_S3_EVENT_UPLOAD_BUCKET,
      );

      logger.debug(
        `Processing ingestion event ${
          enableRedirectToSecondaryQueue ? "" : "secondary"
        }`,
        {
          projectId: job.data.payload.authCheck.scope.projectId,
          payload: job.data.payload.data,
        },
      );

      // Download all events from folder into a local array
      const clickhouseEntityType = getClickhouseEntityType(
        job.data.payload.data.type,
      );

      let eventFiles: { file: string; createdAt: Date }[] = [];
      const events: IngestionEventType[] = [];
      // Track each S3 file with its event ids and processing status for fine-grained seen-cache control
      const s3Files: S3ProcessedFile[] = [];

      // Check if we should skip S3 list operation
      const shouldSkipS3List =
        // The producer sets skipS3List to true if it's an OTel observation
        job.data.payload.data.skipS3List && job.data.payload.data.fileKey;
      const s3Prefix = `${env.LANGFUSE_S3_EVENT_UPLOAD_PREFIX}${job.data.payload.authCheck.scope.projectId}/${clickhouseEntityType}/${job.data.payload.data.eventBodyId}/`;

      let totalS3DownloadSizeBytes = 0;

      if (shouldSkipS3List) {
        // Direct file download - skip S3 list operation
        const filePath = `${s3Prefix}${job.data.payload.data.fileKey}.json`;
        eventFiles = [{ file: filePath, createdAt: new Date() }];
        const s3FileKey = `${job.data.payload.data.fileKey}.json`;

        try {
          const file = await s3Client.download(filePath);
          const fileSize = file.length;

          recordHistogram("langfuse.ingestion.s3_file_size_bytes", fileSize, {
            skippedS3List: "true",
          });
          totalS3DownloadSizeBytes += fileSize;

          const parsedFile = JSON.parse(file);
          const fileEvents = Array.isArray(parsedFile) ? parsedFile : [parsedFile];
          events.push(...fileEvents);
          s3Files.push({
            s3FileKey,
            eventIds: fileEvents.map((e: IngestionEventType) => e.id),
            processed: true,
          });
        } catch (e) {
          logger.error(
            `Failed to download or parse S3 file ${filePath} for project ${job.data.payload.authCheck.scope.projectId} and event ${job.data.payload.data.eventBodyId}`,
            e,
          );
          // Remove the failed file from eventFiles so it is not cached as "seen" below
          eventFiles = eventFiles.filter((f) => f.file !== filePath);
          s3Files.push({
            s3FileKey,
            eventIds: [],
            processed: false,
          });
        }
      } else {
        eventFiles = await s3Client.listFiles(s3Prefix);

        // Process files in batches
        // If a user has 5k events, this will likely take 100 seconds.
        const downloadAndParseFile = async (fileRef: { file: string }) => {
          const s3FileKey = fileRef.file.split("/").pop() ?? fileRef.file;
          try {
            const file = await s3Client.download(fileRef.file);
            const fileSize = file.length;

            recordHistogram("langfuse.ingestion.s3_file_size_bytes", fileSize, {
              skippedS3List: "false",
            });
            totalS3DownloadSizeBytes += fileSize;

            const parsedFile = JSON.parse(file);
            const fileEvents = Array.isArray(parsedFile) ? parsedFile : [parsedFile];
            return {
              file: fileRef.file,
              events: fileEvents,
              success: true as const,
              s3FileKey,
              eventIds: fileEvents.map((e: IngestionEventType) => e.id),
            };
          } catch (e) {
            logger.error(
              `Failed to download or parse S3 file ${fileRef.file} for project ${job.data.payload.authCheck.scope.projectId} and event ${job.data.payload.data.eventBodyId}`,
              e,
            );
            return {
              file: fileRef.file,
              events: [],
              success: false as const,
              s3FileKey,
              eventIds: [],
            };
          }
        };

        const S3_CONCURRENT_READS = env.LANGFUSE_S3_CONCURRENT_READS;
        const batches = chunk(eventFiles, S3_CONCURRENT_READS);
        const successfullyProcessedFiles: string[] = [];
        for (const batch of batches) {
          const batchResults = await Promise.all(
            batch.map(downloadAndParseFile),
          );
          for (const result of batchResults) {
            events.push(...result.events);
            s3Files.push({
              s3FileKey: result.s3FileKey,
              eventIds: result.eventIds,
              processed: result.success,
            });
            if (result.success) {
              successfullyProcessedFiles.push(result.file);
            }
          }
        }
        // Only keep files in eventFiles that were successfully processed,
        // so failed files are not cached as "seen" below and may be retried.
        eventFiles = eventFiles.filter((f) =>
          successfullyProcessedFiles.includes(f.file),
        );
      }

      recordDistribution(
        "langfuse.ingestion.count_files_distribution",
        eventFiles.length,
        {
          kind: clickhouseEntityType,
        },
      );
      span?.setAttribute(
        "langfuse.ingestion.event.count_files",
        eventFiles.length,
      );
      span?.setAttribute("langfuse.ingestion.event.kind", clickhouseEntityType);
      span?.setAttribute(
        "langfuse.ingestion.s3_all_files_size_bytes",
        totalS3DownloadSizeBytes,
      );

      const firstS3WriteTime =
        eventFiles
          .map((fileRef) => fileRef.createdAt)
          .sort()
          .shift() ?? new Date();

      if (events.length === 0) {
        logger.warn(
          `No events found for project ${job.data.payload.authCheck.scope.projectId} and event ${job.data.payload.data.eventBodyId}`,
        );
        return;
      }

      // Perform merge of those events
      if (!redis) throw new Error("Redis not available");
      if (!prisma) throw new Error("Prisma not available");

      // Determine whether to forward to staging events table
      // Use explicit flag from job payload if provided, otherwise fall back to env flags
      const forwardToEventsTable =
        job.data.payload.data.forwardToEventsTable ??
        v4WritesToEventsTable(env);

      const mergeResult = await new IngestionService(
        redis,
        prisma,
        clickhouseWriter,
        clickhouseClient(),
      ).mergeAndWrite(
        getClickhouseEntityType(events[0].type),
        job.data.payload.authCheck.scope.projectId,
        job.data.payload.data.eventBodyId,
        firstS3WriteTime,
        events,
        forwardToEventsTable,
        s3Files,
      );

      // Determine whether we should retry based on critical vs secondary failures.
      // Retry only when critical operations failed (main table write, session upsert, dataset item lookup).
      // Secondary failures (staging table write, eval queue, wrapper trace, score validation)
      // are logged but do not trigger a retry since the core data was persisted safely.
      const criticalOpsFailed =
        mergeResult.critical.mainTableWrite.status === "failed" ||
        mergeResult.critical.sessionUpsert.status === "failed" ||
        mergeResult.critical.datasetItemLookup.status === "failed";

      // Check if any secondary operations failed (for logging / observability).
      const secondaryOpsFailed =
        mergeResult.secondary.stagingTableWrite.status === "failed" ||
        mergeResult.secondary.traceUpsertQueue.status === "failed" ||
        mergeResult.secondary.wrapperTraceWrite.status === "failed" ||
        mergeResult.secondary.scoreValidation.status === "failed";

      if (criticalOpsFailed) {
        // Critical data loss scenario: core record was not written.
        // Do NOT set "seen" cache, so that S3 files remain eligible for reprocessing.
        // Instead of throwing (which triggers BullMQ retry with exponential backoff),
        // re-route the failed events through processEventBatch for a clean retry
        // via the standard S3 + IngestionQueue path. This preserves full retry
        // semantics, monitoring, and avoids data loss due to seen-cache misordering.
        const firstCriticalError =
          mergeResult.critical.mainTableWrite.error ??
          mergeResult.critical.sessionUpsert.error ??
          mergeResult.critical.datasetItemLookup.error ??
          mergeResult.error ??
          new Error("Unknown critical mergeAndWrite failure");
        logger.error(
          `Critical mergeAndWrite failure for project ${job.data.payload.authCheck.scope.projectId} eventBody ${job.data.payload.data.eventBodyId} - re-routing via processEventBatch for retry`,
          {
            eventType: job.data.payload.data.type,
            eventCount: mergeResult.eventCount,
            critical: mergeResult.critical,
            secondary: secondaryOpsFailed ? mergeResult.secondary : undefined,
            error: firstCriticalError,
          },
        );

        // Re-queue the raw events through the standard ingestion pipeline.
        // This will re-upload to S3 (new file, not conflicting with existing)
        // and create a new IngestionQueue job with full retry semantics.
        // The original S3 files remain unmarked as "seen" and will be re-listed
        // when the new job runs, so events are merged correctly.
        if (mergeResult.rawEvents.length > 0) {
          try {
            const authCheck = job.data.payload.authCheck as {
              validKey: true;
              scope: {
                projectId: string;
                accessLevel: "project" | "scores";
              };
            };
            await processEventBatch(mergeResult.rawEvents, authCheck, {
              delay: 5000, // small delay to let transient issues resolve
              source: "ingestion-retry",
            });
            recordIncrement(
              "langfuse.ingestion.merge_and_write.retry_rerouted",
              mergeResult.eventCount,
              {
                projectId: job.data.payload.authCheck.scope.projectId,
                eventType: job.data.payload.data.type,
              },
            );
          } catch (requeueError) {
            logger.error(
              `Failed to re-queue failed events for project ${job.data.payload.authCheck.scope.projectId} eventBody ${job.data.payload.data.eventBodyId}`,
              requeueError,
            );
            traceException(requeueError);
            // If re-queue also fails, fall back to throwing so BullMQ retries the original job
            throw firstCriticalError;
          }
        }

        // Do not throw - the original job is ack'd successfully.
        // The retry is handled by the new processEventBatch flow above.
        return;
      }

      // Core data is safely persisted. Mark only fully processed S3 files as seen.
      // Secondary ops are best-effort and do not justify reprocessing the entire eventBody.
      const processedFiles = mergeResult.s3Files.filter((f) => f.processed);

      if (secondaryOpsFailed) {
        logger.warn(
          `mergeAndWrite succeeded with secondary failures for project ${job.data.payload.authCheck.scope.projectId} eventBody ${job.data.payload.data.eventBodyId} - marking ${processedFiles.length}/${mergeResult.s3Files.length} files as seen anyway`,
          {
            eventType: job.data.payload.data.type,
            secondary: mergeResult.secondary,
          },
        );
      }

      // Set "seen" keys in Redis only for successfully processed files, after successful critical-path merge.
      // We use Promise.all internally instead of a redis.pipeline since autoPipelining should handle it correctly
      // while being redis cluster aware.
      // IMPORTANT: This must happen AFTER successful critical merge so that failed events can be retried.
      if (env.LANGFUSE_ENABLE_REDIS_SEEN_EVENT_CACHE === "true" && redis) {
        try {
          await Promise.all(
            processedFiles.map((s3File) =>
              redis!.set(
                `langfuse:ingestion:recently-processed:${job.data.payload.authCheck.scope.projectId}:${job.data.payload.data.type}:${job.data.payload.data.eventBodyId}:${s3File.s3FileKey.replace(".json", "")}`,
                "1",
                "EX",
                60 * 5, // 5 minutes
              ),
            ),
          );
        } catch (e) {
          logger.warn(
            `Failed to set recently-processed cache after successful merge. Events may be reprocessed (idempotent, so safe).`,
            e,
          );
        }
      }
    } catch (e) {
      // Check if this is a SlowDown error and mark the project for secondary queue
      if (isS3SlowDownError(e)) {
        const projectId = job.data.payload.authCheck.scope.projectId;
        logger.warn(
          "S3 SlowDown error during ingestion processing, marking project for secondary queue",
          { projectId, error: e },
        );
        await markProjectS3Slowdown(projectId);
      }

      logger.error(
        `Failed job ingestion processing for ${job.data.payload.authCheck.scope.projectId}`,
        e,
      );
      traceException(e);
      throw e;
    }
  };
};

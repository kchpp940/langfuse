import { randomUUID } from "crypto";
import { z } from "zod";

import { env } from "../../env";
import {
  InvalidRequestError,
  LangfuseNotFoundError,
  UnauthorizedError,
} from "../../errors";
import { AuthHeaderValidVerificationResultIngestion } from "../auth/types";
import { getClickhouseEntityType } from "../clickhouse/schemaUtils";
import {
  getCurrentSpan,
  instrumentAsync,
  recordDistribution,
  recordIncrement,
} from "../instrumentation";
import { logger } from "../logger";
import { QueueJobs } from "../queues";
import { IngestionQueue } from "../redis/ingestionQueue";
import { redis } from "../redis/redis";
import {
  eventTypes,
  createIngestionEventSchema,
  IngestionEventType,
} from "./types";
import {
  StorageService,
  StorageServiceFactory,
} from "../services/StorageService";
import { isTraceIdInSample } from "./sampling";
import {
  isS3SlowDownError,
  markProjectS3Slowdown,
} from "../redis/s3SlowdownTracking";

let s3StorageServiceClient: StorageService;

const getS3StorageServiceClient = (bucketName: string): StorageService => {
  if (!s3StorageServiceClient) {
    s3StorageServiceClient = StorageServiceFactory.getInstance({
      bucketName,
      accessKeyId: env.LANGFUSE_S3_EVENT_UPLOAD_ACCESS_KEY_ID,
      secretAccessKey: env.LANGFUSE_S3_EVENT_UPLOAD_SECRET_ACCESS_KEY,
      endpoint: env.LANGFUSE_S3_EVENT_UPLOAD_ENDPOINT,
      region: env.LANGFUSE_S3_EVENT_UPLOAD_REGION,
      forcePathStyle: env.LANGFUSE_S3_EVENT_UPLOAD_FORCE_PATH_STYLE === "true",
      awsSse: env.LANGFUSE_S3_EVENT_UPLOAD_SSE,
      awsSseKmsKeyId: env.LANGFUSE_S3_EVENT_UPLOAD_SSE_KMS_KEY_ID,
    });
  }
  return s3StorageServiceClient;
};

/**
 * Get the delay for the event based on the event type. Uses delay if set, 0 if current UTC timestamp is not between
 * 23:45 and 00:15, and env.LANGFUSE_INGESTION_QUEUE_DELAY_MS otherwise.
 * We need the delay around date boundaries to avoid duplicates for out-of-order processing of events.
 * @param delay - Delay overwrite. Used if non-null.
 */
const getDelay = (delay: number | null, source: "api" | "otel") => {
  if (delay !== null) {
    return delay;
  }
  const now = new Date();
  const hours = now.getUTCHours();
  const minutes = now.getUTCMinutes();

  if ((hours === 23 && minutes >= 45) || (hours === 0 && minutes <= 15)) {
    return env.LANGFUSE_INGESTION_QUEUE_DELAY_MS;
  }

  if (source === "otel") {
    return 0;
  }

  // Use 5s here to avoid duplicate processing on the worker. If the ingestion delay is set to a lower value,
  // we use this instead.
  // Values should be revisited based on a cost/performance trade-off.
  return Math.min(5000, env.LANGFUSE_INGESTION_QUEUE_DELAY_MS);
};

/**
 * Options for event batch processing.
 * @property delay - Delay in ms to wait before processing events in the batch.
 * @property source - Source of the events for metrics tracking (e.g., "otel", "api").
 * @property isLangfuseInternal - Whether the events are being ingested by Langfuse internally (e.g. traces created for prompt experiments).
 * @property forwardToEventsTable - Whether to forward events to the staging events table for batch propagation. If undefined, falls back to environment flags.
 */
type ProcessEventBatchOptions = {
  delay?: number | null;
  source?: "api" | "otel";
  isLangfuseInternal?: boolean;
  forwardToEventsTable?: boolean;
};

/**
 * Processes a batch of events.
 * @param input - Batch of IngestionEventType. Will validate the types first thing and return errors if they are invalid.
 * @param authCheck - AuthHeaderValidVerificationResultIngestion
 * @param options - (Optional) Options for the event batch processing.
 */
export const processEventBatch = async (
  input: unknown[],
  authCheck: AuthHeaderValidVerificationResultIngestion,
  options: ProcessEventBatchOptions = {},
): Promise<{
  successes: { id: string; status: number }[];
  errors: {
    id: string;
    status: number;
    message?: string;
    error?: string;
  }[];
}> => {
  if (input.length === 0) {
    return { successes: [], errors: [] };
  }
  const {
    delay = null,
    source = "api",
    isLangfuseInternal = false,
    forwardToEventsTable,
  } = options;

  // add context of api call to the span
  const currentSpan = getCurrentSpan();
  recordIncrement("langfuse.ingestion.event", input.length, { source });
  recordDistribution("langfuse.ingestion.event_distribution", input.length, {
    source,
  });

  currentSpan?.setAttribute("langfuse.ingestion.batch_size", input.length);
  currentSpan?.setAttribute(
    "langfuse.project.id",
    authCheck.scope.projectId ?? "",
  );
  if (authCheck.scope.orgId)
    currentSpan?.setAttribute("langfuse.org.id", authCheck.scope.orgId);
  if (authCheck.scope.plan)
    currentSpan?.setAttribute("langfuse.org.plan", authCheck.scope.plan);

  /**************
   * VALIDATION *
   **************/
  if (!authCheck.scope.projectId) {
    throw new UnauthorizedError("Missing project ID");
  }

  const validationErrors: { id: string; error: unknown }[] = [];
  const authenticationErrors: { id: string; error: unknown }[] = [];

  const ingestionSchema = createIngestionEventSchema(isLangfuseInternal);
  const batch: z.infer<typeof ingestionSchema>[] = input
    .flatMap((event) => {
      const parsed = ingestionSchema.safeParse(event);
      if (!parsed.success) {
        validationErrors.push({
          id:
            typeof event === "object" && event && "id" in event
              ? typeof event.id === "string"
                ? event.id
                : "unknown"
              : "unknown",
          error: new InvalidRequestError(parsed.error.message),
        });
        return [];
      }
      if (!isAuthorized(parsed.data, authCheck)) {
        authenticationErrors.push({
          id: parsed.data.id,
          error: new UnauthorizedError("Access Scope Denied"),
        });
        return [];
      }
      return [parsed.data];
    })
    .flatMap((event) => {
      if (event.type === eventTypes.SDK_LOG) {
        // Log SDK_LOG events, but remove them from further processing
        logger.info("SDK Log Event", { event });
        return [];
      }
      return [event];
    });

  const sortedBatch = sortBatch(batch);

  // We group events by eventBodyId which allows us to store and process them
  // as one which reduces infra interactions per event. Only used in the S3 case.
  const sortedBatchByEventBodyId = sortedBatch.reduce(
    (
      acc: Record<
        string,
        {
          data: IngestionEventType[];
          key: string;
          eventBodyId: string;
          type: (typeof eventTypes)[keyof typeof eventTypes];
        }
      >,
      event,
    ) => {
      if (!event.body?.id) {
        return acc;
      }
      const key = `${getClickhouseEntityType(event.type)}-${event.body.id}`;
      if (!acc[key]) {
        acc[key] = {
          data: [],
          key: event.id,
          type: event.type,
          eventBodyId: event.body.id,
        };
      }
      acc[key].data.push(event);
      return acc;
    },
    {},
  );

  /********************
   * ASYNC PROCESSING *
   ********************/
  const s3UploadFailedEventBodyIds = new Set<string>();
  const s3UploadErrors: { id: string; error: unknown }[] = [];

  await instrumentAsync({ name: "s3-upload-events" }, async () => {
    const s3UploadPromises = Object.keys(sortedBatchByEventBodyId).map(
      async (id) => {
        const eventData = sortedBatchByEventBodyId[id];
        const { data, key, type, eventBodyId } = eventData;
        const bucketPath = `${env.LANGFUSE_S3_EVENT_UPLOAD_PREFIX}${authCheck.scope.projectId}/${getClickhouseEntityType(type)}/${eventBodyId}/${key}.json`;
        try {
          await getS3StorageServiceClient(
            env.LANGFUSE_S3_EVENT_UPLOAD_BUCKET,
          ).uploadJson(bucketPath, data);
          return { eventBodyId, eventData, success: true };
        } catch (error) {
          return { eventBodyId, eventData, success: false, error };
        }
      },
    );
    const results = await Promise.all(s3UploadPromises);
    results.forEach((result) => {
      if (!result.success) {
        const { eventBodyId, eventData, error } = result;
        s3UploadFailedEventBodyIds.add(eventBodyId);
        eventData.data.forEach((event) => {
          s3UploadErrors.push({ id: event.id, error });
        });

        if (isS3SlowDownError(error)) {
          logger.warn(
            "S3 SlowDown error during upload, marking project for secondary queue",
            {
              projectId: authCheck.scope.projectId,
              error,
            },
          );
          markProjectS3Slowdown(authCheck.scope.projectId!).catch(() => {});
        }

        logger.error("Failed to upload event to S3", {
          error,
          eventBodyId,
        });
      }
    });
  });

  const eventBodyIdsToProcess = Object.keys(sortedBatchByEventBodyId).filter(
    (id) => !s3UploadFailedEventBodyIds.has(id),
  );

  if (!redis) {
    throw new Error("Redis not initialized, aborting event processing");
  }

  const projectIdsToSkipS3List =
    env.LANGFUSE_SKIP_S3_LIST_FOR_OBSERVATIONS_PROJECT_IDS?.split(",") ?? [];

  const queueAddResults = await Promise.allSettled(
    eventBodyIdsToProcess.map(async (id) => {
      const eventData = sortedBatchByEventBodyId[id];
      const shardingKey = `${authCheck.scope.projectId}-${eventData.eventBodyId}`;
      const queue = IngestionQueue.getInstance({ shardingKey });

      const isDatasetRunItemEvent =
        getClickhouseEntityType(eventData.type) === "dataset_run_item";
      const isObservationEvent =
        getClickhouseEntityType(eventData.type) === "observation";

      const isOtelOrSkipS3Project =
        authCheck.scope.projectId !== null &&
        (source === "otel" ||
          projectIdsToSkipS3List.includes(authCheck.scope.projectId));

      const shouldSkipS3List =
        isDatasetRunItemEvent || (isObservationEvent && isOtelOrSkipS3Project);

      const { isSampled, isSamplingConfigured } = isTraceIdInSample({
        projectId: authCheck.scope.projectId,
        event: eventData.data[0],
      });

      if (!isSampled) {
        recordIncrement("langfuse.ingestion.sampling", eventData.data.length, {
          projectId: authCheck.scope.projectId ?? "<not set>",
          sampling_decision: "out",
        });

        return {
          eventData,
          result: Promise.resolve(),
          sampledOut: true,
        };
      }

      if (isSamplingConfigured) {
        recordIncrement("langfuse.ingestion.sampling", eventData.data.length, {
          projectId: authCheck.scope.projectId ?? "<not set>",
          sampling_decision: "in",
        });
      }

      if (!queue) {
        return {
          eventData,
          result: Promise.reject(
            new Error("Failed to instantiate ingestion queue"),
          ),
          sampledOut: false,
        };
      }

      return {
        eventData,
        result: queue.add(
          QueueJobs.IngestionJob,
          {
            id: randomUUID(),
            timestamp: new Date(),
            name: QueueJobs.IngestionJob as const,
            payload: {
              data: {
                type: eventData.type,
                eventBodyId: eventData.eventBodyId,
                fileKey: eventData.key,
                skipS3List: shouldSkipS3List,
                forwardToEventsTable,
              },
              authCheck: authCheck as {
                validKey: true;
                scope: {
                  projectId: string;
                  accessLevel: "project" | "scores";
                };
              },
            },
          },
          { delay: getDelay(delay, source) },
        ),
        sampledOut: false,
      };
    }),
  );

  const queueAddErrors: { id: string; error: unknown }[] = [];
  const successfullyQueuedEventIds: string[] = [];

  for (const result of queueAddResults) {
    if (result.status === "fulfilled") {
      const { eventData, sampledOut } = result.value;
      if (sampledOut) {
        eventData.data.forEach((event) => {
          successfullyQueuedEventIds.push(event.id);
        });
      } else {
        try {
          await result.value.result;
          eventData.data.forEach((event) => {
            successfullyQueuedEventIds.push(event.id);
          });
        } catch (error) {
          eventData.data.forEach((event) => {
            queueAddErrors.push({ id: event.id, error });
          });
          logger.error("Failed to add ingestion queue job", {
            error,
            eventBodyId: eventData.eventBodyId,
          });
        }
      }
    } else {
      const eventData =
        result.reason &&
        typeof result.reason === "object" &&
        "eventData" in result.reason
          ? (
              result.reason as {
                eventData: (typeof sortedBatchByEventBodyId)[string];
              }
            ).eventData
          : null;
      if (eventData) {
        eventData.data.forEach((event) => {
          queueAddErrors.push({ id: event.id, error: result.reason });
        });
      }
      logger.error("Failed to process event batch queue add", {
        error: result.reason,
      });
    }
  }

  return aggregateBatchResult(
    [
      ...validationErrors,
      ...authenticationErrors,
      ...s3UploadErrors,
      ...queueAddErrors,
    ],
    sortedBatch
      .filter((event) => successfullyQueuedEventIds.includes(event.id))
      .map((event) => ({ id: event.id, result: event })),
    authCheck.scope.projectId,
  );
};

const isAuthorized = (
  event: IngestionEventType,
  authScope: AuthHeaderValidVerificationResultIngestion,
): boolean => {
  if (event.type === eventTypes.SDK_LOG) {
    return true;
  }

  if (event.type === eventTypes.SCORE_CREATE) {
    return (
      authScope.scope.accessLevel === "scores" ||
      authScope.scope.accessLevel === "project"
    );
  }

  return authScope.scope.accessLevel === "project";
};

/**
 * Sorts a batch of ingestion events. Orders by: updating events last, sorted by timestamp asc.
 */
const sortBatch = (batch: IngestionEventType[]) => {
  const updateEvents: (typeof eventTypes)[keyof typeof eventTypes][] = [
    eventTypes.GENERATION_UPDATE,
    eventTypes.SPAN_UPDATE,
    eventTypes.OBSERVATION_UPDATE, // legacy event type
  ];
  const updates = batch
    .filter((event) => updateEvents.includes(event.type))
    .sort((a, b) => {
      return new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime();
    });
  const others = batch
    .filter((event) => !updateEvents.includes(event.type))
    .sort((a, b) => {
      return new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime();
    });

  // Return the array with non-update events first, followed by update events
  return [...others, ...updates];
};

export const aggregateBatchResult = (
  errors: Array<{ id: string; error: unknown }>,
  results: Array<{ id: string; result: unknown }>,
  projectId?: string,
) => {
  const returnedErrors: {
    id: string;
    status: number;
    message?: string;
    error?: string;
  }[] = [];

  const successes: {
    id: string;
    status: number;
  }[] = [];

  errors.forEach((error) => {
    if (error.error instanceof InvalidRequestError) {
      returnedErrors.push({
        id: error.id,
        status: 400,
        message: "Invalid request data",
        error: error.error.message,
      });
    } else if (error.error instanceof UnauthorizedError) {
      returnedErrors.push({
        id: error.id,
        status: 401,
        message: "Authentication error",
        error: error.error.message,
      });
    } else if (error.error instanceof LangfuseNotFoundError) {
      returnedErrors.push({
        id: error.id,
        status: 404,
        message: "Resource not found",
        error: error.error.message,
      });
    } else {
      returnedErrors.push({
        id: error.id,
        status: 500,
        error: "Internal Server Error",
      });
    }
  });

  if (returnedErrors.length > 0) {
    logger.warn("Error processing events", {
      errors: returnedErrors,
      "langfuse.project.id": projectId,
    });
  }

  results.forEach((result) => {
    successes.push({
      id: result.id,
      status: 201,
    });
  });

  return { successes, errors: returnedErrors };
};

import type { OpenCodeClient } from "@opencode/client/effect";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schedule from "effect/Schedule";

import * as OpenCodeRuntime from "./OpenCodeRuntime.ts";
import type { OpenCode2Model } from "./status.ts";

const PROVIDER_NAME_TIMEOUT = "3 seconds";

/**
 * The models carry the payload and the names are decoration, so a provider read is bounded as
 * well as optional: one that never answers must not hold the models back.
 */
export const loadOpenCode2Catalog = (
  client: OpenCodeClient,
  directory: string,
): Effect.Effect<ReadonlyArray<OpenCode2Model>, OpenCodeRuntime.OpenCodeRuntimeError> => {
  const location = { directory };
  return Effect.all(
    {
      models: client.model.list({ location }).pipe(
        // Plugin activation is the only retryable API failure. The loader bounds
        // the whole read, including retries, so repeated 503s cannot hang refresh.
        Effect.retry({
          times: 5,
          while: (error) => error._tag === "ServiceUnavailableError" && error.service === "plugin",
          schedule: Schedule.spaced("250 millis"),
        }),
      ),
      providers: client.provider
        .list({ location })
        .pipe(Effect.timeout(PROVIDER_NAME_TIMEOUT), Effect.option),
    },
    { concurrency: "unbounded" },
  ).pipe(
    Effect.map(({ models, providers }) => {
      const providerNames = new Map(
        (Option.getOrUndefined(providers)?.data ?? []).map((provider) => [
          provider.id,
          provider.name,
        ]),
      );
      return models.data.map((model) => {
        const providerName = providerNames.get(model.providerID);
        return {
          providerID: model.providerID,
          id: model.id,
          name: model.name,
          ...(providerName ? { providerName } : {}),
          variants: model.variants.map((variant) => ({ id: variant.id })),
        };
      });
    }),
    Effect.mapError(
      (cause) =>
        new OpenCodeRuntime.OpenCodeRuntimeError({
          operation: "model.list",
          detail: "The OpenCode server could not list its models.",
          cause,
        }),
    ),
  );
};

import * as P from "effect/Predicate";

export type DeepPartial<T> = T extends readonly (infer U)[]
  ? readonly DeepPartial<U>[]
  : T extends object
    ? { [K in keyof T]?: DeepPartial<T[K]> }
    : T;

/**
 * Drops `__proto__` own keys at every depth, preserving object identity when
 * there is nothing to strip. Assignment through such a key would invoke the
 * prototype setter instead of defining a property.
 */
function stripProtoKeys(value: unknown, seen: Map<object, unknown> = new Map()): unknown {
  if (Array.isArray(value)) {
    let changed = false;
    const next = value.map((item) => {
      const cleaned = stripProtoKeys(item, seen);
      changed ||= cleaned !== item;
      return cleaned;
    });
    return changed ? next : value;
  }
  if (!P.isObject(value)) return value;
  const cached = seen.get(value);
  if (cached !== undefined) return cached;
  seen.set(value, value);
  let next: Record<string, unknown> | undefined;
  for (const [key, child] of Object.entries(value)) {
    const cleaned = stripProtoKeys(child, seen);
    if (key === "__proto__" || cleaned !== child) {
      next ??= { ...value };
      if (key === "__proto__") delete next[key];
      else next[key] = cleaned;
    }
  }
  if (next === undefined) return value;
  seen.set(value, next);
  return next;
}

export function deepMerge<T extends Record<string, unknown>>(current: T, patch: DeepPartial<T>): T {
  if (!P.isObject(current) || !P.isObject(patch)) {
    return patch as T;
  }
  const cleanPatch = stripProtoKeys(patch) as DeepPartial<T>;

  const next = { ...current } as Record<string, unknown>;
  for (const [key, value] of Object.entries(cleanPatch)) {
    if (value === undefined || key === "__proto__") continue;

    const existing = next[key];
    next[key] = P.isObject(existing) && P.isObject(value) ? deepMerge(existing, value) : value;
  }

  return next as T;
}

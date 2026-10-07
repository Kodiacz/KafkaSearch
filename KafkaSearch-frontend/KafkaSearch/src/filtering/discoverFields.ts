import type { CompareOp } from "../types/kafka";

export type FieldType =
  | "string"
  | "number"
  | "boolean"
  | "null"
  | "array-of-scalars"
  | "array-of-objects";

export interface DiscoveredField {
  path: string;
  type: FieldType;
  availableOps: CompareOp[];
}

const NUMBER_OPS: CompareOp[] = [
  "Equal",
  "NotEqual",
  "GreaterThan",
  "GreaterThanOrEqual",
  "LessThan",
  "LessThanOrEqual",
  "Exists"
];
const STRING_OPS: CompareOp[] = [
  "Equal",
  "NotEqual",
  "Contains",
  "StartsWith",
  "EndsWith",
  "Exists"
];
const DATE_STRING_OPS: CompareOp[] = [
  ...STRING_OPS,
  "GreaterThan",
  "GreaterThanOrEqual",
  "LessThan",
  "LessThanOrEqual"
];
const BOOL_OPS: CompareOp[] = ["Equal", "NotEqual", "Exists"];
const NULL_OPS: CompareOp[] = ["Exists", "Equal", "NotEqual"];

export function discoverFields(
  sample: unknown,
  prefix = ""
): DiscoveredField[] {
  if (sample === null || typeof sample !== "object") return [];

  return Object.entries(sample as Record<string, unknown>).flatMap(
    ([key, value]) => {
      const path = prefix ? `${prefix}.${key}` : key;

      if (Array.isArray(value)) {
        if (value.length === 0) return []; // nothing to infer a shape from — see caveat below

        const first = value[0];
        return typeof first === "object" && first !== null
          ? [{ path, type: "array-of-objects" as const, availableOps: [] }] // drills in via Any, not Compare
          : [
              {
                path,
                type: "array-of-scalars" as const,
                availableOps: STRING_OPS
              }
            ];
      }

      if (value !== null && typeof value === "object") {
        return discoverFields(value, path); // not itself filterable — just expands the tree
      }

      if (typeof value === "number")
        return [{ path, type: "number", availableOps: NUMBER_OPS }];
      if (typeof value === "boolean")
        return [{ path, type: "boolean", availableOps: BOOL_OPS }];
      if (typeof value === "string") {
        return [
          {
            path,
            type: "string",
            availableOps: looksLikeDate(value) ? DATE_STRING_OPS : STRING_OPS
          }
        ];
      }
      if (value === null) {
        return [{ path, type: "null", availableOps: NULL_OPS }];
      }
      return [];
    }
  );
}

function looksLikeDate(value: string): boolean {
  // cheap, deliberately conservative: ISO-ish shape, not just "anything Date.parse accepts"
  // (Date.parse is notoriously loose — e.g. it'll happily parse "10" or "October")
  return (
    /^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2})?/.test(value) &&
    !isNaN(Date.parse(value))
  );
}

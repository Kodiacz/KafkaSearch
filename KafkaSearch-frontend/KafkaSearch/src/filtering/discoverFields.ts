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
  arrayPaths: string[]
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

// export function discoverFields(
//   sample: unknown,
//   prefix = ""
// ): DiscoveredField[] {
//   if (sample === null || typeof sample !== "object") return [];

//   return Object.entries(sample as Record<string, unknown>).flatMap(
//     ([key, value]) => {
//       const path = prefix ? `${prefix}.${key}` : key;

//       if (Array.isArray(value)) {
//         if (value.length === 0) return []; // nothing to infer a shape from — see caveat below

//         if (value !== null && typeof value === "object") {
//           return discoverFields(value, path); // not itself filterable — just expands the tree
//         }

//         if (typeof value === "number")
//           return [{ path, type: "number", availableOps: NUMBER_OPS }];
//         if (typeof value === "boolean")
//           return [{ path, type: "boolean", availableOps: BOOL_OPS }];
//         if (typeof value === "string") {
//           return [
//             {
//               path,
//               type: "string",
//               availableOps: looksLikeDate(value) ? DATE_STRING_OPS : STRING_OPS
//             }
//           ];
//         }
//         if (value === null) {
//           return [{ path, type: "null", availableOps: NULL_OPS }];
//         }
//         return [];

//         // const first = value[0];

//         // return typeof first === "object" && first !== null
//         //     ? [{ path, type: "array-of-objects" as const, availableOps: [] }] // drills in via Any, not Compare
//         //     : [
//         //         {
//         //           path,
//         //           type: "array-of-scalars" as const,
//         //           availableOps: STRING_OPS
//         //         }
//         //       ];
//       }

//       if (value !== null && typeof value === "object") {
//         return discoverFields(value, path); // not itself filterable — just expands the tree
//       }

//       if (typeof value === "number")
//         return [{ path, type: "number", availableOps: NUMBER_OPS }];
//       if (typeof value === "boolean")
//         return [{ path, type: "boolean", availableOps: BOOL_OPS }];
//       if (typeof value === "string") {
//         return [
//           {
//             path,
//             type: "string",
//             availableOps: looksLikeDate(value) ? DATE_STRING_OPS : STRING_OPS
//           }
//         ];
//       }
//       if (value === null) {
//         return [{ path, type: "null", availableOps: NULL_OPS }];
//       }
//       return [];
//     }
//   );
// }

// function looksLikeDate(value: string): boolean {
//   // cheap, deliberately conservative: ISO-ish shape, not just "anything Date.parse accepts"
//   // (Date.parse is notoriously loose — e.g. it'll happily parse "10" or "October")
//   return (
//     /^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2})?/.test(value) &&
//     !isNaN(Date.parse(value))
//   );
// }

const MAX_ARRAY_ELEMENTS = 20;

const isObject = (v: unknown): v is Record<string, unknown> =>
  typeof v === "object" && v !== null && !Array.isArray(v);

export function discoverFields(samples: unknown[]): DiscoveredField[] {
  const found = new Map<string, DiscoveredField>();
  for (const s of samples) collect(s, "", [], found);

  const all = [...found.values()];
  // drop "null" entries that turned out to be objects/arrays in another message
  return all.filter(
    (f) => !(f.type === "null" && all.some((o) => o.path.startsWith(f.path + ".")))
  );
}

function collect(
  node: unknown,
  path: string,
  arrays: string[],
  found: Map<string, DiscoveredField>
) {
  if (Array.isArray(node)) {
    const objects = node.filter(isObject);
    if (objects.length === 0) {
      if (path && node.length > 0) record(found, path, "array-of-scalars", arrays);
      return;
    }
    record(found, path, "array-of-objects", arrays);
    for (const el of objects.slice(0, MAX_ARRAY_ELEMENTS))
      collectObject(el, path, [...arrays, path], found);
    return;
  }
  if (isObject(node)) return collectObject(node, path, arrays, found);
  if (!path) return;

  if (node === null) record(found, path, "null", arrays);
  else if (typeof node === "number") record(found, path, "number", arrays);
  else if (typeof node === "boolean") record(found, path, "boolean", arrays);
  else if (typeof node === "string") record(found, path, "string", arrays, node);
}

function collectObject(
  obj: Record<string, unknown>,
  path: string,
  arrays: string[],
  found: Map<string, DiscoveredField>
) {
  for (const [key, value] of Object.entries(obj))
    collect(value, path ? `${path}.${key}` : key, arrays, found);
}

function record(
  found: Map<string, DiscoveredField>,
  path: string,
  type: FieldType,
  arrays: string[],
  stringValue?: string
) {
  const existing = found.get(path);
  if (existing && !(existing.type === "null" && type !== "null")) return; // first real type wins

  found.set(path, {
    path,
    type,
    arrayPaths: arrays,
    availableOps: opsFor(type, stringValue)
  });
}

function opsFor(type: FieldType, stringValue?: string): CompareOp[] {
  switch (type) {
    case "number":
      return NUMBER_OPS;
    case "boolean":
      return BOOL_OPS;
    case "null":
      return NULL_OPS;
    case "string":
      return stringValue !== undefined && looksLikeDate(stringValue)
        ? DATE_STRING_OPS
        : STRING_OPS;
    case "array-of-scalars":
      return STRING_OPS;
    case "array-of-objects":
      return []; // not filterable directly, reached through an `any` node
  }
}

function looksLikeDate(value: string): boolean {
  // cheap, deliberately conservative: ISO-ish shape, not just "anything Date.parse accepts"
  // (Date.parse is notoriously loose — e.g. it'll happily parse "10" or "October")
  return (
    /^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2})?/.test(value) &&
    !isNaN(Date.parse(value))
  );
}

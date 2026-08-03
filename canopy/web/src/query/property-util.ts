/*
 * Licensed to Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Apache Software Foundation (ASF) licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// property-util.ts — small utils ported from the handoff's data.jsx (PROP_VALUE_TYPES /
// PROP_VALUE_LABEL) and code.jsx (looksLikeJSON / startsLikeJSON / tryFormat).
//
// DEVIATION FROM THE HANDOFF/DESIGN: docs/property-design.md §7 lists a tag
// value type dropdown of string/int/float/binary/timestamp, but
// model.v1.TagValue (the real wire type for property tags, see
// api/proto/banyandb/model/v1/common.proto) has NO float variant — its oneof
// is null/str/str_array/int/int_array/binary_data/timestamp. There is no
// server-side representation for "float" tags, so it is omitted here; the
// mock's own PROP_VALUE_TYPES (str/int/str_array) undersold the real
// wire surface, so this list adds int_array/binary/timestamp to match what
// TagValue actually supports.
export interface PropValueTypeDef {
  readonly value: 'str' | 'int' | 'str_array' | 'int_array' | 'binary' | 'timestamp';
  readonly label: string;
}

export const PROP_VALUE_TYPES: readonly PropValueTypeDef[] = [
  { value: 'str', label: 'string' },
  { value: 'int', label: 'int' },
  { value: 'str_array', label: 'string[]' },
  { value: 'int_array', label: 'int[]' },
  { value: 'binary', label: 'binary (base64)' },
  { value: 'timestamp', label: 'timestamp' },
];

export const PROP_VALUE_LABEL = (v: string): string =>
  PROP_VALUE_TYPES.find((t) => t.value === v)?.label ?? 'string';

/** Does this string look like a JSON document/array (and actually parse)? */
export function looksLikeJSON(s: string): boolean {
  const t = (s ?? '').trim();
  if (!(t.startsWith('{') || t.startsWith('['))) return false;
  try {
    JSON.parse(t);
    return true;
  } catch {
    return false;
  }
}

/** Looser than looksLikeJSON: starts like JSON even if not yet valid (live-editing affordance). */
export function startsLikeJSON(s: string): boolean {
  const t = (s ?? '').trim();
  return t.startsWith('{') || t.startsWith('[');
}

/** Pretty-print a JSON string; returns the input unchanged if it doesn't parse. */
export function tryFormat(s: string): string {
  try {
    return JSON.stringify(JSON.parse(s), null, 2);
  } catch {
    return s;
  }
}

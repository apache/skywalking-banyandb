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

import CodeMirror from 'codemirror';

// BydbQL keywords
const BYDBQL_KEYWORDS = [
  'SELECT',
  'FROM',
  'WHERE',
  'ORDER BY',
  'LIMIT',
  'OFFSET',
  'AND',
  'OR',
  'NOT',
  'IN',
  'LIKE',
  'BETWEEN',
  'IS',
  'NULL',
  'TRUE',
  'FALSE',
  'AS',
  'DISTINCT',
  'GROUP BY',
  'HAVING',
  'TIME',
  'ASC',
  'DESC',
];

// BydbQL entity types
const ENTITY_TYPES = ['STREAM', 'MEASURE', 'TRACE', 'PROPERTY', 'TOPN'];

// Store for schema data
let schemaData = {
  groups: [],
  schemas: {},
};

// Function to update schema data
export function updateSchemaData(groups, schemas) {
  schemaData.groups = groups || [];
  schemaData.schemas = schemas || {};
}

// Function to get word at cursor
function getWordAt(cm, pos) {
  const line = cm.getLine(pos.line);
  const start = pos.ch;
  const end = pos.ch;

  let wordStart = start;
  let wordEnd = end;

  // Find word boundaries
  while (wordStart > 0 && /\w/.test(line.charAt(wordStart - 1))) {
    wordStart--;
  }
  while (wordEnd < line.length && /\w/.test(line.charAt(wordEnd))) {
    wordEnd++;
  }

  return {
    word: line.slice(wordStart, wordEnd),
    start: wordStart,
    end: wordEnd,
  };
}

// Analyze the query context to determine what to suggest
function getQueryContext(cm, cursor) {
  const currentLine = cm.getLine(cursor.line);
  const textBeforeCursor = cm.getRange({ line: 0, ch: 0 }, cursor);
  const upperText = textBeforeCursor.toUpperCase();

  // Check if we're after FROM
  if (/\bFROM\s+$/i.test(textBeforeCursor)) {
    return { type: 'entity_type' };
  }

  // Check if we're after entity type (STREAM, MEASURE, etc.)
  const entityMatch = textBeforeCursor.match(/\bFROM\s+(STREAM|MEASURE|TRACE|PROPERTY|TOPN)\s+$/i);
  if (entityMatch) {
    return { type: 'schema', entityType: entityMatch[1].toLowerCase() };
  }

  // Check if we're after schema name expecting 'in'
  const schemaMatch = textBeforeCursor.match(/\bFROM\s+(STREAM|MEASURE|TRACE|PROPERTY|TOPN)\s+\w+\s+$/i);
  if (schemaMatch) {
    return { type: 'in_keyword' };
  }

  // Check if we're after 'in' expecting group name
  const inMatch = textBeforeCursor.match(/\bFROM\s+(STREAM|MEASURE|TRACE|PROPERTY|TOPN)\s+\w+\s+in\s+$/i);
  if (inMatch) {
    return { type: 'group', entityType: inMatch[1].toLowerCase() };
  }

  // Default: suggest keywords
  return { type: 'keyword' };
}

// Generate hint list based on context
function generateHints(context, word) {
  const hints = [];
  const lowerWord = word.toLowerCase();

  switch (context.type) {
    case 'entity_type':
      // Suggest entity types (STREAM, MEASURE, etc.)
      ENTITY_TYPES.forEach((type) => {
        if (type.toLowerCase().startsWith(lowerWord)) {
          hints.push({
            text: type,
            displayText: type,
            className: 'bydbql-hint-entity-type',
          });
        }
      });
      break;

    case 'schema':
      // Suggest schema names for the given entity type
      const schemas = schemaData.schemas[context.entityType] || [];
      schemas.forEach((schema) => {
        if (schema.toLowerCase().startsWith(lowerWord)) {
          hints.push({
            text: schema,
            displayText: schema,
            className: 'bydbql-hint-schema',
          });
        }
      });
      break;

    case 'in_keyword':
      // Suggest 'in' keyword
      if ('in'.startsWith(lowerWord)) {
        hints.push({
          text: 'in',
          displayText: 'in',
          className: 'bydbql-hint-keyword',
        });
      }
      break;

    case 'group':
      // Suggest group names
      schemaData.groups.forEach((group) => {
        if (group.toLowerCase().startsWith(lowerWord)) {
          hints.push({
            text: group,
            displayText: group,
            className: 'bydbql-hint-group',
          });
        }
      });
      break;

    case 'keyword':
    default:
      // Suggest keywords
      BYDBQL_KEYWORDS.forEach((keyword) => {
        if (keyword.toLowerCase().startsWith(lowerWord)) {
          hints.push({
            text: keyword,
            displayText: keyword,
            className: 'bydbql-hint-keyword',
          });
        }
      });

      // Also suggest entity types
      ENTITY_TYPES.forEach((type) => {
        if (type.toLowerCase().startsWith(lowerWord)) {
          hints.push({
            text: type,
            displayText: type,
            className: 'bydbql-hint-entity-type',
          });
        }
      });
      break;
  }

  return hints;
}

// Main hint function
CodeMirror.registerHelper('hint', 'bydbql', function (cm) {
  const cursor = cm.getCursor();
  const { word, start, end } = getWordAt(cm, cursor);
  const context = getQueryContext(cm, cursor);
  const hints = generateHints(context, word);

  if (hints.length === 0) {
    return null;
  }

  return {
    list: hints,
    from: CodeMirror.Pos(cursor.line, start),
    to: CodeMirror.Pos(cursor.line, end),
  };
});


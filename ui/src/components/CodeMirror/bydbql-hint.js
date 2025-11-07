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
import { SupportedIndexRuleTypes } from '../common/data';

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
  'SHOW',
  'TOP',
  'UNION',
  'INTERSECT',
  'EXCEPT',
  'AGGREGATE BY',
  'WITH QUERY_TRACE',
  'ON',
  'STAGES',
  'GROUP',
  'BY',
  'MATCH',
  'SUM',
  'MEAN',
  'AVG',
  'COUNT',
  'MAX',
  'MIN',
  'TAG',
];

// BydbQL entity types
const ENTITY_TYPES = ['STREAM', 'MEASURE', 'TRACE', 'PROPERTY', 'TOPN'];
let schemasAndGroups = {
  groups: [],
  schemas: {},
  schemaToGroups: {},
  indexRulesByType: {},
  indexRulesByGroup: {},
};

export function updateSchemasAndGroups(groups, schemas, schemaToGroups, indexRuleData = {}) {
  schemasAndGroups.groups = groups || [];
  schemasAndGroups.schemas = schemas || {};
  schemasAndGroups.schemaToGroups = schemaToGroups || {};

  const { indexRuleSchemas = {}, indexRuleGroups = {}, indexRuleNameLookup = {} } = indexRuleData;

  const indexRulesByType = {};
  for (const [type, rules] of Object.entries(indexRuleSchemas)) {
    const normalizedType = typeof type === 'string' ? type.toLowerCase() : type;
    const uniqueRules = Array.from(new Set((rules || []).filter((rule) => typeof rule === 'string')));
    indexRulesByType[normalizedType] = uniqueRules.sort((a, b) => a.localeCompare(b));
  }

  const indexRulesByGroup = {};
  for (const [type, ruleMap] of Object.entries(indexRuleGroups)) {
    const normalizedType = typeof type === 'string' ? type.toLowerCase() : type;
    const typeNameLookup = indexRuleNameLookup?.[type] || indexRuleNameLookup?.[normalizedType] || {};
    const groupRuleSets = {};

    for (const [ruleKey, groupList] of Object.entries(ruleMap || {})) {
      const displayName = typeNameLookup[ruleKey] || ruleKey;
      (groupList || []).forEach((group) => {
        const normalizedGroup = typeof group === 'string' ? group.toLowerCase() : group;
        if (!normalizedGroup) {
          return;
        }
        if (!groupRuleSets[normalizedGroup]) {
          groupRuleSets[normalizedGroup] = new Set();
        }
        groupRuleSets[normalizedGroup].add(displayName);
      });
    }

    indexRulesByGroup[normalizedType] = Object.fromEntries(
      Object.entries(groupRuleSets).map(([groupKey, ruleSet]) => [
        groupKey,
        [...ruleSet].sort((a, b) => a.localeCompare(b)),
      ]),
    );
  }

  schemasAndGroups.indexRulesByType = indexRulesByType;
  schemasAndGroups.indexRulesByGroup = indexRulesByGroup;
}

function getWordAt(cm, pos) {
  const line = cm.getLine(pos.line);

  let wordStart = pos.ch;
  let wordEnd = pos.ch;
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

function extractFromClauseContext(text) {
  const fromClauseRegex =
    /\bFROM\s+(STREAM|MEASURE|TRACE|PROPERTY|TOPN)\s+(\w+)(?:\s+IN\s+([\s\S]*?)(?=\bWHERE\b|\bORDER\b|\bGROUP\b|\bTIME\b|\bLIMIT\b|\bOFFSET\b|\bWITH\b|\bHAVING\b|\bON\b|$))?/gi;
  let match;
  let lastContext = null;

  while ((match = fromClauseRegex.exec(text)) !== null) {
    const entityType = match[1]?.toLowerCase();
    const schemaName = match[2];
    const rawGroups = match[3] || '';

    const groups = rawGroups
      .replace(/[()]/g, ' ')
      .split(',')
      .map((group) => group.trim())
      .filter(Boolean);

    lastContext = {
      entityType,
      schemaName,
      groups,
    };
  }

  return lastContext;
}

function getQueryContext(cm, cursor) {
  const textBeforeCursor = cm.getRange({ line: 0, ch: 0 }, cursor);

  const orderByRegex = /\bORDER\s+BY\b[\s\w.,-]*$/i;
  const orderByMatch = orderByRegex.exec(textBeforeCursor);
  if (orderByMatch) {
    const textBeforeOrderBy = textBeforeCursor.slice(0, orderByMatch.index);
    const fromContext = extractFromClauseContext(textBeforeOrderBy);
    if (fromContext && SupportedIndexRuleTypes.includes(fromContext.entityType)) {
      return {
        type: 'entity_order_by',
        entityType: fromContext.entityType,
        groupNames: fromContext.groups || [],
      };
    }
    return { type: 'order_by' };
  }

  // Check if we're typing after 'in' (group name context)
  const inMatch = textBeforeCursor.match(/\bFROM\s+(STREAM|MEASURE|TRACE|PROPERTY|TOPN)\s+(\w+)\s+in\s+(\w*)$/i);
  if (inMatch) {
    return { type: 'group', entityType: inMatch[1].toLowerCase(), schemaName: inMatch[2] };
  }

  // Check if we're typing after schema name (expecting 'in')
  const schemaMatch = textBeforeCursor.match(/\bFROM\s+(STREAM|MEASURE|TRACE|PROPERTY|TOPN)\s+(\w+)\s+(\w*)$/i);
  if (schemaMatch) {
    return { type: 'in_keyword' };
  }

  // Check if we're typing after entity type (schema name context)
  const entityMatch = textBeforeCursor.match(/\bFROM\s+(STREAM|MEASURE|TRACE|PROPERTY|TOPN)\s+(\w*)$/i);
  if (entityMatch) {
    return { type: 'schema', entityType: entityMatch[1].toLowerCase() };
  }

  // Check if we're typing after FROM (entity type context)
  if (/\bFROM\s+(\w*)$/i.test(textBeforeCursor)) {
    return { type: 'entity_type' };
  }

  // Default: suggest keywords
  return { type: 'keyword' };
}

// Generate hint list based on context
function generateHints(context, word) {
  const hints = [];
  const lowerWord = word ? word.toLowerCase() : '';

  switch (context.type) {
    case 'entity_type':
      // Suggest entity types (STREAM, MEASURE, etc.)
      for (const type of ENTITY_TYPES) {
        if (!lowerWord || type.toLowerCase().startsWith(lowerWord)) {
          hints.push({
            text: type,
            displayText: type,
            className: 'bydbql-hint-entity-type',
          });
        }
      }
      break;

    case 'schema':
      // Suggest schema names for the given entity type
      const schemas = schemasAndGroups.schemas[context.entityType] || [];
      for (const schema of schemas) {
        if (!lowerWord || schema.toLowerCase().startsWith(lowerWord)) {
          hints.push({
            text: schema,
            displayText: schema,
            className: 'bydbql-hint-schema',
          });
        }
      }
      break;

    case 'in_keyword':
      // Suggest 'in' keyword
      if (!lowerWord || 'in'.startsWith(lowerWord)) {
        hints.push({
          text: 'in',
          displayText: 'in',
          className: 'bydbql-hint-keyword',
        });
      }
      break;

    case 'group': {
      const schemaKey = context.schemaName ? context.schemaName.toLowerCase() : '';
      const schemaGroupsMap = schemasAndGroups.schemaToGroups[context.entityType] || {};
      const relatedGroups = schemaKey ? schemaGroupsMap[schemaKey] || [] : [];
      const targetGroups = relatedGroups.length > 0 ? relatedGroups : schemasAndGroups.groups;

      for (const group of targetGroups) {
        if (!lowerWord || group.toLowerCase().startsWith(lowerWord)) {
          hints.push({
            text: group,
            displayText: group,
            className: 'bydbql-hint-group',
          });
        }
      }
      break;
    }

    case 'entity_order_by': {
      const entityType = context.entityType || '';
      const normalizedGroups = (context.groupNames || []).map((group) => group.toLowerCase());
      const indexRulesByGroup = schemasAndGroups.indexRulesByGroup?.[entityType] || {};
      const aggregatedRuleSet = new Set();

      for (const group of normalizedGroups) {
        const rules = indexRulesByGroup[group] || [];
        for (const rule of rules) {
          aggregatedRuleSet.add(rule);
        }
      }

      const fallbackRules = schemasAndGroups.indexRulesByType?.[entityType] || [];
      const candidates = aggregatedRuleSet.size > 0
        ? [...aggregatedRuleSet].sort((a, b) => a.localeCompare(b))
        : fallbackRules.slice();

      for (const rule of candidates) {
        if (!lowerWord || rule.toLowerCase().startsWith(lowerWord)) {
          hints.push({
            text: rule,
            displayText: rule,
            className: 'bydbql-hint-index-rule',
          });
        }
      }

      if (!lowerWord || 'ASC'.toLowerCase().startsWith(lowerWord)) {
        hints.push({
          text: 'ASC',
          displayText: 'ASC',
          className: 'bydbql-hint-keyword',
        });
      }
      if (!lowerWord || 'DESC'.toLowerCase().startsWith(lowerWord)) {
        hints.push({
          text: 'DESC',
          displayText: 'DESC',
          className: 'bydbql-hint-keyword',
        });
      }
      break;
    }

    case 'order_by': {
      const orderKeywords = ['ASC', 'DESC'];
      for (const keyword of orderKeywords) {
        if (!lowerWord || keyword.toLowerCase().startsWith(lowerWord)) {
          hints.push({
            text: keyword,
            displayText: keyword,
            className: 'bydbql-hint-keyword',
          });
        }
      }
      break;
    }

    case 'keyword':
    default:
      for (const keyword of BYDBQL_KEYWORDS) {
        if (!lowerWord || keyword.toLowerCase().startsWith(lowerWord)) {
          hints.push({
            text: keyword,
            displayText: keyword,
            className: 'bydbql-hint-keyword',
          });
        }
      }

      for (const type of ENTITY_TYPES) {
        if (!lowerWord || type.toLowerCase().startsWith(lowerWord)) {
          hints.push({
            text: type,
            displayText: type,
            className: 'bydbql-hint-entity-type',
          });
        }
      }
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

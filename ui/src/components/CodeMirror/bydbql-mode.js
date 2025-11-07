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

// Define BydbQL mode extending SQL
CodeMirror.defineMode('bydbql', function (config) {
  const sqlMode = CodeMirror.getMode(config, 'text/x-sql');

  const entityTypes = {
    STREAM: true,
    MEASURE: true,
    TRACE: true,
    PROPERTY: true,
    TOPN: true,
  };

  // BydbQL-specific keywords
  const bydbqlKeywords = {
    SELECT: true,
    FROM: true,
    WHERE: true,
    ORDER: true,
    BY: true,
    ASC: true,
    DESC: true,
    LIMIT: true,
    OFFSET: true,
    AND: true,
    OR: true,
    NOT: true,
    IN: true,
    LIKE: true,
    BETWEEN: true,
    IS: true,
    NULL: true,
    TRUE: true,
    FALSE: true,
    AS: true,
    DISTINCT: true,
    ALL: true,
    ANY: true,
    SOME: true,
    EXISTS: true,
    CASE: true,
    WHEN: true,
    THEN: true,
    ELSE: true,
    END: true,
    UNION: true,
    INTERSECT: true,
    EXCEPT: true,
    GROUP: true,
    HAVING: true,
    TIME: true,
    SHOW: true,
    TOP: true,
    AGGREGATE: true,
    BY: true,
    IN: true,
    NOT: true,
    MATCH: true,
    SUM: true,
    MEAN: true,
    AVG: true,
    COUNT: true,
    MAX: true,
    MIN: true,
    TAG: true,
  };

  const isWord = (value) => /^[A-Za-z_]\w*$/.test(value);

  return {
    startState: function () {
      return {
        sqlState: CodeMirror.startState(sqlMode),
      };
    },

    copyState: function (state) {
      return {
        sqlState: CodeMirror.copyState(sqlMode, state.sqlState),
      };
    },

    token: function (stream, state) {
      const style = sqlMode.token(stream, state.sqlState);
      if (style === 'comment' || style === 'string') {
        return style;
      }

      const current = stream.current();
      if (!isWord(current)) {
        return style;
      }

      const upperWord = current.toUpperCase();
      if (entityTypes[upperWord]) {
        return 'entity-type';
      }
      if (bydbqlKeywords[upperWord]) {
        return 'keyword';
      }

      return 'variable-2';
    },

    indent: function (state, textAfter) {
      return sqlMode.indent ? sqlMode.indent(state.sqlState, textAfter) : CodeMirror.Pass;
    },

    electricChars: sqlMode.electricChars,
    blockCommentStart: '/*',
    blockCommentEnd: '*/',
    lineComment: '--',
  };
});

// Set MIME type for BydbQL
CodeMirror.defineMIME('text/x-bydbql', 'bydbql');

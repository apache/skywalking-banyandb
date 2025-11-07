<!--
  ~ Licensed to Apache Software Foundation (ASF) under one or more contributor
  ~ license agreements. See the NOTICE file distributed with
  ~ this work for additional information regarding copyright
  ~ ownership. Apache Software Foundation (ASF) licenses this file to you under
  ~ the Apache License, Version 2.0 (the "License"); you may
  ~ not use this file except in compliance with the License.
  ~ You may obtain a copy of the License at
  ~
  ~     http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

<template>
  <div class="in-coder-panel">
    <textarea ref="textarea" v-model="code"></textarea>
  </div>
</template>

<script>
  import { onMounted, ref, watch } from 'vue';
  import CodeMirror from 'codemirror';
  import 'codemirror/lib/codemirror.css';
  import 'codemirror/mode/yaml/yaml.js';
  import 'codemirror/mode/sql/sql.js';
  import 'codemirror/mode/css/css.js';
  import 'codemirror/addon/lint/yaml-lint.js';
  import 'codemirror/theme/dracula.css';
  import './bydbql-mode.js';
  import './bydbql-hint.js';
  import jsYaml from 'js-yaml';
  window.jsyaml = jsYaml;
  export default {
    components: {},
    props: {
      modelValue: {
        type: String,
        default: ``,
      },
      mode: {
        type: String,
        default: 'yaml',
      },
      lint: {
        type: Boolean,
        default: true,
      },
      readonly: {
        type: Boolean,
        default: false,
      },
      theme: {
        type: String,
        default: 'dracula',
      },
      styleActiveLine: {
        type: Boolean,
        default: true,
      },
      autoRefresh: {
        type: Boolean,
        default: true,
      },
      enableHint: {
        type: Boolean,
        default: false,
      },
      extraKeys: {
        type: Object,
        default: () => ({}),
      },
    },
    emits: ['update:modelValue', 'ready'],
    setup(props, { emit }) {
      const textarea = ref(null);
      const code = ref(props.modelValue);
      let coder;
      watch(
        () => props.modelValue,
        (val) => {
          const currentValue = coder?.getValue();
          if (val !== currentValue) {
            coder?.setValue(val);
          }
        },
      );

      // Get mode based on prop
      const getModeString = () => {
        if (props.mode === 'bydbql') {
          return 'text/x-bydbql';
        }
        if (props.mode === 'yaml') {
          return 'text/x-yaml';
        }
        if (props.mode === 'css') {
          return 'text/css';
        }
        return 'text/x-yaml';
      };

      const options = {
        mode: getModeString(),
        tabSize: 2,
        theme: props.theme,
        lineNumbers: true,
        line: true,
        readOnly: props.readonly,
        lint: props.lint,
        gutters: props.lint ? ['CodeMirror-lint-markers'] : [],
        styleActiveLine: props.styleActiveLine,
        autoRefresh: props.autoRefresh,
        height: '500px',
        extraKeys: props.extraKeys,
      };

      const initialize = async () => {
        try {
          if (props.lint) {
            await import('codemirror/addon/lint/lint.js');
            await import('codemirror/addon/lint/lint.css');
          }
          if (props.autoRefresh) {
            await import('codemirror/addon/display/autorefresh');
          }
          if (props.styleActiveLine) {
            await import('codemirror/addon/selection/active-line');
          }
          if (props.enableHint) {
            await import('codemirror/addon/hint/show-hint.js');
            await import('codemirror/addon/hint/show-hint.css');
          }
        } catch (e) {
          console.error('Error loading CodeMirror addons:', e);
        }

        coder = CodeMirror.fromTextArea(textarea.value, options);

        coder.on('blur', (coder) => {
          const newValue = coder.getValue();
          emit('update:modelValue', newValue);
        });

        // Enable automatic autocomplete when typing (on keyup events)
        if (props.enableHint) {
          coder.on('keyup', (cm, event) => {
            // Don't show hints for special keys
            const excludedKeys = [
              8, // Backspace
              9, // Tab
              13, // Enter
              16,
              17,
              18, // Shift, Ctrl, Alt
              20, // Caps Lock
              27, // Escape
              33,
              34,
              35,
              36,
              37,
              38,
              39,
              40, // Page/Arrow keys
            ];

            if (!cm.state.completionActive && !excludedKeys.includes(event.keyCode)) {
              CodeMirror.commands.autocomplete(cm, CodeMirror.hint.bydbql, { completeSingle: false });
            }
          });
        }

        // Emit ready event with coder instance
        emit('ready', coder);
      };

      onMounted(() => {
        initialize();
      });

      const checkYaml = async (val) => {
        jsYaml.load(val);
      };

      return {
        code,
        options,
        textarea,
        checkYaml,
      };
    },
  };
</script>

<style lang="scss" scoped>
  .in-coder-panel {
    width: 100%;
    height: 100%;
    :deep(.CodeMirror) {
      border: 1px solid #44475a;
      height: 100%;
      width: 100%;
      .CodeMirror-code {
        line-height: 20px;
      }
    }
    :deep(.cm-entity-type) {
      color: #bd93f9;
    }
  }
</style>

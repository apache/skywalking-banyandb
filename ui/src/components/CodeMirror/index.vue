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
import { onMounted, ref, watch } from 'vue'
import CodeMirror from 'codemirror'
import 'codemirror/lib/codemirror.css'
import 'codemirror/mode/yaml/yaml.js'
import 'codemirror/mode/css/css.js'
import 'codemirror/addon/lint/yaml-lint.js'
import 'codemirror/theme/rubyblue.css'
import jsYaml from 'js-yaml'
window.jsyaml = jsYaml
export default {
  components: {},
  props: {
    modelValue: {
      type: String,
      default: ``
    },
    mode: {
      type: String,
      default: 'yaml'
    },
    lint: {
      type: Boolean,
      default: true
    },
    readonly: {
      type: Boolean,
      default: false
    },  
    theme: {
      type: String,
      default: 'rubyblue'
    },
    styleActiveLine: {
      type: Boolean,
      default: true
    },
    autoRefresh: {
      type: Boolean,
      default: true
    }
  },
  emits: ['update:modelValue'],
  setup(props, { emit }) {
    const textarea = ref(null)
    const code = ref(props.modelValue)
    let coder
    watch(
      () => props.modelValue,
      val => {
        coder?.setValue(val)
      }
    )
    const options = {
      mode:  'text/x-yaml',
      tabSize: 2,
      theme: props.theme,
      lineNumbers: true,
      line: true,
      readOnly: props.readonly,
      lint: props.lint,
      gutters: ['CodeMirror-lint-markers'],
      styleActiveLine: props.styleActiveLine,
      autoRefresh: props.autoRefresh,
      height: '500px'
    }
    const initialize = async () => {
      try {
       /*  let theme = `codemirror/theme/${props.theme}.css`
        await import(theme) */
        if (props.lint) {
          await import('codemirror/addon/lint/lint.js')
          await import('codemirror/addon/lint/lint.css')
        }
        /* if (props.mode) {
          await import(`codemirror/mode/${props.mode}/${props.mode}.js`)
        } */
        if (props.autoRefresh) {
          await import('codemirror/addon/display/autorefresh')
        }
        if (props.styleActiveLine) {
          await import('codemirror/addon/selection/active-line')
        }
      } catch (e) {
      }
      coder = CodeMirror.fromTextArea(textarea.value, options)
      coder.on('blur', coder => {
        const newValue = coder.getValue()
        emit('update:modelValue', newValue)
      })
    }
    onMounted(() => {
      initialize()
    })
    const checkYaml = async val => {
      jsYaml.load(val)
    }
    return {
      code,
      options,
      textarea,
      checkYaml
    }
  }
}
</script>

<style lang="scss" scoped>
.in-coder-panel {
  width: 100%;
  height: 100%;
  :deep(.CodeMirror) {
    border: 1px solid #eee;
    height: 100%;
    width: 100%;
    .CodeMirror-code {
      line-height: 19px;
    }
  }
}
</style>
<style>
.CodeMirror-lint-tooltip {
  z-index: 10000 !important;
}
</style>


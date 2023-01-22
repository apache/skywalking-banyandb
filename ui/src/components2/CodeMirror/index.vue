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
import 'codemirror/mode/javascript/javascript.js'
import 'codemirror/mode/yaml/yaml.js'
import 'codemirror/mode/css/css.js'
import 'codemirror/addon/lint/yaml-lint.js'
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
      default: 'javascript'
    },
    lint: {
      // codemirror仅支持html、css、json、javascript、yaml这几种，请手动引入，且需要下载相关插件，具体插件参考源码(node_modules/codemirror/addon/lint/)或官方文档
      type: Boolean,
      default: false
    },
    readonly: {
      type: Boolean,
      default: false
    },
    // 主题
    theme: {
      type: String,
      default: 'base16-dark' // 编辑器主题色
    },
    // 高亮选中行
    styleActiveLine: {
      type: Boolean,
      default: true
    },
    // 自动刷新
    autoRefresh: {
      type: Boolean,
      default: true
    }
  },
  emits: ['update:modelValue'],
  setup(props, { emit }) {
    const textarea = ref(null)
    const code = ref(props.modelValue)
    // 编辑器实例
    let coder
    watch(
      () => props.modelValue,
      val => {
        coder?.setValue(val)
      }
    )
    const options = {
      mode: props.mode,
      // 缩进格式
      tabSize: 2,
      // 主题，对应主题库 JS 需要提前引入
      theme: props.theme,
      // 行号码
      lineNumbers: true,
      line: true,
      // extraKeys: {'Ctrl': 'autocomplete'},//自定义快捷键
      readOnly: props.readonly,
      lint: props.lint,
      gutters: ['CodeMirror-lint-markers'],
      // 光标背景行高亮
      styleActiveLine: props.styleActiveLine,
      // 自动刷新
      autoRefresh: props.autoRefresh,
      height: '500px'
    }
    const initialize = async () => {
      try {
      // 动态引入相关依赖
        await import(`codemirror/theme/${props.theme}.css`)
        if (props.lint) {
          await import('codemirror/addon/lint/lint.js')
          await import('codemirror/addon/lint/lint.css')
        }
        if (props.mode) {
          await import(`codemirror/mode/${props.mode}/${props.mode}.js`)
        }
        if (props.autoRefresh) {
          await import('codemirror/addon/display/autorefresh')
        }
        if (props.styleActiveLine) {
          await import('codemirror/addon/selection/active-line')
        }
      } catch (e) {
      }
      // 初始化编辑器实例，传入需要被实例化的文本域对象和默认配置
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


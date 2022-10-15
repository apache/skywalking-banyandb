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

import { createApp, nextTick } from 'vue'
import { createPinia } from 'pinia'

import App from './App.vue'
import router from './router'

import axios from 'axios'
import mitt from 'mitt'

import * as echarts from 'echarts/core'
import { BarChart } from 'echarts/charts'
import {
    TitleComponent,
    TooltipComponent,
    GridComponent,
    DatasetComponent,
    TransformComponent
} from 'echarts/components'
import { LabelLayout, UniversalTransition } from 'echarts/features'
import { CanvasRenderer } from 'echarts/renderers'

import { ElLoading, ElMessage } from 'element-plus'
import ElementPlus from 'element-plus'
import 'element-plus/dist/index.css'
import './styles/custom.scss'
import './styles/main.scss'
import * as ElIcon from '@element-plus/icons-vue'

const app = createApp(App)

for (let iconName in ElIcon){
    app.component(iconName, ElIcon[iconName])
}
echarts.use([
    TitleComponent,
    TooltipComponent,
    GridComponent,
    DatasetComponent,
    TransformComponent,
    BarChart,
    LabelLayout,
    UniversalTransition,
    CanvasRenderer
])
app.config.globalProperties.$http = axios
app.config.globalProperties.$loading = ElLoading
app.config.globalProperties.$loadingCreate = () => {
    app.config.globalProperties.instance = ElLoading.service({
        text: 'loading...',
        spinner: 'el-icon-loading',
        background: 'rgba(0, 0, 0, 0.8)',
    })
}
app.config.globalProperties.$loadingClose = () => {
    nextTick(() => {
        app.config.globalProperties.instance.close()
    })
}
app.config.globalProperties.$message = ElMessage
app.config.globalProperties.$message.error = (status, text) => {
    ElMessage({
        message: status + statusText,
        type: 'error',
    })
}
app.config.globalProperties.$message.errorNet = () => {
    ElMessage({
        message: 'Error: Please check the network connection!',
        type: 'error'
    })
}
app.config.globalProperties.$message.success = () => {
    ElMessage({
        message: "OK",
        type: 'success'
    })
}
app.config.globalProperties.mittBus = new mitt()
app.use(createPinia())
app.use(router)
app.use(ElementPlus)

app.mount('#app')
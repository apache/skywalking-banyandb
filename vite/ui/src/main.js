import { createApp } from 'vue'
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

const app = createApp(App)

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
app.$http = axios
app.$loading = ElLoading
app.$loading.create = () => {
    app.$loading.instance = ElLoading.service({
        text: 'loading...',
        spinner: 'el-icon-loading',
        background: 'rgba(0, 0, 0, 0.8)',
    })
}
app.$loading.close = () => {
    nextTick(() => {
        app.$loading.instance.close()
    })
}
app.$message = ElMessage
app.$message.error = (status, text) => {
    ElMessage({
        message: status + statusText,
        type: 'error',
    })
}
app.$message.errorNet = () => {
    ElMessage({
        message: 'Error: Please check the network connection!',
        type: 'error'
    })
}
app.$message.success = () => {
    ElMessage({
        message: "OK",
        type: 'success'
    })
}

app.use(createPinia())
app.use(router)
app.use(ElementPlus)

app.mount('#app')
app.config.globalProperties.mittBus = new mitt()
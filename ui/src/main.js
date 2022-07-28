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

import Vue from 'vue'
import axios from 'axios'
import { Button, Container, Header, Main, Aside, Menu, MenuItem, MenuItemGroup, Submenu, Image, Loading, Tooltip, Tag, Card, Drawer, MessageBox, Message, DatePicker, Input, Table, TableColumn, Pagination } from 'element-ui'
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
import App from './App.vue'
import router from './router'
import store from './store'
import './assets/custom.scss'
import './assets/main.scss'


Vue.config.productionTip = false

Vue.use(Button)
Vue.use(Container)
Vue.use(Header)
Vue.use(Main)
Vue.use(Aside)
Vue.use(Menu)
Vue.use(MenuItem)
Vue.use(MenuItemGroup)
Vue.use(Submenu)
Vue.use(Image)
Vue.use(Tooltip)
Vue.use(Tag)
Vue.use(Card)
Vue.use(Drawer)
Vue.use(DatePicker)
Vue.use(Input)
Vue.use(Table)
Vue.use(TableColumn)
Vue.use((Pagination))
Vue.prototype.$confirm = MessageBox.confirm
Vue.prototype.$loading = Loading
Vue.prototype.$message = Message
Vue.prototype.$message.error = (status, statusText) => {
  Message({
    message: status + statusText,
    type: 'error',
  })
}
Vue.prototype.$message.errorNet = () => {
  Message({
    message: 'Error: Please check the network connection!',
    type: 'error'
  })
}
Vue.prototype.$message.success = () => {
  Message({
    message: "OK",
    type: 'success'
  })
}
Vue.prototype.$loading.create = () => {
  Vue.prototype.$loading.instance = Loading.service({
    text: '拼命加载中',
    spinner: 'el-icon-loading',
    background: 'rgba(0, 0, 0, 0.8)',
  })
}
Vue.prototype.$loading.close = () => {
  Vue.nextTick(() => {
    Vue.prototype.$loading.instance.close()
  })
}
Vue.prototype.$bus = new Vue()

Vue.prototype.$http = axios

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

new Vue({
  router,
  store,
  render: h => h(App)
}).$mount('#app')

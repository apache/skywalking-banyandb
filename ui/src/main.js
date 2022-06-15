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
import './plugins/axios'
import { Button, Container, Header, Main, Aside, Menu, MenuItem, Image, Loading } from 'element-ui'
import App from './App.vue'
import router from './router'
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
Vue.use(Image)
Vue.prototype.$loading = Loading
Vue.prototype.$loading.create = () => {
  Vue.prototype.$loading.instance = Loading.service({
    text: '拼命加载中',
    spinner: 'el-icon-loading',
    background: 'rgba(0, 0, 0, 0.8)',
  })
}
Vue.prototype.$loading.close = () => {
  Vue.nextTick(() => {
    // 以服务的方式调用的 Loading 需要异步关闭
    Vue.prototype.$loading.instance.close()
  })
}


new Vue({
  router,
  render: h => h(App)
}).$mount('#app')

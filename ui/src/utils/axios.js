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

// axios response interceptors file

import axios from "axios"
import { ElMessage } from "element-plus"

const axiosService = axios.create({
    // baseURL: "http://34.92.85.178:18913",// process.env.VUE_APP_BASE_API,
    timeout: 30000
})

axiosService.interceptors.request.use(
    config => {
        /**
         * TODO
         * Configuration before request
         */
        // console.log(config)
        return config
    },
    error => {
        /**
         * TODO
         * do some error handling
         */
        console.log(error)
        return Promise.reject(error)
    }
)

// re request
function reRequest(err) {
    let againReq = new Promise((resolve) => {
        console.log('request:' + err.config.url + 'Request failed, re request');
        resolve();
    })
    return againReq.then(() => {
        return axiosService(err.config);
    })
}

// axios response interceptors
axiosService.interceptors.response.use(
    response => {
        const res = response.data
        /**
         * TODO
         * Data processing operation
         */
        console.log('response', response)
        if(response.status == 200) {
            return Promise.resolve(response)
        } else {
            return Promise.reject(response)
        }
    },
    error => {
        /**
         * TODO
         * do some error handling
         */
        console.log(error)
        console.log(error.message)
        const resErr = error.data
        console.log(resErr)
        
        let msg = error.data && error.data.message ? error.data.message : error.message
        ElMessage({
            message: msg,
            type: "error",
            duration: 3000
        })
        return Promise.reject(error)
    }
)

export default axiosService
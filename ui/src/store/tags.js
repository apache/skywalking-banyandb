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
import { Message } from "element-ui"

export default {
    state: {
        tagsList: [],
        currentMenu: null,
    },
    mutations: {
        selectMenu(state, val) {
            // console.log(val)
            if (state.tagsList.length == 9) {
                Message({
                    message: "Open up to 8 files at the same time! Please close some files and try again!",
                    type: "warning"
                })
            } else {
                state.currentMenu = val
                let result = state.tagsList.findIndex(item => item.metadata.group === val.metadata.group && item.metadata.type === val.metadata.type && item.metadata.name === val.metadata.name)
                result === -1 ? state.tagsList.push(val) : ''
                window.sessionStorage.setItem('tagsList', JSON.stringify(state.tagsList))
                window.sessionStorage.setItem('currentMenu', JSON.stringify(state.currentMenu))
            }
        },
        closeTag(state, val) {
            let result = state.tagsList.findIndex(item => item.metadata.group === val.metadata.group && item.metadata.type === val.metadata.type && item.metadata.name === val.metadata.name)
            result === -1 ? '' : state.tagsList.splice(result, 1)
            if (state.tagsList.length === 0) state.currentMenu = null
            window.sessionStorage.setItem('tagsList', JSON.stringify(state.tagsList))
            window.sessionStorage.setItem('currentMenu', JSON.stringify(state.currentMenu))
        },
        changeTag(state, val) {

        }
    }
}
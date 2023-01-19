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

import { defineStore } from "pinia"
import { ElMessage } from 'element-plus'

export default defineStore('tags', {
    state() {
        return {
            tagsList: [],
            currentMenu: null
        }
    },
    actions: {
        selectMenu(val) {
            let result = this.tagsList.findIndex(item => item.metadata.group === val.metadata.group && item.metadata.type === val.metadata.type && item.metadata.name === val.metadata.name)
            if (result === -1) {
                if (this.tagsList.length == 9) {
                    ElMessage({
                        message: "Open up to 9 files at the same time! Please close some files and try again!",
                        type: "warning"
                    })
                } else {
                    this.currentMenu = val
                    this.tagsList.push(val)
                    window.sessionStorage.setItem('tagsList', JSON.stringify(this.tagsList))
                    window.sessionStorage.setItem('currentMenu', JSON.stringify(this.currentMenu))
                }
            } else {
                this.currentMenu = val
                window.sessionStorage.setItem('tagsList', JSON.stringify(this.tagsList))
                window.sessionStorage.setItem('currentMenu', JSON.stringify(this.currentMenu))
            }
        },
        closeTag(val) {
            let result = this.tagsList.findIndex(item => item.metadata.group === val.metadata.group && item.metadata.type === val.metadata.type && item.metadata.name === val.metadata.name)
            result === -1 ? '' : this.tagsList.splice(result, 1)
            if (this.tagsList.length === 0) this.currentMenu = null
            window.sessionStorage.setItem('tagsList', JSON.stringify(this.tagsList))
            window.sessionStorage.setItem('currentMenu', JSON.stringify(this.currentMenu))
        },
        changeTag(val) {

        }
    }
})
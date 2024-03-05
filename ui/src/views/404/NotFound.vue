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

<script setup>
import { ElImage } from 'element-plus'
import userImg from '@/assets/banyan-db.svg'
</script>

<template>
    <div class="display-view">
        <div class="inner-component">
            <el-image :src="userImg" class="display-image" fit="fill">
                <div slot="error" class="image-slot">
                    <i class="el-icon-picture-outline"></i>
                </div>
            </el-image>
            <div class="display-message">
                <h1 class="message">404 Page Not Found</h1>
                <router-link to="/" class="go-home-link">Go Home</router-link>
            </div>
        </div>
    </div>
</template>

<style lang="scss" scoped>
.display-view {
    display: flex;
    justify-content: center;
    align-items: center;
    height: 100%;
}

.inner-component {
    display: flex;
    justify-content: center;
    align-items: center;
    gap: 2rem;
}

.display-image {
  height: 12rem;
}

.display-message {
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    font-family: var(--font-family-main);
}

.message {
    font-size: 2.5rem; 
}

.go-home-link {
    text-decoration: none;
    color: inherit;
    font-weight: bold;
    font-size: large;
    background-color: #ffffff;
    padding: 1rem;
    border-radius: 1rem;
    box-shadow: 0px 0px 10px rgba(0, 0, 0, 0.1);
    transition: color 0.3s, transform 0.3s;
}

.go-home-link:hover {
    color: #2c3e50;
    transform: scale(1.1);
}
</style>


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

const fs = require("fs");

const nvmVersion = fs.readFileSync(".node-version").toString().trim();
const desired = `v${nvmVersion}`;
const running = process.version;

if (!running.startsWith(desired)) {
  console.error(
    `You are running Node ${running} but version ${desired} is expected. ` +
      `Use nvm or another version manager to install ${desired}, and then activate it.`
  );
  process.exit(1);
}

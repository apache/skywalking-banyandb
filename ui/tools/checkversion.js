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

const path = require('path');
const fs = require('fs');
const jsonfile = require('jsonfile');
const packageJsonPath = path.join(process.cwd(), 'package.json')

fs.accessSync(packageJsonPath);
const engines = jsonfile.readFileSync(packageJsonPath).engines;
if (!engines) {
  console.error('✘ No engines found in package.json');
  process.exit(1); 
}
const nodeVersion = engines['node']
const desired = `v${nodeVersion}`
const running = process.version;

if (!running.startsWith(desired)) {
  console.error(
    `You are running Node ${running} but version ${desired} is expected. ` +
      `Use nvm or another version manager to install ${desired}, and then activate it.`
  );
  process.exit(1);
}

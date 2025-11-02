/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

export class mutationObserver {
  static mutationObserverMap = new Map();

  static create(key, callback) {
    const observer = new MutationObserver(callback);
    mutationObserver.mutationObserverMap.set(key, observer);
  }

  static observe(key, target, options) {
    const observer = mutationObserver.mutationObserverMap.get(key);
    if (observer) {
      observer.observe(target, options);
    }
  }

  static deleteObserve(key) {
    this.disconnect(key);
    this.mutationObserverMap.delete(key);
  }

  static disconnect(key) {
    const observer = this.mutationObserverMap.get(key);
    if (observer) {
      observer.disconnect();
    }
  }
}

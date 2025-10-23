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

import { ElNotification } from "element-plus";

export default (text) => {
  if (!text) {
    return;
  }
  // Clipboard functionality is restricted in production HTTP environments for security reasons.
  // In development, clipboard is allowed even over HTTP to ease testing.
  if (process.env.NODE_ENV === "production" && location.protocol === "http:") {
    ElNotification({
      title: "Warning",
      message: "Clipboard is not supported in HTTP environments",
      type: "warning",
    });
    return;
  }
  if (!navigator.clipboard) {
    ElNotification({
      title: "Warning",
      message: "Clipboard is not supported",
      type: "warning",
    });
    return;
  }
  navigator.clipboard
    .writeText(text)
    .then(() => {
      ElNotification({
        title: "Success",
        message: "Copied",
        type: "success",
      });
    })
    .catch((err) => {
      ElNotification({
        title: "Warning",
        message: err,
        type: "warning",
      });
    });
};

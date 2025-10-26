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
import { ref, computed } from 'vue';

export const adjustPercentValue = (value) => {
  if (value <= 0) {
    return 0;
  }
  if (value >= 100) {
    return 100;
  }
  return value;
};

const calculateX = ({ parentRect, x, opositeX, isSmallerThanOpositeX }) => {
  let value = ((x - parentRect.left) / (parentRect.right - parentRect.left)) * 100;
  if (isSmallerThanOpositeX) {
    if (value >= opositeX) {
      value = opositeX - 1;
    }
  } else if (value <= opositeX) {
    value = opositeX + 1;
  }
  return adjustPercentValue(value);
};

export const useRangeTimestampHandler = ({
  rootEl,
  minTimestamp,
  maxTimestamp,
  selectedTimestamp,
  isSmallerThanOpositeX,
  setTimestamp,
}) => {
  const currentX = ref(NaN);
  const mouseDownX = ref(NaN);
  const isDragging = ref(false);
  const selectedTimestampComputed = ref(selectedTimestamp);
  const opositeX = computed(() => {
    return ((selectedTimestampComputed.value - minTimestamp) / (maxTimestamp - minTimestamp)) * 100;
  });

  const onMouseMove = (e) => {
    if (!rootEl) {
      return;
    }
    const x = calculateX({
      parentRect: rootEl.getBoundingClientRect(),
      x: e.pageX,
      opositeX: opositeX.value,
      isSmallerThanOpositeX,
    });
    currentX.value = x;
  };

  const onMouseUp = (e) => {
    if (!rootEl) {
      return;
    }

    const x = calculateX({
      parentRect: rootEl.getBoundingClientRect(),
      x: e.pageX,
      opositeX: opositeX.value,
      isSmallerThanOpositeX,
    });
    const timestamp = (x / 100) * (maxTimestamp - minTimestamp) + minTimestamp;
    selectedTimestampComputed.value = timestamp;
    setTimestamp(timestamp);
    currentX.value = undefined;
    mouseDownX.value = undefined;
    isDragging.value = false;

    document.removeEventListener('mousemove', onMouseMove);
    document.removeEventListener('mouseup', onMouseUp);
  };

  const onMouseDown = (e) => {
    if (!rootEl) {
      return;
    }
    const x = calculateX({
      parentRect: rootEl.getBoundingClientRect(),
      x: e.currentTarget.getBoundingClientRect().x + 3,
      opositeX: opositeX.value,
      isSmallerThanOpositeX,
    });
    currentX.value = x;
    mouseDownX.value = x;
    isDragging.value = true;

    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
  };

  return { currentX, mouseDownX, onMouseDown, isDragging };
};

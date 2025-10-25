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

/* global Indexable */

export function buildSegmentForest(data, traceId) {
  const refSpans = [];
  const segmentGroup = {};
  const segmentIdGroup = [];
  const fixSpans = [];
  const segmentHeaders = [];

  if (!data || data.length === 0) {
    return { roots: [], fixSpansSize: 0, refSpans };
  }

  for (const span of data) {
    if (span.refs && span.refs.length) {
      refSpans.push(...span.refs);
    }
    if (span.parentSpanId === -1) {
      segmentHeaders.push(span);
    } else {
      const item = data.find(
        (i) => i.traceId === span.traceId && i.segmentId === span.segmentId && i.spanId === span.spanId - 1,
      );
      const content = fixSpans.find(
        (i) =>
          i.traceId === span.traceId &&
          i.segmentId === span.segmentId &&
          i.spanId === span.spanId - 1 &&
          i.parentSpanId === span.spanId - 2,
      );
      if (!item && !content) {
        fixSpans.push({
          traceId: span.traceId,
          segmentId: span.segmentId,
          spanId: span.spanId - 1,
          parentSpanId: span.spanId - 2,
          refs: [],
          endpointName: `VNode: ${span.segmentId}`,
          serviceCode: "VirtualNode",
          type: `[Broken] ${span.type}`,
          peer: "",
          component: `VirtualNode: #${span.spanId - 1}`,
          isError: true,
          isBroken: true,
          layer: "Broken",
          tags: [],
          logs: [],
          startTime: 0,
          endTime: 0,
        });
      }
    }
  }

  for (const span of segmentHeaders) {
    if (span.refs && span.refs.length) {
      let exit = null;
      for (const ref of span.refs) {
        const e = data.find(
          (i) =>
            ref.traceId === i.traceId && ref.parentSegmentId === i.segmentId && ref.parentSpanId === i.spanId,
        );
        if (e) {
          exit = e;
        }
      }
      if (!exit) {
        const ref = span.refs[0];
        const parentSpanId = ref.parentSpanId > -1 ? 0 : -1;
        const content = fixSpans.find(
          (i) =>
            i.traceId === ref.traceId &&
            i.segmentId === ref.parentSegmentId &&
            i.spanId === ref.parentSpanId &&
            i.parentSpanId === parentSpanId,
        );
        if (!content) {
          fixSpans.push({
            traceId: ref.traceId,
            segmentId: ref.parentSegmentId,
            spanId: ref.parentSpanId,
            parentSpanId,
            refs: [],
            endpointName: `VNode: ${ref.parentSegmentId}`,
            serviceCode: "VirtualNode",
            type: `[Broken] ${ref.type}`,
            peer: "",
            component: `VirtualNode: #${ref.parentSpanId}`,
            isError: true,
            isBroken: true,
            layer: "Broken",
            tags: [],
            logs: [],
            startTime: 0,
            endTime: 0,
          });
        }
        if (parentSpanId > -1) {
          const exists = fixSpans.find(
            (i) =>
              i.traceId === ref.traceId &&
              i.segmentId === ref.parentSegmentId &&
              i.spanId === 0 &&
              i.parentSpanId === -1,
          );
          if (!exists) {
            fixSpans.push({
              traceId: ref.traceId,
              segmentId: ref.parentSegmentId,
              spanId: 0,
              parentSpanId: -1,
              refs: [],
              endpointName: `VNode: ${ref.parentSegmentId}`,
              serviceCode: "VirtualNode",
              type: `[Broken] ${ref.type}`,
              peer: "",
              component: `VirtualNode: #0`,
              isError: true,
              isBroken: true,
              layer: "Broken",
              tags: [],
              logs: [],
              startTime: 0,
              endTime: 0,
            });
          }
        }
      }
    }
  }

  for (const i of [...fixSpans, ...data]) {
    i.label = i.endpointName || "no operation name";
    i.key = Math.random().toString(36).substring(2, 36);
    i.children = [];
    if (segmentGroup[i.segmentId]) {
      segmentGroup[i.segmentId].push(i);
    } else {
      segmentIdGroup.push(i.segmentId);
      segmentGroup[i.segmentId] = [i];
    }
  }

  for (const id of segmentIdGroup) {
    const currentSegment = segmentGroup[id].sort((a, b) => b.parentSpanId - a.parentSpanId);
    for (const s of currentSegment) {
      const index = currentSegment.findIndex((i) => i.spanId === s.parentSpanId);
      if (index > -1) {
        if (
          (currentSegment[index].isBroken && currentSegment[index].parentSpanId === -1) ||
          !currentSegment[index].isBroken
        ) {
          currentSegment[index].children?.push(s);
          currentSegment[index].children?.sort((a, b) => a.spanId - b.spanId);
        }
      }
      if (s.isBroken) {
        const children = data.filter(
          (span) =>
            !!span.refs?.find(
              (d) => d.traceId === s.traceId && d.parentSegmentId === s.segmentId && d.parentSpanId === s.spanId,
            ),
        );
        if (children.length) {
          s.children?.push(...children);
        }
      }
    }
    segmentGroup[id] = currentSegment[currentSegment.length - 1];
  }

  for (const id of segmentIdGroup) {
    for (const ref of segmentGroup[id].refs || []) {
      if (ref.traceId === traceId) {
        traverseTree(segmentGroup[ref.parentSegmentId], ref.parentSpanId, ref.parentSegmentId, segmentGroup[id]);
      }
    }
  }

  const roots = [];
  for (const i in segmentGroup) {
    let pushed = false;
    for (const ref of segmentGroup[i].refs || []) {
      if (!segmentGroup[ref.parentSegmentId]) {
        roots.push(segmentGroup[i]);
        pushed = true;
        break;
      }
    }
    if (
      !pushed &&
      (!segmentGroup[i].refs || segmentGroup[i].refs.length === 0) &&
      segmentGroup[i].parentSpanId === -1
    ) {
      roots.push(segmentGroup[i]);
    }
  }

  return { roots, fixSpansSize: fixSpans.length, refSpans };
}

export function convertTree(d, spans) {
  // Convert timestamps for current node
  d.endTime = new Date(d.endTime).getTime();
  d.startTime = new Date(d.startTime).getTime();
  d.duration = Number(d.duration);
  d.label = d.message;
  
  if (d.children && d.children.length > 0) {
    let dur = d.endTime - d.startTime;
    for (const i of d.children) {
      i.endTime = new Date(i.endTime).getTime();
      i.startTime = new Date(i.startTime).getTime();
      dur -= i.endTime - i.startTime;
    }
    d.dur = dur < 0 ? 0 : dur;
    for (const i of d.children) {
      convertTree(i, spans);
    }
  } else {
    d.dur = d.endTime - d.startTime;
  }
  
  return d;
}

function traverseTree(node, spanId, segmentId, data) {
  if (!node || node.isBroken) {
    return;
  }
  if (node.spanId === spanId && node.segmentId === segmentId) {
    node.children?.push(data);
    return;
  }
  for (const nodeItem of node.children || []) {
    traverseTree(nodeItem, spanId, segmentId, data);
  }
}

export function getAllNodes(tree) {
  const nodes = [];
  const stack = [tree];

  while (stack.length > 0) {
    const node = stack.pop();
    nodes.push(node);

    if (node?.children && node.children.length > 0) {
      for (let i = node.children.length - 1; i >= 0; i--) {
        stack.push(node.children[i]);
      }
    }
  }

  return nodes;
}


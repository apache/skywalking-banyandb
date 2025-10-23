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

export default class TraceUtil {
  static buildTraceDataList(data) {
    return Array.from(new Set(data.map((span) => span.serviceCode)));
  }

  static changeStatisticsTree(data) {
    const result = new Map();
    const traceTreeRef = this.changeTreeCore(data);
    traceTreeRef.segmentMap.forEach((span) => {
      const groupRef = span.endpointName + ":" + span.type;
      if (span.children && span.children.length > 0) {
        this.calculationChildren(span.children, result);
        this.collapse(span);
      }
      if (result.get(groupRef) === undefined) {
        result.set(groupRef, []);
        result.get(groupRef).push(span);
      } else {
        result.get(groupRef).push(span);
      }
    });
    return result;
  }

  static changeTreeCore(data) {
    // set a breakpoint at this line
    if (data.length === 0) {
      return {
        segmentMap: new Map(),
        segmentIdGroup: [],
      };
    }
    const segmentGroup = {};
    const segmentMap = new Map();
    const segmentIdGroup = [];
    const fixSpans = [];
    const segmentHeaders = [];
    data.forEach((span) => {
      if (span.parentSpanId === -1) {
        segmentHeaders.push(span);
      } else {
        const index = data.findIndex((patchSpan) => {
          return patchSpan.segmentId === span.segmentId && patchSpan.spanId === span.spanId - 1;
        });
        const content = fixSpans.find(
          (i) =>
            i.traceId === span.traceId &&
            i.segmentId === span.segmentId &&
            i.spanId === span.spanId - 1 &&
            i.parentSpanId === span.spanId - 2,
        );
        if (index === -1 && !content) {
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
    });
    segmentHeaders.forEach((span) => {
      if (span.refs && span.refs.length) {
        span.refs.forEach((ref) => {
          const index = data.findIndex((patchSpan) => {
            return ref.parentSegmentId === patchSpan.segmentId && ref.parentSpanId === patchSpan.spanId;
          });
          if (index === -1) {
            // create a known broken node.
            const parentSpanId = ref.parentSpanId > -1 ? 0 : -1;
            const item = fixSpans.find(
              (i) =>
                i.traceId === ref.traceId &&
                i.segmentId === ref.parentSegmentId &&
                i.spanId === ref.parentSpanId &&
                i.parentSpanId === parentSpanId,
            );
            if (!item) {
              fixSpans.push({
                traceId: ref.traceId,
                segmentId: ref.parentSegmentId,
                spanId: ref.parentSpanId,
                parentSpanId: parentSpanId,
                refs: [],
                endpointName: `VNode: ${ref.parentSegmentId}`,
                serviceCode: "VirtualNode",
                type: `[Broken] ${ref.type}`,
                peer: "",
                component: `VirtualNode: #${parentSpanId}`,
                isError: true,
                isBroken: true,
                layer: "Broken",
                tags: [],
                logs: [],
                startTime: 0,
                endTime: 0,
              });
            }
            // if root broken node is not exist, create a root broken node.
            if (parentSpanId > -1) {
              const item = fixSpans.find(
                (i) =>
                  i.traceId === ref.traceId &&
                  i.segmentId === ref.parentSegmentId &&
                  i.spanId === 0 &&
                  i.parentSpanId === -1,
              );
              if (!item) {
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
        });
      }
    });
    [...fixSpans, ...data].forEach((fixSpan) => {
      fixSpan.label = fixSpan.endpointName || "no operation name";
      fixSpan.children = [];
      const id = fixSpan.segmentId || "top";
      if (segmentGroup[id] === undefined) {
        segmentIdGroup.push(id);
        segmentGroup[id] = [];
        segmentGroup[id].push(fixSpan);
      } else {
        segmentGroup[id].push(fixSpan);
      }
    });

    segmentIdGroup.forEach((segmentId) => {
      const currentSegmentSet = segmentGroup[segmentId].sort((a, b) => b.parentSpanId - a.parentSpanId);
      currentSegmentSet.forEach((curSegment) => {
        const index = currentSegmentSet.findIndex(
          (curSegment2) => curSegment2.spanId === curSegment.parentSpanId,
        );
        if (index !== -1) {
          if (
            (currentSegmentSet[index].isBroken && currentSegmentSet[index].parentSpanId === -1) ||
            !currentSegmentSet[index].isBroken
          ) {
            currentSegmentSet[index].children.push(curSegment);
            currentSegmentSet[index].children.sort((a, b) => a.spanId - b.spanId);
          }
        }
        if (curSegment.isBroken) {
          const children = data.filter((span) =>
            span.refs.find(
              (d) =>
                d.traceId === curSegment.traceId &&
                d.parentSegmentId === curSegment.segmentId &&
                d.parentSpanId === curSegment.spanId,
            ),
          );
          if (children.length) {
            curSegment.children = curSegment.children || [];
            curSegment.children.push(...children);
          }
        }
      });
      segmentMap.set(segmentId, currentSegmentSet[currentSegmentSet.length - 1]);
    });

    return {
      segmentMap,
      segmentIdGroup,
    };
  }

  static collapse(span) {
    if (span.children) {
      let dur = span.endTime - span.startTime;
      span.children.forEach((child) => {
        dur -= child.endTime - child.startTime;
      });
      span.dur = dur < 0 ? 0 : dur;
      span.children.forEach((child) => this.collapse(child));
    }
  }

  static calculationChildren(nodes, result) {
    nodes.forEach((node) => {
      const groupRef = node.endpointName + ":" + node.type;
      if (node.children && node.children.length > 0) {
        this.calculationChildren(node.children, result);
      }
      if (result.get(groupRef) === undefined) {
        result.set(groupRef, []);
        result.get(groupRef).push(node);
      } else {
        result.get(groupRef).push(node);
      }
    });
  }
}


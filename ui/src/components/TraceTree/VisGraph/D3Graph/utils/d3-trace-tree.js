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

import * as d3 from "d3";
import d3tip from "d3-tip";

export default class TraceMap {
  constructor({ el, handleSelectSpan }) {
    this.i = 0;
    this.el = null;
    this.handleSelectSpan = null;
    this.topSlow = [];
    this.height = 0;
    this.width = 0;
    this.topChild = [];
    this.body = null;
    this.tip = null;
    this.svg = null;
    this.treemap = null;
    this.data = null;
    this.row = null;
    this.minTimestamp = 0;
    this.maxTimestamp = 0;
    this.xScale = null;
    this.root = null;
    this.topSlowMax = [];
    this.topSlowMin = [];
    this.topChildMax = [];
    this.topChildMin = [];
    this.nodeUpdate = null;
    this.selectedNode = null;

    this.el = el;
    this.handleSelectSpan = handleSelectSpan;
    this.i = 0;
    this.topSlow = [];
    this.topChild = [];
    this.width = el.getBoundingClientRect().width - 10;
    this.height = el.getBoundingClientRect().height - 10;
    d3.select(`.${this.el?.className} .d3-trace-tree`).remove();
    this.body = d3
      .select(this.el)
      .append("svg")
      .attr("class", "d3-trace-tree")
      .attr("width", this.width > 0 ? this.width : 10)
      .attr("height", this.height > 0 ? this.height : 10);
    this.tip = d3tip()
      .attr("class", "d3-tip")
      .offset([-8, 0])
      .html(
        (d) => `
      <div class="mb-5">${d.data.label}</div>
      ${d.data.dur ? '<div class="sm">SelfDuration: ' + d.data.dur + "ms</div>" : ""}
      ${
        d.data.endTime - d.data.startTime
          ? '<div class="sm">TotalDuration: ' + (d.data.endTime - d.data.startTime) + "ms</div>"
          : ""
      }
      `,
      );
    this.svg = this.body.append("g").attr("transform", () => `translate(120, 0)`);
    this.svg.call(this.tip);
  }
  init({ data, row, selectedMaxTimestamp, selectedMinTimestamp }) {
    d3.select("#trace-action-box").style("display", "none");
    this.treemap = d3.tree().size([row.length * 35, this.width]);
    this.row = row;
    this.data = data;
    this.minTimestamp = selectedMinTimestamp ?? Number(d3.min(this.row.map((i) => i.startTime)));
    this.maxTimestamp =
      selectedMaxTimestamp ?? Number(d3.max(this.row.map((i) => i.endTime - this.minTimestamp)));
    this.xScale = d3
      .scaleLinear()
      .range([0, 100])
      .domain([0, this.maxTimestamp - this.minTimestamp]);

    this.body.call(this.getZoomBehavior(this.svg));
    this.root = d3.hierarchy(this.data, (d) => d.children);
    this.root.x0 = this.height / 2;
    this.root.y0 = 0;
    this.topSlow = [];
    this.topChild = [];
    const that = this;
    this.root.children?.forEach(collapse);
    this.topSlowMax = this.topSlow.sort((a, b) => b - a)[0];
    this.topSlowMin = this.topSlow.sort((a, b) => b - a)[4];
    this.topChildMax = this.topChild.sort((a, b) => b - a)[0];
    this.topChildMin = this.topChild.sort((a, b) => b - a)[4];
    this.update(this.root);
    // Collapse the node and all it's children
    function collapse(d) {
      if (d.children) {
        let dur = d.data.endTime - d.data.startTime;
        d.children.forEach((i) => {
          dur -= i.data.endTime - i.data.startTime;
        });
        d.dur = dur < 0 ? 0 : dur;
        that.topSlow.push(dur);
        that.topChild.push(d.children.length);
        d.childrenLength = d.children.length;
        d.children.forEach(collapse);
      }
    }
  }
  draw() {
    this.update(this.root);
  }
  update(source) {
    const t = this;
    const that = this;
    const treeData = this.treemap(this.root);
    const nodes = treeData.descendants();
    const links = treeData.descendants().slice(1);

    nodes.forEach(function (d) {
      d.y = d.depth * 140;
    });

    const node = this.svg.selectAll("g.trace-node").data(nodes, (d) => {
      return d.id || (d.id = ++this.i);
    });
    d3.select("svg.d3-trace-tree").on("click", function (event) {
      if (event.target === this) {
        d3.select("#trace-action-box").style("display", "none");
        t.selectedNode && t.selectedNode.classed("highlighted", false);
        t.clearParentHighlight();
      }
    });

    const nodeEnter = node
      .enter()
      .append("g")
      .attr("class", "trace-node")
      .attr("id", (d) => `trace-node-${d.id}`)
      .attr("cursor", "pointer")
      .attr("transform", function () {
        return "translate(" + source.y0 + "," + source.x0 + ")";
      })
      .on("mouseover", function (event, d) {
        that.tip.show(d, this);
        if (!that.timeUpdate) {
          return;
        }
        const _node = that.timeUpdate._groups[0].filter((group) => group.__data__.id === that.i + 1);
        if (_node.length) {
          that.timeTip.show(d, _node[0].children[1]);
        }
      })
      .on("mouseout", function (event, d) {
        that.tip.hide(d, this);
        if (!that.timeUpdate) {
          return;
        }
        const _node = that.timeUpdate._groups[0].filter((group) => group.__data__.id === that.i + 1);
        if (_node.length) {
          that.timeTip.hide(d, _node[0].children[1]);
        }
      });
    nodeEnter
      .append("circle")
      .attr("r", 8)
      .attr("cy", "-12")
      .attr("cx", function (d) {
        return d.children || d._children ? -15 : 110;
      })
      .attr("fill", "none")
      .attr("stroke", "#e66")
      .style("opacity", (d) => {
        const events = d.data.attachedEvents;
        if (events && events.length) {
          return 0.5;
        } else {
          return 0;
        }
      });
    nodeEnter
      .append("text")
      .attr("x", function (d) {
        const events = d.data.attachedEvents || [];
        let p = d.children || d._children ? -18 : 107;
        p = events.length > 9 ? p - 2 : p;
        return p;
      })
      .attr("dy", "-0.8em")
      .attr("fill", "#e66")
      .style("font-size", "10px")
      .text((d) => {
        const events = d.data.attachedEvents;
        if (events && events.length) {
          return `${events.length}`;
        } else {
          return "";
        }
      });
    nodeEnter
      .append("circle")
      .attr("class", "node")
      .attr("r", 2)
      .attr("stroke", `#ccc`)
      .attr("stroke-width", 2.5)
      .attr("fill", (d) => (d.data.children.length ? `#ccc` : "#fff"));
    nodeEnter
      .append("text")
      .attr("class", "trace-node-text")
      .attr("font-size", 11)
      .attr("dy", "-0.5em")
      .attr("x", function (d) {
        return d.children || d._children ? -45 : 15;
      })
      .attr("text-anchor", function (d) {
        return d.children || d._children ? "end" : "start";
      })
      .text((d) =>
        d.data.label.length > 19
          ? (d.data.isError ? "◉ " : "") + d.data.label.slice(0, 10) + "..."
          : (d.data.isError ? "◉ " : "") + d.data.label,
      )
      .attr("fill", (d) =>
        !d.data.isError ? "#3d444f" : "#E54C17",
      );
    nodeEnter
      .append("text")
      .attr("class", "node-text")
      .attr("x", function (d) {
        return d.children || d._children ? -30 : 15;
      })
      .attr("dy", "1.5em")
      .attr("fill", "#bbb")
      .attr("text-anchor", function (d) {
        return d.children || d._children ? "end" : "start";
      })
      .style("font-size", "10px")
      .text((d) => {
        const label = d.data.component
          ? " - " + (d.data.component.length > 10 ? d.data.label.slice(0, 10) + "..." : d.data.component)
          : "";
        return `${d.data.layer || ""}${label}`;
      });
    nodeEnter
      .append("rect")
      .attr("rx", 1)
      .attr("ry", 1)
      .attr("height", 2)
      .attr("width", 100)
      .attr("x", function (d) {
        return d.children || d._children ? "-110" : "10";
      })
      .attr("y", -1)
      .style("fill", "#00000020");
    nodeEnter
      .append("rect")
      .attr("rx", 1)
      .attr("ry", 1)
      .attr("height", 2)
      .attr("width", (d) => {
        let spanStart = d.data.startTime;
        let spanEnd = d.data.endTime;
        if (!spanEnd || !spanStart) return 0;
        if (spanStart > this.maxTimestamp || spanEnd < this.minTimestamp) return 0;
        if (spanStart < this.minTimestamp) spanStart = this.minTimestamp;
        if (spanEnd > this.maxTimestamp) spanEnd = this.maxTimestamp;
        return this.xScale(spanEnd - spanStart) + 1 || 0;
      })
      .attr("x", (d) => {
        let spanStart = d.data.startTime;
        let spanEnd = d.data.endTime;
        if (!spanEnd || !spanStart) {
          return 0;
        }
        if (spanStart > this.maxTimestamp || spanEnd < this.minTimestamp) return 0;
        if (spanStart < this.minTimestamp) spanStart = this.minTimestamp;
        if (spanEnd > this.maxTimestamp) spanEnd = this.maxTimestamp;
        if (d.children || d._children) {
          return -110 + this.xScale(spanStart - this.minTimestamp);
        }
        return 10 + this.xScale(spanStart - this.minTimestamp);
      })
      .attr("y", -1)
      .style("fill", `#ccc`);
    const nodeUpdate = nodeEnter.merge(node);
    this.nodeUpdate = nodeUpdate;
    nodeUpdate
      .transition()
      .duration(600)
      .attr("transform", function (d) {
        return "translate(" + d.y + "," + d.x + ")";
      });
    nodeUpdate
      .select("circle.node")
      .attr("r", 5)
      .attr("cursor", "pointer")
      .on("click", function (event, d) {
        event.stopPropagation();
        t.tip.hide(d, this);
        d3.select(this.parentNode).classed("highlighted", true);
        const nodeBox = this.getBoundingClientRect();
        const svgBox = d3.select(`.${t.el?.className} .d3-trace-tree`).node().getBoundingClientRect();
        const offsetX = nodeBox.x - svgBox.x;
        const offsetY = nodeBox.y - svgBox.y;
        d3.select("#trace-action-box")
          .style("display", "block")
          .style("left", `${offsetX + 30}px`)
          .style("top", `${offsetY + 40}px`);
        t.selectedNode?.classed("highlighted", false);
        t.selectedNode = d3.select(this.parentNode);
        if (t.handleSelectSpan) {
          t.handleSelectSpan(d);
        }
        t.root.descendants().map((node) => {
          d3.select(`#trace-node-${node.id}`).classed("highlightedParent", false);
          return node;
        });
      })
      .on("dblclick", function (event, d) {
        event.stopPropagation();
        t.tip.hide(d, this);
        if (d.data.children.length === 0) return;
        click(d);
      })
      .on("contextmenu", function (event, d) {
        event.stopPropagation();
        t.tip.hide(d, this);
        if (d.data.children.length === 0) return;
        click(d);
      });
    const nodeExit = node
      .exit()
      .transition()
      .duration(600)
      .attr("transform", function () {
        return "translate(" + source.y + "," + source.x + ")";
      })
      .remove();

    nodeExit.select("circle").attr("r", 1e-6);

    nodeExit.select("text").style("fill-opacity", 1e-6);

    const link = this.svg
      .selectAll("path.tree-link")
      .data(links, function (d) {
        return d.id;
      })
      .style("stroke-width", 1.5);

    const linkEnter = link
      .enter()
      .insert("path", "g")
      .attr("class", "tree-link")
      .attr("d", function () {
        const o = { x: source.x0, y: source.y0 };
        return diagonal(o, o);
      })
      .attr("stroke", "rgba(0, 0, 0, 0.1)")
      .style("stroke-width", 1.5)
      .style("fill", "none");

    const linkUpdate = linkEnter.merge(link);
    linkUpdate
      .transition()
      .duration(600)
      .attr("d", function (d) {
        return diagonal(d, d.parent);
      });
    link
      .exit()
      .transition()
      .duration(600)
      .attr("d", function () {
        const o = { x: source.x, y: source.y };
        return diagonal(o, o);
      })
      .style("stroke-width", 1.5)
      .remove();

    nodes.forEach(function (d) {
      d.x0 = d.x;
      d.y0 = d.y;
    });
    function diagonal(s, d) {
      return `M ${s.y} ${s.x}
      C ${(s.y + d.y) / 2} ${s.x}, ${(s.y + d.y) / 2} ${d.x},
      ${d.y} ${d.x}`;
    }
    function click(d) {
      if (d.children) {
        d._children = d.children;
        d.children = null;
      } else {
        d.children = d._children;
        d._children = null;
      }
      that.update(d);
    }
  }
  clearParentHighlight() {
    return this.root.descendants().map((node) => {
      d3.select(`#trace-node-${node.id}`).classed("highlightedParent", false);
      return node;
    });
  }
  highlightParents(span) {
    if (!span) {
      return;
    }
    const nodes = this.clearParentHighlight();
    const parentSpan = nodes.find(
      (node) =>
        span.spanId === node.data.spanId &&
        span.segmentId === node.data.segmentId &&
        span.traceId === node.data.traceId,
    );
    if (!parentSpan) return;
    d3.select(`#trace-node-${parentSpan.id}`).classed("highlightedParent", true);
    d3.select("#trace-action-box").style("display", "none");
    this.selectedNode.classed("highlighted", false);
    const container = document.querySelector(".trace-chart .charts");
    const containerRect = container?.getBoundingClientRect();
    if (!containerRect) return;
    const targetElement = document.querySelector(`#trace-node-${parentSpan.id}`);
    if (!targetElement) return;
    const targetRect = targetElement.getBoundingClientRect();
    container?.scrollTo({
      left: targetRect.left - containerRect.left + container?.scrollLeft,
      top: targetRect.top - containerRect.top + container?.scrollTop - 100,
      behavior: "smooth",
    });
  }
  setDefault() {
    d3.selectAll(".time-inner").style("opacity", 1);
    d3.selectAll(".time-inner-duration").style("opacity", 0);
    d3.selectAll(".trace-tree-node-selfdur").style("opacity", 0);
    d3.selectAll(".trace-tree-node-selfchild").style("opacity", 0);
    this.nodeUpdate._groups[0].forEach((i) => {
      d3.select(i).style("opacity", 1);
    });
  }
  getTopChild() {
    d3.selectAll(".time-inner").style("opacity", 1);
    d3.selectAll(".time-inner-duration").style("opacity", 0);
    d3.selectAll(".trace-tree-node-selfdur").style("opacity", 0);
    d3.selectAll(".trace-tree-node-selfchild").style("opacity", 1);
    this.nodeUpdate._groups[0].forEach((i) => {
      d3.select(i).style("opacity", 0.2);
      if (i.__data__.data.children.length >= this.topChildMin && i.__data__.data.children.length <= this.topChildMax) {
        d3.select(i).style("opacity", 1);
      }
    });
  }
  getTopSlow() {
    d3.selectAll(".time-inner").style("opacity", 0);
    d3.selectAll(".time-inner-duration").style("opacity", 1);
    d3.selectAll(".trace-tree-node-selfchild").style("opacity", 0);
    d3.selectAll(".trace-tree-node-selfdur").style("opacity", 1);
    this.nodeUpdate._groups[0].forEach((i) => {
      d3.select(i).style("opacity", 0.2);
      if (i.__data__.data.dur >= this.topSlowMin && i.__data__.data.dur <= this.topSlowMax) {
        d3.select(i).style("opacity", 1);
      }
    });
  }
  getZoomBehavior(g) {
    return d3
      .zoom()
      .scaleExtent([0.3, 10])
      .on("zoom", (d) => {
        g.attr("transform", d3.zoomTransform(this.svg.node())).attr(
          `translate(${d.transform.x},${d.transform.y})scale(${d.transform.k})`,
        );
      });
  }
}


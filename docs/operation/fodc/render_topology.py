#!/usr/bin/env python3
# Licensed to Apache Software Foundation (ASF) under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Apache Software Foundation (ASF) licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Render a BanyanDB cluster topology by joining the FODC proxy's
/cluster/topology (node inventory) with queue Prometheus metrics (live edges).
Request edges come from the publisher metrics (queue_pub) by default; for
edges the publisher does not record -- chiefly liaison->warm/cold query
fan-out on older servers -- the subscriber metrics (queue_sub) fill in, read
from the receiver's side and flipped back to the true sender->receiver
direction. Lifecycle tier-migration edges take their path from the topology
`calls` and their weight from the banyandb_lifecycle_migration_* family (the
tier-migration mirror of queue_pub) whenever a migration has run.
Output is Graphviz DOT and/or Mermaid. Stdlib only.
If Prometheus is behind Grafana's datasource proxy with basic auth,
set PROM_USER / PROM_PASS in the environment."""
import argparse, base64, json, os, urllib.parse, urllib.request

TIER_COLOR = {"hot": "#e57373", "warm": "#ffb74d", "cold": "#64b5f6", "": "#cfd8dc"}


def http_get_json(url, auth=None):
    req = urllib.request.Request(url)
    if auth:
        req.add_header("Authorization", "Basic " + base64.b64encode(auth.encode()).decode())
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.load(resp)


def prom_query(prom, expr, auth):
    url = prom.rstrip("/") + "/api/v1/query?" + urllib.parse.urlencode({"query": expr})
    return http_get_json(url, auth)["data"]["result"]


def short(name):
    return name.split(".")[0] if name else "(unknown)"


def primary_role(roles):
    for want in ("ROLE_LIAISON", "ROLE_DATA", "ROLE_META"):
        if want in (roles or []):
            return want.replace("ROLE_", "").lower()
    return "node"


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--proxy", required=True, help="FODC proxy base URL, e.g. http://HOST:17913")
    ap.add_argument("--prom", required=True, help="Prometheus base URL (or Grafana datasource-proxy base)")
    ap.add_argument("--job", default="fodc-proxy", help="Prometheus job label for the FODC proxy")
    ap.add_argument("--window", default="5m", help="rate() window, e.g. 5m")
    ap.add_argument("--p99-warn", type=float, default=0.5, help="edge turns red above this p99 (seconds)")
    ap.add_argument("--lifecycle-port", default="17914",
                    help="port that identifies lifecycle nodes; their topology calls are the hot->warm->cold migration edges")
    ap.add_argument("--format", choices=["dot", "mermaid", "both"], default="both")
    args = ap.parse_args()

    auth = None
    if os.environ.get("PROM_USER"):
        auth = "%s:%s" % (os.environ["PROM_USER"], os.environ.get("PROM_PASS", ""))

    # 1) Node inventory from /cluster/topology.
    topo = http_get_json(args.proxy.rstrip("/") + "/cluster/topology")
    nodes, podname2name, podname2lifecycle = {}, {}, {}
    for n in topo.get("nodes", []):
        name = (n.get("metadata") or {}).get("name", "")
        if not name:
            continue
        labels = n.get("labels") or {}
        nodes[name] = {"role": primary_role(n.get("roles")), "tier": labels.get("type", ""),
                       "pod": labels.get("pod_name", ""), "container": labels.get("container_name", ""),
                       "status": n.get("status") or ""}
        # Lifecycle sidecars share their pod (and pod_name) with the co-located
        # data node but never originate queue metrics, so they must not shadow
        # the data node in the pod_name -> node map -- otherwise sub-side edges
        # received by that data node would be misattributed to the :17914 node.
        is_lifecycle = name.endswith(":" + str(args.lifecycle_port))
        if labels.get("pod_name") and not is_lifecycle:
            podname2name[labels["pod_name"]] = name
        # Fallback: map the bare hostname (DNS label) to the full node name so a
        # metric's pod_name still resolves when the proxy did not tag the node with
        # a pod_name label. Skip lifecycle nodes for the same reason as above;
        # prefer the explicit pod_name mapping.
        host = name.split(".")[0].split(":")[0]
        if host and not is_lifecycle and host not in podname2name:
            podname2name[host] = name
        # The banyandb_lifecycle_migration_* series are scraped from the lifecycle
        # sidecar (container_name="lifecycle") but share the data pod's pod_name,
        # so they resolve through a separate map to the pod's :17914 identity.
        if is_lifecycle:
            if labels.get("pod_name"):
                podname2lifecycle.setdefault(labels["pod_name"], name)
            if host:
                podname2lifecycle.setdefault(host, name)

    def local_name(pod):
        return podname2name.get(pod, pod)

    def ensure_node(name, role="", tier=""):
        if name and name not in nodes:
            nodes[name] = {"role": role or "node", "tier": tier, "pod": "", "container": "", "status": ""}

    j, w = '{job=~"%s"}' % args.job, args.window
    edge_by = "pod_name, node_role, node_type, remote_node, remote_role, remote_tier, operation"

    def rate_by(metric, by):
        return prom_query(args.prom, "sum by (%s) (rate(%s%s[%s]))" % (by, metric, j, w), auth)

    def p99_by(metric):
        return prom_query(args.prom, "histogram_quantile(0.99, sum by (le, pod_name, remote_node)"
                          " (rate(%s%s[%s])))" % (metric, j, w), auth)

    # 2) Live edges from one queue-metric family, accumulated into a directed
    #    edge map. The publisher (queue_pub) scrape target is the sender, so the
    #    flow is pod_name -> remote_node. The subscriber (queue_sub) scrape
    #    target is the receiver and remote_node is the sender, so its flow is
    #    remote_node -> pod_name -- the edge is flipped. The sub view is what
    #    surfaces liaison->warm/cold query fan-out on older servers whose
    #    publish path predates the query/control instrumentation. Sub series
    #    with remote_role="lifecycle" are inbound tier-migration traffic, not
    #    request pipeline, and are skipped (the migration layer is weighted
    #    from the publisher-side banyandb_lifecycle_migration_* family).
    def build_edges(thr, p99, err, byt, invert, resolve_local=None, skip_remote_role=None):
        resolve = resolve_local or local_name
        es = {}
        for s in thr:
            m = s["metric"]
            pod_node, peer = resolve(m.get("pod_name", "")), m.get("remote_node", "")
            if not pod_node or not peer:
                continue
            if skip_remote_role and m.get("remote_role", "") == skip_remote_role:
                continue
            pod_role, pod_tier = primary_role([m.get("node_role", "")]), m.get("node_type", "")
            peer_role, peer_tier = m.get("remote_role", ""), m.get("remote_tier", "")
            if invert:
                source, target = peer, pod_node
                ensure_node(source, role=peer_role, tier=peer_tier)
                ensure_node(target, role=pod_role, tier=pod_tier)
            else:
                source, target = pod_node, peer
                ensure_node(source, role=pod_role, tier=pod_tier)
                ensure_node(target, role=peer_role, tier=peer_tier)
            op = m.get("operation", "?")
            e = es.setdefault((source, target), {"ops": {}, "p99": 0.0, "err": 0.0, "bytes": 0.0})
            e["ops"][op] = e["ops"].get(op, 0.0) + float(s["value"][1])

        def add_scalar(rows, field):
            for s in rows:
                pod_node = resolve(s["metric"].get("pod_name", ""))
                peer = s["metric"].get("remote_node", "")
                key = (peer, pod_node) if invert else (pod_node, peer)
                if key in es:
                    try:
                        es[key][field] = max(es[key][field], float(s["value"][1]))
                    except ValueError:
                        pass
        add_scalar(p99, "p99"); add_scalar(err, "err")
        if byt:
            add_scalar(byt, "bytes")
        return es

    # Publisher is authoritative for direction and weight; the subscriber only
    # fills edges the publisher does not record. One side per edge, never
    # summed, so throughput is never double-counted.
    pub_edges = build_edges(
        rate_by("banyandb_queue_pub_total_finished", edge_by),
        p99_by("banyandb_queue_pub_total_latency_bucket"),
        rate_by("banyandb_queue_pub_total_err", "pod_name, remote_node"),
        rate_by("banyandb_queue_pub_sent_bytes", "pod_name, remote_node"),
        invert=False)
    sub_edges = build_edges(
        rate_by("banyandb_queue_sub_total_finished", edge_by),
        p99_by("banyandb_queue_sub_total_latency_bucket"),
        rate_by("banyandb_queue_sub_total_err", "pod_name, remote_node"),
        None,  # sub records received_bytes, not sent_bytes; query/control carry no bytes
        invert=True, skip_remote_role="lifecycle")
    edges = dict(pub_edges)
    for key, sub_edge in sub_edges.items():
        edges.setdefault(key, sub_edge)  # default pub; fall back to sub for edges pub lacks

    # 3) Lifecycle migration edges. The PATH comes from /cluster/topology `calls`
    #    (structural; present even between scheduled runs): keep only edges whose
    #    source is a lifecycle node (name ends with the lifecycle port), drop the
    #    liaison route-table copies and the data<->data property-gossip mesh. The
    #    WEIGHT comes from the banyandb_lifecycle_migration_* family -- the
    #    tier-migration mirror of queue_pub, scraped from the lifecycle sidecar
    #    (container_name="lifecycle"), whose pod_name resolves to the pod's
    #    :17914 lifecycle identity rather than its data node.
    lc_suffix = ":" + str(args.lifecycle_port)
    migrations = {}
    for c in topo.get("calls", []):
        src, dst = c.get("source", ""), c.get("target", "")
        if src.endswith(lc_suffix) and dst:
            ensure_node(src, role="lifecycle")
            ensure_node(dst)
            migrations.setdefault((src, dst), None)
    mig_weights = build_edges(
        rate_by("banyandb_lifecycle_migration_total_finished", edge_by),
        p99_by("banyandb_lifecycle_migration_total_latency_bucket"),
        rate_by("banyandb_lifecycle_migration_total_err", "pod_name, remote_node"),
        rate_by("banyandb_lifecycle_migration_sent_bytes", "pod_name, remote_node"),
        invert=False, resolve_local=lambda pod: podname2lifecycle.get(pod, pod))
    for key, e in mig_weights.items():
        ensure_node(key[0], role="lifecycle")
        ensure_node(key[1])
        migrations[key] = e  # weighted when a migration has run; else structural

    if args.format in ("dot", "both"):
        print(render_dot(nodes, edges, migrations, args.p99_warn))
    if args.format in ("mermaid", "both"):
        print(render_mermaid(nodes, edges, migrations, args.p99_warn))


def edge_label(e):
    ops = ", ".join("%s %.1f/s" % (op, r) for op, r in sorted(e["ops"].items()))
    parts = [ops, "p99 %.0fms" % (e["p99"] * 1000)]
    if e["bytes"] > 0:
        parts.append("%.0f KB/s" % (e["bytes"] / 1024))
    if e["err"] > 0:
        parts.append("ERR %.2f/s" % e["err"])
    return "\\n".join(parts)


def pod_groups(nodes):
    """Pods hosting more than one node (a data node plus its lifecycle
    sidecar) -- rendered as one enclosing boundary per pod."""
    by_pod = {}
    for name, a in nodes.items():
        if a.get("pod"):
            by_pod.setdefault(a["pod"], []).append(name)
    return {pod: sorted(names) for pod, names in by_pod.items() if len(names) > 1}


def node_label(name, a, in_pod):
    """Bare nodes show the node name; nodes inside a pod boundary show their
    container name (data / lifecycle) -- the boundary already carries the pod
    name. The tier line stays on both."""
    base = (a.get("container") or short(name)) if in_pod else short(name)
    return base, a["tier"]


def render_dot(nodes, edges, migrations, p99_warn):
    def node_line(name, a, indent="  ", in_pod=False):
        shape = "box" if a["role"] == "liaison" else "cylinder" if a["role"] == "data" else "ellipse"
        pen = ', color="#c62828", penwidth=2, style="filled,dashed"' if a["status"] and a["status"] != "online" else ""
        base, tier = node_label(name, a, in_pod)
        label = base + (("\\n" + tier) if tier else "")
        return '%s"%s" [label="%s", shape=%s, fillcolor="%s"%s];' \
            % (indent, name, label, shape, TIER_COLOR.get(a["tier"], "#cfd8dc"), pen)

    out = ["digraph banyandb_topology {", "  rankdir=LR;", '  node [style=filled, fontname="sans"];']
    groups = pod_groups(nodes)
    grouped = {n for names in groups.values() for n in names}
    for pod, names in sorted(groups.items()):  # data + lifecycle sharing a pod
        out.append('  subgraph "cluster_%s" {' % pod)
        out.append('    label="pod %s"; style=rounded; color="#9e9e9e"; fontsize=10;' % pod)
        for name in names:
            out.append(node_line(name, nodes[name], "    ", in_pod=True))
        out.append("  }")
    for name, a in sorted(nodes.items()):
        if name not in grouped:
            out.append(node_line(name, a))
    for (local, remote), e in sorted(edges.items()):  # write pipeline (weighted)
        red = e["err"] > 0 or e["p99"] > p99_warn
        pw = 1.0 + min(4.0, sum(e["ops"].values()) ** 0.25)
        out.append('  "%s" -> "%s" [label="%s", color="%s", penwidth=%.1f, fontsize=10];'
                   % (local, remote, edge_label(e), "#c62828" if red else "#607d8b", pw))
    for (src, dst), e in sorted(migrations.items()):  # lifecycle migration (dashed)
        label = "migrate" if e is None else "migrate\\n" + edge_label(e)
        out.append('  "%s" -> "%s" [label="%s", style=dashed, color="#8e24aa", fontsize=9];' % (src, dst, label))
    out.append("}")
    return "\n".join(out)


def render_mermaid(nodes, edges, migrations, p99_warn):
    def nid(name):
        return "n_" + "".join(c if c.isalnum() else "_" for c in name)

    def node_def(name, a, in_pod=False):
        base, tier = node_label(name, a, in_pod)
        label = base + (("<br/>" + tier) if tier else "")
        return '%s[("%s")]' % (nid(name), label) if a["role"] == "data" else '%s["%s"]' % (nid(name), label)

    out = ["```mermaid", "graph LR"]
    groups = pod_groups(nodes)
    grouped = {n for names in groups.values() for n in names}
    for pod, names in sorted(groups.items()):  # data + lifecycle sharing a pod
        out.append('  subgraph %s["pod %s"]' % ("pod_" + "".join(c if c.isalnum() else "_" for c in pod), pod))
        for name in names:
            out.append("    " + node_def(name, nodes[name], in_pod=True))
        out.append("  end")
    for name, a in sorted(nodes.items()):
        if name not in grouped:
            out.append("  " + node_def(name, a))
    red_idx, i = [], 0
    for (local, remote), e in sorted(edges.items()):  # write pipeline (weighted)
        out.append('  %s -- "%s" --> %s' % (nid(local), edge_label(e).replace("\\n", " · "), nid(remote)))
        if e["err"] > 0 or e["p99"] > p99_warn:
            red_idx.append(i)
        i += 1
    for (src, dst), e in sorted(migrations.items()):  # lifecycle migration (dashed)
        label = "migrate" if e is None else "migrate · " + edge_label(e).replace("\\n", " · ")
        out.append('  %s -. "%s" .-> %s' % (nid(src), label, nid(dst)))
    for name, a in sorted(nodes.items()):
        out.append("  style %s fill:%s" % (nid(name), TIER_COLOR.get(a["tier"], "#cfd8dc")))
    for idx in red_idx:
        out.append("  linkStyle %d stroke:#c62828,stroke-width:2px" % idx)
    out.append("```")
    return "\n".join(out)


if __name__ == "__main__":
    main()

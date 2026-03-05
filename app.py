"""
Layer10 — Memory Graph Explorer (Streamlit)
Full interactive dashboard: graph visualization, search, entity detail, merge audit.
Replaces Flask API + viz/index.html in a single Streamlit app.
"""

import os
import sys
import json
import subprocess
import shutil
import threading
import time
from pathlib import Path

# Ensure project root and src are on path
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))
os.chdir(PROJECT_ROOT)

import streamlit as st
from dotenv import load_dotenv

load_dotenv()

PYTHON_EXE = sys.executable

st.set_page_config(
    page_title="Layer10 — Memory Graph Explorer",
    page_icon="⬡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Dark theme CSS ────────────────────────────────────────────────────────────
st.markdown("""
<style>
    /* Dark background */
    .stApp { background-color: #0a0b0f; }
    section[data-testid="stSidebar"] { background-color: #10131a; }
    
    /* Header */
    .logo-row { display:flex; align-items:center; gap:10px; margin-bottom:10px; }
    .logo-dot { width:10px; height:10px; background:#4f9eff; border-radius:50%;
                animation: pulse 2s ease-in-out infinite; }
    @keyframes pulse { 0%,100%{opacity:1;transform:scale(1)} 50%{opacity:.5;transform:scale(.7)} }
    .logo-text { font-size:22px; font-weight:800; color:#fff; font-family:'Segoe UI',sans-serif; }
    .logo-text span { color:#4f9eff; }
    
    /* Stat pills */
    .stat-row { display:flex; gap:18px; margin:8px 0 16px; flex-wrap:wrap; }
    .stat-pill { display:flex; align-items:center; gap:6px; font-size:12px; color:#6b7a99;
                 font-family:'Consolas',monospace; }
    .stat-pill .dot { width:8px; height:8px; border-radius:50%; }
    .dot-issue { background:#4f9eff; }
    .dot-person { background:#3ecf8e; }
    .dot-component { background:#ffd166; }
    .dot-claim { background:#b57bee; }
    .dot-label { background:#ff8c42; }
    
    /* Cards */
    .entity-card { background:#161b26; border:1px solid #1e2535; border-radius:8px;
                   padding:14px; margin-bottom:10px; }
    .entity-card:hover { border-color:#2a3348; }
    .entity-title { font-size:14px; color:#c9d1e0; font-weight:600; margin-bottom:4px; }
    .entity-meta { font-size:11px; color:#3d4f70; }
    
    /* Claim cards */
    .claim-card { background:#161b26; border:1px solid #1e2535; border-radius:8px;
                  padding:12px; margin-bottom:8px; }
    .claim-type { font-size:10px; text-transform:uppercase; letter-spacing:1px;
                  color:#7b5ea7; margin-bottom:4px; font-weight:600; }
    .claim-value { font-size:12px; color:#c9d1e0; line-height:1.5; margin-bottom:6px; }
    .conf-bar { display:flex; align-items:center; gap:6px; }
    .conf-track { width:60px; height:4px; background:#1e2535; border-radius:2px; overflow:hidden; }
    .conf-fill { height:100%; border-radius:2px; }
    .conf-text { font-size:10px; color:#6b7a99; }
    
    /* Evidence */
    .evidence-box { background:#0a0b0f; border-left:3px solid #7b5ea7; padding:8px 12px;
                    font-size:11px; color:#6b7a99; line-height:1.6; border-radius:0 4px 4px 0;
                    margin-top:6px; font-style:italic; }
    .evidence-src { font-size:10px; color:#3d4f70; margin-top:4px; }
    .evidence-src a { color:#4f9eff; text-decoration:none; }
    
    /* Search result */
    .result-card { background:#161b26; border:1px solid #1e2535; border-radius:8px;
                   padding:14px; margin-bottom:10px; transition:border-color .15s; }
    .result-card:hover { border-color:#2a3348; }
    .result-score { float:right; font-size:11px; color:#3d4f70; font-weight:600; }
    .result-title { font-size:13px; color:#c9d1e0; margin-bottom:4px; font-weight:500; }
    .result-summary { font-size:11px; color:#6b7a99; line-height:1.5; margin-bottom:6px; }
    
    /* Tags */
    .tag { display:inline-block; font-size:10px; padding:2px 8px; border-radius:4px;
           background:#10131a; border:1px solid #2a3348; color:#6b7a99; margin-right:4px; }
    .tag-open { border-color:#3ecf8e; color:#3ecf8e; }
    .tag-closed { border-color:#6b7a99; color:#6b7a99; }
    .tag-issue { background:rgba(79,158,255,.12); border-color:#4f9eff44; color:#4f9eff; }
    .tag-person { background:rgba(62,207,142,.12); border-color:#3ecf8e44; color:#3ecf8e; }
    .tag-component { background:rgba(255,209,102,.12); border-color:#ffd16644; color:#ffd166; }
    .tag-claim { background:rgba(181,123,238,.12); border-color:#b57bee44; color:#b57bee; }
    
    /* Log items */
    .log-item { background:#161b26; border:1px solid #1e2535; border-radius:6px;
                padding:10px 12px; margin-bottom:6px; }
    .log-action { color:#ffd166; font-size:11px; font-weight:600; margin-bottom:3px; }
    .log-detail { color:#3d4f70; font-size:10px; }
    
    /* Section headers */
    .section-hdr { font-size:10px; text-transform:uppercase; letter-spacing:1.5px;
                   color:#3d4f70; margin:20px 0 10px; font-weight:500; }
    
    /* Override streamlit defaults */
    .stMetric label { color:#6b7a99 !important; }
    .stMetric [data-testid="stMetricValue"] { color:#c9d1e0 !important; }
    h1, h2, h3 { color:#c9d1e0 !important; }
    .stTabs [data-baseweb="tab-list"] { gap: 2px; }
    .stTabs [data-baseweb="tab"] { color: #6b7a99; }
    .stMarkdown { color: #c9d1e0; }
</style>
""", unsafe_allow_html=True)


# ── Data Loading (cached) ────────────────────────────────────────────────────

@st.cache_resource
def get_neo4j_db():
    try:
        from graph_builder import MemoryGraphDB
        return MemoryGraphDB()
    except Exception as e:
        st.error(f"Neo4j connection failed: {e}")
        return None


@st.cache_resource
def get_qdrant_client():
    try:
        from retrieval import get_qdrant_client as _get
        return _get()
    except Exception as e:
        st.error(f"Qdrant connection failed: {e}")
        return None


@st.cache_data(ttl=300)
def load_graph_data(limit=120, min_confidence=0.5):
    db = get_neo4j_db()
    if not db:
        return {"nodes": [], "edges": [], "stats": {}}
    try:
        issues = db.run("MATCH (i:Issue) RETURN i.id AS id, i.title AS title, i.state AS state, i.url AS url, i.created_at AS created_at, i.summary AS summary, i.number AS number LIMIT $limit", {"limit": limit})
        persons = db.run("MATCH (p:Person) RETURN p.id AS id, p.login AS login, p.display_name AS display_name LIMIT 200")
        components = db.run("MATCH (c:Component) RETURN c.id AS id, c.name AS name, c.description AS description LIMIT 100")
        labels = db.run("MATCH (l:Label) RETURN l.id AS id, l.name AS name LIMIT 100")
        claims = db.run("MATCH (cl:Claim) WHERE cl.confidence >= $min_conf AND (cl.superseded_by IS NULL OR cl.superseded_by = '') RETURN cl.id AS id, cl.claim_type AS claim_type, cl.subject_id AS subject_id, cl.object_id AS object_id, cl.value AS value, cl.confidence AS confidence LIMIT 300", {"min_conf": min_confidence})
        rels = db.run("MATCH (a)-[r]->(b) WHERE type(r) IN ['AUTHORED','HAS_LABEL','DUPLICATE_OF','HAS_SUBJECT','HAS_OBJECT'] RETURN a.id AS source_id, type(r) AS rel_type, b.id AS target_id LIMIT 500")

        nodes = []
        for i in issues:
            nodes.append({"id": i["id"], "type": "Issue", **i})
        for p in persons:
            nodes.append({"id": p["id"], "type": "Person", **p})
        for c in components:
            nodes.append({"id": c["id"], "type": "Component", **c})
        for cl in claims:
            nodes.append({"id": cl["id"], "type": "Claim", **cl})

        edges = [{"source": r["source_id"], "target": r["target_id"], "type": r["rel_type"]}
                 for r in rels if r["source_id"] and r["target_id"]]

        return {
            "nodes": nodes,
            "edges": edges,
            "stats": {"Issue": len(issues), "Person": len(persons), "Component": len(components),
                      "Label": len(labels), "Claim": len(claims), "Edges": len(edges)},
        }
    except Exception as e:
        st.error(f"Graph query error: {e}")
        return {"nodes": [], "edges": [], "stats": {}}


@st.cache_data(ttl=300)
def load_graph_stats():
    path = Path("outputs/graph_stats.json")
    if path.exists():
        return json.loads(path.read_text())
    return {}


def get_entity_claims(entity_id):
    db = get_neo4j_db()
    if not db:
        return []
    try:
        return db.run("""
        MATCH (cl:Claim)-[:HAS_SUBJECT|HAS_OBJECT]->(e {id: $id})
        OPTIONAL MATCH (cl)-[:SUPPORTED_BY]->(ev:Evidence)
        RETURN cl.id AS claim_id, cl.claim_type AS claim_type,
               cl.value AS value, cl.confidence AS confidence,
               cl.valid_from AS valid_from, cl.valid_until AS valid_until,
               collect({excerpt: ev.excerpt, url: ev.url, timestamp: ev.timestamp, source_id: ev.source_id}) AS evidence
        LIMIT 50
        """, {"id": entity_id})
    except:
        return []


def get_issue_detail(number):
    db = get_neo4j_db()
    if not db:
        return None, []
    try:
        issue = db.run("""
        MATCH (i:Issue {id: $id})
        OPTIONAL MATCH (p:Person)-[:AUTHORED]->(i)
        OPTIONAL MATCH (i)-[:HAS_LABEL]->(l:Label)
        OPTIONAL MATCH (i)-[:DUPLICATE_OF]->(dup:Issue)
        RETURN i.id AS id, i.title AS title, i.state AS state,
               i.body_excerpt AS body_excerpt, i.url AS url,
               i.created_at AS created_at, i.closed_at AS closed_at,
               i.summary AS summary,
               collect(DISTINCT p.login) AS authors,
               collect(DISTINCT l.name) AS labels,
               dup.id AS duplicate_of
        """, {"id": f"issue:{number}"})

        claims = db.run("""
        MATCH (cl:Claim)-[:SUPPORTED_BY]->(ev:Evidence)-[:FROM_SOURCE]->(i:Issue {id: $id})
        OPTIONAL MATCH (cl)-[:SUPPORTED_BY]->(ev2:Evidence)
        RETURN cl.id AS claim_id, cl.claim_type AS claim_type,
               cl.value AS value, cl.confidence AS confidence,
               cl.subject_id AS subject_id, cl.object_id AS object_id,
               cl.valid_from AS valid_from, cl.superseded_by AS superseded_by,
               collect({excerpt: ev2.excerpt, url: ev2.url}) AS evidence
        LIMIT 20
        """, {"id": f"issue:{number}"})

        return (issue[0] if issue else None), claims
    except:
        return None, []


def do_search(question, top_k=8):
    qdrant = get_qdrant_client()
    db = get_neo4j_db()
    if not qdrant or not db:
        return None
    try:
        from retrieval import retrieve_context_pack
        return retrieve_context_pack(question=question, client=qdrant, db=db, top_k=top_k)
    except Exception as e:
        st.error(f"Search error: {e}")
        return None


def load_merge_data():
    merges, conflicts, dupes = [], [], {}
    try:
        p = Path("outputs/merge_audit_log.json")
        if p.exists():
            merges = json.loads(p.read_text())
    except:
        pass
    try:
        p = Path("outputs/dedup/conflicts.json")
        if p.exists():
            conflicts = json.loads(p.read_text())
    except:
        pass
    try:
        p = Path("outputs/dedup/artifact_dupes.json")
        if p.exists():
            dupes = json.loads(p.read_text())
    except:
        pass
    return merges, conflicts, dupes


# ── D3 Graph Visualization (embedded HTML) ────────────────────────────────────

def render_d3_graph(nodes, edges):
    """Render an interactive D3 force graph inside Streamlit via an iframe."""
    import json as _json

    nodes_json = _json.dumps(nodes)
    edges_json = _json.dumps(edges)

    html = f"""
    <div id="graph-container" style="width:100%;height:600px;background:#0a0b0f;border-radius:8px;border:1px solid #1e2535;position:relative;overflow:hidden;">
      <svg id="graph-svg" style="width:100%;height:100%"></svg>
      <div style="position:absolute;bottom:12px;left:12px;display:flex;gap:14px;">
        <div style="display:flex;align-items:center;gap:5px;font-size:10px;color:#3d4f70;font-family:monospace">
          <div style="width:8px;height:8px;border-radius:50%;background:#4f9eff"></div>Issue</div>
        <div style="display:flex;align-items:center;gap:5px;font-size:10px;color:#3d4f70;font-family:monospace">
          <div style="width:8px;height:8px;border-radius:50%;background:#3ecf8e"></div>Person</div>
        <div style="display:flex;align-items:center;gap:5px;font-size:10px;color:#3d4f70;font-family:monospace">
          <div style="width:8px;height:8px;border-radius:50%;background:#ffd166"></div>Component</div>
        <div style="display:flex;align-items:center;gap:5px;font-size:10px;color:#3d4f70;font-family:monospace">
          <div style="width:8px;height:8px;border-radius:50%;background:#b57bee"></div>Claim</div>
      </div>
    </div>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/d3/7.9.0/d3.min.js"></script>
    <script>
    (function() {{
      const nodes = {nodes_json};
      const edges = {edges_json};
      
      const container = document.getElementById('graph-container');
      const W = container.clientWidth, H = container.clientHeight;
      const svg = d3.select('#graph-svg').attr('width', W).attr('height', H);
      
      const zoom = d3.zoom().scaleExtent([0.15, 5]).on('zoom', e => g.attr('transform', e.transform));
      svg.call(zoom);
      const g = svg.append('g');
      
      const COLOR = {{ Issue:'#4f9eff', Person:'#3ecf8e', Component:'#ffd166', Claim:'#b57bee', Label:'#ff8c42' }};
      const RADIUS = {{ Issue:8, Person:7, Component:9, Claim:5, Label:6 }};
      
      const nodeIds = new Set(nodes.map(n => n.id));
      const validEdges = edges.filter(e => nodeIds.has(e.source) && nodeIds.has(e.target));
      
      const sim = d3.forceSimulation(nodes)
        .force('link', d3.forceLink(validEdges).id(d => d.id).distance(55).strength(0.35))
        .force('charge', d3.forceManyBody().strength(-180))
        .force('center', d3.forceCenter(W/2, H/2))
        .force('collision', d3.forceCollide().radius(d => (RADIUS[d.type]||6)+5));
      
      const link = g.append('g').selectAll('line').data(validEdges).join('line')
        .attr('stroke', '#8899bb').attr('stroke-width', 1.5).attr('stroke-opacity', 0.85);
      
      const node = g.append('g').selectAll('g').data(nodes).join('g')
        .attr('cursor', 'pointer')
        .call(d3.drag()
          .on('start', (e,d) => {{ if(!e.active) sim.alphaTarget(0.3).restart(); d.fx=d.x; d.fy=d.y; }})
          .on('drag', (e,d) => {{ d.fx=e.x; d.fy=e.y; }})
          .on('end', (e,d) => {{ if(!e.active) sim.alphaTarget(0); d.fx=null; d.fy=null; }})
        );
      
      node.append('circle')
        .attr('r', d => RADIUS[d.type]||6)
        .attr('fill', d => COLOR[d.type]||'#666')
        .attr('fill-opacity', 0.85)
        .attr('stroke', d => COLOR[d.type]||'#666')
        .attr('stroke-width', 1.5)
        .attr('stroke-opacity', 0.35);
      
      node.append('text')
        .attr('dy', d => -(RADIUS[d.type]||6)-4)
        .attr('text-anchor', 'middle')
        .attr('font-size', '8px')
        .attr('fill', '#6b7a99')
        .attr('pointer-events', 'none')
        .text(d => {{
          const label = d.title || d.login || d.name || d.value || d.id;
          return label.length > 20 ? label.slice(0,18)+'…' : label;
        }});
      
      node.append('title').text(d => {{
        const label = d.title || d.login || d.name || d.value || d.id;
        return d.type + ': ' + label;
      }});
      
      sim.on('tick', () => {{
        link.attr('x1',d=>d.source.x).attr('y1',d=>d.source.y)
            .attr('x2',d=>d.target.x).attr('y2',d=>d.target.y);
        node.attr('transform', d=>`translate(${{d.x}},${{d.y}})`);
      }});
    }})();
    </script>
    """
    return html


# ── Render helpers ────────────────────────────────────────────────────────────

def render_claim_html(c):
    conf = round((c.get("confidence") or 0.7) * 100)
    color = "#3ecf8e" if conf > 80 else "#ffd166" if conf > 60 else "#f06292"
    evidence_html = ""
    for ev in (c.get("evidence") or []):
        if ev and ev.get("excerpt"):
            evidence_html += f'<div class="evidence-box">{ev["excerpt"]}</div>'
            src = ev.get("source_id", "")
            url = ev.get("url", "")
            ts = (ev.get("timestamp") or "")[:10]
            evidence_html += f'<div class="evidence-src">📎 {src}'
            if url:
                evidence_html += f' · <a href="{url}" target="_blank">source ↗</a>'
            if ts:
                evidence_html += f' · {ts}'
            evidence_html += '</div>'

    return f"""
    <div class="claim-card">
      <div class="claim-type">{c.get("claim_type", "")}</div>
      <div class="claim-value">{c.get("value") or c.get("subject_id", "")}</div>
      <div class="conf-bar">
        <div class="conf-track"><div class="conf-fill" style="width:{conf}%;background:{color}"></div></div>
        <span class="conf-text">{conf}%</span>
        {f'<span class="tag">{c["valid_from"][:10]}</span>' if c.get("valid_from") else ''}
      </div>
      {evidence_html}
    </div>"""


# ── MAIN APP ──────────────────────────────────────────────────────────────────

# Header
st.markdown("""
<div class="logo-row">
  <div class="logo-dot"></div>
  <div class="logo-text">layer<span>10</span></div>
</div>
""", unsafe_allow_html=True)

# Load data
graph = load_graph_data()
stats = graph.get("stats", {})

# Stats row
st.markdown(f"""
<div class="stat-row">
  <div class="stat-pill"><div class="dot dot-issue"></div>{stats.get('Issue',0)} issues</div>
  <div class="stat-pill"><div class="dot dot-person"></div>{stats.get('Person',0)} persons</div>
  <div class="stat-pill"><div class="dot dot-component"></div>{stats.get('Component',0)} components</div>
  <div class="stat-pill"><div class="dot dot-claim"></div>{stats.get('Claim',0)} claims</div>
  <div class="stat-pill"><div class="dot dot-label"></div>{stats.get('Label',0)} labels</div>
</div>
""", unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("### � Pipeline Runner")

    current_repo = os.getenv("GITHUB_REPO", "microsoft/vscode")
    new_repo = st.text_input("GitHub Repo", value=current_repo,
                             placeholder="owner/repo  (e.g. facebook/react)")
    max_issues = st.slider("Max issues to fetch", 10, 200, 50, step=10)

    col_run1, col_run2 = st.columns(2)
    skip_fetch = col_run1.checkbox("Skip fetch", False)
    skip_extract = col_run2.checkbox("Skip extract", False)
    skip_graph = col_run1.checkbox("Skip graph", False)
    skip_vector = col_run2.checkbox("Skip vector", False)

    # Initialise session state for pipeline
    if "pipeline_running" not in st.session_state:
        st.session_state.pipeline_running = False
    if "pipeline_log" not in st.session_state:
        st.session_state.pipeline_log = ""
    if "pipeline_done" not in st.session_state:
        st.session_state.pipeline_done = False

    run_clicked = st.button("▶ Run Pipeline", type="primary",
                            disabled=st.session_state.pipeline_running,
                            use_container_width=True)

    if run_clicked and not st.session_state.pipeline_running:
        # ── Update .env with new repo ─────────────────────────────────────
        env_path = PROJECT_ROOT / ".env"
        env_text = env_path.read_text()
        if f"GITHUB_REPO={current_repo}" in env_text:
            env_text = env_text.replace(f"GITHUB_REPO={current_repo}",
                                        f"GITHUB_REPO={new_repo}")
        elif "GITHUB_REPO=" in env_text:
            import re
            env_text = re.sub(r"GITHUB_REPO=.*", f"GITHUB_REPO={new_repo}", env_text)
        else:
            env_text += f"\nGITHUB_REPO={new_repo}\n"
        env_path.write_text(env_text)
        os.environ["GITHUB_REPO"] = new_repo

        # ── Clear old output cache when switching repos ───────────────────
        if new_repo != current_repo and not skip_fetch:
            for folder in ["raw_issues", "extractions", "dedup", "context_packs"]:
                p = PROJECT_ROOT / "outputs" / folder
                if p.exists():
                    shutil.rmtree(p)
            for f in (PROJECT_ROOT / "outputs").glob("*.json"):
                f.unlink(missing_ok=True)

        # ── Build command ─────────────────────────────────────────────────
        cmd = [PYTHON_EXE, str(PROJECT_ROOT / "run_pipeline.py"),
               "--max-issues", str(max_issues)]
        if skip_fetch:
            cmd.append("--skip-fetch")
        if skip_extract:
            cmd.append("--skip-extract")
        if skip_graph:
            cmd.append("--skip-graph")
        if skip_vector:
            cmd.append("--skip-vector")

        st.session_state.pipeline_running = True
        st.session_state.pipeline_done = False
        st.session_state.pipeline_log = f"$ {' '.join(cmd)}\n\n"

        # ── Run in subprocess (non-blocking with thread) ──────────────────
        def _run_pipeline():
            try:
                proc = subprocess.Popen(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, cwd=str(PROJECT_ROOT), bufsize=1,
                    env={**os.environ, "GITHUB_REPO": new_repo,
                         "PYTHONUNBUFFERED": "1"},
                )
                for line in proc.stdout:
                    st.session_state.pipeline_log += line
                proc.wait()
                rc = proc.returncode
                st.session_state.pipeline_log += (
                    f"\n{'✅ PIPELINE COMPLETE' if rc == 0 else '❌ PIPELINE FAILED (exit '+str(rc)+')'}\n"
                )
            except Exception as ex:
                st.session_state.pipeline_log += f"\n❌ Error: {ex}\n"
            finally:
                st.session_state.pipeline_running = False
                st.session_state.pipeline_done = True
                # Clear cached data so the dashboard reloads from fresh DB
                load_graph_data.clear()
                load_graph_stats.clear()

        thread = threading.Thread(target=_run_pipeline, daemon=True)
        thread.start()
        st.rerun()

    # Show live status
    if st.session_state.pipeline_running:
        st.info("⏳ Pipeline running…")
        time.sleep(2)
        st.rerun()

    if st.session_state.pipeline_done:
        if "COMPLETE" in st.session_state.pipeline_log:
            st.success("✅ Pipeline finished!")
        else:
            st.error("❌ Pipeline had errors")

    st.markdown("---")
    st.markdown("### �🔍 Explorer")

    st.markdown("**Node Filters**")
    col1, col2 = st.columns(2)
    show_issues = col1.checkbox("Issues", True)
    show_persons = col2.checkbox("People", True)
    show_components = col1.checkbox("Components", True)
    show_claims = col2.checkbox("Claims", True)

    st.markdown("---")
    st.markdown("**Entity List**")

    # Filter nodes for sidebar list
    visible_types = set()
    if show_issues:
        visible_types.add("Issue")
    if show_persons:
        visible_types.add("Person")
    if show_components:
        visible_types.add("Component")
    if show_claims:
        visible_types.add("Claim")

    sidebar_nodes = [n for n in graph["nodes"] if n["type"] in visible_types]
    type_order = {"Issue": 0, "Component": 1, "Person": 2, "Claim": 3}
    sidebar_nodes.sort(key=lambda n: type_order.get(n["type"], 9))

    type_colors = {"Issue": "#4f9eff", "Person": "#3ecf8e", "Component": "#ffd166", "Claim": "#b57bee"}

    # Show entity list in sidebar
    for n in sidebar_nodes[:80]:
        label = n.get("title") or n.get("login") or n.get("name") or n.get("value") or n["id"]
        if len(label) > 40:
            label = label[:38] + "…"
        color = type_colors.get(n["type"], "#666")
        meta = ""
        if n["type"] == "Issue":
            meta = f"#{n.get('number', '')} · {n.get('state', '')}"
        elif n["type"] == "Claim":
            meta = f"{n.get('claim_type', '')} · {round((n.get('confidence') or 0) * 100)}%"

        st.markdown(f"""
        <div class="entity-card" style="padding:8px 12px;margin-bottom:4px">
          <div style="display:flex;align-items:flex-start;gap:8px">
            <div style="width:7px;height:7px;border-radius:50%;background:{color};margin-top:5px;flex-shrink:0"></div>
            <div>
              <div style="font-size:11px;color:#c9d1e0;line-height:1.3">{label}</div>
              {'<div style="font-size:10px;color:#3d4f70;margin-top:1px">'+meta+'</div>' if meta else ''}
            </div>
          </div>
        </div>
        """, unsafe_allow_html=True)

    if len(sidebar_nodes) > 80:
        st.caption(f"… and {len(sidebar_nodes) - 80} more")


# ── Main content tabs ─────────────────────────────────────────────────────────

tab_graph, tab_search, tab_detail, tab_merges, tab_pipeline = st.tabs(["🕸️ Graph", "🔍 Search", "📋 Entity Detail", "📊 Merges & Audit", "🚀 Pipeline Log"])

# ── TAB: GRAPH ────────────────────────────────────────────────────────────────
with tab_graph:
    filtered_nodes = [n for n in graph["nodes"] if n["type"] in visible_types]
    node_ids = set(n["id"] for n in filtered_nodes)
    filtered_edges = [e for e in graph["edges"] if e["source"] in node_ids and e["target"] in node_ids]

    if filtered_nodes:
        html = render_d3_graph(filtered_nodes, filtered_edges)
        st.components.v1.html(html, height=620, scrolling=False)
        st.caption(f"Showing {len(filtered_nodes)} nodes, {len(filtered_edges)} edges")
    else:
        st.info("No graph data. Run the pipeline first: `python run_pipeline.py --max-issues 50`")


# ── TAB: SEARCH ──────────────────────────────────────────────────────────────
with tab_search:
    st.markdown("### Semantic Search")
    st.markdown("Ask anything about VSCode issues — powered by Qdrant vector search + Neo4j graph.")

    query = st.text_input("🔍 Ask a question", placeholder="What are the most common terminal bugs?")

    if query:
        with st.spinner("Searching…"):
            pack = do_search(query)

        if pack:
            results = pack.get("ranked_results", [])
            st.markdown(f"**{len(results)} results** for *\"{query}\"*")

            for r in results:
                score = round((r.get("score", 0)) * 100)
                title = r.get("title") or r.get("value") or r.get("id", "")
                summary = r.get("summary", "")
                state = r.get("state", "")
                labels = r.get("labels", [])
                url = r.get("url", "")
                rtype = r.get("type", "")

                labels_html = " ".join(f'<span class="tag">{l}</span>' for l in labels[:3])
                state_html = f'<span class="tag tag-{state}">{state}</span>' if state else ""

                ev_html = ""
                for ex in (r.get("evidence_excerpts") or [])[:1]:
                    if ex:
                        ev_html = f'<div class="evidence-box">{ex[:200]}</div>'

                st.markdown(f"""
                <div class="result-card">
                  <span class="result-score">{score}%</span>
                  <div class="result-title">{title}</div>
                  {'<div class="result-summary">'+summary[:150]+'</div>' if summary else ''}
                  <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;margin-top:4px">
                    <span class="tag tag-{rtype.lower()}">{rtype}</span>
                    {state_html} {labels_html}
                    {'<a href="'+url+'" target="_blank" style="font-size:10px;color:#4f9eff;text-decoration:none">↗ GitHub</a>' if url else ''}
                  </div>
                  {ev_html}
                </div>
                """, unsafe_allow_html=True)

            # Citations
            citations = pack.get("citations", [])
            if citations:
                st.markdown("---")
                st.markdown("**Citations**")
                for c in citations:
                    st.markdown(f"- [{c.get('title', c.get('id', ''))}]({c.get('url', '#')}) — `{c.get('ref', '')}`")
        else:
            st.warning("No results. Make sure the pipeline has been run and Qdrant is accessible.")


# ── TAB: ENTITY DETAIL ───────────────────────────────────────────────────────
with tab_detail:
    st.markdown("### Entity Detail")

    # Issue lookup
    col1, col2 = st.columns([1, 2])
    with col1:
        issue_number = st.number_input("Issue #", min_value=1, value=None, step=1, placeholder="e.g. 299425")
    with col2:
        entity_id = st.text_input("Or Entity ID", placeholder="e.g. github:bpasero or component:terminal")

    if issue_number:
        issue_data, claims = get_issue_detail(int(issue_number))
        if issue_data:
            st.markdown(f"""
            <div class="entity-card">
              <div class="entity-title">{issue_data.get('title', '')}</div>
              <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;margin:6px 0">
                <span class="tag tag-issue">Issue</span>
                <span class="tag tag-{'open' if issue_data.get('state')=='open' else 'closed'}">{issue_data.get('state','')}</span>
                {'<a href="'+issue_data['url']+'" target="_blank" style="font-size:10px;color:#4f9eff;text-decoration:none">↗ GitHub</a>' if issue_data.get('url') else ''}
              </div>
              <div class="entity-meta">Authors: {', '.join(issue_data.get('authors', []))}</div>
              <div class="entity-meta">Labels: {', '.join(issue_data.get('labels', []))}</div>
              {'<div style="font-size:11px;color:#6b7a99;line-height:1.5;margin-top:8px">'+issue_data.get('summary','')+'</div>' if issue_data.get('summary') else ''}
            </div>
            """, unsafe_allow_html=True)

            if claims:
                st.markdown('<div class="section-hdr">Claims & Evidence</div>', unsafe_allow_html=True)
                for c in claims:
                    st.markdown(render_claim_html(c), unsafe_allow_html=True)
            else:
                st.info("No claims extracted for this issue.")
        else:
            st.warning(f"Issue #{issue_number} not found in the graph.")

    elif entity_id:
        claims = get_entity_claims(entity_id)
        if claims:
            st.markdown(f'<div class="section-hdr">Claims for {entity_id}</div>', unsafe_allow_html=True)
            for c in claims:
                st.markdown(render_claim_html(c), unsafe_allow_html=True)
        else:
            st.info(f"No claims found for entity `{entity_id}`.")


# ── TAB: MERGES & AUDIT ──────────────────────────────────────────────────────
with tab_merges:
    st.markdown("### Merge Audit Log")

    merges, conflicts, dupes = load_merge_data()

    # Duplicates
    st.markdown(f'<div class="section-hdr">Duplicate Issues ({len(dupes)})</div>', unsafe_allow_html=True)
    if dupes:
        for dup, canonical in dupes.items():
            st.markdown(f"""
            <div class="log-item">
              <div class="log-action">DUPLICATE_DETECTED</div>
              <div class="log-detail">{dup} → {canonical}</div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.caption("No duplicates detected.")

    # Conflicts
    st.markdown(f'<div class="section-hdr">Conflicts ({len(conflicts)})</div>', unsafe_allow_html=True)
    if conflicts:
        for c in conflicts:
            st.markdown(f"""
            <div class="log-item">
              <div class="log-action" style="color:#f06292">CONFLICT: {c.get('key','')}</div>
              <div class="log-detail">Claims: {', '.join(c.get('claims', []))}</div>
              <div class="log-detail">Values: {' | '.join(str(v) for v in c.get('values', []))}</div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.caption("No conflicts found.")

    # Merge events
    st.markdown(f'<div class="section-hdr">Merge Events ({len(merges)})</div>', unsafe_allow_html=True)
    if merges:
        for m in merges[:50]:
            action = m.get("action", "MERGE")
            detail = json.dumps(m, default=str)[:150]
            st.markdown(f"""
            <div class="log-item">
              <div class="log-action">{action}</div>
              <div class="log-detail">{detail}</div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.caption("No merge events.")

# ── TAB: PIPELINE LOG ────────────────────────────────────────────────────────
with tab_pipeline:
    st.markdown("### 🚀 Pipeline Log")
    st.markdown(f"**Current repo:** `{os.getenv('GITHUB_REPO', 'microsoft/vscode')}`")

    log_text = st.session_state.get("pipeline_log", "")
    if log_text:
        st.code(log_text, language="text", line_numbers=False)
    else:
        st.info("No pipeline has been run yet. Use the sidebar to configure and launch one.")

    if st.session_state.get("pipeline_done"):
        if st.button("🔄 Refresh Dashboard Data"):
            load_graph_data.clear()
            load_graph_stats.clear()
            st.rerun()

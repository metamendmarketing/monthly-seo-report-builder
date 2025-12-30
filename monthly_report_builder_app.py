import io, os, re, json, datetime, base64
import email.utils
from typing import Dict, Optional, List, Tuple, Any

import streamlit as st
from openai import OpenAI

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.generator import BytesGenerator
from email import policy

APP_TITLE = "Metamend Monthly SEO Email Builder"
DEFAULT_MODEL = "gpt-5.2"
TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), "templates", "monthly_email_template.html")

# ---------- helpers ----------
def ss_init(key: str, default):
    if key not in st.session_state:
        st.session_state[key] = default

def strip_code_fences(s: str) -> str:
    s = (s or "").strip()
    if s.startswith("```"):
        s = re.sub(r"^```[a-zA-Z]*\n", "", s)
        s = re.sub(r"\n```$", "", s)
    return s.strip()

def _safe_json_load(s: str) -> Any:
    s = strip_code_fences(s)
    try:
        return json.loads(s)
    except Exception:
        m = re.search(r"(\{.*\}|\[.*\])", s, flags=re.S)
        if not m:
            return None
        try:
            return json.loads(m.group(0))
        except Exception:
            return None

def get_api_key() -> Optional[str]:
    try:
        if "OPENAI_API_KEY" in st.secrets:
            v = str(st.secrets["OPENAI_API_KEY"]).strip()
            return v or None
    except Exception:
        pass
    v = (os.getenv("OPENAI_API_KEY") or "").strip()
    return v or None

def load_template() -> str:
    with open(TEMPLATE_PATH, "r", encoding="utf-8") as f:
        return f.read()

def html_escape(s: str) -> str:
    return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def bullets_to_html(items: List[str]) -> str:
    items = [i.strip() for i in (items or []) if i and i.strip()]
    if not items:
        return ""
    # Keep styles minimal so Outlook inherits the user's default font (e.g., Aptos).
    lis = "\n".join([f'<li style="margin:6px 0;">{html_escape(i)}</li>' for i in items])
    return f'<ul style="margin:8px 0 0 20px;padding:0;">{lis}</ul>'

def section_block(title: str, body_html: str) -> str:
    if not body_html.strip():
        return ""
    # Use simple div blocks (not nested tables) and inherit typography from the template wrapper.
    return f"""
<div style="margin:0 0 12px 0;">
  <div style="font-weight:700;margin:0 0 6px 0;">{html_escape(title)}</div>
  <div style="margin:0;">{body_html}</div>
</div>
""".strip()

def image_block(cid: str, caption: str = "") -> str:
    cap = ""
    if (caption or "").strip():
        cap = f'<div style="font-size:10.5pt;color:#374151;margin-top:6px;line-height:1.35;">{html_escape(caption)}</div>'
    return f"""
<div style="margin:10px 0 12px 0;">
  <img src="cid:{cid}" style="width:100%;height:auto;max-width:900px;border:1px solid #e5e7eb;display:block;" />
  {cap}
</div>
""".strip()

def build_eml(subject: str, html_body: str, images: List[Tuple[str, bytes]]) -> bytes:
    msg = MIMEMultipart("related")
    msg["Subject"] = subject or "SEO Monthly Update"
    # Make .eml open as an editable draft in Outlook-compatible clients
    msg["To"] = msg.get("To", "")
    msg["From"] = msg.get("From", os.getenv("DEFAULT_FROM_EMAIL", "kosborne@metamend.com"))
    msg["Date"] = email.utils.formatdate(localtime=True)
    msg["X-Unsent"] = "1"
    # Some clients also respect this header name
    msg["X-Unsent-Flag"] = "1"
    msg["MIME-Version"] = "1.0"

    # Outlook can sometimes show the text/plain part when opening .eml files.
    # To avoid a duplicated/strange top block, include HTML only.
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    for cid, b in images:
        img = MIMEImage(b)
        img.add_header("Content-ID", f"<{cid}>")
        img.add_header("Content-Disposition", "inline", filename=f"{cid}.png")
        msg.attach(img)

    buf = io.BytesIO()
    BytesGenerator(buf, policy=policy.default).flatten(msg)
    return buf.getvalue()

def gpt_generate_email(client: OpenAI, model: str, payload: dict, synthesis_images: List[bytes]) -> Tuple[dict, str]:
    # Keep the same section structure across modes. The ONLY thing that changes by verbosity
    # is how much context is included within the same sections.
    v = (payload.get("verbosity_level") or "Quick scan").strip().lower()
    if v.startswith("quick"):
        schema = {
            "subject": "string",
            "monthly_overview": "2-3 sentences (max)",
            "key_highlights": ["3-4 bullets (max)"],
            "wins_progress": ["2-3 bullets (max)"],
            "blockers": ["1-3 bullets (max)"],
            "completed_tasks": ["3-5 bullets (max)"],
            "outstanding_tasks": ["3-5 bullets (max)"],
            "image_captions": [{"file_name":"exact filename","caption":"optional","suggested_section":"wins_progress|key_highlights|blockers|completed_tasks|outstanding_tasks"}],
            "dashthis_line": "short 1 sentence"
        }
    elif v.startswith("deep"):
        schema = {
            "subject": "string",
            "monthly_overview": "3-4 sentences (max)",
            "key_highlights": ["4-6 bullets (max)"],
            "wins_progress": ["3-6 bullets (max)"],
            "blockers": ["2-5 bullets (max)"],
            "completed_tasks": ["5-10 bullets (max)"],
            "outstanding_tasks": ["5-10 bullets (max)"],
            "image_captions": [{"file_name":"exact filename","caption":"optional","suggested_section":"wins_progress|key_highlights|blockers|completed_tasks|outstanding_tasks"}],
            "dashthis_line": "1-2 sentences (max)"
        }
    else:
        # Standard
        schema = {
            "subject": "string",
            "monthly_overview": "3-4 sentences (max)",
            "key_highlights": ["3-5 bullets (max)"],
            "wins_progress": ["3-5 bullets (max)"],
            "blockers": ["2-4 bullets (max)"],
            "completed_tasks": ["4-8 bullets (max)"],
            "outstanding_tasks": ["4-8 bullets (max)"],
            "image_captions": [{"file_name":"exact filename","caption":"optional","suggested_section":"wins_progress|key_highlights|blockers|completed_tasks|outstanding_tasks"}],
            "dashthis_line": "1 sentence"
        }

    system = """You are a senior SEO consultant writing a MONTHLY client update email.

Style and tone:
- Write like a real person emailing a client you know well.
- Friendly, professional, and focused on what matters to the client.
- Use plain English (avoid marketing jargon, buzzwords, and hype).
- Contractions are encouraged where natural.
- Do not include over-the-top pleasantries.
- Do NOT mention confidence labels.
- Do not say things like "Technical updates shipped" or "knocked out technical updates" or "wrapped up technical updates".

Examples of preferred language style:
- “We addressed several technical issues that were causing…”
- “We resolved a canonical redirect issue that was causing Google to crawl fewer pages.”
- “We identified and corrected an indexing issue affecting…”
- “We updated internal linking to support…”

Content rules:
- Use Omni notes as the source of truth for work completed, in-progress work, blockers, and context.
- Do NOT invent metrics, results, or causality. If impact obviously correlates to work completed or insights gained from screenshots mention it, otherwise say nothing about it.
- If uploaded screenshots show explicit labels, numbers, or statuses, reference them to provide context, observations or an insight, if there is a direct correlation between results or work completed mention it.
- Avoid repeating exact date ranges. Prefer language like “this month, in December we, during the period”.

Verbosity control:
Adjust wording based on CONTEXT.verbosity_level. Do NOT add new sections in any mode.
- Quick scan (default): ultra brief and scannable.
  * Monthly Overview: max 2–3 sentences.
  * Pick only the most important items; drop routine maintenance/cadence items unless they were a major focus this month.
  * Bullets should be short and skimmable (no semicolons, no multi-clause sentences).
- Standard: normal monthly email. Monthly Overview 3–4 sentences. Bullets may include brief context (a short clause).
- Deep dive: most explanation, but within the SAME sections and without adding new bullets beyond the limits implied by the schema.
  * Add context inside the existing bullets (one extra sentence max per bullet), not extra bullets.

Noise filtering:
- Do not include “reporting about reporting” (e.g., “we sent the monthly report”) unless it materially affected delivery.
- Deduplicate repeated items across sections.

Output requirements:
- Output MUST be valid JSON only and must match the schema provided. No markdown or extra commentary.
"""

    prompt = (
        "Create a monthly SEO update email draft.\n\n"
        f"CONTEXT:\n{json.dumps(payload, indent=2)}\n\n"
        f"OUTPUT SCHEMA:\n{json.dumps(schema, indent=2)}"
    )

    content = [{"type":"input_text","text":prompt}]
    for im in synthesis_images:
        content.append({"type":"input_image","image_url":"data:image/png;base64," + base64.b64encode(im).decode()})

    resp = client.responses.create(
        model=model,
        input=[{"role":"system","content":system},{"role":"user","content":content}],
        temperature=0.25,
    )
    raw = resp.output_text or ""
    data = _safe_json_load(raw)
    return (data if isinstance(data, dict) else {"_parse_failed": True, "_error": "No JSON"}), raw

# ---------- UI ----------
# Centered, single-column layout so users can scroll straight down to the draft.
st.set_page_config(page_title=APP_TITLE, layout="centered")
st.markdown("""
<style>
  /* Align with quarterly tool: compact headers, consistent spacing */
  .block-container { padding-top: 1.2rem; padding-bottom: 2.2rem; }
  h1 { margin-bottom: 0.2rem; }
  /* Slightly tighter section spacing */
  div[data-testid="stVerticalBlock"] > div:has(> hr) { margin-top: 0.6rem; margin-bottom: 0.6rem; }
</style>
""", unsafe_allow_html=True)

st.markdown(f"# {APP_TITLE}")
st.caption("Builds a monthly SEO update email (HTML) and an Outlook-ready .eml with inline screenshots.")

api_key = get_api_key()
if not api_key:
    st.error("Missing OPENAI_API_KEY. Add it to Streamlit secrets or set OPENAI_API_KEY env var.")
    st.stop()

today = datetime.date.today()
ss_init("client_name","")
ss_init("website","")
ss_init("month_label", today.strftime("%B %Y"))
ss_init("report_range", (today.replace(day=1), today))
ss_init("dashthis_url","")

ss_init("omni_notes_paste_input","")
ss_init("omni_notes_pasted","")
ss_init("omni_added", False)
ss_init("verbosity_level", "Quick scan")


ss_init("uploaded_files", [])
ss_init("raw","")
ss_init("email_json", {})
ss_init("image_assignments", {})
ss_init("image_captions", {})

with st.expander("Inputs", expanded=True):
    st.subheader("Details")
    st.session_state.client_name = st.text_input("Client name", value=st.session_state.client_name)
    st.session_state.website = st.text_input("Website", value=st.session_state.website, placeholder="https://...")
    st.session_state.month_label = st.text_input("Month label", value=st.session_state.month_label, placeholder="March 2026")

    rr = st.session_state.report_range
    st.session_state.report_range = st.date_input("Reporting range", value=rr, key="monthly_reporting_range")

    st.session_state.dashthis_url = st.text_input("DashThis report URL", value=st.session_state.dashthis_url)

    st.divider()
    st.subheader("Upload files + Omni notes")

    uploaded = st.file_uploader(
        "Upload screenshots / supporting docs (optional)",
        type=["png","jpg","jpeg","pdf","docx","txt"],
        accept_multiple_files=True
    ) or []
    st.session_state.uploaded_files = uploaded

    st.markdown("**Paste Omni notes from Client Dashboard.**")
    omni_cols = st.columns([6, 2, 2])
    with omni_cols[0]:
        st.text_input(
            "omni_notes_paste_input_label",
            placeholder="Paste Omni notes here…",
            key="omni_notes_paste_input",
            label_visibility="collapsed",
        )

    def _omni_add():
        txt = (st.session_state.get("omni_notes_paste_input") or "").strip()
        if txt:
            st.session_state.omni_notes_pasted = txt
            st.session_state.omni_added = True

    def _omni_clear():
        st.session_state.omni_notes_pasted = ""
        st.session_state.omni_added = False
        st.session_state["omni_notes_paste_input"] = ""

    with omni_cols[1]:
        st.button("Add", on_click=_omni_add, type="secondary", use_container_width=True)
    with omni_cols[2]:
        st.button("Clear", on_click=_omni_clear, type="secondary", use_container_width=True)

    if (st.session_state.omni_notes_pasted or "").strip():
        st.success("Omni work summary notes were detected and will be used for the report.")

st.subheader("Generate")
with st.expander("Settings", expanded=False):
    model = st.text_input("Model", value=DEFAULT_MODEL)
    show_raw = st.toggle("Show GPT output (troubleshooting)", value=False)
    st.radio(
        "Email length",
        ["Quick scan", "Standard", "Deep dive"],
        key="verbosity_level",
        help="Quick scan is ultra brief. Standard adds more context. Deep dive adds the most explanation within the same sections (no extra sections).",
    )

can_generate = bool((st.session_state.omni_notes_pasted or "").strip())
def _normalize_email_json(data: dict, verbosity_level: str) -> dict:
    """Hard guardrails so 'Quick scan' is materially shorter even if the model over-writes."""
    v = (verbosity_level or "Quick scan").strip().lower()
    if not isinstance(data, dict):
        return {}

    # Sentence limiter for overview
    def limit_sentences(txt: str, max_sentences: int) -> str:
        t = (txt or "").strip()
        if not t:
            return t
        parts = re.split(r"(?<=[.!?])\s+", t)
        parts = [p.strip() for p in parts if p.strip()]
        return " ".join(parts[:max_sentences])

    # List limiter
    def limit_list(key: str, max_items: int) -> None:
        items = data.get(key) or []
        if isinstance(items, list):
            data[key] = [str(x).strip() for x in items if str(x).strip()][:max_items]

    if v.startswith("quick"):
        data["monthly_overview"] = limit_sentences(data.get("monthly_overview", ""), 3)
        limit_list("key_highlights", 4)
        limit_list("wins_progress", 3)
        limit_list("blockers", 3)
        limit_list("completed_tasks", 5)
        limit_list("outstanding_tasks", 5)
        # Keep screenshots light in quick-scan mode.
        caps = data.get("image_captions") or []
        if isinstance(caps, list):
            data["image_captions"] = caps[:1]
    elif v.startswith("standard"):
        data["monthly_overview"] = limit_sentences(data.get("monthly_overview", ""), 4)
        limit_list("key_highlights", 5)
        limit_list("wins_progress", 5)
        limit_list("blockers", 4)
        limit_list("completed_tasks", 8)
        limit_list("outstanding_tasks", 8)
    else:  # deep dive
        data["monthly_overview"] = limit_sentences(data.get("monthly_overview", ""), 4)
        limit_list("key_highlights", 6)
        limit_list("wins_progress", 6)
        limit_list("blockers", 5)
        limit_list("completed_tasks", 10)
        limit_list("outstanding_tasks", 10)

    # Ensure basic fields exist
    data.setdefault("subject", "SEO Monthly Update")
    data.setdefault("dashthis_line", "For detailed performance, please use the DashThis dashboard link above.")
    return data


if st.button("Generate Email Draft", type="primary", disabled=not can_generate, use_container_width=True):
    client = OpenAI(api_key=api_key)

    # Synthesis images (only images are sent to the model)
    synthesis_images: List[bytes] = []
    for f in (uploaded or []):
        if f.name.lower().endswith((".png", ".jpg", ".jpeg")):
            synthesis_images.append(f.getvalue())

    payload = {
        "client_name": st.session_state.client_name.strip(),
        "website": st.session_state.website.strip(),
        "month_label": st.session_state.month_label.strip(),
        "reporting_period": f"{st.session_state.report_range[0]} to {st.session_state.report_range[1]}",
        "dashthis_url": st.session_state.dashthis_url.strip(),
        "omni_notes": st.session_state.omni_notes_pasted.strip(),
        "verbosity_level": st.session_state.get("verbosity_level", "Quick scan"),
        "uploaded_files": [f.name for f in (uploaded or [])],
    }

    with st.spinner("Drafting email..."):
        data, raw = gpt_generate_email(client, model, payload, synthesis_images)
        data = _normalize_email_json(data if isinstance(data, dict) else {}, payload["verbosity_level"])
        st.session_state.email_json = data
        st.session_state.raw = raw

    # Seed image assignment/captions suggestions
    for item in (st.session_state.email_json.get("image_captions") or []):
        fn = (item.get("file_name") or "").strip()
        if fn:
            suggested = (item.get("suggested_section") or "").strip()
            allowed_secs = {"key_highlights","wins_progress","blockers","completed_tasks","outstanding_tasks"}
            if suggested not in allowed_secs:
                suggested = "key_highlights"
            st.session_state.image_assignments.setdefault(fn, suggested)
            st.session_state.image_captions.setdefault(fn, item.get("caption") or "")

# Ensure every uploaded screenshot is included somewhere (auto placement).
for _f in st.session_state.uploaded_files:
    if _f.name.lower().endswith((".png",".jpg",".jpeg")):
        _fn = _f.name
        if st.session_state.image_assignments.get(_fn) not in {"key_highlights","wins_progress","blockers","completed_tasks","outstanding_tasks"}:
            st.session_state.image_assignments[_fn] = "key_highlights"
        st.session_state.image_captions.setdefault(_fn, "")

st.divider()
st.subheader("Draft (editable)")
data = st.session_state.email_json or {}
if not data:
    st.info("Generate a draft to begin. Omni notes are required; screenshots are optional.")
    st.stop()

# Keep the top of the page simple: subject + overview, with the rest in an expander.
subject = st.text_input("Subject", value=data.get("subject", ""))
monthly_overview = st.text_area("Monthly overview", value=data.get("monthly_overview", ""), height=120)

with st.expander("Edit sections", expanded=True):
    key_highlights = st.text_area("Key highlights (one per line)", value="\n".join(data.get("key_highlights") or []), height=150)
    wins_progress = st.text_area("Wins & progress (one per line)", value="\n".join(data.get("wins_progress") or []), height=170)
    blockers = st.text_area("Blockers / risks (one per line)", value="\n".join(data.get("blockers") or []), height=140)
    completed_tasks = st.text_area("Completed tasks (one per line)", value="\n".join(data.get("completed_tasks") or []), height=170)
    outstanding_tasks = st.text_area("Outstanding tasks (one per line)", value="\n".join(data.get("outstanding_tasks") or []), height=170)
    dashthis_line = st.text_area("DashThis line", value=data.get("dashthis_line", ""), height=70)

    st.divider()
    st.subheader("Screenshots (optional)")
    imgs = [f for f in (st.session_state.uploaded_files or []) if f.name.lower().endswith((".png",".jpg",".jpeg"))]
    if not imgs:
        st.caption("No screenshots uploaded.")
    else:
        with st.expander("Optional: adjust screenshot placement / captions", expanded=False):
            st.caption("By default, the app will place screenshots automatically. Use this only if you want to override placement or edit captions.")
            section_options = ["key_highlights","wins_progress","blockers","completed_tasks","outstanding_tasks"]
            for f in imgs:
                fn = f.name
                a, b, c = st.columns([2.2, 1.1, 2.3])
                with a:
                    st.write(fn)
                with b:
                    current = st.session_state.image_assignments.get(fn)
                    if current not in section_options:
                        current = section_options[0]
                    sel = st.selectbox(
                        "Section",
                        section_options,
                        index=section_options.index(current),
                        key=f"assign_{fn}",
                    )
                    st.session_state.image_assignments[fn] = sel
    
                with c:
                    cap = st.text_input("Caption", value=st.session_state.image_captions.get(fn,""), key=f"cap_{fn}")
                    st.session_state.image_captions[fn] = cap
    
    st.divider()
    st.subheader("Export")

    template = load_template()

    def _lines(s: str) -> List[str]:
        return [x.strip() for x in (s or "").splitlines() if x.strip()]

    highlights_list = _lines(key_highlights)
    wins_list = _lines(wins_progress)
    blockers_list = _lines(blockers)
    completed_list = _lines(completed_tasks)
    outstanding_list = _lines(outstanding_tasks)

    sec_high = section_block("Key highlights", bullets_to_html(highlights_list))
    sec_wins = section_block("Wins & progress", bullets_to_html(wins_list))
    sec_blk = section_block("Blockers / risks", bullets_to_html(blockers_list))
    sec_done = section_block("Completed tasks", bullets_to_html(completed_list))
    sec_next = section_block("Outstanding / rolling", bullets_to_html(outstanding_list))

    # Build CID map for all uploaded images (even if not placed, .eml can include; HTML will only reference placed)
    uploaded_map = {f.name: f.getvalue() for f in (st.session_state.uploaded_files or []) if f.name.lower().endswith((".png",".jpg",".jpeg"))}
    cids: Dict[str,str] = {}
    image_parts: List[Tuple[str, bytes]] = []
    image_mimes: Dict[str, str] = {}
    for i, fn in enumerate(sorted(uploaded_map.keys())):
        cid = f"img{i+1}"
        cids[fn] = cid
        image_parts.append((cid, uploaded_map[fn]))
        ext = fn.lower().rsplit(".", 1)[-1]
        image_mimes[cid] = "image/png" if ext == "png" else "image/jpeg"

    def append_images(section_html: str, section_key: str) -> str:
        out = [section_html] if section_html else []
        for fn, sec in st.session_state.image_assignments.items():
            if sec == section_key and fn in cids:
                out.append(image_block(cids[fn], st.session_state.image_captions.get(fn,"")))
        return "\n".join([x for x in out if x])

    sec_high = append_images(sec_high, "key_highlights")
    sec_wins = append_images(sec_wins, "wins_progress")
    sec_blk = append_images(sec_blk, "blockers")
    sec_done = append_images(sec_done, "completed_tasks")
    sec_next = append_images(sec_next, "outstanding_tasks")

    html_out = (template
        .replace("{{CLIENT_NAME}}", html_escape(st.session_state.client_name.strip() or "Client"))
        .replace("{{MONTH_LABEL}}", html_escape(st.session_state.month_label.strip() or "Monthly"))
        .replace("{{WEBSITE}}", html_escape(st.session_state.website.strip() or ""))
        .replace("{{MONTHLY_OVERVIEW}}", html_escape(monthly_overview or ""))
        .replace("{{DASHTHIS_URL}}", html_escape(st.session_state.dashthis_url.strip() or ""))
        .replace("{{DASHTHIS_LINE}}", html_escape(dashthis_line or ""))
        .replace("{{SECTION_KEY_HIGHLIGHTS}}", sec_high)
        .replace("{{SECTION_WINS_PROGRESS}}", sec_wins)
        .replace("{{SECTION_BLOCKERS}}", sec_blk)
        .replace("{{SECTION_COMPLETED_TASKS}}", sec_done)
        .replace("{{SECTION_OUTSTANDING_TASKS}}", sec_next)
    )

    eml_bytes = build_eml(subject, html_out, image_parts)
    # Build a preview-friendly HTML where cid: images are replaced with data URIs.
    preview_html = html_out
    for cid, b in image_parts:
        mime = image_mimes.get(cid, "image/png")
        data_uri = f"data:{mime};base64," + base64.b64encode(b).decode("utf-8")
        preview_html = preview_html.replace(f"cid:{cid}", data_uri)


    st.download_button("Download HTML", data=html_out.encode("utf-8"), file_name="monthly_seo_update.html", mime="text/html")
    st.download_button("Download .eml (Outlook-ready)", data=eml_bytes, file_name="monthly_seo_update.eml", mime="message/rfc822")

    with st.expander("Preview HTML"):
        st.components.v1.html(preview_html, height=600, scrolling=True)

    with st.expander("Copy/paste HTML (optional)"):
        st.code(html_out, language="html")

    if show_raw and st.session_state.raw:
        with st.expander("GPT output (raw)"):
            st.code(st.session_state.raw)
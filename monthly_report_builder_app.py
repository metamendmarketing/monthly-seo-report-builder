import io, os, re, json, datetime, base64
import email.utils
from typing import Dict, Optional, List, Tuple, Any

import streamlit as st

DEFAULT_TEMPLATE_HTML = """<!doctype html>
<html>
  <body style="margin:0;padding:0;background:#ffffff;">
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;background:#ffffff;">
      <tr>
        <!-- Left-aligned, full-width email body (no centered card). -->
        <td style="padding:18px 24px;background:#ffffff;
                   font-family:Aptos,Calibri,Arial,Helvetica,sans-serif;
                   font-size:12pt;line-height:1.45;color:#111827;
                   mso-fareast-font-family:Aptos;mso-bidi-font-family:Aptos;
                   mso-line-height-rule:exactly;">

          <!-- Title -->
          <div style="font-size:16pt;color:#1257c7;margin:0 0 4px 0;">{{CLIENT_NAME}} - SEO Monthly Update</div>
          <div style="font-size:10.5pt;color:#6b7280;margin:0 0 14px 0;">{{MONTH_LABEL}} · {{WEBSITE}}</div>

          <!-- Overview -->
          <div style="white-space:pre-wrap;margin:0 0 12px 0;">{{MONTHLY_OVERVIEW}}</div>

          <!-- DashThis (near the top) -->
          <div style="margin:0 0 12px 0;">
            <strong>DashThis Analytics dashboard:</strong>
            <a href="{{DASHTHIS_URL}}" style="color:#0b5bd3;font-weight:700;text-decoration:underline;">View live performance</a>
          </div>

          <!-- Divider -->
          <hr style="border:0;border-top:1px solid #d1d5db;margin:12px 0;" />

          <!-- Sections (no nested tables; inherit font) -->
          {{SECTION_KEY_HIGHLIGHTS}}
          <hr style="border:0;border-top:1px solid #d1d5db;margin:12px 0;" />

          {{SECTION_WINS_PROGRESS}}
          <hr style="border:0;border-top:1px solid #d1d5db;margin:12px 0;" />

          {{SECTION_BLOCKERS}}
          <hr style="border:0;border-top:1px solid #d1d5db;margin:12px 0;" />

          {{SECTION_COMPLETED_TASKS}}
          <hr style="border:0;border-top:1px solid #d1d5db;margin:12px 0;" />

          {{SECTION_OUTSTANDING_TASKS}}


          <!-- Closing line (keep minimal; Outlook signature should follow naturally) -->
          <div style="margin:14px 0 0 0;">Please let me know if you have any questions.</div>

<div style="margin:14px 0 0 0;">Thank you!</div>

        </td>
      </tr>
    </table>
  </body>
</html>
"""

import re
import os
import sys
import subprocess
import asyncio

# Optional PDF export via Playwright (Chromium print-to-PDF). Hidden if unavailable.
PLAYWRIGHT_AVAILABLE = True
try:
    from playwright.sync_api import sync_playwright
except Exception:
    PLAYWRIGHT_AVAILABLE = False

_PW_BOOTSTRAPPED = False

def ensure_playwright_chromium() -> None:
    """Best-effort Chromium install for Streamlit Cloud/container environments."""
    global _PW_BOOTSTRAPPED
    if _PW_BOOTSTRAPPED:
        return
    _PW_BOOTSTRAPPED = True

    # Never run this bootstrap on Windows/local dev.
    if os.name == "nt":
        return
    if not PLAYWRIGHT_AVAILABLE:
        return

    in_cloud = bool(
        os.environ.get("STREAMLIT_CLOUD")
        or os.environ.get("STREAMLIT_SHARING")
        or os.environ.get("STREAMLIT_RUNTIME_ENV")
        or os.environ.get("STREAMLIT_DEPLOYMENT")
        or os.environ.get("STREAMLIT_SERVER_HEADLESS")
        or os.environ.get("K_REVISION")      # Cloud Run
        or os.environ.get("RENDER")
        or os.environ.get("FLY_APP_NAME")
        or os.environ.get("ENABLE_PLAYWRIGHT_BOOTSTRAP") == "1"
    )
    if not in_cloud:
        return

    try:
        subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        pass

def html_to_pdf_bytes(html: str) -> bytes:
    ensure_playwright_chromium()
    if not PLAYWRIGHT_AVAILABLE:
        raise RuntimeError("Playwright is not available.")
    html = html or ""
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1100, "height": 1400})
        page.set_content(html, wait_until="networkidle")
        pdf_bytes = page.pdf(
            format="Letter",
            print_background=True,
            margin={"top": "0.75in", "bottom": "0.75in", "left": "0.75in", "right": "0.75in"},
        )
        browser.close()
        return pdf_bytes

# Playwright on Windows needs ProactorEventLoopPolicy for subprocess support.
if os.name == "nt":
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    except Exception:
        pass
from openai import OpenAI

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.generator import BytesGenerator
from email import policy

APP_TITLE = "Metamend Monthly SEO Email Builder"
DEFAULT_MODEL = "gpt-5.2"

# Canned opening lines (used by the Opening line suggestions)
CANNED_OPENERS = [
    "Hope you’re doing well — please see your monthly SEO status update below.",
    "Sharing this month’s SEO update below, including the key wins, opportunities, and next steps.",
    "Here’s your monthly SEO progress update — we’ve highlighted what moved, what it means, and what we’re prioritizing next.",
    "Below is the monthly SEO status update for {month_label}.",
    "Hope you’re having a great holiday season — please see your monthly SEO status update below.",
]


# --- Email signature presets (optional) ---
SIGNATURE_OPTIONS = ["None", "Kevin", "Simon", "Alisa", "Billy"]

SIGNATURE_DATA = {
    "Kevin": {
        "name": "Kevin Osborne",
        "title": "",
        "phone": "",
        "org": "Metamend Digital Marketing",
        "linkedin": "",
    },
    "Simon": {
        "name": "Simon Vreeswijk",
        "title": "",
        "phone": "",
        "org": "Metamend Digital Marketing",
        "linkedin": "",
    },
    "Alisa": {
        "name": "Alisa Miriev",
        "title": "Digital Marketing Analyst",
        "phone": "M: (416) 902-3245",
        "org": "Metamend Digital Marketing",
        "linkedin": "",
    },
    "Billy": {
        "name": "Billy Gacek",
        "title": "Paid Search Manager",
        "phone": "M: 1.778.875.8558",
        "org": "Metamend Digital Marketing",
        "linkedin": "https://www.linkedin.com/in/billygacek/",
    },
}

# Embedded Metamend logo for signatures (CID: sig_logo)
SIGNATURE_LOGO_PNG_B64 = """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
RU5ErkJggg=="""

def render_signature_html(choice: str) -> str:
    """Return Outlook-friendly signature HTML (Aptos 12px)."""
    if not choice or choice == "None":
        return ""
    sig = SIGNATURE_DATA.get(choice)
    if not sig:
        return ""
    # Inline styles for maximum email client compatibility
    font = "font-family:Aptos, 'Segoe UI', Arial, sans-serif; font-size:12pt; mso-ansi-font-size:12pt; mso-bidi-font-size:12pt; color:#000;"
    name_html = html_escape(sig.get("name",""))
    title_html = html_escape(sig.get("title",""))
    phone_html = html_escape(sig.get("phone",""))
    org_html = html_escape(sig.get("org",""))
    linkedin = (sig.get("linkedin") or "").strip()
    linkedin_html = ""
    if linkedin:
        href = html_escape(linkedin)
        linkedin_html = f'<div style="margin:2px 0 0 0; padding:0;"><a href="{href}" style="color:#0563C1; text-decoration:underline;">Linkedin</a></div>'
    # Only include lines that exist (Simon has no phone; Kevin has only org)
    title_line = f'<div style="margin:2px 0 0 0; padding:0;">{title_html}</div>' if title_html else ""
    phone_line = f'<div style="margin:2px 0 0 0; padding:0;">{phone_html}</div>' if phone_html else ""
    org_line = f'<div style="margin:2px 0 0 0; padding:0;">{org_html}</div>' if org_html else ""
    return f"""
    <div style="margin-top:16px;">
      <div style="{font}">
        <div style="font-weight:700; margin:0; padding:0;">{name_html}</div>
        {title_line}
        {org_line}
        {phone_line}
        {linkedin_html}
        <div style="margin-top:8px;">
          <img src="cid:sig_logo" alt="Metamend" style="display:block; height:34px; border:0; outline:none; text-decoration:none;" />
        </div>
      </div>
    </div>
    """
TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), "monthly_email_template.html")
if not os.path.exists(TEMPLATE_PATH):
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
        cap = f'<div style="font-size:10.5pt;color:#374151;margin-top:6px;line-height:1.35;font-style:italic;">{html_escape(caption)}</div>'
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
TEMPLATE_HTML = load_template()

st.set_page_config(page_title=APP_TITLE, layout="centered")
st.markdown("""
<style>
  /* Align with quarterly tool: compact headers, consistent spacing */
  .block-container { padding-top: 1.2rem; padding-bottom: 2.2rem; }
  h1 { margin-bottom: 0.2rem; font-size: 1.75rem; }
  /* Slightly tighter section spacing */
  div[data-testid="stVerticalBlock"] > div:has(> hr) { margin-top: 0.6rem; margin-bottom: 0.6rem; }
</style>
""", unsafe_allow_html=True)

st.markdown(f"<h1 style=\"font-size:1.75rem; margin:0 0 0.75rem 0;\">{APP_TITLE}</h1>", unsafe_allow_html=True)
st.caption("Builds a monthly SEO update email (HTML) and an Outlook-ready .eml with inline screenshots.")

api_key = get_api_key()
if not api_key:
    st.error("Missing OPENAI_API_KEY. Add it to Streamlit secrets or set OPENAI_API_KEY env var.")
    st.stop()

today = datetime.date.today()
ss_init("client_name","")
ss_init("website","")
ss_init("month_label", today.strftime("%B %Y"))
ss_init("dashthis_url","")
ss_init("signature_choice","None")

ss_init("recipient_first_name","")
ss_init("opening_line_choice","Custom…")
ss_init("opening_line","")
ss_init("show_opening_suggestions", False)

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
    st.session_state.dashthis_url = st.text_input("DashThis report URL", value=st.session_state.dashthis_url)

    # Email signature (optional) — appended to bottom of the email
    st.session_state.signature_choice = st.selectbox(
        "Email signature (Optional)",
        options=SIGNATURE_OPTIONS,
        index=SIGNATURE_OPTIONS.index(st.session_state.get("signature_choice", "None")) if st.session_state.get("signature_choice", "None") in SIGNATURE_OPTIONS else 0,
    )

    # Recipient + Opening line (optional)
    st.session_state.recipient_first_name = st.text_input(
        "Recipient first name(s) (Optional)",
        value=st.session_state.get("recipient_first_name", ""),
    )

    # Opening line (optional) — set via Suggestions popover (no separate field)
    # Keep the currently selected suggestion in sync (if the current opening line matches a canned option)
    cur_ol = (st.session_state.get("opening_line") or "").strip()
    if cur_ol and cur_ol in CANNED_OPENERS:
        st.session_state.opening_line_choice = cur_ol
    else:
        st.session_state.opening_line_choice = ""

    st.markdown("**Opening line (Optional)**")
    with st.popover("Suggestions", use_container_width=False):
        def _apply_opener_choice():
            choice = (st.session_state.get("opening_line_choice") or "").strip()
            if not choice:
                return
            ml = (st.session_state.get("month_label") or "").strip()
            st.session_state.opening_line = choice.replace("{month_label}", ml if ml else "this month")

        st.selectbox(
            "Opening line suggestions",
            options=[""] + CANNED_OPENERS,
            key="opening_line_choice",
            format_func=lambda x: "Select a suggestion…" if x == "" else x,
            on_change=_apply_opener_choice,
        )

        st.text_area(
            "Opening line (custom)",
            key="opening_line",
            placeholder="e.g., Hope you’re doing well — please see your monthly SEO status update below.",
            height=90,
        )

        st.caption("Pick a canned opener to fill the line, or type your own. Your latest text is what will be used.")

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
    st.subheader("Screenshots Placement")
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


    # Greeting block (optional): salutation + opening line (both editable)
    rec = (st.session_state.get("recipient_first_name") or "").strip()
    opener = (st.session_state.get("opening_line") or "").strip()
    greeting_parts = []
    if rec:
        greeting_parts.append(f'<div style="margin:0 0 6px 0;">Hi {html_escape(rec)},</div>')
    if opener:
        greeting_parts.append(f'<div style="margin:0 0 12px 0;">{html_escape(opener)}</div>')
    greeting_block_html = "\n".join(greeting_parts) if greeting_parts else ""

    # Signature block (optional) appended at bottom of email
    signature_choice = st.session_state.get("signature_choice", "None")
    signature_block_html = render_signature_html(signature_choice)

    # If a signature is selected, embed the signature logo as an inline CID image so it renders in Outlook and Preview.
    if signature_choice and signature_choice != "None" and signature_block_html:
        try:
            _sig_b64 = (SIGNATURE_LOGO_PNG_B64 or "").strip().replace("\n", "")
            _sig_bytes = base64.b64decode(_sig_b64)
            # Avoid duplicates if rerun
            if not any(cid == "sig_logo" for cid, _ in image_parts):
                image_parts.append(("sig_logo", _sig_bytes))
                image_mimes["sig_logo"] = "image/png"
        except Exception:
            # Fail quietly: signature will render without the logo rather than breaking generation/export.
            pass

    if 'TEMPLATE_HTML' not in globals() or TEMPLATE_HTML is None:
    TEMPLATE_HTML = DEFAULT_TEMPLATE_HTML

html_out = (TEMPLATE_HTML
        .replace("{{CLIENT_NAME}}", html_escape(st.session_state.client_name.strip() or "Client"))
        .replace("{{MONTH_LABEL}}", html_escape(st.session_state.month_label.strip() or "Monthly"))
        .replace("{{WEBSITE}}", html_escape(st.session_state.website.strip() or ""))
        .replace("{{GREETING_BLOCK}}", greeting_block_html)
        .replace("{{SIGNATURE_BLOCK}}", signature_block_html)
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
    with st.expander("Preview HTML"):
        st.components.v1.html(preview_html, height=600, scrolling=True)
st.divider()
with st.container(border=True):
    st.subheader("Export")

    # Filenames (computed locally to avoid Streamlit rerun scope issues)
    _client_name_for_files = (st.session_state.get("client_name") or "").strip()
    _safe_client_name = re.sub(r"[^A-Za-z0-9]+", "", _client_name_for_files) or "monthly"

    _month_label_for_files = (st.session_state.get("month_label") or "").strip()
    _safe_month_label = re.sub(r"\s+", "-", _month_label_for_files)
    _safe_month_label = re.sub(r"[^A-Za-z0-9\-]+", "", _safe_month_label) or "Month"

    eml_filename = f"{_safe_client_name}-seo-update.eml"
    pdf_filename = f"{_safe_client_name}-Monthly-SEO-Report-{_safe_month_label}.pdf"

    col_eml, col_pdf = st.columns(2)
    with col_eml:
        st.download_button(
            "Download .eml (Outlook-ready)",
            data=eml_bytes,
            file_name=eml_filename,
            mime="message/rfc822",
        )

    with col_pdf:
        if PLAYWRIGHT_AVAILABLE:
            try:
                pdf_bytes = html_to_pdf_bytes(preview_html)
                st.download_button(
                    "Download PDF",
                    data=pdf_bytes,
                    file_name=pdf_filename,
                    mime="application/pdf",
                )
            except Exception as _pdf_exc:
                st.caption(f"PDF export unavailable: {_pdf_exc}")
        else:
            st.caption("PDF export unavailable (Playwright/Chromium not installed).")

    with st.expander("Copy/paste HTML (optional)"):
        st.code(html_out, language="html")

if show_raw and st.session_state.raw:
        with st.expander("GPT output (raw)"):
            st.code(st.session_state.raw)
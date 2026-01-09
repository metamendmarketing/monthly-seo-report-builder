import io, os, re, json, datetime, base64
import copy
import sys, subprocess, asyncio
import email.utils
from typing import Dict, Optional, List, Tuple, Any

from pathlib import Path
import streamlit as st

import pandas as pd

# Use a repo-local Playwright browser cache (works on Streamlit Cloud)
if os.name != 'nt':
    os.environ.setdefault('PLAYWRIGHT_BROWSERS_PATH', os.path.join(os.getcwd(), '.cache', 'ms-playwright'))

import re
import uuid
import os
import asyncio

# Optional PDF export via Playwright (Chromium print-to-PDF). Hidden if unavailable.
PLAYWRIGHT_AVAILABLE = True
try:
    from playwright.sync_api import sync_playwright
except Exception:
    PLAYWRIGHT_AVAILABLE = False


_PW_BOOTSTRAPPED = False

def ensure_playwright_chromium(force: bool = False) -> None:
    """Ensure Playwright Chromium is installed.

    On Streamlit Community Cloud, Python packages install fine but Playwright browser
    binaries are not automatically downloaded. We install Chromium into
    PLAYWRIGHT_BROWSERS_PATH (set near the imports).
    """
    global _PW_BOOTSTRAPPED
    if _PW_BOOTSTRAPPED and not force:
        return
    _PW_BOOTSTRAPPED = True

    if os.name == "nt":
        return
    if not PLAYWRIGHT_AVAILABLE:
        return

    browsers_path = os.environ.get("PLAYWRIGHT_BROWSERS_PATH") or os.path.join(os.getcwd(), ".cache", "ms-playwright")

    # If Chromium already exists, do nothing.
    try:
        if not force and os.path.isdir(browsers_path):
            for name in os.listdir(browsers_path):
                if name.startswith("chromium"):
                    return
    except Exception:
        pass

    try:
        env = os.environ.copy()
        env["PLAYWRIGHT_BROWSERS_PATH"] = browsers_path
        subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium"],
            check=False,
            env=env,
        )
    except Exception:
        pass

def html_to_pdf_bytes(html: str) -> bytes:
    """Render the Preview HTML to a PDF using Playwright Chromium."""
    if not PLAYWRIGHT_AVAILABLE:
        raise RuntimeError("Playwright is not available.")
    html = html or ""

    # Ensure Chromium is installed (handles fresh local envs and Streamlit Cloud rebuilds)
    try:
        ensure_playwright_chromium(force=False)
    except Exception:
        pass

    def _render_once() -> bytes:
        with sync_playwright() as p:
            try:
                browser = p.chromium.launch(args=["--no-sandbox", "--disable-dev-shm-usage"])
            except Exception as e:
                if "Executable doesn't exist" in str(e) or "playwright install" in str(e):
                    try:
                        ensure_playwright_chromium(force=True)
                    except Exception:
                        pass
                    browser = p.chromium.launch(args=["--no-sandbox", "--disable-dev-shm-usage"])
                else:
                    raise
            page = browser.new_page(viewport={"width": 1100, "height": 1400})
            page.set_content(html, wait_until="networkidle")
            pdf_bytes = page.pdf(
                format="Letter",
                print_background=True,
                margin={"top": "0.75in", "bottom": "0.75in", "left": "0.75in", "right": "0.75in"},
            )
            browser.close()
            return pdf_bytes

    try:
        ensure_playwright_chromium()
        return _render_once()
    except Exception as e:
        msg = str(e)
        # If browser binaries are missing, install Chromium and retry once.
        if ("Executable doesn't exist" in msg) or ("playwright install" in msg):
            ensure_playwright_chromium(force=True)
            return _render_once()
        raise


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

# ------------------------------
# Utilities
# ------------------------------
def _slugify(value) -> str:
    """Safe slug for Streamlit keys and IDs."""
    if value is None:
        return "none"
    if isinstance(value, dict):
        # prefer common label keys if present
        for k in ("label", "title", "name", "metric", "fact", "note"):
            v = value.get(k)
            if isinstance(v, str) and v.strip():
                value = v
                break
        else:
            value = str(value)
    elif not isinstance(value, str):
        value = str(value)
    value = (value or "").strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = value.strip("-")
    return value or "item"




def _build_screenshot_summary_text(item: dict) -> str:
    """Build a concise, report-friendly summary string from a screenshot summary dict.

    Accepts flexible shapes. Prefers explicit 'summary' / 'extracted_summary' fields,
    otherwise composes from common keys like stats/issues/urls/movers/notes.
    """
    if not isinstance(item, dict):
        return str(item)

    # If an explicit summary exists, use it
    for k in ("summary", "extracted_summary", "summary_text", "description"):
        v = item.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()

    parts = []

    def _take_str(key):
        v = item.get(key)
        if isinstance(v, str) and v.strip():
            parts.append(v.strip())

    # Common structured keys
    for key in ("headline", "what_it_shows", "context"):
        _take_str(key)

    # Stats-like blocks
    stats = item.get("stats") or item.get("kpis") or item.get("metrics")
    if isinstance(stats, dict) and stats:
        # show a few key:value pairs
        kv = []
        for kk, vv in list(stats.items())[:6]:
            kv.append(f"{kk}: {vv}")
        if kv:
            parts.append("Stats: " + "; ".join(kv))
    elif isinstance(stats, list) and stats:
        # if list of dicts/strings
        vals = []
        for s in stats[:6]:
            if isinstance(s, dict):
                # try label/value
                label = s.get("label") or s.get("metric") or s.get("name") or ""
                value = s.get("value") if "value" in s else s.get("val")
                if label and value is not None:
                    vals.append(f"{label}: {value}")
                else:
                    vals.append(str(s))
            else:
                vals.append(str(s))
        if vals:
            parts.append("Stats: " + "; ".join(vals))

    # Notable items (kept neutral; no diagnosis)
    for key in ("notables", "notes", "highlights"):
        v = item.get(key)
        if isinstance(v, list) and v:
            parts.append("Notes: " + "; ".join([str(x) for x in v[:6]]))
        elif isinstance(v, str) and v.strip():
            parts.append(v.strip())

    # Movers / URLs / queries
    movers = item.get("movers") or item.get("top_movers") or item.get("top_items")
    if isinstance(movers, list) and movers:
        # keep very short
        sample = []
        for mvr in movers[:5]:
            if isinstance(mvr, dict):
                name = mvr.get("name") or mvr.get("title") or mvr.get("url") or mvr.get("query") or ""
                delta = mvr.get("delta") or mvr.get("change") or mvr.get("pct") or mvr.get("clicks") or ""
                if name:
                    sample.append(f"{name} ({delta})" if delta else name)
                else:
                    sample.append(str(mvr))
            else:
                sample.append(str(mvr))
        if sample:
            parts.append("Top items: " + "; ".join(sample))

    # Fallback: stringify a few fields
    if not parts:
        for k in ("issues_found", "urls", "queries"):
            v = item.get(k)
            if isinstance(v, list) and v:
                parts.append(f"{k}: " + "; ".join([str(x) for x in v[:6]]))

    return "\n".join(parts).strip()


APP_TITLE = "Metamend - Monthly SEO Report Builder"
DEFAULT_MODEL = "gpt-5.2"

# Canned opening lines (used by the Opening line suggestions)
CANNED_OPENERS = [
    "Hope you're doing well! Please see your monthly SEO status update below.",
    "Sharing this month's SEO update below, including the key wins, opportunities, and next steps.",
    "Here's your monthly SEO progress update - we've highlighted what moved, what it means, and what we're prioritizing next.",
    "Below is the monthly SEO status update for {month_label}.",
    "Hope you had an enjoyable weekend! Please see your monthly SEO status update below.",
    "Hope you're having a great holiday season — please see your monthly SEO status update below.",
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
TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), "templates", "monthly_email_template.html")

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

def _normalize_email_json(data: dict, verbosity_level: str = "Quick scan") -> dict:
    """Normalize GPT email JSON output into a stable shape for downstream rendering.

    This is a defensive helper: it should never raise, and it should preserve any
    valid content the model already returned while ensuring required keys exist.
    """
    if not isinstance(data, dict):
        data = {}

    v = (verbosity_level or "Quick scan").strip().lower()

    def _as_str(x):
        return "" if x is None else str(x).strip()

    def _as_list(x):
        if x is None:
            return []
        if isinstance(x, list):
            return x
        # Sometimes the model returns a single string; split lightly on newlines/bullets.
        if isinstance(x, str):
            s = x.strip()
            if not s:
                return []
            parts = [p.strip(" \t-•") for p in re.split(r"\n+|\r+|\u2022", s) if p.strip()]
            return parts
        return []

    def _clamp_list(items, max_n):
        items = [ _as_str(i) for i in items if _as_str(i) ]
        return items[:max_n] if max_n is not None else items

    # Normalize core fields
    out = {}
    out["subject"] = _as_str(data.get("subject")) or "SEO Monthly Update"
    out["monthly_overview"] = _as_str(data.get("monthly_overview"))

    # Section bullet arrays
    out["main_kpis"] = _as_list(data.get("main_kpis"))


    # Top opportunities (optional): dict with {queries:[], pages:[]}
    top_opp = data.get("top_opportunities")
    if not isinstance(top_opp, dict):
        top_opp = {}
    out["top_opportunities"] = {
        "queries": _as_list(top_opp.get("queries")),
        "pages": _as_list(top_opp.get("pages")),
    }

    out["key_highlights"] = _as_list(data.get("key_highlights"))
    out["wins_progress"] = _as_list(data.get("wins_progress"))
    out["blockers"] = _as_list(data.get("blockers"))
    out["completed_tasks"] = _as_list(data.get("completed_tasks"))
    out["outstanding_tasks"] = _as_list(data.get("outstanding_tasks"))

    # DashThis line (optional)
    out["dashthis_line"] = _as_str(data.get("dashthis_line"))

    # Image captions: list[dict] with expected keys
    caps = data.get("image_captions")
    if not isinstance(caps, list):
        caps = []
    norm_caps = []
    allowed_secs = {"key_highlights","main_kpis","wins_progress","blockers","completed_tasks","outstanding_tasks"}
    for it in caps:
        if not isinstance(it, dict):
            continue
        fn = _as_str(it.get("file_name"))
        if not fn:
            continue
        sec = _as_str(it.get("suggested_section"))
        if sec not in allowed_secs:
            sec = "key_highlights"
        norm_caps.append({
            "file_name": fn,
            "caption": _as_str(it.get("caption")),
            "suggested_section": sec,
        })
    out["image_captions"] = norm_caps

    # Verbosity-based clamps (match the schema limits used in gpt_generate_email)
    if v.startswith("quick"):
        out["main_kpis"] = _clamp_list(out["main_kpis"], 5)
        out["top_opportunities"]["queries"] = _clamp_list(out["top_opportunities"]["queries"], 5)
        out["top_opportunities"]["pages"] = _clamp_list(out["top_opportunities"]["pages"], 5)
        out["key_highlights"] = _clamp_list(out["key_highlights"], 4)
        out["wins_progress"] = _clamp_list(out["wins_progress"], 3)
        out["blockers"] = _clamp_list(out["blockers"], 3)
        out["completed_tasks"] = _clamp_list(out["completed_tasks"], 5)
        out["outstanding_tasks"] = _clamp_list(out["outstanding_tasks"], 5)
    elif v.startswith("deep"):
        out["main_kpis"] = _clamp_list(out["main_kpis"], 7)
        out["top_opportunities"]["queries"] = _clamp_list(out["top_opportunities"]["queries"], 5)
        out["top_opportunities"]["pages"] = _clamp_list(out["top_opportunities"]["pages"], 5)
        out["key_highlights"] = _clamp_list(out["key_highlights"], 6)
        out["wins_progress"] = _clamp_list(out["wins_progress"], 6)
        out["blockers"] = _clamp_list(out["blockers"], 5)
        out["completed_tasks"] = _clamp_list(out["completed_tasks"], 10)
        out["outstanding_tasks"] = _clamp_list(out["outstanding_tasks"], 10)
    else:
        # Standard
        out["main_kpis"] = _clamp_list(out["main_kpis"], 7)
        out["key_highlights"] = _clamp_list(out["key_highlights"], 5)
        out["wins_progress"] = _clamp_list(out["wins_progress"], 5)
        out["blockers"] = _clamp_list(out["blockers"], 4)
        out["completed_tasks"] = _clamp_list(out["completed_tasks"], 8)
        out["outstanding_tasks"] = _clamp_list(out["outstanding_tasks"], 8)

    # Final cleanup: ensure all list fields are lists of strings
    for k in ["main_kpis","key_highlights","wins_progress","blockers","completed_tasks","outstanding_tasks"]:
        out[k] = [ _as_str(x) for x in (out.get(k) or []) if _as_str(x) ]

    # Final cleanup: top opportunities lists
    try:
        out["top_opportunities"]["queries"] = [ _as_str(x) for x in (out.get("top_opportunities", {}).get("queries") or []) if _as_str(x) ]
        out["top_opportunities"]["pages"] = [ _as_str(x) for x in (out.get("top_opportunities", {}).get("pages") or []) if _as_str(x) ]
    except Exception:
        out["top_opportunities"] = {"queries": [], "pages": []}

    return out


def get_api_key() -> Optional[str]:
    try:
        if "OPENAI_API_KEY" in st.secrets:
            v = str(st.secrets["OPENAI_API_KEY"]).strip()
            return v or None
    except Exception:
        pass
    v = (os.getenv("OPENAI_API_KEY") or "").strip()
    return v or None


# -----------------------------
# Evidence extraction (Two-pass)
# -----------------------------
# This app uses a two-step process:
# 1) Evidence extraction: parse uploads + produce a structured, high-confidence evidence summary.
# 2) Writing: generate the email draft using Omni notes as primary narrative + the evidence summary as support.

MAX_SUPPORTING_TEXT_CHARS = 180_000
MAX_DOC_CHARS_PER_FILE = 60_000
MAX_TABLE_ROWS = 80
MAX_LIST_ROWS = 50  # cap list-style outputs shown in UI / payload
MAX_TABLE_COLS = 50

def _safe_decode_text(b: bytes) -> str:
    for enc in ("utf-8", "utf-16", "latin-1"):
        try:
            return b.decode(enc)
        except Exception:
            continue
    return b.decode("utf-8", errors="ignore")

def _normalize_ws(s: str) -> str:
    s = (s or "").replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"[ \t]+\n", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

def _clamp(s: str, n: int) -> str:
    if not s:
        return ""
    return s if len(s) <= n else (s[:n] + "\n\n[TRUNCATED]")

def _extract_pdf_text(data: bytes) -> str:
    """Extract text from PDF bytes with best-available tech.

    Priority order (best to fallback):
      1) PyMuPDF (fitz) - generally best layout-aware extraction
      2) pdfplumber - good text extraction with layout hints
      3) PyPDF2 - basic fallback

    This function must be defensive and return "" on failure.
    """
    # 1) PyMuPDF / fitz
    try:
        import fitz  # type: ignore
        parts = []
        with fitz.open(stream=data, filetype="pdf") as doc:
            for p_i in range(len(doc)):
                page = doc.load_page(p_i)
                # "text" keeps reading order reasonable; avoid dict output (too big)
                t = (page.get_text("text") or "").strip()
                if t:
                    parts.append(f"[PDF page {p_i+1}]\n{t}")
        out = _normalize_ws("\n\n".join(parts))
        if out:
            return out
    except Exception:
        pass

    # 2) pdfplumber
    try:
        import pdfplumber  # type: ignore
        parts = []
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            for p_i, page in enumerate(pdf.pages):
                t = (page.extract_text() or "").strip()
                if t:
                    parts.append(f"[PDF page {p_i+1}]\n{t}")
        out = _normalize_ws("\n\n".join(parts))
        if out:
            return out
    except Exception:
        pass

    # 3) PyPDF2
    try:
        from PyPDF2 import PdfReader  # type: ignore
        parts = []
        reader = PdfReader(io.BytesIO(data))
        for p_i, page in enumerate(reader.pages):
            t = (page.extract_text() or "").strip()
            if t:
                parts.append(f"[PDF page {p_i+1}]\n{t}")
        return _normalize_ws("\n\n".join(parts))
    except Exception:
        return ""

def _extract_pdf_tables(data: bytes) -> List[Dict[str, Any]]:
    """Best-effort extraction of simple tables from PDFs.

    Uses pdfplumber when available. Returns a list of small previews in the
    same shape as other table previews so downstream can treat them uniformly.
    """
    previews: List[Dict[str, Any]] = []
    try:
        import pdfplumber  # type: ignore
    except Exception:
        return previews

    def _table_to_preview(table: List[List[Any]]) -> Dict[str, Any]:
        # Normalize ragged rows
        rows = [[("" if c is None else str(c).strip()) for c in (r or [])] for r in (table or [])]
        rows = [r for r in rows if any((c or "").strip() for c in r)]
        if not rows:
            return {"shape": [0, 0], "headers": [], "rows": [], "truncated": False, "numeric_stats": {}}

        # Determine max cols and clamp
        max_cols = min(MAX_TABLE_COLS, max(len(r) for r in rows))
        rows2 = [ (r + [""] * (max_cols - len(r)))[:max_cols] for r in rows ]

        # Heuristic: first row is headers if it looks non-numeric and mostly unique
        hdr_candidate = rows2[0]
        def _is_numberish(x: str) -> bool:
            try:
                float(str(x).replace(",", ""))
                return True
            except Exception:
                return False

        nonnum = sum(1 for c in hdr_candidate if c and not _is_numberish(c))
        unique = len({c.lower() for c in hdr_candidate if c}) == len([c for c in hdr_candidate if c])
        use_hdr = nonnum >= max(1, int(0.6 * max_cols)) and unique

        headers = hdr_candidate if use_hdr else [f"col_{i+1}" for i in range(max_cols)]
        data_rows = rows2[1:] if use_hdr else rows2

        truncated = len(data_rows) > MAX_TABLE_ROWS
        data_rows = (data_rows[:MAX_TABLE_ROWS] if truncated else data_rows)

        return {
            "shape": [len(rows2) - (1 if use_hdr else 0), max_cols],
            "headers": headers,
            "rows": data_rows,
            "truncated": bool(truncated),
            "numeric_stats": {},
        }

    try:
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            for p_i, page in enumerate(pdf.pages):
                try:
                    tables = page.extract_tables() or []
                except Exception:
                    tables = []
                for t_i, table in enumerate(tables[:6]):  # cap tables per page
                    prev = _table_to_preview(table)
                    # only keep meaningful previews
                    if prev.get("shape", [0, 0])[0] > 0 and prev.get("shape", [0, 0])[1] > 0:
                        prev["page"] = p_i + 1
                        prev["table_index"] = t_i + 1
                        previews.append(prev)
                # Hard cap overall to avoid bloat
                if len(previews) >= 24:
                    break
    except Exception:
        return previews

    return previews


def _df_to_preview(df: "pd.DataFrame") -> Dict[str, Any]:
    """Convert a dataframe to the standard table preview dict."""
    try:
        headers = [str(c) for c in df.columns.tolist()]
        rows = df.astype(str).fillna("").values.tolist()
        truncated = len(rows) > MAX_TABLE_ROWS
        if truncated:
            rows = rows[:MAX_TABLE_ROWS]
        return {
            "shape": [int(df.shape[0]), int(df.shape[1])],
            "headers": headers,
            "rows": rows,
            "truncated": bool(truncated),
            "numeric_stats": {},
        }
    except Exception:
        return {"shape": [0, 0], "headers": [], "rows": [], "truncated": False, "numeric_stats": {}}


def _render_pdf_page_image(doc: Any, page_index: int, zoom: float = 2.0) -> "Any":
    """Render a PDF page to a PIL image for OCR. Returns None on failure."""
    try:
        import fitz  # type: ignore
        from PIL import Image  # type: ignore
    except Exception:
        return None
    try:
        page = doc.load_page(page_index)
        mat = fitz.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=mat, alpha=False)
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        return img
    except Exception:
        return None


def _ocr_pdf_page_words(doc: Any, page_index: int, zoom: float = 2.0, timeout_s: int = 20) -> List[Dict[str, Any]]:
    """OCR a PDF page and return word boxes in PDF coordinate space (approx).

    Returns list of dict: {text, x0, y0, x1, y1, conf}.
    Coordinate space is in rendered image pixels; we also include scale to PDF.
    """
    words: List[Dict[str, Any]] = []
    try:
        import pytesseract  # type: ignore
    except Exception:
        return words

    img = _render_pdf_page_image(doc, page_index, zoom=zoom)
    if img is None:
        return words

    try:
        data = pytesseract.image_to_data(img, output_type=pytesseract.Output.DICT, timeout=timeout_s)
        n = len(data.get("text", []))
        for i in range(n):
            t = (data["text"][i] or "").strip()
            if not t:
                continue
            try:
                conf = float(data.get("conf", ["-1"])[i])
            except Exception:
                conf = -1.0
            x, y, w, h = data["left"][i], data["top"][i], data["width"][i], data["height"][i]
            words.append({"text": t, "x0": float(x), "y0": float(y), "x1": float(x + w), "y1": float(y + h), "conf": conf, "zoom": float(zoom)})
    except Exception:
        return []

    return words


def _words_to_lines(words: List[Dict[str, Any]], y_tol: float = 10.0) -> List[List[Dict[str, Any]]]:
    """Group word boxes into lines using y-centroid clustering."""
    if not words:
        return []
    items = []
    for w in words:
        ymid = (w["y0"] + w["y1"]) / 2.0
        items.append((ymid, w))
    items.sort(key=lambda t: (t[0], t[1]["x0"]))

    lines: List[List[Dict[str, Any]]] = []
    cur: List[Dict[str, Any]] = []
    cur_y: Optional[float] = None
    for ymid, w in items:
        if cur_y is None:
            cur_y = ymid
            cur = [w]
            continue
        if abs(ymid - cur_y) <= y_tol:
            cur.append(w)
            # update running average y
            cur_y = (cur_y * (len(cur) - 1) + ymid) / float(len(cur))
        else:
            cur.sort(key=lambda ww: ww["x0"])
            lines.append(cur)
            cur = [w]
            cur_y = ymid
    if cur:
        cur.sort(key=lambda ww: ww["x0"])
        lines.append(cur)
    return lines


_NUM_TOKEN_RE = re.compile(r"^[\$\(\)\+\-]?\d[\d,]*([.]\d+)?%?$")

def _clean_num_token(s: str) -> str:
    s = (s or "").strip()
    # Fix common PDF/OCR split artifacts: "1, 30 7.4%" -> "1,307.4%"
    s = re.sub(r"(\d),\s+(\d)", r"\1,\2", s)
    s = re.sub(r"(\d)\s+(\d)", r"\1\2", s)
    s = s.replace("O", "0") if _NUM_TOKEN_RE.match(s.replace("O","0")) else s
    return s


def _extract_rows_from_token_lines(
    token_lines: List[List[str]],
    min_numeric: int = 2,
    max_cols: int = 10
) -> List[List[str]]:
    """Extract rows from tokenized lines: keep lines with >=min_numeric numeric tokens.

    Returns rows as [label, num1, num2, ...].
    """
    rows: List[List[str]] = []
    for toks in token_lines:
        toks = [t.strip() for t in toks if t.strip()]
        if not toks:
            continue
        nums = [i for i,t in enumerate(toks) if _NUM_TOKEN_RE.match(_clean_num_token(t))]
        if len(nums) < min_numeric:
            continue
        first_num = nums[0]
        label = " ".join(toks[:first_num]).strip()
        if not label:
            # if label empty, treat first token as label
            label = toks[0]
            first_num = 1 if len(toks) > 1 else 0
        values = [_clean_num_token(t) for t in toks[first_num:]]
        row = [label] + values
        row = row[:max_cols]
        rows.append(row)
    return rows


def _tokenize_text_lines(text: str) -> List[List[str]]:
    lines = []
    for ln in (text or "").splitlines():
        ln = ln.strip()
        if not ln:
            continue
        # split on runs of spaces while keeping tokens like "google / cpc"
        toks = re.split(r"\s{2,}|\t", ln)
        if len(toks) == 1:
            toks = ln.split()
        lines.append([t for t in toks if t.strip()])
    return lines


def _extract_pdf_section_tables(data: bytes, enable_ocr: bool = True) -> List[Dict[str, Any]]:
    """Extract 'clean table per section' best-effort from dashboard-style PDFs.

    Strategy:
      - Use pdfplumber text as baseline.
      - Optionally OCR each page to recover tile/chart text that isn't embedded.
      - Split into sections by known headings present in this report template.
      - For each section, produce one or more table previews.
    """
    previews: List[Dict[str, Any]] = []

    # Load with fitz once (used for OCR rendering)
    try:
        import fitz  # type: ignore
    except Exception:
        enable_ocr = False
        fitz = None  # type: ignore

    # Baseline extracted text per page (pdfplumber tends to be best for this template)
    page_texts: List[str] = []
    try:
        import pdfplumber  # type: ignore
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            for p in pdf.pages:
                page_texts.append(p.extract_text() or "")
    except Exception:
        # fallback to fitz if pdfplumber unavailable
        if fitz is None:
            return previews
        try:
            with fitz.open(stream=data, filetype="pdf") as doc:
                for i in range(len(doc)):
                    page_texts.append(doc.load_page(i).get_text("text") or "")
        except Exception:
            return previews

    ocr_lines_by_page: List[List[List[str]]] = [[] for _ in range(len(page_texts))]
    if enable_ocr and fitz is not None:
        try:
            with fitz.open(stream=data, filetype="pdf") as doc:
                for i in range(len(doc)):
                    base_t = page_texts[i] if i < len(page_texts) else ""
                    digit_count = sum(1 for ch in (base_t or "") if ch.isdigit())
                    should_ocr = ("..." in (base_t or "")) or (digit_count < 80)
                    if not should_ocr:
                        continue
                    words = _ocr_pdf_page_words(doc, i, zoom=2.0, timeout_s=20)
                    # keep only reasonable confidence words; allow -1 (unknown) but prefer >=40
                    words2 = [w for w in words if (w.get("conf", -1) >= 40) or (w.get("conf", -1) == -1)]
                    lines = _words_to_lines(words2, y_tol=12.0)
                    token_lines = [[w["text"] for w in ln] for ln in lines]
                    ocr_lines_by_page[i] = token_lines
        except Exception:
            pass

    # Section identification (by page; this report is consistent)
    section_by_page = {}
    for i, t in enumerate(page_texts):
        tt = (t or "").upper()
        if "NUMBER OF VISITORS" in tt and "SITE TRAFFIC" in tt:
            section_by_page[i] = "Site Traffic"
        elif "NUMBER OF ORDERS" in tt:
            section_by_page[i] = "Orders"
        elif "CONVERSION RATE" in tt:
            section_by_page[i] = "Conversion Rate"
        elif re.search(r"\bSALES\b", tt):
            section_by_page[i] = "Sales"
        elif "GOOGLE ADS" in tt:
            section_by_page[i] = "Google Ads"
        elif "MICROSOFT ADS" in tt:
            section_by_page[i] = "Microsoft Ads"
        elif "TOP QUERIES" in tt or "\nNOTES" in tt or tt.strip().startswith("NOTES"):
            section_by_page[i] = "Notes & Top Queries"
        else:
            section_by_page[i] = f"Page {i+1}"

    def add_preview(section: str, table_name: str, df: "pd.DataFrame"):
        pv = _df_to_preview(df)
        pv["section"] = section
        pv["table_name"] = table_name
        previews.append(pv)

    for i, base_text in enumerate(page_texts):
        section = section_by_page.get(i, f"Page {i+1}")

        base_lines = _tokenize_text_lines(base_text)
        ocr_token_lines = ocr_lines_by_page[i] or []
        # Merge: prefer base, but add OCR lines that aren't present (rough)
        base_join = set(" ".join([w.lower() for w in ln]) for ln in base_lines)
        merged_lines = list(base_lines)
        for ln in ocr_token_lines:
            key = " ".join([w.lower() for w in ln if w.strip()])
            if key and key not in base_join:
                merged_lines.append([w for w in ln if w.strip()])

        # --- KPI tiles (generic) ---
        # Look for lines that contain multiple known KPI labels; next line often contains the values.
        kpi_labels = ["SESSIONS","TOTAL USERS","TRANSACTIONS","PURCHASE REVENUE","REVENUE","CONVERSIONS","CLICKS","IMPRESSIONS","COST","SPEND","AVERAGE ORDER VALUE","PURCHASE RATE","CONVERSION RATE"]
        kpi_rows = []
        for idx_ln, ln in enumerate(merged_lines[:-1]):
            up = " ".join(ln).upper()
            if any(lbl in up for lbl in kpi_labels) and sum(1 for lbl in kpi_labels if lbl in up) >= 2:
                labels = [tok for tok in re.split(r"\s{1,}", up) if tok.strip()]
                # Use the raw line tokens rather than split again
                label_line = " ".join(ln)
                val_line = " ".join(merged_lines[idx_ln+1])
                # crude: split labels by double spaces in base extraction; if not, fall back to known sequence
                # We'll detect label chunks by scanning for known phrases in order.
                chunks = []
                tmp = label_line
                # prioritize longer labels
                ordered = sorted(set(kpi_labels), key=lambda s: -len(s))
                found = []
                for lbl in ordered:
                    if lbl.lower() in tmp.lower():
                        found.append(lbl)
                # For this template, the line is usually the labels in order. We'll just use the tokens in ln grouped by "  " if present in base line
                chunks = re.split(r"\s{2,}", label_line.strip())
                if len(chunks) <= 1:
                    # fallback: split by known labels occurrences
                    chunks = []
                    rest = label_line
                    for lbl in ["SESSIONS","TOTAL USERS","TRANSACTIONS","CONVERSION RATE","PURCHASE REVENUE","REVENUE","CLICKS","IMPRESSIONS","COST","SPEND","CONVERSIONS","AVERAGE ORDER VALUE","PURCHASE RATE"]:
                        if lbl.lower() in rest.lower():
                            chunks.append(lbl)
                            # remove first occurrence
                            rest = re.sub(re.escape(lbl), "", rest, flags=re.I, count=1).strip()
                    chunks = [c.strip() for c in chunks if c.strip()]
                val_chunks = re.split(r"\s{2,}", val_line.strip())
                if len(val_chunks) < len(chunks):
                    val_chunks = val_line.split()
                # build rows
                for j, lbl in enumerate(chunks):
                    val = val_chunks[j] if j < len(val_chunks) else ""
                    if lbl.strip() and val.strip():
                        kpi_rows.append([lbl.strip(), _clean_num_token(val.strip())])
                if kpi_rows:
                    break
        if kpi_rows:
            df = pd.DataFrame(kpi_rows, columns=["Metric", "Current"])
            add_preview(section, "KPIs", df)

        # --- Common breakdown tables (label + value + optional delta) ---
        # Extract rows with at least 1 numeric token; but keep label/value shapes.
        breakdown_rows = _extract_rows_from_token_lines(merged_lines, min_numeric=1, max_cols=6)
        # Filter out obvious junk headers
        junk = {"PREVIOU", "PREVIOUS", "PERIOD", "YEAR", "TOTAL", "REPORT", "ECO", "NOTES", "QUERIES", "CLICKS", "IMPRESSIONS", "AVG.", "CTR", "POSITION"}
        cleaned = []
        for r in breakdown_rows:
            lab = (r[0] or "").strip()
            if not lab:
                continue
            if lab.upper() in junk:
                continue
            # avoid KPI rows duplicated
            if any(lab.upper() == (kr[0].upper() if kr else "") for kr in kpi_rows):
                continue
            cleaned.append(r)

        if cleaned:
            # Heuristic: if many rows contain "/" it's likely Source/Medium table
            slash_ratio = sum(1 for r in cleaned if "/" in r[0]) / float(len(cleaned))
            if slash_ratio >= 0.3:
                # keep only rows with slash
                rows2 = [r for r in cleaned if "/" in r[0]]
                if rows2:
                    maxlen = max(len(r) for r in rows2)
                    cols = ["Source / Medium"] + [f"Value {k}" for k in range(1, maxlen)]
                    df = pd.DataFrame([r + [""]*(maxlen-len(r)) for r in rows2], columns=cols)
                    add_preview(section, "By Source / Medium", df)

            # Channel tables often have short labels without "/"
            chan_rows = [r for r in cleaned if ("/" not in r[0]) and (len(r[0]) <= 24) and re.search(r"[A-Za-z]", r[0])]
            if len(chan_rows) >= 3:
                maxlen = max(len(r) for r in chan_rows)
                cols = ["Channel"] + [f"Value {k}" for k in range(1, maxlen)]
                df = pd.DataFrame([r + [""]*(maxlen-len(r)) for r in chan_rows], columns=cols)
                add_preview(section, "By Channel", df)

        # --- Notes & Top Queries (page 7) ---
        if section == "Notes & Top Queries":
            # Notes: lines beginning with digit
            note_rows = []
            for ln in merged_lines:
                if len(ln) >= 2 and re.match(r"^\d+$", ln[0]):
                    note_rows.append([ln[0], " ".join(ln[1:]).strip()])
            if note_rows:
                add_preview(section, "Notes", pd.DataFrame(note_rows, columns=["#", "Note"]))

            # Top queries: look for rows with >=4 numeric tokens
            tq_rows = []
            in_tq = False
            for ln in merged_lines:
                s = " ".join(ln)
                if "TOP" in s.upper() and "QUER" in s.upper():
                    in_tq = True
                    continue
                if not in_tq:
                    continue
                # stop when hit NOTES
                if "OTES" in s.upper() or s.strip().upper() == "NOTES":
                    break
                toks = [t.strip() for t in ln if t.strip()]
                nums = [t for t in toks if _NUM_TOKEN_RE.match(_clean_num_token(t))]
                if len(nums) >= 4:
                    # find first numeric index
                    idxs = [ii for ii,t in enumerate(toks) if _NUM_TOKEN_RE.match(_clean_num_token(t))]
                    first = idxs[0]
                    query = " ".join(toks[:first]).strip()
                    vals = [_clean_num_token(t) for t in toks[first:]]
                    if query:
                        tq_rows.append([query]+vals)
            if tq_rows:
                maxlen = max(len(r) for r in tq_rows)
                cols = ["Query","Clicks","Δ Clicks","Impressions","Δ Impressions","CTR","Δ CTR","Avg Position","Δ Avg Position"][:maxlen]
                # pad
                df = pd.DataFrame([r+[""]*(maxlen-len(r)) for r in tq_rows], columns=cols)
                add_preview(section, "Top Queries", df)

    return previews



def _extract_docx_text(data: bytes) -> str:
    try:
        import docx  # type: ignore
        d = docx.Document(io.BytesIO(data))
        paras = [p.text for p in d.paragraphs if (p.text or "").strip()]
        return _normalize_ws("\n".join(paras))
    except Exception:
        return ""

def _df_preview(df) -> Dict[str, Any]:
    try:
        import pandas as pd  # type: ignore
        df2 = df.copy()
        if df2.shape[1] > MAX_TABLE_COLS:
            df2 = df2.iloc[:, :MAX_TABLE_COLS]
        truncated = df2.shape[0] > MAX_TABLE_ROWS
        dfp = df2.head(MAX_TABLE_ROWS) if truncated else df2
        headers = [str(c) for c in dfp.columns.tolist()]
        rows = dfp.fillna("").astype(str).values.tolist()
        # light numeric stats for hinting
        numeric_cols = [c for c in df2.columns if pd.api.types.is_numeric_dtype(df2[c])]
        stats = {}
        for c in numeric_cols[:12]:
            col = df2[c].dropna()
            if len(col) == 0:
                continue
            stats[str(c)] = {"min": float(col.min()), "max": float(col.max()), "mean": float(col.mean())}
        return {
            "shape": [int(df.shape[0]), int(df.shape[1])],
            "headers": headers,
            "rows": rows,
            "truncated": bool(truncated),
            "numeric_stats": stats,
        }
    except Exception as e:
        return {"error": str(e)}

def _extract_kpis_from_table_preview(table_rows: Any, source_ref: str) -> List[Dict[str, Any]]:
    """Heuristic extraction of KPI-like rows from a small table preview (often from PDFs like DashThis).

    Accepts either:
      - preview dict with keys: headers, rows
      - list[dict] rows
      - list[list] rows
    Always fails safe (returns []) rather than raising.
    """
    headers: List[str] = []
    # Some callers may accidentally pass the full preview dict; normalize.
    if isinstance(table_rows, dict):
        headers = [str(h) for h in (table_rows.get("headers") or []) if h is not None]
        table_rows = table_rows.get("rows") or []

    if not isinstance(table_rows, list) or not table_rows:
        return []

    # Determine row shape
    first = table_rows[0]
    if isinstance(first, dict):
        keys = list(first.keys())
    elif isinstance(first, (list, tuple)):
        if headers and len(headers) >= 2:
            keys = headers
        else:
            keys = [f"col{i+1}" for i in range(len(first))]
    else:
        return []

    if len(keys) < 2:
        return []
    # If too wide, it's likely not a KPI tile table
    if len(keys) > 6:
        return []

    # Cap work
    rows = table_rows[:MAX_LIST_ROWS]
    out: List[Dict[str, Any]] = []

    def _numish(v: Any) -> bool:
        s = str(v).strip()
        if not s or s.lower() in {"nan", "none"}:
            return False
        s2 = re.sub(r"[,$%\s]", "", s)
        return bool(re.match(r"^[+-]?(\d+\.?\d*|\d*\.?\d+)$", s2))

    def _to_dict(r: Any) -> Dict[str, Any]:
        if isinstance(r, dict):
            return r
        if isinstance(r, (list, tuple)):
            d: Dict[str, Any] = {}
            for i, v in enumerate(r):
                k = keys[i] if i < len(keys) else f"col{i+1}"
                d[str(k)] = v
            return d
        return {}

    for raw in rows:
        r = _to_dict(raw)
        if not r:
            continue

        # KPI tables are usually (label, value, maybe delta/period)
        # Pick metric label from the first non-numeric-ish cell
        metric = ""
        value = None
        delta = None

        ordered_vals = [r.get(k) for k in keys if k in r]
        # Fallback if keys didn't align perfectly
        if not ordered_vals:
            ordered_vals = list(r.values())

        # metric: first non-num cell
        for v in ordered_vals:
            if v is None:
                continue
            if not _numish(v):
                metric = str(v).strip()
                break

        # value: first numish cell
        for v in ordered_vals:
            if v is None:
                continue
            if _numish(v):
                value = str(v).strip()
                break

        # delta: numish with % or +/- if present
        for v in ordered_vals:
            if v is None:
                continue
            s = str(v).strip()
            if ("%" in s or s.startswith(("+", "-"))) and _numish(s):
                delta = s
                break

        if not metric or value is None:
            continue

        out.append({
            "metric": metric,
            "value": value,
            "delta": delta,
            "evidence_ref": source_ref,
            "confidence": "Medium",
        })

    return out
def build_supporting_context(uploaded_files: List[Any]) -> Dict[str, Any]:
    """Parse non-image uploads into structured evidence for the model."""
    supporting: Dict[str, Any] = {"documents": [], "tables": [], "notes": [], "_by_file": {}}
    total_chars = 0

    # Lazy availability checks
    has_pandas = True
    has_pdfplumber = True
    has_pypdf2 = True
    has_docx = True
    has_fitz = True
    try:
        import pandas  # noqa
    except Exception:
        has_pandas = False
    try:
        import pdfplumber  # noqa
    except Exception:
        has_pdfplumber = False
    try:
        import PyPDF2  # noqa
    except Exception:
        has_pypdf2 = False
    try:
        import docx  # noqa
    except Exception:
        has_docx = False
    try:
        import fitz  # noqa
    except Exception:
        has_fitz = False

    for f in uploaded_files or []:
        name = getattr(f, "name", "uploaded_file")
        lower = name.lower()
        data = f.getvalue() if hasattr(f, "getvalue") else (f.read() if hasattr(f, "read") else b"")
        try:
            if hasattr(f, "seek"):
                f.seek(0)
        except Exception:
            pass

        # Skip images here
        if lower.endswith((".png", ".jpg", ".jpeg", ".webp")):
            continue

        if lower.endswith(".pdf"):
            t = _extract_pdf_text(data)
            if t.strip():
                t = _clamp(t, MAX_DOC_CHARS_PER_FILE)
                supporting["documents"].append({"filename": name, "type": "pdf", "text": t})
                supporting["_by_file"].setdefault(name, {"documents": [], "tables": []})["documents"].append({"type": "pdf", "text": t})
                total_chars += len(t)
            else:
                supporting["notes"].append(f"Could not extract text from PDF: {name}")

            # Best-effort table extraction (helps with PDF exports that contain embedded tables)
            try:
                pdf_tables = _extract_pdf_tables(data)
                for pv in (pdf_tables or []):
                    # Represent each table like an Excel sheet preview
                    page = pv.get("page", "")
                    t_i = pv.get("table_index", "")
                    sheet_label = f"PDF page {page} table {t_i}".strip()
                    supporting["tables"].append({"filename": name, "type": "pdf", "sheet": sheet_label, "table": pv})
                    supporting["_by_file"].setdefault(name, {"documents": [], "tables": []})["tables"].append({"type": "pdf", "sheet": sheet_label, "table": pv})
            except Exception:
                pass

            # Best-effort "clean tables per section" extraction for dashboard-style PDFs (includes OCR fallback)
            try:
                section_tables = _extract_pdf_section_tables(data, enable_ocr=True)
                for pv in (section_tables or []):
                    section = pv.get("section", "PDF")
                    table_name = pv.get("table_name", "Table")
                    sheet_label = f"{section} - {table_name}".strip(" -")
                    supporting["tables"].append({"filename": name, "type": "pdf", "sheet": sheet_label, "table": pv})
                    supporting["_by_file"].setdefault(name, {"documents": [], "tables": []})["tables"].append({"type": "pdf", "sheet": sheet_label, "table": pv})
            except Exception:
                pass

            continue

        if lower.endswith(".docx"):
            t = _extract_docx_text(data)
            if t.strip():
                t = _clamp(t, MAX_DOC_CHARS_PER_FILE)
                supporting["documents"].append({"filename": name, "type": "docx", "text": t})
                supporting["_by_file"].setdefault(name, {"documents": [], "tables": []})["documents"].append({"type": "docx", "text": t})
                total_chars += len(t)
            else:
                supporting["notes"].append(f"Could not extract text from DOCX: {name}")
            continue

        if lower.endswith((".txt", ".md", ".log")):
            t = _normalize_ws(_safe_decode_text(data))
            if t.strip():
                t = _clamp(t, MAX_DOC_CHARS_PER_FILE)
                supporting["documents"].append({"filename": name, "type": "text", "text": t})
                supporting["_by_file"].setdefault(name, {"documents": [], "tables": []})["documents"].append({"type": "text", "text": t})
                total_chars += len(t)
            continue

            continue


        if lower.endswith((".xlsx", ".xls", ".xlsm")):
            if not has_pandas:
                supporting["notes"].append(f"Cannot parse Excel (pandas/openpyxl not installed): {name}")
                continue
            try:
                import pandas as pd  # type: ignore
                bio = io.BytesIO(data)

                # Robust engine fallback:
                # - Prefer openpyxl (best for .xlsx)
                # - If openpyxl isn't available or fails, let pandas pick an engine.
                try:
                    xl = pd.ExcelFile(bio, engine="openpyxl")
                except Exception:
                    try:
                        bio.seek(0)
                    except Exception:
                        pass
                    xl = pd.ExcelFile(bio)

                added_any = False
                for sheet in xl.sheet_names[:12]:
                    try:
                        df = xl.parse(sheet_name=sheet)
                        preview = _df_preview(df)
                        kind = _detect_gsc_table_kind(sheet, preview.get("headers") or [])
                        supporting["tables"].append({"filename": name, "type": "xlsx", "sheet": sheet, "table": preview, "_gsc_kind": kind})
                        supporting["_by_file"].setdefault(name, {"tables": []})["tables"].append({"type": "xlsx", "sheet": sheet, "table": preview, "_gsc_kind": kind})
                        added_any = True
                    except Exception as se:
                        supporting["notes"].append(f"Excel sheet parse error for {name} / {sheet}: {se}")

                if not added_any:
                    supporting["notes"].append(f"Excel parsed but no sheets could be read: {name}")

            except Exception as e:
                supporting["notes"].append(f"Excel parse error for {name}: {e}")
            continue

        
        if lower.endswith(".csv"):
            # CSV exports (including GA4) often include metadata lines before the header.
            try:
                import pandas as pd  # type: ignore
            except Exception:
                supporting["notes"].append(f"Cannot parse CSV (pandas not installed): {name}")
                # Still register the file so it appears in UI/debug
                supporting["_by_file"].setdefault(name, {"documents": [], "tables": [], "notes": []})["notes"].append(
                    "CSV parse skipped: pandas not installed"
                )
                continue

            supporting["_by_file"].setdefault(name, {"documents": [], "tables": [], "notes": []})

            def _read_csv_ga4_robust(raw: bytes):
                # Decode small prefix for header detection
                text = raw.decode("utf-8", errors="ignore")
                lines = text.splitlines()

                # Find first plausible header line (non-empty, not starting with '#', contains comma)
                header_idx = None
                for i, ln in enumerate(lines[:50]):
                    s = (ln or "").strip()
                    if not s:
                        continue
                    if s.startswith("#"):
                        continue
                    if "," in s:
                        header_idx = i
                        break

                skiprows = header_idx if header_idx is not None else 0

                # First attempt: ignore GA4 metadata lines and sniff delimiter
                try:
                    return pd.read_csv(io.BytesIO(raw), comment="#", engine="python", sep=None, skiprows=skiprows)
                except Exception:
                    # Fallback: strict comma with same skiprows
                    return pd.read_csv(io.BytesIO(raw), comment="#", sep=",", skiprows=skiprows)

            try:
                df = _read_csv_ga4_robust(data)
                # Clean up unnamed columns
                df = df.loc[:, [c for c in df.columns if str(c).strip() and not str(c).lower().startswith("unnamed")]]
                supporting["tables"].append({"filename": name, "type": "csv", "table": _df_preview(df)})
                supporting["_by_file"][name]["tables"].append({"type": "csv", "sheet": "CSV", "table": _df_preview(df)})
            except Exception as e:
                err = f"CSV parse error for {name}: {e}"
                supporting["notes"].append(err)
                supporting["_by_file"][name]["notes"].append(err)
            continue
        msg = f"Unsupported file type for parsing: {name}"
        supporting["notes"].append(msg)
        supporting["_by_file"].setdefault(name, {"documents": [], "tables": [], "notes": []})["notes"].append(msg)

        if total_chars > MAX_SUPPORTING_TEXT_CHARS:
            supporting["notes"].append("Supporting context truncated due to size limits.")
            break

    supporting["_extraction_stats"] = {
        "documents_count": len(supporting.get("documents", [])),
        "tables_count": len(supporting.get("tables", [])),
        "notes_count": len(supporting.get("notes", [])),
        "has_pandas": has_pandas,
        "has_pdfplumber": has_pdfplumber,
        "has_pypdf2": has_pypdf2,
        "has_docx": has_docx,
        "has_fitz": has_fitz,
    }
    return supporting

EVIDENCE_SCHEMA = {
    "type": "object",
    "properties": {
        "main_kpis": {"type": "array", "items": {"type": "object", "properties": {
            "metric": {"type": "string"},
            "value": {"type": "string"},
            "delta": {"type": "string"},
            "period": {"type": "string"},
            "evidence_ref": {"type": "string"},
            "confidence": {"type": "string"},
        }, "required": ["metric","value","evidence_ref","confidence"], "additionalProperties": False}},
        "noteworthy_wins": {"type": "array", "items": {"type": "object", "properties": {
            "claim": {"type": "string"},
            "why_it_matters": {"type": "string"},
            "evidence_ref": {"type": "string"},
            "confidence": {"type": "string"},
        }, "required": ["claim","evidence_ref","confidence"], "additionalProperties": False}},
        "risks_or_anomalies": {"type": "array", "items": {"type": "object", "properties": {
            "claim": {"type": "string"},
            "context": {"type": "string"},
            "evidence_ref": {"type": "string"},
            "confidence": {"type": "string"},
        }, "required": ["claim","evidence_ref","confidence"], "additionalProperties": False}},
        "movers": {"type": "array", "items": {"type": "object", "properties": {
            "entity_type": {"type": "string"},  # page|query
            "entity": {"type": "string"},
            "movement": {"type": "string"},
            "evidence_ref": {"type": "string"},
            "confidence": {"type": "string"},
        }, "required": ["entity_type","entity","evidence_ref","confidence"], "additionalProperties": False}},
        "work_to_results_links": {"type": "array", "items": {"type": "object", "properties": {
            "work_item": {"type": "string"},
            "observed_signal": {"type": "string"},
            "language": {"type": "string"},  # suggested cautious phrasing
            "evidence_ref": {"type": "string"},
            "confidence": {"type": "string"},
        }, "required": ["work_item","observed_signal","evidence_ref","confidence"], "additionalProperties": False}},
        "notes": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["main_kpis","noteworthy_wins","risks_or_anomalies","movers","work_to_results_links","notes"],
    "additionalProperties": False,
}

EVIDENCE_SYSTEM_PROMPT = """You are a meticulous SEO analyst.
Task: Extract HIGH-CONFIDENCE evidence from supporting_context (documents/tables) and provided screenshots.
Return ONLY JSON matching the schema.

Rules:
- Be comprehensive: attempt to pull the most important KPIs and notable changes.
- Only include claims you can ground in evidence. Every item MUST include evidence_ref pointing to filename and page/sheet when possible.
- Confidence must be one of: High, Medium, Low. Prefer High only when numbers/labels are explicit.
- Do not editorialize. Do not write an email. Do not mention limitations like 'in this workspace'.""".strip()

def run_evidence_extraction(client: OpenAI, model: str, omni_notes: str, supporting_context: Dict[str, Any], image_parts_for_model: List[Tuple[str, bytes, str]]) -> Dict[str, Any]:
    supporting_json = json.dumps(supporting_context, ensure_ascii=False)
    user_text = f"""Omni notes (for context only; do not invent results):
{omni_notes}

Supporting context (parsed from uploads):
{supporting_json}

Now extract evidence per schema.""".strip()

    content = [{"type": "input_text", "text": user_text}]
    # Attach images (downscaled already elsewhere) for extraction
    for name, b, mt in image_parts_for_model:
        # Provide filename BEFORE the image so the model can reliably map file_name -> image.
        content.append({"type": "input_text", "text": f"Image filename: {name}"})
        b64 = base64.b64encode(b).decode("utf-8")
        content.append({"type": "input_image", "image_url": f"data:{mt};base64,{b64}"})

        # Call the model. Some OpenAI SDK versions do not support `response_format=` for responses.create.
    # We therefore ask for strict JSON in the prompt and then parse best-effort.
    try:
        resp = client.responses.create(
            model=model,
            input=[
                {"role": "system", "content": EVIDENCE_SYSTEM_PROMPT},
                {"role": "user", "content": content},
            ],
        )
    except TypeError:
        resp = client.responses.create(
            model=model,
            input=[
                {"role": "system", "content": EVIDENCE_SYSTEM_PROMPT},
                {"role": "user", "content": content},
            ],
        )

    raw = getattr(resp, "output_text", "") or ""
    if not raw:
        return {"main_kpis": [], "noteworthy_signals": {"positive": [], "negative": [], "neutral": []},
                "page_movers": [], "query_movers": [], "work_to_results_links": [], "notes": ["No output_text"]}

    # Best-effort JSON extraction
    m = re.search(r"\{[\s\S]*\}", raw)
    if not m:
        return {"main_kpis": [], "noteworthy_signals": {"positive": [], "negative": [], "neutral": []},
                "page_movers": [], "query_movers": [], "work_to_results_links": [], "notes": ["No JSON found in output"]}

    try:
        return json.loads(m.group(0))
    except Exception:
        return {"main_kpis": [], "noteworthy_signals": {"positive": [], "negative": [], "neutral": []},
                "page_movers": [], "query_movers": [], "work_to_results_links": [], "notes": ["JSON parse failed"]}



def load_template() -> str:
    """Load HTML template from disk; fall back to embedded template on failure."""
    try:
        with open(TEMPLATE_PATH, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return DEFAULT_TEMPLATE_HTML

TEMPLATE_HTML = load_template()

def html_escape(s: str) -> str:
    return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def bullets_to_html(items: List[str]) -> str:
    items = [i.strip() for i in (items or []) if i and i.strip()]
    if not items:
        return ""
    # Keep styles minimal so Outlook inherits the user's default font (e.g., Aptos).
    lis = "\n".join([f'<li style="margin:6px 0;">{html_escape(i)}</li>' for i in items])
    return f'<ul style="margin:8px 0 0 20px;padding:0;">{lis}</ul>'

def _format_gsc_opportunity_item(row: Any) -> str:
    """Format a GSC opportunity row dict into a concise string.

    Expected row shape: {"item": str, "impressions": int/str, "ctr": "0.80%" or float, "position": float/str}
    Safe for partial/missing keys.
    """
    if isinstance(row, str):
        return row.strip()
    if not isinstance(row, dict):
        return str(row).strip()
    item = str(row.get("item") or "").strip()
    if not item:
        item = str(row.get("query") or row.get("page") or row.get("url") or "").strip()
    if not item:
        return ""
    imps = row.get("impressions")
    ctr = row.get("ctr")
    pos = row.get("position")
    parts = []
    try:
        if imps not in (None, "", 0, "0"):
            parts.append(f"{int(float(str(imps).replace(',',''))):,} impressions")
    except Exception:
        if str(imps).strip():
            parts.append(f"{str(imps).strip()} impressions")
    # CTR formatting
    try:
        if isinstance(ctr, str) and ctr.strip():
            c = ctr.strip()
            # normalize "0.008" or "0.8" into percent if it looks numeric and <= 1
            if re.match(r"^-?\d+(\.\d+)?$", c):
                val = float(c)
                if val <= 1.0:
                    parts.append(f"{val:.2%} CTR")
                else:
                    parts.append(f"{val:.2f}% CTR")
            else:
                parts.append(f"{c} CTR" if "CTR" not in c.upper() else c)
        elif ctr is not None and ctr != "":
            val = float(ctr)
            if val <= 1.0:
                parts.append(f"{val:.2%} CTR")
            else:
                parts.append(f"{val:.2f}% CTR")
    except Exception:
        pass
    # Position
    try:
        if pos not in (None, ""):
            parts.append(f"avg pos {round(float(pos), 1)}")
    except Exception:
        if str(pos).strip():
            parts.append(f"avg pos {str(pos).strip()}")
    if parts:
        return f"{item} — " + ", ".join(parts)
    return item

def _derive_top_opportunities_from_insight(insight_payload: Any, max_items: int = 5) -> Dict[str, List[str]]:
    """Derive Top Opportunities lists from INSIGHT_MODEL.data_signals opportunity arrays."""
    out = {"queries": [], "pages": []}
    if not isinstance(insight_payload, dict):
        return out
    ds = insight_payload.get("data_signals") or {}
    if not isinstance(ds, dict):
        return out

    def _take_unique(rows: Any) -> List[str]:
        if not isinstance(rows, list):
            return []
        seen = set()
        items = []
        for r in rows:
            s = _format_gsc_opportunity_item(r)
            if not s:
                continue
            key = s.lower()
            if key in seen:
                continue
            seen.add(key)
            items.append(s)
            if len(items) >= max_items:
                break
        return items

    out["queries"] = _take_unique(ds.get("opportunity_queries") or [])
    out["pages"] = _take_unique(ds.get("opportunity_pages") or [])
    return out

def top_opportunities_subsection_html(top_opportunities: Any) -> str:
    """Render the Top Opportunities subsection HTML to be placed inside the Main KPI's section."""
    if not isinstance(top_opportunities, dict):
        return ""
    q = top_opportunities.get("queries") or []
    p = top_opportunities.get("pages") or []
    if not isinstance(q, list):
        q = []
    if not isinstance(p, list):
        p = []
    q = [str(x).strip() for x in q if str(x).strip()][:5]
    p = [str(x).strip() for x in p if str(x).strip()][:5]
    if not q and not p:
        return ""
    # Use existing list helper for consistent bullet styling.
    q_html = bullets_to_html(q)
    p_html = bullets_to_html(p)
    parts = ['<div style="margin:10px 0 0 0;">',
             '<div style="font-weight:700;margin:0 0 6px 0;">Top Opportunities</div>']
    if q_html:
        parts.append('<div style="margin:0 0 6px 0;"><div style="font-weight:600;">Queries</div>' + q_html + '</div>')
    if p_html:
        parts.append('<div style="margin:0;"><div style="font-weight:600;">Pages</div>' + p_html + '</div>')
    parts.append('</div>')
    return "".join(parts)


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

def build_eml(subject: str, html_body: str, images) -> bytes:
    """Build an Outlook-friendly .eml file.

    Always returns bytes. Images are optional; when provided, they are attached inline
    with Content-ID so the HTML can reference them (cid:...).
    """
    msg = MIMEMultipart("related")
    msg["Subject"] = subject or "SEO Monthly Update"
    msg["To"] = msg.get("To", "")
    msg["From"] = msg.get("From", os.getenv("DEFAULT_FROM_EMAIL", "kosborne@metamend.com"))
    msg["Date"] = email.utils.formatdate(localtime=True)
    # Make .eml open as an editable draft in Outlook-compatible clients
    msg["X-Unsent"] = "1"
    msg["X-Unsent-Flag"] = "1"
    msg["MIME-Version"] = "1.0"

    # HTML body (required)
    msg.attach(MIMEText(html_body or "", "html", "utf-8"))

    # Inline images (optional)
    if images:
        for cid, b in images:
            # Normalize payload to bytes
            data_bytes = b
            if isinstance(data_bytes, dict):
                data_bytes = data_bytes.get("bytes") or data_bytes.get("data") or b""
            if isinstance(data_bytes, str):
                # data URL or base64 string
                try:
                    import base64
                    if data_bytes.startswith("data:image"):
                        _, encoded = data_bytes.split(",", 1)
                        data_bytes = base64.b64decode(encoded)
                    else:
                        data_bytes = base64.b64decode(data_bytes)
                except Exception:
                    data_bytes = b""
            if isinstance(data_bytes, (bytearray, memoryview)):
                data_bytes = bytes(data_bytes)
            if not isinstance(data_bytes, (bytes,)) or not data_bytes:
                continue

            subtype = _detect_image_subtype(data_bytes)
            if not subtype:
                # Skip unknown/invalid image data but still produce a valid .eml
                continue

            img = MIMEImage(data_bytes, _subtype=subtype)
            safe_cid = str(cid or "").strip() or f"image-{uuid.uuid4().hex[:8]}"
            img.add_header("Content-ID", f"<{safe_cid}>")
            img.add_header("Content-Disposition", "inline", filename=f"{safe_cid}.{subtype}")
            msg.attach(img)

    return msg.as_bytes()

def _detect_image_subtype(data: bytes) -> str | None:
    """Detect image subtype for MIMEImage without using imghdr (removed in Python 3.13)."""
    # Prefer Pillow if available (most reliable)
    try:
        from PIL import Image  # type: ignore
        import io as _io
        with Image.open(_io.BytesIO(data)) as img:
            fmt = (img.format or "").lower().strip()
            if fmt == "jpg":
                return "jpeg"
            return fmt or None
    except Exception:
        pass

    # Fallback: magic bytes
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "png"
    if data.startswith(b"\xff\xd8\xff"):
        return "jpeg"
    if data.startswith(b"RIFF") and len(data) >= 12 and data[8:12] == b"WEBP":
        return "webp"
    if data.startswith(b"GIF87a") or data.startswith(b"GIF89a"):
        return "gif"
    return None


def _json_deepcopy(obj: Any) -> Any:
    try:
        return json.loads(json.dumps(obj))
    except Exception:
        return copy.deepcopy(obj)

def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        s = str(x).strip().replace(",", "")
        if s == "":
            return None
        return float(s)
    except Exception:
        return None

def _find_col(headers: List[str], needles: List[str]) -> Optional[int]:
    """Return the column index whose header matches any needle.

    Robustness improvements:
    - Accept singular/plural variants (query/queries, country/countries, etc.)
    - Accept 'Top queries' style headers (needle is substring-ish)
    - Normalize whitespace + punctuation
    """
    def norm(s: str) -> str:
        s = str(s or "").strip().lower()
        s = re.sub(r"[\s\u00A0]+", " ", s)
        s = re.sub(r"[^a-z0-9 ]+", "", s)
        return s.strip()

    hs_raw = [str(h) for h in headers]
    hs = [norm(h) for h in hs_raw]

    def variants(n: str) -> List[str]:
        n = norm(n)
        out = {n}
        # basic pluralization helpers
        if n.endswith("y"):
            out.add(n[:-1] + "ies")
        if not n.endswith("s"):
            out.add(n + "s")
        if n.endswith("s"):
            out.add(n[:-1])
        return [v for v in out if v]

    needles_v = []
    for n in needles:
        needles_v.extend(variants(n))

    for i, h in enumerate(hs):
        for n in needles_v:
            if h == n:
                return i
            # substring match both directions to catch 'top queries' vs 'query'
            if n in h or h in n:
                return i
            # token containment (e.g., 'top queries' contains token 'queries')
            if all(tok in h.split() for tok in n.split()):
                return i
    return None

def _table_rows_as_dicts(table_preview: Dict[str, Any]) -> List[Dict[str, Any]]:
    headers = table_preview.get("headers") or []
    rows = table_preview.get("rows") or []
    out = []
    for r in rows:
        d = {}
        for i, h in enumerate(headers):
            if i < len(r):
                d[str(h)] = r[i]
            else:
                d[str(h)] = ""
        out.append(d)
    return out

def _detect_gsc_table_kind(sheet: str, headers: List[str]) -> str:
    """Classify GSC export tables by sheet name / headers.

    Important: sheet is often plural ('Queries'), so check both singular/plural tokens.
    """
    s = (sheet or "").strip().lower()
    # Primary: sheet name
    if "query" in s or "queries" in s:
        return "queries"
    if "page" in s or "pages" in s:
        return "pages"
    if "country" in s or "countries" in s:
        return "countries"
    if "device" in s or "devices" in s:
        return "devices"
    if "search appearance" in s or "appearance" in s:
        return "search_appearance"
    if "date" in s or "chart" in s or "performance" in s:
        return "chart"

    # Fallback: infer by header labels (common: 'Top queries', 'Top pages')
    hs = [str(h or "").strip().lower() for h in (headers or [])]
    if any("query" in h or "queries" in h for h in hs):
        return "queries"
    if any("page" in h or "pages" in h or "url" == h for h in hs):
        return "pages"
    if any("country" in h or "countries" in h for h in hs):
        return "countries"
    if any("device" in h or "devices" in h for h in hs):
        return "devices"
    if any("appearance" in h for h in hs):
        return "search_appearance"

    return "unknown"

def _compute_gsc_totals(rows: List[Dict[str, Any]], click_key: str, impr_key: str) -> Tuple[float, float]:
    clicks = 0.0
    imps = 0.0
    for r in rows:
        c = _safe_float(r.get(click_key))
        i = _safe_float(r.get(impr_key))
        if c is not None:
            clicks += c
        if i is not None:
            imps += i
    return clicks, imps

def _build_data_signals(supporting_context: Dict[str, Any]) -> Dict[str, Any]:
    tables = supporting_context.get("tables") or []
    # If multiple files provide tables, prefer the file that most resembles a GSC export
    by_file = supporting_context.get("_by_file") or {}
    gsc_sheet_names = {"chart","queries","pages","countries","devices","search appearance","filters"}
    best_gsc_file = None
    best_score = 0
    for fname, blob in by_file.items():
        tabs = set((t.get("sheet") or "").strip().lower() for t in (blob.get("tables") or []))
        score = len(tabs & gsc_sheet_names)
        if score > best_score:
            best_score = score
            best_gsc_file = fname
    if best_gsc_file and best_score >= 2:
        tables = [t for t in tables if (t.get("filename") == best_gsc_file)]
    data_signals = {
        "kpis": [],
        "top_queries": [],
        "top_pages": [],
        "opportunity_queries": [],
        "opportunity_pages": [],
        "distribution_breakdowns": {"devices": [], "countries": [], "search_appearance": []},
        "trend_notes": [],
    }

    # Collect candidate GSC tables
    gsc_tables = []
    for t in tables:
        sheet = t.get("sheet") or ""
        preview = t.get("table") or {}
        headers = preview.get("headers") or []
        # Require core metrics
        if _find_col(headers, ["clicks"]) is None or _find_col(headers, ["impressions"]) is None:
            continue
        kind = t.get("_gsc_kind") or _detect_gsc_table_kind(sheet, headers)
        gsc_tables.append((kind, t))

    # Prefer totals from Chart if present; otherwise take first available totals
    totals_clicks = None
    totals_imps = None
    totals_ref = None

    for kind, t in gsc_tables:
        if kind not in ("chart", "unknown", "queries", "pages", "countries", "devices", "search_appearance"):
            continue
        preview = t.get("table") or {}
        headers = preview.get("headers") or []
        rows = _table_rows_as_dicts(preview)
        ci = _find_col(headers, ["clicks"])
        ii = _find_col(headers, ["impressions"])
        if ci is None or ii is None:
            continue
        click_key = str(headers[ci])
        impr_key = str(headers[ii])
        clicks, imps = _compute_gsc_totals(rows, click_key, impr_key)
        if clicks <= 0 and imps <= 0:
            continue
        # Prefer chart
        if totals_clicks is None or kind == "chart":
            totals_clicks, totals_imps = clicks, imps
            totals_ref = f"{t.get('filename')} / {t.get('sheet')}"
            if kind == "chart":
                break

    if totals_clicks is not None and totals_imps is not None:
        data_signals["kpis"].append({
            "metric": "GSC Clicks",
            "value": f"{int(round(totals_clicks)):,}",
            "period": "",
            "delta": "",
            "evidence_ref": totals_ref or "GSC export",
            "confidence": "High",
        })
        data_signals["kpis"].append({
            "metric": "GSC Impressions",
            "value": f"{int(round(totals_imps)):,}",
            "period": "",
            "delta": "",
            "evidence_ref": totals_ref or "GSC export",
            "confidence": "High",
        })
        # Derived CTR (safe, but mark Medium)
        if totals_imps > 0:
            ctr = totals_clicks / totals_imps
            data_signals["kpis"].append({
                "metric": "GSC CTR (derived)",
                "value": f"{ctr:.2%}",
                "period": "",
                "delta": "",
                "evidence_ref": totals_ref or "GSC export (derived from totals)",
                "confidence": "Medium",
            })

    # Build lists for queries/pages and breakdowns
    def top_n(kind: str, dim_needles: List[str], target_list: List[Dict[str, Any]], n: int = MAX_LIST_ROWS):
        for k, t in gsc_tables:
            if k != kind:
                continue
            preview = t.get("table") or {}
            headers = preview.get("headers") or []
            rows = _table_rows_as_dicts(preview)
            dim_i = _find_col(headers, dim_needles)
            ci = _find_col(headers, ["clicks"])
            ii = _find_col(headers, ["impressions"])
            ctri = _find_col(headers, ["ctr"])
            posi = _find_col(headers, ["position", "avg position"])
            if dim_i is None or ci is None:
                continue
            dim_key = str(headers[dim_i])
            click_key = str(headers[ci])
            impr_key = str(headers[ii]) if ii is not None else None
            ctr_key = str(headers[ctri]) if ctri is not None else None
            pos_key = str(headers[posi]) if posi is not None else None

            # sort by clicks
            def _click_val(r):
                v = _safe_float(r.get(click_key))
                return v if v is not None else -1.0
            rows_sorted = sorted(rows, key=_click_val, reverse=True)
            for r in rows_sorted[:n]:
                item = str(r.get(dim_key) or "").strip()
                if not item:
                    continue
                clicks = _safe_float(r.get(click_key))
                imps = _safe_float(r.get(impr_key)) if impr_key else None
                ctr = _safe_float(r.get(ctr_key)) if ctr_key else None
                pos = _safe_float(r.get(pos_key)) if pos_key else None
                target_list.append({
                    "item": item,
                    "clicks": int(clicks) if clicks is not None else "",
                    "impressions": int(imps) if imps is not None else "",
                    "ctr": f"{ctr:.2%}" if isinstance(ctr, float) and ctr <= 1.0 else (f"{ctr:.2f}" if ctr is not None else ""),
                    "position": round(pos, 2) if pos is not None else "",
                    "evidence_ref": f"{t.get('filename')} / {t.get('sheet')} (top rows)",
                    "confidence": "High" if clicks is not None else "Medium",
                })
            break

    top_n("queries", ["query", "queries", "top queries", "top query"], data_signals["top_queries"], n=MAX_LIST_ROWS)
    top_n("pages", ["page", "pages", "top pages", "url"], data_signals["top_pages"], n=MAX_LIST_ROWS)

    # opportunities: high impressions, low ctr, pos 8-20
    def opportunities(kind: str, dim_needles: List[str], out_list: List[Dict[str, Any]], n: int = MAX_LIST_ROWS):
        for k, t in gsc_tables:
            if k != kind:
                continue
            preview = t.get("table") or {}
            headers = preview.get("headers") or []
            rows = _table_rows_as_dicts(preview)
            dim_i = _find_col(headers, dim_needles)
            ci = _find_col(headers, ["clicks"])
            ii = _find_col(headers, ["impressions"])
            ctri = _find_col(headers, ["ctr"])
            posi = _find_col(headers, ["position", "avg position"])
            if dim_i is None or ii is None or ci is None:
                continue
            dim_key = str(headers[dim_i])
            click_key = str(headers[ci])
            impr_key = str(headers[ii])
            ctr_key = str(headers[ctri]) if ctri is not None else None
            pos_key = str(headers[posi]) if posi is not None else None

            candidates = []
            for r in rows:
                item = str(r.get(dim_key) or "").strip()
                if not item:
                    continue
                imps = _safe_float(r.get(impr_key))
                clicks = _safe_float(r.get(click_key))
                pos = _safe_float(r.get(pos_key)) if pos_key else None
                ctr = _safe_float(r.get(ctr_key)) if ctr_key else (clicks / imps if (imps and clicks is not None) else None)
                if imps is None or imps < 200:
                    continue
                if pos is not None and (pos < 8 or pos > 20):
                    continue
                if ctr is not None and ctr > 0.03:
                    continue
                candidates.append((imps, item, clicks, ctr, pos))

            candidates.sort(key=lambda x: x[0], reverse=True)  # impressions desc
            for imps, item, clicks, ctr, pos in candidates[:n]:
                out_list.append({
                    "item": item,
                    "impressions": int(imps) if imps is not None else "",
                    "clicks": int(clicks) if clicks is not None else "",
                    "ctr": f"{ctr:.2%}" if isinstance(ctr, float) and ctr <= 1.0 else "",
                    "position": round(pos, 2) if pos is not None else "",
                    "why_it_matters": "High impressions with low CTR and mid SERP position (opportunity).",
                    "evidence_ref": f"{t.get('filename')} / {t.get('sheet')} (opportunity filter)",
                    "confidence": "Medium",
                })
            break

    opportunities("queries", ["query"], data_signals["opportunity_queries"], n=MAX_LIST_ROWS)
    opportunities("pages", ["page", "url"], data_signals["opportunity_pages"], n=MAX_LIST_ROWS)

    # breakdowns
    top_n("countries", ["country"], data_signals["distribution_breakdowns"]["countries"], n=8)
    top_n("devices", ["device"], data_signals["distribution_breakdowns"]["devices"], n=6)
    top_n("search_appearance", ["search appearance", "appearance"], data_signals["distribution_breakdowns"]["search_appearance"], n=6)

    # trend notes: if chart has date col
    for kind, t in gsc_tables:
        if kind != "chart":
            continue
        preview = t.get("table") or {}
        headers = preview.get("headers") or []
        date_i = _find_col(headers, ["date"])
        ci = _find_col(headers, ["clicks"])
        ii = _find_col(headers, ["impressions"])
        if date_i is None or ci is None:
            continue
        rows = _table_rows_as_dicts(preview)
        date_key = str(headers[date_i])
        click_key = str(headers[ci])
        # note best day by clicks
        best = None
        for r in rows:
            d = str(r.get(date_key) or "").strip()
            c = _safe_float(r.get(click_key))
            if d and c is not None:
                if best is None or c > best[1]:
                    best = (d, c)
        if best:
            data_signals["trend_notes"].append({
                "note": f"Highest-click day in the export preview: {best[0]} ({int(best[1])} clicks).",
                "evidence_ref": f"{t.get('filename')} / {t.get('sheet')}",
                "confidence": "Medium",
            })
        break
    # --- Fallbacks for Top Queries / Top Pages ---
    def _fallback_top(kind: str, out_list: List[Dict[str, Any]], n: int = MAX_LIST_ROWS):
        if out_list:
            return
        for k, t in gsc_tables:
            if k != kind:
                continue
            preview = t.get("table") or {}
            headers = preview.get("headers") or []
            rows = _table_rows_as_dicts(preview)
            if not headers or not rows:
                continue

            metric_needles = ["click", "impression", "ctr", "position", "avg position"]
            dim_i = None
            for i, h in enumerate(headers):
                hn = str(h).lower()
                if any(m in hn for m in metric_needles):
                    continue
                dim_i = i
                break

            ci = _find_col(headers, ["click"])
            ii = _find_col(headers, ["impression"])
            ctri = _find_col(headers, ["ctr"])
            posi = _find_col(headers, ["position"])

            if dim_i is None or ci is None or ii is None:
                continue

            scored = []
            for r in rows:
                clicks = _safe_float(r.get(headers[ci]))
                imps = _safe_float(r.get(headers[ii]))
                if clicks is None and imps is None:
                    continue
                scored.append((clicks or 0.0, r))
            scored.sort(key=lambda x: x[0], reverse=True)

            for _, r in scored[:n]:
                out_list.append({
                    "item": str(r.get(headers[dim_i], "")).strip(),
                    "clicks": int(_safe_float(r.get(headers[ci])) or 0),
                    "impressions": int(_safe_float(r.get(headers[ii])) or 0),
                    "ctr": "",
                    "position": "",
                    "evidence_ref": f"{t.get('filename')} / {t.get('sheet')}",
                    "confidence": "High",
                })
            break

    _fallback_top("queries", data_signals["top_queries"], n=MAX_LIST_ROWS)
    _fallback_top("pages", data_signals["top_pages"], n=MAX_LIST_ROWS)

    # --- Explicit last-resort fallback for Top Pages (robust against 'Top pages' headers) ---
    if not data_signals.get("top_pages"):
        for k, t in gsc_tables:
            if k not in ("pages", "unknown"):
                continue
            preview = t.get("table") or {}
            headers = preview.get("headers") or []
            rows = _table_rows_as_dicts(preview)
            if not headers or not rows:
                continue
            dim_i = _find_col(headers, ["top pages", "pages", "page", "url"])
            ci = _find_col(headers, ["clicks"])
            ii = _find_col(headers, ["impressions"])
            ctri = _find_col(headers, ["ctr"])
            posi = _find_col(headers, ["position", "avg position"])
            if dim_i is None or ci is None:
                continue
            dim_key = str(headers[dim_i])
            click_key = str(headers[ci])
            impr_key = str(headers[ii]) if ii is not None else None
            ctr_key = str(headers[ctri]) if ctri is not None else None
            pos_key = str(headers[posi]) if posi is not None else None

            def _click_val(r):
                v = _safe_float(r.get(click_key))
                return v if v is not None else -1.0

            rows_sorted = sorted(rows, key=_click_val, reverse=True)
            for r in rows_sorted[:MAX_LIST_ROWS]:
                item = str(r.get(dim_key) or "").strip()
                if not item:
                    continue
                clicks = _safe_float(r.get(click_key))
                imps = _safe_float(r.get(impr_key)) if impr_key else None
                ctr = _safe_float(r.get(ctr_key)) if ctr_key else None
                pos = _safe_float(r.get(pos_key)) if pos_key else None
                data_signals["top_pages"].append({
                    "item": item,
                    "clicks": int(clicks) if clicks is not None else "",
                    "impressions": int(imps) if imps is not None else "",
                    "ctr": f"{ctr:.2%}" if isinstance(ctr, float) and ctr <= 1.0 else (f"{ctr:.2f}" if ctr is not None else ""),
                    "position": round(pos, 2) if pos is not None else "",
                    "evidence_ref": f"{t.get('filename')} / {t.get('sheet')} (top rows)",
                    "confidence": "High" if clicks is not None else "Medium",
                })

            # Also derive opportunity pages from the same Pages table if missing
            if not data_signals.get("opportunity_pages"):
                try:
                    for r in rows_sorted:
                        item = str(r.get(dim_key) or "").strip()
                        if not item:
                            continue
                        imps = _safe_float(r.get(impr_key)) if impr_key else None
                        clicks = _safe_float(r.get(click_key))
                        ctr = _safe_float(r.get(ctr_key)) if ctr_key else None
                        pos = _safe_float(r.get(pos_key)) if pos_key else None

                        # Opportunity heuristic: impressions present, low ctr, mid SERP position
                        if imps is None or clicks is None or pos is None:
                            continue
                        if imps < 100:
                            continue
                        # ctr can be 0-1 or 0-100 depending on source; normalize
                        ctr_norm = ctr
                        if isinstance(ctr_norm, float) and ctr_norm > 1.0:
                            ctr_norm = ctr_norm / 100.0
                        if ctr_norm is None:
                            continue
                        if ctr_norm > 0.03:
                            continue
                        if pos < 8 or pos > 20:
                            continue

                        data_signals["opportunity_pages"].append({
                            "item": item,
                            "impressions": int(imps),
                            "clicks": int(clicks),
                            "ctr": f"{ctr_norm:.2%}",
                            "position": round(pos, 2),
                            "why_it_matters": "High impressions with low CTR and mid SERP position (opportunity).",
                            "evidence_ref": f"{t.get('filename')} / {t.get('sheet')} (opportunity filter)",
                            "confidence": "Medium",
                        })
                        if len(data_signals["opportunity_pages"]) >= MAX_LIST_ROWS:
                            break
                except Exception:
                    pass
            break
    # Record source selection so UI can group by reporting document
    data_signals["_gsc_source"] = best_gsc_file

    # Supplemental KPIs from non-GSC documents (e.g., DashThis PDF exports)
    supplemental: Dict[str, List[Dict[str, Any]]] = {}
    for fname, blob in (by_file or {}).items():
        if best_gsc_file and fname == best_gsc_file:
            continue
        # Skip if no tables
        tbls = blob.get("tables") or []
        kpis_out: List[Dict[str, Any]] = []
        for t in tbls:
            preview = t.get("table") or []
            # Only try KPI extraction on PDF-derived tables / small tables
            if t.get("type") not in {"pdf_table", "pdf_table_pymupdf", "pdf_text_table", "csv"} and not str(t.get("type","")).startswith("pdf"):
                continue
            src = f"{t.get('filename')} / {t.get('sheet') or 'PDF'}"
            kpis_out.extend(_extract_kpis_from_table_preview(preview, src))
        if kpis_out:
            # Cap at 50 and preserve order
            supplemental[fname] = kpis_out[:MAX_LIST_ROWS]
    data_signals["supplemental_kpis_by_source"] = supplemental

    return data_signals

SCREENSHOT_SUMMARY_SYSTEM = """You are a senior SEO reporting analyst.
Your job is to extract performance-relevant signals from a screenshot for inclusion in a monthly client report.

Rules:
- Extract ONLY what is visible in the screenshot. Do not invent metrics, labels, or causes.
- Do NOT diagnose SEO issues. Do NOT provide recommendations. Do NOT critique UI (e.g., truncated titles).
- You MAY lightly frame significance in non-causal language (e.g., "suggests", "may indicate", "aligns with").
- Keep language client-ready and concise.

Return strict JSON with keys:
- performance_summary (string): 2-5 sentences describing the key visible performance movement/patterns.
- report_note (string): 1-2 sentences suitable to include in a report (neutral, non-causal).
- highlights (array of strings): short bullets of notable visible items (e.g., top rising pages/queries, "Previously 0" entries, big deltas).
- visible_metrics (array of objects): {label, value, context, evidence_ref} for any explicit KPIs/deltas you can read.
- confidence (Low|Medium|High)
""".strip()

def _summarize_screenshot(client: OpenAI, model: str, filename: str, img_bytes: bytes, mime: str) -> Dict[str, Any]:
    """Summarize a screenshot into report-ready, non-diagnostic performance notes."""
    try:
        content = [
            {"type": "input_text", "text": f"Screenshot filename: {filename}"},
            {"type": "input_image", "image_url": f"data:{mime};base64," + base64.b64encode(img_bytes).decode("utf-8")},
        ]
        resp = client.responses.create(
            model=model,
            input=[
                {"role": "system", "content": SCREENSHOT_SUMMARY_SYSTEM},
                {"role": "user", "content": content},
            ],
            temperature=0.2,
        )
        data = _safe_json_load(resp.output_text or "")
        if isinstance(data, dict):
            # Back-compat: map older schema keys if present
            if "performance_summary" not in data:
                # Try to build from common legacy keys
                legacy_parts = []
                for k in ("summary", "extracted_summary", "summary_text", "description", "headline", "what_it_shows", "context"):
                    v = data.get(k)
                    if isinstance(v, str) and v.strip():
                        legacy_parts.append(v.strip())
                # If older stats/issues exist, convert to highlights
                highlights = data.get("highlights")
                if not isinstance(highlights, list):
                    highlights = []
                if isinstance(data.get("urls_or_topics"), list):
                    highlights.extend([str(x) for x in data.get("urls_or_topics")[:8]])
                if isinstance(data.get("stats_found"), list):
                    for s in data.get("stats_found")[:6]:
                        if isinstance(s, dict):
                            lbl = s.get("label") or s.get("metric") or ""
                            val = s.get("value") if "value" in s else s.get("val")
                            if lbl and val is not None:
                                highlights.append(f"{lbl}: {val}")
                data["highlights"] = highlights[:12]
                data["performance_summary"] = " ".join(legacy_parts).strip()

            data.setdefault("report_note", "")
            data.setdefault("highlights", [])
            data.setdefault("visible_metrics", [])
            data.setdefault("confidence", "Low")

            # Ensure file_name exists for UI/payload
            data["file_name"] = str(data.get("file_name") or filename).strip() or filename

            # Normalize confidence
            c = str(data.get("confidence") or "Low").title()
            if c not in {"Low", "Medium", "High"}:
                c = "Low"
            data["confidence"] = c

            # Hard safety: remove audit-style keys if present
            for k in ("issues_found", "technical_issues", "content_ux_issues", "serp_market_notes", "other_findings"):
                if k in data:
                    data.pop(k, None)

            return data
    except Exception:
        pass

    return {
        "file_name": filename,
        "performance_summary": "",
        "report_note": "",
        "highlights": [],
        "visible_metrics": [],
        "confidence": "Low",
    }

def _parse_work_context_from_omni(omni_notes: str) -> Dict[str, Any]:
    """Deterministically parse Omni work summaries into structured work context.

    Mandatory + first-class behavior:
    - If omni_notes is non-empty, we always attempt to parse it.
    - We NEVER rely on an LLM for this parsing.
    - We handle common Omni formats with numbered headings + subsection headings.

    Output buckets:
      completed[], in_progress[], planned[], blockers[], comms[], themes[]
    Each item is a dict with:
      item, type, targets, assignee, details, evidence_ref, confidence

    Notes on robustness:
    - Omni exports sometimes contain footnote lines like "1" / "2" on their own line.
    - Some lines may have leading bullets/Unicode dashes/zero-width chars. We normalize.
    - If we cannot detect a "Work Tasks" heading, we still parse when we see status headings
      like "Completed" / "In Progress / Ongoing" / "Added but Not Yet Started".
    """
    out = {
        "completed": [],
        "in_progress": [],
        "planned": [],
        "blockers": [],
        "comms": [],
        "themes": [],
    }

    raw = (omni_notes or "")
    if not raw.strip():
        return out

    # --- normalization helpers ---
    def _norm(s: str) -> str:
        if s is None:
            return ""
        s = s.replace("\u00a0", " ").replace("\u200b", "").replace("\ufeff", "")
        return s

    def clean_line(s: str) -> str:
        s = _norm(s).strip()
        # strip leading bullets/dashes/quotes/odd punctuation
        s = re.sub(r"^[^\w]+", "", s).strip()
        return s

    # split + clean
    lines = [clean_line(l) for l in _norm(raw).splitlines()]
    # drop empties and footnote-only numeric lines
    lines2 = []
    for l in lines:
        if not l:
            continue
        if re.fullmatch(r"\d+", l):
            continue
        lines2.append(l)

    # --- tag/target extraction ---
    def tag_type(text: str) -> str:
        low = (text or "").lower()
        if any(k in low for k in ["redirect", "sitemap", "crawl", "index", "canonical", "search functionality", "catalog search", "ftp"]):
            return "technical"
        if any(k in low for k in ["content", "faq", "duplicate", "category page", "top-level category", "copy", "unique content"]):
            return "content"
        if any(k in low for k in ["analytics", "ga4", "google analytics", "baseline", "tracking", "measurement"]):
            return "analytics"
        if any(k in low for k in ["schema", "structured data", "merchant", "made in", "country of origin"]):
            return "schema"
        if any(k in low for k in ["sort", "filter", "ux", "discoverability", "best-selling", "highest-rated"]):
            return "cro"
        return "other"

    _TARGET_MAP = [
        ("faq", ["faq"]),
        ("duplicate content", ["duplicate", "duplication"]),
        ("sitemap", ["sitemap"]),
        ("redirects", ["redirect"]),
        ("canonicals", ["canonical"]),
        ("indexing", ["index", "indexing"]),
        ("crawlability", ["crawl", "crawlability"]),
        ("catalog search", ["catalog search", "search functionality", "site search", "vehicle categories", "search inconsistencies"]),
        ("category pages", ["category page", "top-level category", "category pages"]),
        ("ga baseline", ["baseline traffic", "baseline view", "google analytics", "ga4"]),
        ("made in usa", ["made in u.s.a", "made in usa", "made in u.s.a.", "made in : usa", "country of origin"]),
        ("product schema", ["schema", "structured data"]),
        ("sorting", ["default sorting", "best-selling", "highest-rated"]),
        ("product list", ["product list", "top-selling", "top selling"]),
    ]

    def extract_targets(text: str) -> str:
        low = (text or "").lower()
        hits = []
        for label, keys in _TARGET_MAP:
            if any(k in low for k in keys):
                hits.append(label)
        # Also capture explicit URLs if present
        urls = re.findall(r"(https?://\S+)", text or "")
        hits.extend([u.rstrip(").,;") for u in urls])
        # de-dupe
        seen=set(); out_t=[]
        for t in hits:
            if t not in seen:
                out_t.append(t); seen.add(t)
        return ", ".join(out_t)

    # --- section routing ---
    def is_numbered_heading(l: str) -> bool:
        return bool(re.match(r"^\d+\.\s+", l))

    def heading_bucket(line: str) -> Optional[str]:
        low = (line or "").lower().strip().strip(":")
        if low == "completed":
            return "completed"
        if low in ["in progress", "ongoing", "in progress / ongoing", "in progress/ongoing"]:
            return "in_progress"
        if re.search(r"(added but not yet started|not yet started|planned|upcoming)", low):
            return "planned"
        if "blocker" in low or "constraint" in low:
            return "blockers"
        if "communication" in low or ("client" in low and "commun" in low) or ("updates" in low and "client" in low):
            return "comms"
        if "notes" in low or "context" in low or "strategic direction" in low or "status overview" in low or "key highlights" in low or "wins" in low:
            return "themes"
        return None

    # Parse a task block (task line + optional assignee + optional detail lines)
    def consume_task(i: int, bucket: str) -> int:
        task = lines2[i]
        assignee = ""
        details = []
        j = i + 1

        if j < len(lines2) and re.match(r"^(assignee|owner)\s*:\s*", lines2[j], re.I):
            assignee = re.sub(r"^(assignee|owner)\s*:\s*", "", lines2[j], flags=re.I).strip()
            j += 1

        while j < len(lines2) and len(details) < 6:
            nxt = lines2[j]
            hb = heading_bucket(nxt)
            # stop if we hit a status bucket header or major numbered section
            if hb in ["completed", "in_progress", "planned", "blockers", "themes", "comms"]:
                break
            if is_numbered_heading(nxt):
                break
            if nxt.endswith(":") and bucket in ["blockers", "themes", "comms"]:
                break
            # ignore stray numeric lines already filtered
            details.append(nxt)
            j += 1

        out[bucket].append({
            "item": task,
            "type": tag_type(task),
            "targets": extract_targets(task),
            "assignee": assignee,
            "details": " ".join(details).strip(),
            "evidence_ref": "Omni notes",
            "confidence": "High" if bucket in ["completed", "in_progress", "planned"] else "Medium",
        })
        return j

    current_major = "themes"
    current_status = None
    in_work_tasks = False
    in_blockers = False
    in_notes_context = False

    i = 0
    while i < len(lines2):
        l = lines2[i]
        low = l.lower()

        # Detect major headings even if not numbered
        if "work tasks" in low or "work tasks (by status)" in low:
            in_work_tasks = True
            in_blockers = False
            in_notes_context = False
            current_major = "themes"
            current_status = None
            i += 1
            continue

        # Switch numbered sections (but keep robustness)
        if is_numbered_heading(l):
            # treat as major section heading text beyond "N. "
            low2 = re.sub(r"^\d+\.\s+", "", low).strip()
            if "work tasks" in low2:
                in_work_tasks = True
                in_blockers = False
                in_notes_context = False
                current_major = "themes"
                current_status = None
            elif "blockers" in low2 or "constraints" in low2:
                in_work_tasks = False
                in_blockers = True
                in_notes_context = False
                current_major = "blockers"
                current_status = None
            elif "notes" in low2 or "context" in low2:
                in_work_tasks = False
                in_blockers = False
                in_notes_context = True
                current_major = "themes"
                current_status = None
            else:
                in_work_tasks = False
                in_blockers = False
                in_notes_context = False
                current_major = "themes"
                current_status = None
            i += 1
            continue

        hb = heading_bucket(l)

        # Fallback: if we see a status header anywhere, assume we're in work tasks
        if hb in ["completed", "in_progress", "planned"] and not in_work_tasks:
            in_work_tasks = True
            current_status = hb
            i += 1
            continue

        if in_work_tasks and hb in ["completed", "in_progress", "planned"]:
            current_status = hb
            i += 1
            continue

        # In blockers/notes sections: subsection labels ending with ":" become items
        if hb in ["blockers", "themes", "comms"] and not in_work_tasks:
            current_major = hb
            i += 1
            continue

        if in_work_tasks and current_status:
            i = consume_task(i, current_status)
            continue

        # Outside work tasks: collect themes/comms/blockers lines as items
        target_bucket = current_major
        if any(k in low for k in ["monthly email", "quarterly", "progress updates", "email summaries", "quarterly reports", "monthly emails"]):
            target_bucket = "comms"

        # Prefer section labels and meaningful statements
        if len(l) >= 12:
            out[target_bucket].append({
                "item": l,
                "type": tag_type(l),
                "targets": extract_targets(l),
                "assignee": "",
                "details": "",
                "evidence_ref": "Omni notes",
                "confidence": "Medium",
            })

        i += 1

    # Drop duplicates within each bucket (same item text)
    for k in out.keys():
        seen=set()
        dedup=[]
        for it in out[k]:
            key=(it.get("item","").strip().lower(), it.get("assignee","").strip().lower())
            if key in seen:
                continue
            seen.add(key)
            dedup.append(it)
        out[k]=dedup

    return out


def _build_seo_observations_from_screens(summaries: List[Dict[str, Any]]) -> Dict[str, Any]:
    obs = {"technical_issues": [], "content_ux_issues": [], "serp_market_notes": [], "other_findings": []}
    for s in summaries:
        for iss in (s.get("issues_found") or []):
            issue = (iss.get("issue") or "").strip()
            details = (iss.get("details") or "").strip()
            where = (iss.get("where") or "").strip()
            sev = (iss.get("severity") or "Medium").title()
            if sev not in SEVERITY_OPTIONS:
                sev = "Medium"
            entry = {
                "what": issue,
                "details": details,
                "where": where,
                "severity": sev,
                "evidence_ref": iss.get("evidence_ref") or "Screenshot",
                "snippet": details[:200] if details else "",
                "confidence": (s.get("confidence") or "Medium").title(),
            }
            low = (issue + " " + details).lower()
            if any(k in low for k in ["canonical","redirect","index","crawl","schema","duplicate","robots","sitemap","404","5xx","core web vitals","cwv"]):
                obs["technical_issues"].append(entry)
            elif any(k in low for k in ["thin","duplicate content","meta","title","description","content","internal link","copy","template"]):
                obs["content_ux_issues"].append(entry)
            elif any(k in low for k in ["serp","feature","merchant","snippet","review","shopping"]):
                obs["serp_market_notes"].append(entry)
            else:
                obs["other_findings"].append(entry)
    return obs

def _match_overlap(text: str, candidates: List[str]) -> bool:
    t = (text or "").lower()
    for c in candidates:
        c2 = (c or "").strip().lower()
        if not c2:
            continue
        if c2 in t:
            return True
    return False

def _normalize_tokens(text: str) -> List[str]:
    t = re.sub(r"[^a-z0-9:/._-]+", " ", (text or "").lower()).strip()
    toks = [x for x in t.split() if x and len(x) > 2]
    return toks

def _best_overlap(a: str, candidates: List[str]) -> Tuple[Optional[str], float]:
    """Return best candidate overlap by token Jaccard; used for cautious linking."""
    a_toks = set(_normalize_tokens(a))
    if not a_toks:
        return None, 0.0
    best = (None, 0.0)
    for c in candidates or []:
        c_toks = set(_normalize_tokens(c))
        if not c_toks:
            continue
        j = len(a_toks & c_toks) / max(1, len(a_toks | c_toks))
        if j > best[1]:
            best = (c, j)
    return best

def _collect_signal_strings(data_signals: Dict[str, Any]) -> Dict[str, List[str]]:
    # URLs
    top_pages = [str(x.get("item") or "") for x in (data_signals.get("top_pages") or []) if isinstance(x, dict)]
    opp_pages = [str(x.get("item") or "") for x in (data_signals.get("opportunity_pages") or []) if isinstance(x, dict)]
    urls = [u for u in top_pages + opp_pages if u]
    # Queries/topics
    top_queries = [str(x.get("item") or "") for x in (data_signals.get("top_queries") or []) if isinstance(x, dict)]
    opp_queries = [str(x.get("item") or "") for x in (data_signals.get("opportunity_queries") or []) if isinstance(x, dict)]
    queries = [q for q in top_queries + opp_queries if q]
    return {"urls": urls, "queries": queries}

def _collect_observation_strings(seo_obs: Dict[str, Any]) -> List[Tuple[str, Dict[str, Any]]]:
    obs = []
    for k in ["technical_issues", "content_ux_issues", "serp_market_notes", "other_findings"]:
        for o in (seo_obs.get(k) or []):
            if isinstance(o, dict):
                s = " ".join([str(o.get("what") or ""), str(o.get("details") or ""), str(o.get("where") or "")]).strip()
                if s:
                    obs.append((s, o))
    return obs

def _build_interpretive_links(work_context: Dict[str, Any], data_signals: Dict[str, Any], seo_obs: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Create cautious links between work items and observed signals/issues.

    Rules:
    - Medium confidence requires explicit overlap (URL, query, or strong token overlap with an observation).
    - Otherwise Low confidence and relationship defaults to no_clear_signal_yet.
    - Never claim causality; suggested language stays cautious.
    """
    sig = _collect_signal_strings(data_signals)
    obs = _collect_observation_strings(seo_obs)

    links: List[Dict[str, Any]] = []

    def add_link(work_item: Dict[str, Any], related: str, relationship: str, confidence: str, refs: List[str], suggested: str) -> None:
        links.append({
            "work_item": work_item.get("item") if isinstance(work_item, dict) else str(work_item),
            "related_signal": related,
            "relationship": relationship,
            "confidence": confidence,
            "evidence_refs": refs,
            "suggested_language": suggested,
        })

    for bucket in ["completed", "in_progress", "planned"]:
        for w in (work_context.get(bucket) or []):
            if not isinstance(w, dict):
                w = {"item": str(w)}
            wi = (w.get("item") or "").strip()
            if not wi:
                continue

            # 1) Link to screenshot/PDF observations
            best_ob, score = _best_overlap(wi + " " + (w.get("targets") or ""), [s for s, _ in obs])
            if best_ob and score >= 0.18:
                # find the corresponding observation dict for refs
                ob_dict = None
                for s_txt, o in obs:
                    if s_txt == best_ob:
                        ob_dict = o
                        break
                relationship = "may_be_contributing_to" if bucket == "completed" else "aligned_with"
                confidence = "Medium" if bucket == "completed" else "Low"
                refs = [x for x in [
                    (w.get("evidence_ref") or ""),
                    (ob_dict.get("evidence_ref") if ob_dict else ""),
                ] if x]
                add_link(
                    w,
                    related=f"Matches observed SEO finding: {(ob_dict.get('what') if ob_dict else best_ob)[:140]}",
                    relationship=relationship,
                    confidence=confidence,
                    refs=refs,
                    suggested=(
                        "This work item aligns with an observed issue; it may help reduce the underlying problem once completed."
                        if bucket != "completed" else
                        "Given this work was completed, monitor whether related pages/queries stabilize or improve over the next reporting window."
                    ),
                )
                continue

            # 2) Link to GSC URLs/queries when overlap is explicit
            best_url, u_score = _best_overlap(wi + " " + (w.get("targets") or ""), sig.get("urls") or [])
            best_q, q_score = _best_overlap(wi, sig.get("queries") or [])
            if (best_url and u_score >= 0.22) or (best_q and q_score >= 0.22):
                if best_url and u_score >= q_score:
                    related = f"Related page appears in performance data: {best_url}"
                    rel_ref = "Data signals: pages"
                else:
                    related = f"Related query/topic appears in performance data: {best_q}"
                    rel_ref = "Data signals: queries"
                relationship = "aligned_with" if bucket != "completed" else "may_be_contributing_to"
                confidence = "Medium" if bucket == "completed" else "Low"
                refs = [x for x in [w.get("evidence_ref") or "", rel_ref] if x]
                add_link(
                    w,
                    related=related,
                    relationship=relationship,
                    confidence=confidence,
                    refs=refs,
                    suggested=(
                        "This work may be contributing to movement in the related page/query; continue monitoring for sustained change."
                        if bucket == "completed" else
                        "Once this work is completed, monitor the related page/query for measurable movement."
                    ),
                )
                continue

            # 3) No clear signal yet (still include for narrative completeness, but keep it cautious and low)
            add_link(
                w,
                related="No explicit overlap found with current performance signals or captured SEO observations.",
                relationship="no_clear_signal_yet",
                confidence="Low",
                refs=[x for x in [w.get("evidence_ref") or ""] if x],
                suggested="No clear signal yet; keep this item in view and reassess after it has been live through a full reporting cycle.",
            )

    return links

def _build_insight_notes(supporting_context: Any, data_signals: Dict[str, Any], seo_obs: Dict[str, Any], work_context: Dict[str, Any], screen_summaries: List[Dict[str, Any]]) -> List[str]:
    """Create transparent, reviewer-friendly notes about what was (and wasn't) available."""
    notes: List[str] = []

    # Omni/work presence
    wc_counts = sum(len(work_context.get(k) or []) for k in ["completed", "in_progress", "planned"])
    if wc_counts:
        notes.append(f"Omni work summary parsed into {wc_counts} work items (Completed/In progress/Planned). Review for accuracy before drafting.")
    else:
        if (supporting_context.get("omni_notes") if isinstance(supporting_context, dict) else ""):
            notes.append("Omni notes were provided but no work items were detected. Consider formatting tasks under Completed / In Progress / Planned headings.")

    # Screenshot presence
    if screen_summaries:
        issues_n = sum(len((s.get("issues_found") or [])) for s in screen_summaries if isinstance(s, dict))
        stats_n = sum(len((s.get("stats_found") or [])) for s in screen_summaries if isinstance(s, dict))
        if issues_n or stats_n:
            notes.append(f"Screenshots analyzed: {len(screen_summaries)} (found {stats_n} stat(s) and {issues_n} potential issue(s)).")
        else:
            notes.append(f"Screenshots analyzed: {len(screen_summaries)} (no clear stats/issues detected).")

    # Data availability
    if not (data_signals.get("kpis") or data_signals.get("top_pages") or data_signals.get("top_queries")):
        notes.append("No strong performance signals detected from uploads. If available, add GSC/GA4 exports (queries/pages) for better data-driven insights.")

    # Interpretive links density
    if (work_context.get("completed") or work_context.get("in_progress") or work_context.get("planned")) and not (data_signals.get("top_pages") or data_signals.get("top_queries")):
        notes.append("Work-to-results linking is limited without page/query performance exports.")

    return notes

def build_insight_model(client: OpenAI, model: str, omni_notes: str, supporting_context: Dict[str, Any], image_triplets: List[Tuple[str, bytes, str]]) -> Dict[str, Any]:
    # Layer A
    data_signals = _build_data_signals(supporting_context)

    # Screenshots summarization (Layer B input)
    screen_summaries = []
    for fn, b, mt in (image_triplets or []):
        screen_summaries.append(_summarize_screenshot(client, model, fn, b, mt))

    seo_observations = _build_seo_observations_from_screens(screen_summaries)

    # Layer C
    work_context = _parse_work_context_from_omni(omni_notes)
    # Ensure Omni notes are present in supporting_context for transparent debug/notes
    if isinstance(supporting_context, dict):
        supporting_context["omni_notes"] = (omni_notes or "").strip()

    # Layer D
    interpretive_links = _build_interpretive_links(work_context, data_signals, seo_observations)

    insight = {
        "data_signals": data_signals,
        "seo_observations": seo_observations,
        "work_context": work_context,
        "interpretive_links": interpretive_links,
        "notes": _build_insight_notes(supporting_context, data_signals, seo_observations, work_context, screen_summaries),
        "debug": {
            "parsed_docs": supporting_context.get("_extraction_stats", {}).get("documents_count", 0),
            "parsed_tables": supporting_context.get("_extraction_stats", {}).get("tables_count", 0),
            "parsed_notes": 1 if (omni_notes or "").strip() else 0,
            "screenshots": len(image_triplets or []),
        },
        "screenshot_summaries": screen_summaries,
    }
    return insight

def _insight_signature(omni_notes: str, uploaded_files: List[Any]) -> str:
    parts = [ (omni_notes or "").strip()[:500] ]
    for f in (uploaded_files or []):
        try:
            parts.append(f"{f.name}:{len(f.getvalue())}")
        except Exception:
            parts.append(getattr(f,"name","file"))
    s = "|".join(parts)
    return str(abs(hash(s)))


def _sanitize_columns(columns: List[Any]) -> List[str]:
    """Make columns non-empty and unique for Streamlit data_editor."""
    cols: List[str] = []
    seen: Dict[str, int] = {}
    for i, c in enumerate(columns or []):
        s = "" if c is None else str(c)
        s = s.strip()
        if s == "":
            s = f"col_{i+1}"
        base = s
        if base in seen:
            seen[base] += 1
            s = f"{base}_{seen[base]}"
        else:
            seen[base] = 1
        cols.append(s)
    return cols

def _df_from_list(items: List[Dict[str, Any]], columns: List[str]) -> pd.DataFrame:
    columns = _sanitize_columns(columns)
    if not items:
        return pd.DataFrame(columns=columns)
    df = pd.DataFrame(items)
    # ensure columns order
    for c in columns:
        if c not in df.columns:
            df[c] = ""
    return df[columns]

def _normalize_table_preview(preview: Any) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Normalize a table preview into a list-of-dicts + ordered columns.

    Supported inputs:
      - {"headers": [...], "rows": [...]}  (common PDF table extraction shape)
      - list[dict]
      - list[list|tuple]
    """
    if preview is None:
        return [], []
    headers: List[str] = []
    rows: Any = preview

    if isinstance(preview, dict):
        headers = preview.get("headers") or preview.get("columns") or []
        headers = _sanitize_columns(headers)
        rows = preview.get("rows") or preview.get("data") or preview.get("table") or []
    if rows is None:
        return [], _sanitize_columns(list(headers)) if headers else []

    if not isinstance(rows, list) or len(rows) == 0:
        return [], _sanitize_columns(list(headers)) if headers else []

    first = rows[0]

    # list of dict rows
    if isinstance(first, dict):
        cols = _sanitize_columns(list(headers) if headers else list(first.keys()))
        norm_rows: List[Dict[str, Any]] = []
        for r in rows:
            if not isinstance(r, dict):
                continue
            norm_rows.append({c: r.get(c, "") for c in cols})
        return norm_rows, cols

    # list of list/tuple rows
    if isinstance(first, (list, tuple)):
        if not headers:
            headers = [f"col_{i+1}" for i in range(len(first))]
        headers = _sanitize_columns(headers)
        cols = list(headers)
        norm_rows = []
        for r in rows:
            if not isinstance(r, (list, tuple)):
                continue
            norm_rows.append({cols[i]: (r[i] if i < len(r) else "") for i in range(len(cols))})
        return norm_rows, cols

    # unknown row shape
    return [], _sanitize_columns(list(headers)) if headers else []


def _df_to_list(df: pd.DataFrame) -> List[Dict[str, Any]]:
    if df is None:
        return []
    out = []
    for _, row in df.fillna("").iterrows():
        d = {str(k): (row[k] if not (isinstance(row[k], float) and pd.isna(row[k])) else "") for k in df.columns}
        # drop completely empty rows
        if all(str(v).strip()=="" for v in d.values()):
            continue
        out.append(d)
    return out

def _reset_editor_keys(prefix: str) -> None:
    """Remove Streamlit widget state keys to force refresh after Undo/Analyze.

    Streamlit's data_editor retains internal widget state. Simply resetting the
    underlying data object often isn't enough. We aggressively clear keys
    associated with this app's evidence editors.
    """
    try:
        keys = list(st.session_state.keys())
    except Exception:
        keys = []
    for k in keys:
        ks = str(k)
        if ks.startswith(prefix) or (prefix in ks):
            try:
                del st.session_state[k]
            except Exception:
                pass


def _k(base_key: str) -> str:
    """Create a widget key that refreshes when editor_nonce changes."""
    try:
        n = int(st.session_state.get("editor_nonce", 0))
    except Exception:
        n = 0
    return f"{base_key}__{n}"

def gpt_generate_email(client: OpenAI, model: str, payload: dict, image_triplets: List[Tuple[str, bytes, str]]) -> Tuple[dict, str]:
    # Keep the same section structure across modes. The ONLY thing that changes by verbosity
    # is how much context is included within the same sections.
    v = (payload.get("verbosity_level") or "Quick scan").strip().lower()
    if v.startswith("quick"):
        schema = {
            "subject": "string",
            "monthly_overview": "2-3 sentences (max)",
            "main_kpis": ["0-12 bullets (only if truly noteworthy; otherwise empty list)"],
            "top_opportunities": {"queries": ["0-5 strings (optional)"], "pages": ["0-5 strings (optional)"]},
            "key_highlights": ["3-4 bullets (max)"],
            "wins_progress": ["2-3 bullets (max)"],
            "blockers": ["1-3 bullets (max)"],
            "completed_tasks": ["3-5 bullets (max)"],
            "outstanding_tasks": ["3-5 bullets (max)"],
            "image_captions": [{"file_name":"exact filename","caption":"optional","suggested_section":"main_kpis|top_opportunities|wins_progress|key_highlights|blockers|completed_tasks|outstanding_tasks"}],
            "dashthis_line": "short 1 sentence"
        }
    elif v.startswith("deep"):
        schema = {
            "subject": "string",
            "monthly_overview": "3-4 sentences (max)",
            "main_kpis": ["0-12 bullets (only if truly noteworthy; otherwise empty list)"],
            "top_opportunities": {"queries": ["0-5 strings (optional)"], "pages": ["0-5 strings (optional)"]},
            "key_highlights": ["4-6 bullets (max)"],
            "wins_progress": ["3-6 bullets (max)"],
            "blockers": ["2-5 bullets (max)"],
            "completed_tasks": ["5-10 bullets (max)"],
            "outstanding_tasks": ["5-10 bullets (max)"],
            "image_captions": [{"file_name":"exact filename","caption":"optional","suggested_section":"main_kpis|top_opportunities|wins_progress|key_highlights|blockers|completed_tasks|outstanding_tasks"}],
            "dashthis_line": "1-2 sentences (max)"
        }
    else:
        # Standard
        schema = {
            "subject": "string",
            "monthly_overview": "3-4 sentences (max)",
            "main_kpis": ["0-12 bullets (only if truly noteworthy; otherwise empty list)"],
            "top_opportunities": {"queries": ["0-5 strings (optional)"], "pages": ["0-5 strings (optional)"]},
            "key_highlights": ["3-5 bullets (max)"],
            "wins_progress": ["3-5 bullets (max)"],
            "blockers": ["2-4 bullets (max)"],
            "completed_tasks": ["4-8 bullets (max)"],
            "outstanding_tasks": ["4-8 bullets (max)"],
            "image_captions": [{"file_name":"exact filename","caption":"optional","suggested_section":"main_kpis|top_opportunities|wins_progress|key_highlights|blockers|completed_tasks|outstanding_tasks"}],
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
- "We addressed several technical issues that were causing..."
- "We resolved a canonical redirect issue that was causing Google to crawl fewer pages."
- "We identified and corrected an indexing issue affecting..."
- "We updated internal linking to support..."

Content rules:
- SPECIAL INSTRUCTIONS OVERRIDE (highest priority):
  - If CONTEXT.special_instructions is non-empty, follow it as the highest-priority guidance for the draft.
  - It may:
    - prohibit mentioning a topic (globally or within a specific section),
    - require specific wording in a specific section,
    - require adding, removing, or rewording specific content,
    - require global transformations (e.g., capitalization, wording normalization),
    - change ordering or emphasis across sections.
  - Apply it literally, even if it conflicts with other stylistic or structural preferences in this prompt.
  - Special Instructions override verbosity limits, section bullet limits, and output-length constraints when in conflict.
  - Non-negotiables still apply: output must be valid JSON matching the schema, and do not fabricate data.

- Special Instructions execution requirements:
  - Special Instructions must be executed exactly as written.
  - All Special Instructions must be executed.
  - You may not selectively apply or partially comply with Special Instructions.
  - If multiple Special Instructions are provided, each one must be followed.
  - Completing one instruction does not excuse failing to complete others.
  - Before finalizing the output, verify that every Special Instruction has been applied.
  - Do NOT reinterpret intent.
  - Do NOT optimize, summarize, or soften instructions.
  - Do NOT substitute a lesser or partial action.
  - Do NOT refuse or hedge unless the instruction is logically impossible to execute.

- Mandatory compliance via reasoning:
  - If a Special Instruction requires an action, you must reason until you find a way to comply using the available inputs.
  - You are expected to analyze INSIGHT_MODEL data, derive or calculate information when necessary, and make judgment calls when explicitly requested.
  - If a Special Instruction asks for subjective judgment (e.g., “you think are valuable”), making that judgment is required.
  - You may NOT respond with statements such as:
    - "There is nothing valuable"
    - "The data does not allow this"
    - "This cannot be determined"
  - Unless the task is logically impossible, you must complete it.

- Additive vs destructive change rules:
  - When a Special Instruction requires adding, inserting, or including content:
    - Preserve all existing content unless explicitly instructed otherwise.
    - Add the required content.
    - Do NOT remove, rewrite, reorder, condense, or substitute existing content.
  - When a Special Instruction requires removal or exclusion:
    - Remove only the explicitly referenced content.
    - Leave all other content untouched.
  - When a Special Instruction requires rewriting or replacing:
    - Rewrite only what is explicitly named.
    - Do not affect surrounding content.
  - If removal or replacement is not explicitly instructed, it is not permitted.

- REQUIRED EXECUTION ORDER (critical):
  - Step 1: Draft the full email content using Omni notes, INSIGHT_MODEL, and all standard rules.
  - Step 2: Apply ALL Special Instructions.
  - Step 3: Apply any Special Instructions that require global transformations (e.g., capitalization of names, exclusions) across the ENTIRE draft.
  - Step 4: Verify that every Special Instruction has been applied.
  - Step 5: Output the final JSON.

- Omni notes are the PRIMARY source of truth for what work happened, what is in progress, what is blocked, and what is planned.
- INSIGHT_MODEL is the PRIMARY source for performance numbers (Layer A), SEO observations (Layer B), and cautious work↔results context (Layer D).
- Supporting_context is SECONDARY and should only be used to clarify or corroborate items already present in the Insight Model.
- Do NOT invent metrics, results, or causality. Only include numbers that appear in INSIGHT_MODEL.data_signals or are directly visible in attached screenshots/PDF text.
- Put KPI numbers in the Main KPIs section whenever possible. Avoid repeating the same numbers across multiple sections.
- Use evidence in Key Highlights or Wins & Progress only when it is truly noteworthy (material movement, clear page/query mover, or evidence that directly supports the work narrative).
- If evidence suggests a relationship between work and results, use cautious language (e.g., "early signal", "may be contributing", "directional lift") unless explicitly stated in Omni notes.
- Never output limitation text such as "couldn't pull KPIs" or "data unavailable". If evidence is missing, omit it.

Status fidelity rules (critical):
- Treat Omni notes as literal statements of status, timing, and certainty.
- Do NOT advance or upgrade the state of work beyond what is explicitly stated in the Omni notes.
- If an Omni note describes a future, pending, or dependency-based action (e.g., "will provide", "is going to", "planned", "expected", "waiting on"), reflect it as such in the draft.
- Do NOT rephrase future or pending items as completed or in-progress work.
- Do NOT imply that inputs were received or used unless Omni notes explicitly confirm receipt or use.
- When in doubt, preserve the original tense and intent of the Omni note rather than smoothing or generalizing it.

Evidence review responsibility:
- Before drafting, review the full INSIGHT_MODEL JSON to understand all available performance data.
- Do not surface all available metrics—select only those that are relevant based on section rules.
- Use the presence or absence of data to guide what is included, not to force coverage.
- If data exists but is not appropriate to include, silently omit it.

Monthly Overview rules (refined):
- The Monthly Overview must be qualitative and work-focused.
- Do NOT include metrics, statistics, percentages, counts, or numeric performance references of any kind in the Monthly Overview.
- The Overview should characterize the month at a high level (e.g., focus, theme, or type of work), not enumerate tasks.
- Prefer framing (“what kind of month this was”) over listing activities.
- Keep the Overview close to the Omni notes wording and intent, with light cleanup only.
- Length limits still apply (based on verbosity mode).

Main KPIs selection rules:
- The Main KPIs section must be a flat bullet list (no tier labels or subheadings).
- If organic revenue, purchases/transactions, conversion rate, or average order value (AOV) are present in INSIGHT_MODEL.data_signals, they MUST be included.
- Treat these organic commerce KPIs as authoritative and client-safe when present.
- Do NOT substitute SEO visibility metrics (GSC clicks, impressions, CTR, average position) in place of organic commerce KPIs when commerce KPIs are available.
- If multiple organic commerce KPIs are present, prioritize them before adding any supporting SEO visibility metrics.
- Prefer organic-only metrics when available. If sitewide metrics are used, label them clearly.
- Follow section bullet limits strictly to avoid KPI overload unless explicitly overridden by Special Instructions.

Top Opportunities section rules (additive):
- Add a section called "Top Opportunities" directly under the Main KPIs section.
- Source this section from the existing GSC opportunity data in INSIGHT_MODEL (high impressions + low CTR queries and pages).
- Include two short lists:
  - Queries (Top 5)
  - Pages (Top 5)
- Each item should include the identifier (query or page) plus impressions and CTR (and avg position if available).
- Keep this section strictly factual and scannable; do not add narrative paragraphs.
- Do not repeat the same query or page already used as a specific example elsewhere in the report unless unavoidable.

Key Highlights section intent:
- Use this section to communicate what is most important for the client to know happened this month.
- Focus on informational significance, not exhaustive coverage.
- Avoid listing routine or process-only tasks unless they meaningfully affected delivery, data quality, or next steps.

Wins & Progress section rules:
- This section should mix execution wins with early performance validation.
- Primary source: Omni work summary wins (completed or clearly in progress).
- Secondary source (optional, max 1–2 bullets): notable non-KPI performance signals from INSIGHT_MODEL (e.g., page-level or query-level movers, CTR improvements, early visibility gains).
- Performance wins must:
  - Relate to areas that were worked on or prioritized during the month.
  - Avoid duplicating metrics already listed in Main KPIs.
  - Be framed as directional signals, not outcomes or attribution.
- Do not include revenue, purchases, or conversion rate claims in this section unless explicitly stated in Omni notes.
- If no meaningful performance wins exist, include only work wins.

Insight synthesis rules (additive):
- If the INSIGHT_MODEL reveals a clear, meaningful pattern or signal, surface ONE short consultant-style insight.
- Insights should synthesize across signals (e.g., work focus + opportunity + timing), not restate a single metric.
- Keep the insight to ONE sentence.
- Do not imply causation; use cautious language such as "signals", "suggests", "sets up", or "we’ll watch for".
- Prefer insights that explain sequencing, prioritization, or context rather than outcomes.
- Placement:
  - Do NOT place insight sentences in the Monthly Overview if they require metrics.
  - Place the insight in Key Highlights or Wins & Progress (not both).

Work and performance context:
- Performance metrics and completed or ongoing work may be referenced together when relevant (outside the Monthly Overview).
- Use neutral, professional language such as "alongside", "while", or "provides baseline context".
- Tracking fixes should be framed as improving data clarity and measurement accuracy, not performance.

Structural deduplication rule:
- Each major work item or theme should be described in detail in only one primary section.
- Other sections may reference the theme only if adding a materially different dimension (e.g., blocker, dependency, or performance signal).
- Avoid restating the same work item across Overview, Key Highlights, Wins & Progress, and Outstanding sections.

Anti-filler / single-home enforcement (critical):
- Each Omni work item or concept may appear in ONLY ONE section as a primary item.
- Do NOT reuse the same work item across multiple sections by paraphrasing it to fit the section.
- Rewording the same idea does NOT count as adding new information.
- If there are fewer distinct wins or highlights this month, prefer fewer bullets over stretching the same concept to fill space.

Screenshot-to-section association:
- If you reference or summarize insight from a screenshot in any section, you MUST assign that screenshot to the SAME section via image_captions[].suggested_section.
- If screenshots show GSC page or query movers (tables of URLs or queries with click gains), include one brief bullet in Wins & Progress summarizing 2–4 examples, and assign those screenshots to wins_progress with a concise caption naming examples.
- If a screenshot is a KPI trend chart or KPI tiles, assign it to main_kpis.

Verbosity control:
Adjust wording based on CONTEXT.verbosity_level. Do NOT add new sections in any mode.
- Quick scan (default):
  - Monthly Overview: max 2–3 sentences.
  - Focus on the most important items; drop routine maintenance unless it was a major focus.
  - Bullets should be short and scannable.
- Standard:
  - Monthly Overview: 3–4 sentences.
  - Bullets may include a short explanatory clause where helpful.
- Deep dive:
  - Provide additional context inside existing bullets (max one extra sentence per bullet).
  - Do not add extra bullets or sections.

Noise filtering:
- Do not include "reporting about reporting" unless it materially affected delivery.
- Deduplicate repeated items across sections.

Output requirements:
- Output MUST be valid JSON only and must match the provided schema exactly.
- Do not include markdown, commentary, or explanatory text.
"""

    prompt = (
        "Create a monthly SEO update email draft.\n\n"
        f"CONTEXT:\n{json.dumps(payload, indent=2)}\n\n"
        f"OUTPUT SCHEMA:\n{json.dumps(schema, indent=2)}"
    )

    content = [{"type":"input_text","text":prompt}]
    # Attach screenshots with filenames so the model can reliably map file_name -> image.
    for fn, b, mt in (image_triplets or []):
        content.append({"type":"input_text","text": f"Screenshot filename: {fn}"})
        content.append({"type":"input_image","image_url": f"data:{mt};base64," + base64.b64encode(b).decode("utf-8")})

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


def generate_monthly_email_draft(client: OpenAI, model: str, payload: dict, image_triplets: List[Tuple[str, bytes, str]]) -> Tuple[dict, str]:
    """Backward-compatible wrapper expected by the UI.

    Returns (email_json, raw_model_output).
    """
    return gpt_generate_email(client=client, model=model, payload=payload, image_triplets=image_triplets)

st.set_page_config(page_title=APP_TITLE, layout="centered")
st.markdown("""
<style>
  /* Align with quarterly tool: compact headers, consistent spacing */
  .block-container { padding-top: 1.2rem; padding-bottom: 2.2rem; }
  h1 { margin-bottom: 0.2rem; font-size: 1.75rem; }
  /* Slightly tighter section spacing */
  div[data-testid="stVerticalBlock"] > div:has(> hr) { margin-top: 0.6rem; margin-bottom: 0.6rem; }
/* --- Metamend masthead --- */
.mm-masthead {
  background: linear-gradient(90deg, #0b0f1a 0%, #162a57 45%, #4786ec 100%);
  border-radius: 16px;
  padding: 16px 18px;
  display: flex;
  align-items: center;
  gap: 16px;
  margin: 30px 0 14px 0;
  box-shadow: 0 10px 28px rgba(0,0,0,0.22);
}
.mm-masthead img {
  height: 56px;
  width: auto;
  display: block;
  object-fit: contain;
}
.mm-masthead-tagline {
  font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Inter, Roboto, Arial, sans-serif;
  font-size: 20px;
  font-weight: 600;
  line-height: 1.2;
  color: rgba(255,255,255,0.92);
  letter-spacing: 0.2px;
  margin: 0;
  padding: 0;
  white-space: nowrap;
}
/* Allow tagline to wrap on small screens */
@media (max-width: 640px) {
  .mm-masthead { flex-wrap: wrap; }
  .mm-masthead-tagline { white-space: normal; }
}

/* --- Expanders: gradient header bar with clean rounded corners --- */
div[data-testid="stExpander"] > details {
  border-radius: 14px;
  overflow: hidden;
  border: 1px solid rgba(71,134,236,0.35);
  background: linear-gradient(90deg, #0b0f1a 0%, #162a57 45%, #4786ec 100%);
}
div[data-testid="stExpander"] > details > summary {
  padding: 0.85rem 1rem;
  color: rgba(255,255,255,0.95);
  font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Inter, Roboto, Arial, sans-serif;
  font-weight: 650;
  letter-spacing: 0.15px;
}
div[data-testid="stExpander"] > details > summary:hover { filter: brightness(1.03); }
div[data-testid="stExpander"] > details[open] > summary {
  border-bottom: 1px solid rgba(255,255,255,0.14);
}
/* Expander content area */
div[data-testid="stExpander"] > details > div[role="region"] {
  background: #ffffff;
  padding: 0.25rem 0.25rem 0.6rem 0.25rem;
}
</style>
""", unsafe_allow_html=True)
def _render_masthead():
    """Render Metamend masthead (logo + tagline) at top of app."""
    try:
        logo_path = (Path(__file__).parent / "logo.png")
        if logo_path.exists():
            b64_logo = base64.b64encode(logo_path.read_bytes()).decode("utf-8")
            st.markdown(
                f"""
                <div class="mm-masthead">
                  <img src="data:image/png;base64,{b64_logo}" alt="Metamend logo" />
                  <div class="mm-masthead-tagline">AI Insight. Human Oversight.</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        else:
            # Fallback: tagline only (keeps layout stable)
            st.markdown(
                '<div class="mm-masthead"><div class="mm-masthead-tagline">AI Insight. Human Oversight.</div></div>',
                unsafe_allow_html=True,
            )
    except Exception:
        # Never block the app if branding fails
        pass

_render_masthead()


st.markdown(f'<h1 style="font-size:1.75rem; margin:0 0 0.75rem 0;">{APP_TITLE}</h1>', unsafe_allow_html=True)
st.caption("This tool combines structured evidence, SEO reasoning, and controlled narrative generation - Builds a monthly SEO update email in Outlook-ready .eml format/optional PDF export.")

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
ss_init("opening_line_choice","Custom...")
ss_init("opening_line","")
ss_init("show_opening_suggestions", False)

ss_init("omni_notes_paste_input","")
ss_init("omni_notes_pasted","")
ss_init("omni_added", False)
ss_init("verbosity_level", "Quick scan")
ss_init("model", DEFAULT_MODEL)
ss_init("show_raw", False)
ss_init("special_instructions","")

ss_init("uploaded_files", [])
ss_init("raw","")
ss_init("email_json", {})
ss_init("image_assignments", {})
ss_init("image_captions", {})

ss_init("analysis_done", False)
ss_init("analysis_signature", "")
ss_init("insight_original", {})
ss_init("insight_current", {})
ss_init("insight_locked", {})
ss_init("insight_locked_enabled", False)
ss_init("insight_editor_cache", {})  # per-section JSON/text cache for reliable undo
ss_init("editor_nonce", 0)  # increments to hard-reset all editors on Undo/Analyze


with st.expander("Inputs", expanded=True):
    st.subheader("Details")
    st.session_state.client_name = st.text_input("Company Name", value=st.session_state.client_name)
    st.session_state.website = st.text_input("Website", value=st.session_state.website, placeholder="https://...")
    st.session_state.month_label = st.text_input("Month (ex: December 2025)", value=st.session_state.month_label, placeholder="March 2026")
    st.session_state.dashthis_url = st.text_input("DashThis report URL", value=st.session_state.dashthis_url)

    # Email signature (optional) — appended to bottom of the email
    st.session_state.signature_choice = st.selectbox(
        "Email signature (Optional)",
        options=SIGNATURE_OPTIONS,
        index=SIGNATURE_OPTIONS.index(st.session_state.get("signature_choice", "None")) if st.session_state.get("signature_choice", "None") in SIGNATURE_OPTIONS else 0,
    )

    # Recipient + Opening line (optional)
    st.session_state.recipient_first_name = st.text_input(
        "Recipient first name(s)",
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
            format_func=lambda x: "Select a suggestion..." if x == "" else x,
            on_change=_apply_opener_choice,
        )

        st.text_area(
            "Opening line (custom)",
            key="opening_line",
            placeholder="e.g., Hope you're doing well — please see your monthly SEO status update below.",
            height=90,
        )

        st.caption("Pick a canned opener to fill the line, or type your own. Your latest text is what will be used.")

    uploaded = st.file_uploader(
        "Upload screenshots / supporting docs (optional)",
        type=["png","jpg","jpeg","pdf","docx","txt","xlsx","csv"],
        accept_multiple_files=True
    ) or []
    st.session_state.uploaded_files = uploaded

    st.markdown("**Paste Omni notes from Client Dashboard.**")
    omni_cols = st.columns([6, 2, 2])
    with omni_cols[0]:
        st.text_area(
            "omni_notes_paste_input_label",
            placeholder="Paste Omni notes here...",
            key="omni_notes_paste_input",
            height=220,
            label_visibility="collapsed",
        )
        # Auto-sync multi-line Omni notes into the value used for analysis
        st.session_state.omni_notes_pasted = (st.session_state.get("omni_notes_paste_input") or "").strip()
        st.session_state.omni_added = bool(st.session_state.omni_notes_pasted)


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



# Omni notes required (same as V1)
can_analyze = bool((st.session_state.omni_notes_pasted or "").strip())

# If inputs changed since last analysis, invalidate analysis + locks + draft
current_sig = _insight_signature(st.session_state.get("omni_notes_pasted",""), st.session_state.get("uploaded_files") or [])
if st.session_state.get("analysis_signature") and st.session_state.analysis_signature != current_sig:
    st.session_state.analysis_done = False
    st.session_state.insight_original = {}
    st.session_state.insight_current = {}
    st.session_state.insight_locked = {}
    st.session_state.insight_locked_enabled = False
    st.session_state.email_json = {}
    st.session_state.raw = ""
    st.session_state.analysis_signature = current_sig

# Analyze button
if not st.session_state.analysis_done:
    if st.button("Analyze Data", type="primary", disabled=not can_analyze, use_container_width=True):
        client = OpenAI(api_key=api_key)

        # Collect screenshots
        image_triplets: List[Tuple[str, bytes, str]] = []
        for f in (st.session_state.uploaded_files or []):
            fn = f.name
            low = fn.lower()
            if low.endswith((".png", ".jpg", ".jpeg")):
                b = f.getvalue()
                mime = "image/png" if low.endswith(".png") else "image/jpeg"
                image_triplets.append((fn, b, mime))

        with st.spinner("Analyzing and extracting campaign data..."):
            supporting_context = build_supporting_context(st.session_state.uploaded_files or [])
            insight = build_insight_model(
                client=client,
                model=st.session_state.model,
                omni_notes=st.session_state.omni_notes_pasted.strip(),
                supporting_context=supporting_context,
                image_triplets=image_triplets,
            )

        st.session_state.supporting_context = supporting_context
        st.session_state.insight_original = _json_deepcopy(insight)
        st.session_state.insight_current = _json_deepcopy(insight)
        st.session_state.insight_locked = {}
        st.session_state.insight_locked_enabled = False
        st.session_state.analysis_done = True
        st.session_state.analysis_signature = current_sig

        # Clear draft so user must re-generate with new evidence
        st.session_state.email_json = {}
        st.session_state.raw = ""
        st.session_state.editor_nonce = int(st.session_state.get("editor_nonce", 0)) + 1
        _reset_editor_keys("v2_")
        st.rerun()

else:
    st.success("Evidence extracted and ready for review.")

    st.divider()
    st.markdown("## Campaign Data")
    with st.expander("Campaign Data", expanded=True):

        # Undo
        st.session_state.insight_locked_enabled = False  # Locking removed for usability
        top_controls = st.columns([1, 3])
        with top_controls[0]:
            if st.button("Undo edits", type="secondary", use_container_width=True):
                st.session_state.insight_current = _json_deepcopy(st.session_state.insight_original or {})
                st.session_state.insight_locked = {}
                st.session_state.insight_locked_enabled = False
                st.session_state.editor_nonce = int(st.session_state.get("editor_nonce", 0)) + 1
                _reset_editor_keys("v2_")
                st.rerun()

        insight_obj = st.session_state.insight_current or {}
        ds = (insight_obj.get("data_signals") or {})
        wc = (insight_obj.get("work_context") or {})
        screenshot_summaries = insight_obj.get("screenshot_summaries") or []

        tabs = st.tabs(["Digital Signals", "Omni notes", "Advanced / Debug"])

        # ---------------- Digital Signals ----------------
        with tabs[0]:
            st.caption("Edit extracted campaign data if needed prior to report generation.")

            sc = st.session_state.get("supporting_context") or {}
            by_file = sc.get("_by_file") or {}
            gsc_file = ds.get("_gsc_source")

            def _coerce_number(x):
                try:
                    if x is None:
                        return None
                    s = str(x).strip().replace(",", "")
                    if s.endswith("%"):
                        return float(s[:-1]) / 100.0
                    return float(s)
                except Exception:
                    return None

            def _derive_kpis_from_tables(tables, max_rows=12):
                # Build a simple KPI list from "label/value" style rows in any detected table.
                # This is intentionally conservative: it extracts numbers but does NOT interpret them.
                out = []
                seen = set()
                if not isinstance(tables, list):
                    return out
                for t in tables:
                    preview = (t or {}).get("table") or (t or {}).get("rows") or []
                    rows_norm, cols = _normalize_table_preview(preview)
                    if not rows_norm or len(cols or []) < 2:
                        continue
                    c0, c1 = cols[0], cols[1]
                    for r in rows_norm[:200]:
                        label = str((r or {}).get(c0) or "").strip()
                        val = (r or {}).get(c1)
                        if not label or len(label) > 60:
                            continue
                        num = _coerce_number(val)
                        if num is None:
                            continue
                        key = label.lower()
                        if key in seen:
                            continue
                        seen.add(key)
                        out.append({
                            "metric": label,
                            "value": str(val).strip(),
                            "period": "",
                            "delta": "",
                            "evidence_ref": str((t or {}).get("filename") or "") + (f" / {(t or {}).get('sheet')}" if (t or {}).get("sheet") else ""),
                            "confidence": "Medium",
                        })
                        if len(out) >= max_rows:
                            return out
                return out

            def _render_kpi_mini_table(kpis, key_prefix):
                kpi_cols = ["metric","value","period","delta","evidence_ref","confidence"]
                df_k = _df_from_list(kpis or [], kpi_cols)
                df_k = st.data_editor(
                    df_k,
                    key=_k(f"{key_prefix}__kpis"),
                    use_container_width=True,
                    num_rows="dynamic",
                    disabled=st.session_state.insight_locked_enabled,
                )
                return _df_to_list(df_k)[:MAX_LIST_ROWS]

            # ---- GSC Performance Export
            if gsc_file:
                with st.expander(f"GSC Performance Export — {gsc_file}", expanded=False):
                    st.markdown("#### KPI mini table")
                    ds["kpis"] = _render_kpi_mini_table(ds.get("kpis") or [], "v2_gsc")

                    with st.expander("Details (tables)", expanded=False):
                        st.markdown("#### Top queries (≤ 50)")
                        tq_cols = ["item","clicks","impressions","ctr","position","evidence_ref"]
                        df_tq = _df_from_list(ds.get("top_queries") or [], tq_cols).head(MAX_LIST_ROWS)
                        df_tq = st.data_editor(
                            df_tq,
                            key=_k("v2_gsc_top_queries"),
                            use_container_width=True,
                            num_rows="dynamic",
                            disabled=st.session_state.insight_locked_enabled,
                        )
                        ds["top_queries"] = _df_to_list(df_tq)[:MAX_LIST_ROWS]

                        st.divider()
                        st.markdown("#### Top pages (≤ 50)")
                        tp_cols = ["item","clicks","impressions","ctr","position","evidence_ref"]
                        df_tp = _df_from_list(ds.get("top_pages") or [], tp_cols).head(MAX_LIST_ROWS)
                        df_tp = st.data_editor(
                            df_tp,
                            key=_k("v2_gsc_top_pages"),
                            use_container_width=True,
                            num_rows="dynamic",
                            disabled=st.session_state.insight_locked_enabled,
                        )
                        ds["top_pages"] = _df_to_list(df_tp)[:MAX_LIST_ROWS]

            # ---- Other uploaded documents (PDFs, CSV/XLSX tables, etc.)
            other_files = [fn for fn in sorted(by_file.keys()) if fn and fn != gsc_file and not str(fn).lower().endswith((".png",".jpg",".jpeg",".webp"))]
            for fname in other_files:
                fobj = by_file.get(fname) or {}
                tables = fobj.get("tables") or []
                docs = fobj.get("documents") or []
                with st.expander(f"{fname}", expanded=False):

                    # KPI mini table (auto-derived once; then user-editable)
                    doc_kpis_map = ds.setdefault("document_kpis", {})
                    if not isinstance(doc_kpis_map, dict):
                        doc_kpis_map = {}
                        ds["document_kpis"] = doc_kpis_map

                    if fname not in doc_kpis_map or not isinstance(doc_kpis_map.get(fname), list) or len(doc_kpis_map.get(fname) or []) == 0:
                        doc_kpis_map[fname] = _derive_kpis_from_tables(tables, max_rows=12)

                    st.markdown("#### KPI mini table")
                    doc_kpis_map[fname] = _render_kpi_mini_table(doc_kpis_map.get(fname) or [], f"v2_doc_{_slugify(fname)[:24]}")

                    with st.expander("Details (tables / extracted text)", expanded=False):
                        if not tables and not docs:
                            st.caption("No structured tables or document text detected for this file.")
                        # Tables: show one at a time (best for PDFs with many small tables)
                        if tables:
                            labels = []
                            for i, t in enumerate(tables):
                                lab = f"Table {i+1}"
                                if (t.get("sheet") or ""):
                                    lab = f"{t.get('sheet')}"
                                labels.append(lab)
                            sel = st.selectbox(
                                "Table",
                                options=list(range(len(tables))),
                                format_func=lambda i: labels[i] if i < len(labels) else str(i),
                                key=_k(f"v2_src_table_sel__{fname}"),
                            )
                            t = tables[int(sel)]
                            preview = t.get("table") or []
                            rows_norm, cols = _normalize_table_preview(preview)
                            if not rows_norm:
                                raw = t.get("raw") or t.get("text") or ""
                                st.caption("No structured rows detected for this table preview.")
                                if raw:
                                    st.text_area("Raw extracted text", value=str(raw)[:20000], height=200, key=_k(f"v2_src_raw__{fname}_{sel}"))
                            else:
                                df_t = _df_from_list(rows_norm, cols).head(MAX_LIST_ROWS)
                                df_t = st.data_editor(
                                    df_t,
                                    key=_k(f"v2_src_tbl__{fname}_{sel}"),
                                    use_container_width=True,
                                    num_rows="dynamic",
                                    disabled=st.session_state.insight_locked_enabled,
                                )
                                ds.setdefault("source_table_edits", {}).setdefault(fname, {})[str(sel)] = _df_to_list(df_t)[:MAX_LIST_ROWS]
                        # Document text snippet
                        if docs:
                            st.divider()
                            for d in docs[:3]:
                                st.markdown(f"**Extracted text — {d.get('filename','')}**")
                                st.text_area("", value=str(d.get("text") or "")[:20000], height=220, key=_k(f"v2_doc_text__{fname}_{d.get('filename','')}"), disabled=True)

            # ---- Screenshots (supporting evidence)
            if screenshot_summaries:
                with st.expander(f"Screenshots — {len(screenshot_summaries)}", expanded=False):
                    st.caption("Screenshots are treated as supporting evidence. Edit the extracted summary and optional note for the report.")

                    # Build a lookup for preview bytes by filename
                    _img_bytes_by_name = {}
                    try:
                        for uf in (st.session_state.uploaded_files or []):
                            n = getattr(uf, "name", None)
                            if n and str(n).lower().endswith((".png",".jpg",".jpeg",".webp")):
                                _img_bytes_by_name[str(n)] = uf.getvalue()
                    except Exception:
                        _img_bytes_by_name = {}

                    for i, item in enumerate(list(screenshot_summaries or [])):
                        if not isinstance(item, dict):
                            continue
                        fn = str(item.get("file_name") or f"screenshot_{i+1}")

                        with st.expander(fn, expanded=False):
                            if fn in _img_bytes_by_name:
                                st.image(_img_bytes_by_name[fn], caption=fn, use_container_width=True)

                            # Auto-fill summary & report note on first render (user can edit)
                            default_summary = str(item.get("performance_summary") or "").strip() or _build_screenshot_summary_text(item)
                            if not (item.get("extracted_summary") or "").strip():
                                item["extracted_summary"] = default_summary

                            # A short "SEO-consultant friendly" note, still non-diagnostic and non-causal.
                            if not (item.get("note_for_report") or "").strip():
                                # Prefer model-provided report_note; fallback to a trimmed performance_summary.
                                rn = str(item.get("report_note") or "").strip()
                                if rn:
                                    item["note_for_report"] = rn
                                else:
                                    ps = str(item.get("performance_summary") or item.get("extracted_summary") or "").strip()
                                    if ps:
                                        # Keep it short
                                        item["note_for_report"] = ps.splitlines()[0][:220]

                            st.markdown("**Extracted summary (GPT)**")
                            item["extracted_summary"] = st.text_area(
                                "",
                                value=str(item.get("extracted_summary") or ""),
                                height=160,
                                key=_k(f"v2_ss_extracted_summary__{i}"),
                                disabled=st.session_state.insight_locked_enabled,
                            )

                            st.markdown("**Note for report (optional)**")
                            item["note_for_report"] = st.text_area(
                                "",
                                value=str(item.get("note_for_report") or ""),
                                height=90,
                                key=_k(f"v2_ss_note_for_report__{i}"),
                                disabled=st.session_state.insight_locked_enabled,
                            )

                            c = str(item.get("confidence") or "Low")
                            conf = st.selectbox(
                                "Confidence",
                                options=["High","Medium","Low"],
                                index=["High","Medium","Low"].index(c) if c in ["High","Medium","Low"] else 2,
                                key=_k(f"v2_ss_conf__{i}"),
                                disabled=st.session_state.insight_locked_enabled,
                            )
                            item["confidence"] = conf
                            screenshot_summaries[i] = item

        # ---------------- Omni notes ----------------
        with tabs[1]:
            st.caption("Omni work notes are the core narrative for the report. Review and edit the parsed work summary below if needed.")
            st.markdown("### Parsed work summary")
            wc_cols = ["item","type","targets","assignee","details","evidence_ref","confidence"]
            for label, key in [("Completed","completed"),("In progress","in_progress"),("Planned","planned")]:
                st.markdown(f"#### {label}")
                df_w = _df_from_list(wc.get(key) or [], wc_cols)
                df_w = st.data_editor(
                    df_w,
                    key=_k(f"v2_wc_{key}"),
                    use_container_width=True,
                    num_rows="dynamic",
                    disabled=st.session_state.insight_locked_enabled,
                )
                wc[key] = _df_to_list(df_w)

            st.divider()
            b_cols = ["item","type","targets","evidence_ref","confidence"]
            st.markdown("#### Blockers / constraints")
            df_b = _df_from_list(wc.get("blockers") or [], b_cols)
            df_b = st.data_editor(
                df_b,
                key=_k("v2_wc_blockers"),
                use_container_width=True,
                num_rows="dynamic",
                disabled=st.session_state.insight_locked_enabled,
            )
            wc["blockers"] = _df_to_list(df_b)

            st.markdown("#### Themes / context")
            df_t = _df_from_list(wc.get("themes") or [], b_cols)
            df_t = st.data_editor(
                df_t,
                key=_k("v2_wc_themes"),
                use_container_width=True,
                num_rows="dynamic",
                disabled=st.session_state.insight_locked_enabled,
            )
            wc["themes"] = _df_to_list(df_t)

        # ---------------- Debug ----------------
        with tabs[2]:
            with st.expander("Evidence packet preview (debug)", expanded=False):
                sc = st.session_state.get("supporting_context") or {}
                insight_dbg = st.session_state.get("insight_current") or {}

                st.write("Parsed uploads:", sc.get("_extraction_stats", {}))
                st.write("Insight model keys:", sorted(list(insight_dbg.keys())))

                # --- Evidence packet (what the drafter is grounded on) ---
                st.markdown("#### Evidence packet (edited insight_payload)")
                st.download_button(
                    "Download evidence packet JSON",
                    data=json.dumps(insight_dbg, indent=2, ensure_ascii=False).encode("utf-8"),
                    file_name="evidence_packet.json",
                    mime="application/json",
                    key=f"dl_evidence_packet_{st.session_state.editor_nonce}",
                )
                st.json(insight_dbg)

                # --- Full payload (what gets sent to the drafting phase) ---
                full_payload_dbg = {
                    "client_name": st.session_state.client_name.strip(),
                    "website": st.session_state.website.strip(),
                    "month_label": st.session_state.month_label.strip(),
                    "dashthis_url": st.session_state.dashthis_url.strip(),
                    "omni_notes": st.session_state.omni_notes_pasted.strip(),
                    "insight_payload": insight_dbg,
                    "verbosity_level": st.session_state.get("verbosity_level", "Standard"),
                }

                st.markdown("#### Full JSON payload (drafting input)")
                st.download_button(
                    "Download full payload JSON",
                    data=json.dumps(full_payload_dbg, indent=2, ensure_ascii=False).encode("utf-8"),
                    file_name="monthly_report_payload.json",
                    mime="application/json",
                    key=f"dl_full_payload_{st.session_state.editor_nonce}",
                )
                st.json(full_payload_dbg)

        # Persist edits back
        insight_obj["data_signals"] = ds
        insight_obj["work_context"] = wc
        insight_obj["screenshot_summaries"] = screenshot_summaries
        st.session_state.insight_current = insight_obj

    st.divider()
    st.markdown("## Report Draft")
    with st.expander("Report Draft", expanded=True):
        st.caption("Configure generation settings, then generate a draft using the full edited evidence payload.")

        model = st.text_input("Model", value=st.session_state.model)
        st.session_state.model = model.strip() or st.session_state.model

        st.session_state.show_raw = st.toggle("Show GPT output (troubleshooting)", value=bool(st.session_state.show_raw))
        st.radio(
            "Email length",
            ["Quick scan", "Standard", "Deep dive"],
            key="verbosity_level",
            help="Quick scan is ultra brief. Standard adds more context. Deep dive is the most detailed within the same sections (no extra sections).",
        )


        # --- Special Instructions (Optional) ---
        # Applied at draft time as highest-priority guidance.
        st.markdown("**Special Instructions (Optional)**")
        si_cols = st.columns([6, 2])
        with si_cols[0]:
            st.text_area(
                "special_instructions_input_label",
                placeholder="""Examples:
        - Don’t mention [topic] in the email.
        - Don’t mention [topic] in Wins & Progress.
        - Add 2 additional KPI's you think are valuable to the client in Main KPI's.
        - Capitalize all peoples names.
        - Place this sentence in Key Highlights: ...""",
                key="special_instructions",
                height=150,
                label_visibility="collapsed",
            )
        with si_cols[1]:
            def _si_clear():
                st.session_state.special_instructions = ""

            st.button(
                "Clear",
                key="special_instructions_clear_btn",
                on_click=_si_clear,
                type="secondary",
                use_container_width=True,
            )

        # Generate draft button
        if st.button("Generate draft", type="primary", use_container_width=True):
            client = OpenAI(api_key=api_key)

            # Collect screenshots
            image_triplets: List[Tuple[str, bytes, str]] = []
            for f in (st.session_state.uploaded_files or []):
                fn = f.name
                low = fn.lower()
                if low.endswith((".png", ".jpg", ".jpeg")):
                    b = f.getvalue()
                    mime = "image/png" if low.endswith(".png") else "image/jpeg"
                    image_triplets.append((fn, b, mime))

            insight_for_prompt = st.session_state.insight_current

            payload = {
                "client_name": st.session_state.client_name.strip(),
                "website": st.session_state.website.strip(),
                "month_label": st.session_state.month_label.strip(),
                "dashthis_url": st.session_state.dashthis_url.strip(),
                "omni_notes": st.session_state.omni_notes_pasted.strip(),
                "insight_payload": insight_for_prompt,
                "verbosity_level": st.session_state.verbosity_level,
                "special_instructions": (st.session_state.get("special_instructions") or "").strip(),
            }

            with st.spinner("Generating draft..."):
                email_json, raw = generate_monthly_email_draft(client=client, model=st.session_state.model, payload=payload, image_triplets=image_triplets)

            st.session_state.email_json = email_json or {}
            st.session_state.raw = raw or ""

            # Seed screenshot placement/captions suggestions
            for item in (st.session_state.email_json.get("image_captions") or []):
                fn = (item.get("file_name") or "").strip()
                if fn:
                    suggested = (item.get("suggested_section") or "").strip()
                    allowed_secs = {"key_highlights","main_kpis","wins_progress","blockers","completed_tasks","outstanding_tasks"}
                    if suggested not in allowed_secs:
                        suggested = "key_highlights"
                    st.session_state.image_assignments.setdefault(fn, suggested)
                    st.session_state.image_captions.setdefault(fn, item.get("caption") or "")
    data = st.session_state.email_json or {}
    if data:
        st.markdown("### Draft (editable)")


        # Keep the top of the page simple: subject + overview, with the rest in an expander.
        subject = st.text_input("Subject", value=data.get("subject", ""))
        monthly_overview = st.text_area("Monthly overview", value=data.get("monthly_overview", ""), height=120)

        with st.expander("Edit sections", expanded=True):
            key_highlights = st.text_area("Key highlights", value="\n".join(data.get("key_highlights") or []), height=150)
            main_kpis = st.text_area("Main KPI's", value="\n".join(data.get("main_kpis") or []), height=140)

            # Top Opportunities (editable). Defaults from GPT output; if missing, derives from Insight Model opportunities.
            _top_opps_seed = data.get("top_opportunities") or {}
            if not (isinstance(_top_opps_seed, dict) and (_top_opps_seed.get("queries") or _top_opps_seed.get("pages"))):
                _top_opps_seed = _derive_top_opportunities_from_insight(st.session_state.get("insight_current") or {}, max_items=5)

            st.markdown("**Top Opportunities**")
            top_opps_queries_text = st.text_area(
                "Queries (Top 5)",
                value="\n".join((_top_opps_seed.get("queries") or [])[:5]),
                height=110,
            )
            top_opps_pages_text = st.text_area(
                "Pages (Top 5)",
                value="\n".join((_top_opps_seed.get("pages") or [])[:5]),
                height=110,
            )
            wins_progress = st.text_area("Wins & progress", value="\n".join(data.get("wins_progress") or []), height=170)
            blockers = st.text_area("Blockers / risks", value="\n".join(data.get("blockers") or []), height=140)
            completed_tasks = st.text_area("Completed tasks", value="\n".join(data.get("completed_tasks") or []), height=170)
            outstanding_tasks = st.text_area("Outstanding tasks", value="\n".join(data.get("outstanding_tasks") or []), height=170)
            dashthis_line = st.text_area("DashThis line", value=data.get("dashthis_line", ""), height=70)

            st.divider()
            st.subheader("Screenshots Placement")
            imgs = [f for f in (st.session_state.uploaded_files or []) if f.name.lower().endswith((".png",".jpg",".jpeg"))]
            if not imgs:
                st.caption("No screenshots uploaded.")
            else:
                with st.expander("Optional: adjust screenshot placement / captions", expanded=False):
                    st.caption("By default, the app will place screenshots automatically. Use this only if you want to override placement or edit captions.")
                    section_options = ["key_highlights","main_kpis","wins_progress","blockers","completed_tasks","outstanding_tasks"]
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
            main_kpis_list = _lines(main_kpis)
            wins_list = _lines(wins_progress)
            blockers_list = _lines(blockers)
            completed_list = _lines(completed_tasks)
            outstanding_list = _lines(outstanding_tasks)

            sec_high = section_block("Key highlights", bullets_to_html(highlights_list))
            # Top Opportunities (additive): use editor values; safe defaults to empty.
            top_opps = {
                "queries": _lines(top_opps_queries_text)[:5],
                "pages": _lines(top_opps_pages_text)[:5],
            }
            try:
                st.session_state.email_json["top_opportunities"] = copy.deepcopy(top_opps)
            except Exception:
                pass

            # Standalone Top Opportunities section HTML (for templates that support a dedicated placeholder).
            sec_top_opps = ""
            try:
                _q = top_opps.get("queries") or []
                _p = top_opps.get("pages") or []
                _q = [str(x).strip() for x in _q if str(x).strip()][:5]
                _p = [str(x).strip() for x in _p if str(x).strip()][:5]
                if _q or _p:
                    _parts = []
                    if _q:
                        _parts.append('<div style="font-weight:700;margin:0 0 6px 0;">Queries (Top 5)</div>')
                        _parts.append(bullets_to_html(_q))
                    if _p:
                        _parts.append('<div style="font-weight:700;margin:12px 0 6px 0;">Pages (Top 5)</div>')
                        _parts.append(bullets_to_html(_p))
                    sec_top_opps = section_block("Top Opportunities", "\n".join(_parts))
            except Exception:
                sec_top_opps = ""

            # Main KPI's section: KPIs only (Top Opportunities is rendered as its own section when supported).
            sec_kpis = section_block("Main KPI\'s", bullets_to_html(main_kpis_list))

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
            sec_kpis = append_images(sec_kpis, "main_kpis")
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

            # Template compatibility: some templates include an explicit Top Opportunities placeholder.
            # If missing, append the Top Opportunities section directly after Main KPI's.
            _main_kpis_block = sec_kpis
            if "{{SECTION_TOP_OPPORTUNITIES}}" not in TEMPLATE_HTML:
                _main_kpis_block = sec_kpis + "\n" + sec_top_opps


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
                .replace("{{SECTION_MAIN_KPIS}}", _main_kpis_block)
                .replace("{{SECTION_TOP_OPPORTUNITIES}}", sec_top_opps)
                .replace("{{SECTION_WINS_PROGRESS}}", sec_wins)
                .replace("{{SECTION_BLOCKERS}}", sec_blk)
                .replace("{{SECTION_COMPLETED_TASKS}}", sec_done)
                .replace("{{SECTION_OUTSTANDING_TASKS}}", sec_next)
            )

            eml_bytes = build_eml(subject, html_out, image_parts)
            # Validate EML bytes for Streamlit download_button (must be bytes-like)
            if not isinstance(eml_bytes, (bytes, bytearray)) or len(eml_bytes) == 0:
                # Keep None so the download button can be conditionally hidden.
                eml_bytes = None

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
                if isinstance(eml_bytes, (bytes, bytearray)) and len(eml_bytes) > 0:
                    st.download_button(
                        "Download .eml (Outlook-ready)",
                        data=eml_bytes,
                        file_name=eml_filename,
                        mime="message/rfc822",
                    )
                else:
                    st.info("EML export unavailable for this draft (no valid EML bytes were produced). Try regenerating the draft or removing problematic images.")

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

    if st.session_state.show_raw and st.session_state.raw:
            with st.expander("GPT output (raw)"):
                st.code(st.session_state.raw)
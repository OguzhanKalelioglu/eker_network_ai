# pip install fastapi uvicorn google-api-python-client google-auth-httplib2 google-auth
from fastapi import FastAPI, Request as FastAPIRequest, Response
from fastapi import Form, HTTPException
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import base64, os, httpx, json, time, re, asyncio, sqlite3, threading
import csv, ast, unicodedata
from httpx import ReadTimeout
from html import unescape
from email.message import EmailMessage
from typing import List, Dict, Any
from starlette.middleware.sessions import SessionMiddleware
from passlib.hash import pbkdf2_sha256

# Template utilities
def load_template(filename: str) -> str:
    """HTML template dosyasını yükler"""
    template_path = os.path.join(os.path.dirname(__file__), "templates", filename)
    try:
        with open(template_path, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return f"<h1>Template bulunamadı: {filename}</h1>"

def render_layout(page: str, title: str, content: str, extra_styles: str = "", scripts: str = "", header_actions: str = "") -> str:
    """Ortak layout template'ini kullanarak sayfa render eder"""
    layout = load_template("layout.html")
    
    # Aktif sayfa vurgulaması
    logs_active = "active" if page == "logs" else ""
    instructions_active = "active" if page == "instructions" else ""
    emails_active = "active" if page == "emails" else ""
    
    # Sayfa ikonları
    icons = {
        "logs": "fas fa-chart-line",
        "instructions": "fas fa-brain", 
        "emails": "fas fa-envelope"
    }
    
    return layout.replace("{{title}}", title)\
                 .replace("{{page_title}}", title)\
                 .replace("{{page_icon}}", icons.get(page, "fas fa-circle"))\
                 .replace("{{content}}", content)\
                 .replace("{{extra_styles}}", extra_styles)\
                 .replace("{{scripts}}", scripts)\
                 .replace("{{header_actions}}", header_actions)\
                 .replace("{{logs_active}}", logs_active)\
                 .replace("{{instructions_active}}", instructions_active)\
                 .replace("{{emails_active}}", emails_active)

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key="eker-ai-session-key-2025", same_site="lax")

# --- Configuration ---
GCP_PROJECT_ID = "intranet-456210"
PUBSUB_TOPIC_NAME = "projects/intranet-456210/topics/gmail-push-topic"
LAST_HISTORY_ID_FILE = "last_history_id.txt"
DEDUP_FILE = "replied_message_ids.txt"
REPLIED_THREADS_FILE = "replied_thread_ids.txt"
# Instruction ve e-posta ayarları DB'den gelir
# --- End Configuration ---

# If modifying these scopes, delete the file token.json.
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.send",
]
creds = None
# The file token.json stores the user's access and refresh tokens, and is
# created automatically when the authorization flow completes for the first
# time.
if os.path.exists("token.json"):
  creds = Credentials.from_authorized_user_file("token.json", SCOPES)
# If there are no (valid) credentials available, let the user log in.
if not creds or not creds.valid:
  if creds and creds.expired and creds.refresh_token:
    creds.refresh(Request())
  else:
    flow = InstalledAppFlow.from_client_secrets_file(
        "credentials.json", SCOPES
    )
    creds = flow.run_local_server(port=0)
  # Save the credentials for the next run
  with open("token.json", "w") as token:
    token.write(creds.to_json())


gmail = build("gmail", "v1", credentials=creds)

from typing import Optional


def _b64url_decode_to_text(data_b64: str) -> str:
  try:
    return base64.urlsafe_b64decode((data_b64 or "") + "==").decode("utf-8", errors="ignore")
  except Exception:
    try:
      return base64.b64decode((data_b64 or "")).decode("utf-8", errors="ignore")
    except Exception:
      return ""


def _html_to_text(html: str) -> str:
  try:
    s = html or ""
    # Remove scripts/styles
    s = re.sub(r"<\s*(script|style)[^>]*>[\s\S]*?<\/\s*\1\s*>", " ", s, flags=re.IGNORECASE)
    # Line breaks for common tags
    s = re.sub(r"<\s*br\s*/?\s*>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"<\s*/\s*p\s*>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"<\s*/\s*div\s*>", "\n", s, flags=re.IGNORECASE)
    # Strip all tags
    s = re.sub(r"<[^>]+>", " ", s)
    s = unescape(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s
  except Exception:
    return (html or "").strip()


def _extract_body_text(payload: Dict[str, Any]) -> str:
  """MIME payload'dan text/plain veya text/html metni çıkarır (öncelik: text/plain)."""
  best_html: Optional[str] = None

  def walk(part: Dict[str, Any]) -> Optional[str]:
    mime = (part.get("mimeType") or "").lower()
    body = part.get("body") or {}
    data = body.get("data")
    if mime == "text/plain" and data:
      return _b64url_decode_to_text(data)
    if mime == "text/html" and data:
      nonlocal best_html
      try:
        best_html = _b64url_decode_to_text(data)
      except Exception:
        best_html = None
    for child in part.get("parts", []) or []:
      got = walk(child)
      if got:
        return got
    return None

  if not payload:
    return ""
  # Try direct
  got = walk(payload)
  if got:
    return got
  # Fallback to HTML
  if best_html:
    return _html_to_text(best_html)
  return ""

###############################################################################
# Warehouse veri kaynağı (CSV/JSONL): yükleme ve arama yardımcıları              #
###############################################################################

WAREHOUSE_CSV = "warehouse_info.csv"
WAREHOUSE_JSONL = "warehouse_info.jsonl"
_warehouse_records: List[Dict[str, Any]] = []
_warehouse_by_id: Dict[str, Dict[str, Any]] = {}


def _normalize_text(value: str) -> str:
  try:
    v = unicodedata.normalize("NFKD", value or "")
    v = "".join(ch for ch in v if not unicodedata.combining(ch))
    v = v.lower()
    v = re.sub(r"[^a-z0-9\s/:,.-]", " ", v)
    v = re.sub(r"\s+", " ", v).strip()
    return v
  except Exception:
    return (value or "").strip().lower()


def _warehouse_init() -> None:
  """CSV'den bölge kayıtlarını belleğe yükler."""
  try:
    path = os.path.join(os.getcwd(), WAREHOUSE_CSV)
    if not os.path.exists(path):
      _append_log({"type": "warehouse_init", "status": "csv_not_found", "path": path})
      return
    global _warehouse_records, _warehouse_by_id
    _warehouse_records = []
    _warehouse_by_id = {}
    with open(path, "r", encoding="utf-8") as f:
      reader = csv.DictReader(f)
      for row in reader:
        row_id = str(row.get("") or row.get("id") or row.get("index") or "").strip()
        bolge = str(row.get("bolge") or "").strip()
        sehir = str(row.get("sehir") or "").strip()
        adres = str(row.get("adres") or "").strip()
        ips_raw = row.get("ips")
        ips: List[str] = []
        if isinstance(ips_raw, list):
          ips = [str(x) for x in ips_raw]
        else:
          try:
            parsed = ast.literal_eval(ips_raw) if ips_raw else []
            if isinstance(parsed, (list, tuple)):
              ips = [str(x) for x in parsed]
          except Exception:
            ips = [str(ips_raw)] if ips_raw else []
        rec = {
          "region_id": row_id,
          "bolge": bolge,
          "sehir": sehir,
          "adres": adres,
          "ips": ips,
          "norm_bolge": _normalize_text(bolge),
        }
        _warehouse_records.append(rec)
        if row_id:
          _warehouse_by_id[row_id] = rec
    _append_log({"type": "warehouse_init", "status": "ok", "count": len(_warehouse_records)})
  except Exception as e:
    _append_log({"type": "warehouse_init_error", "error": str(e)})


def _warehouse_convert_csv_to_jsonl(csv_path: str, jsonl_path: str) -> int:
  """CSV dosyasını JSONL satırlarına dönüştürür. Dönüşte yazılan kayıt sayısını verir."""
  count = 0
  with open(csv_path, "r", encoding="utf-8") as fin, open(jsonl_path, "w", encoding="utf-8") as fout:
    reader = csv.DictReader(fin)
    for row in reader:
      row_id = str(row.get("") or row.get("id") or row.get("index") or "").strip()
      bolge = str(row.get("bolge") or "").strip()
      sehir = str(row.get("sehir") or "").strip()
      adres = str(row.get("adres") or "").strip()
      ips_raw = row.get("ips")
      ips: List[str] = []
      if isinstance(ips_raw, list):
        ips = [str(x) for x in ips_raw]
      else:
        try:
          parsed = ast.literal_eval(ips_raw) if ips_raw else []
          if isinstance(parsed, (list, tuple)):
            ips = [str(x) for x in parsed]
        except Exception:
          ips = [str(ips_raw)] if ips_raw else []
      rec = {
        "region_id": row_id,
        "bolge": bolge,
        "sehir": sehir,
        "adres": adres,
        "ips": ips,
      }
      fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
      count += 1
  return count


def _warehouse_prepare_sources() -> None:
  """JSONL varsa onu kullan; yoksa CSV'yi JSONL'a çevir. Belleğe toplu yükleme gerekmez."""
  try:
    csv_path = os.path.join(os.getcwd(), WAREHOUSE_CSV)
    jsonl_path = os.path.join(os.getcwd(), WAREHOUSE_JSONL)
    if os.path.exists(jsonl_path):
      _append_log({"type": "warehouse_source", "status": "jsonl_ready", "path": jsonl_path})
      return
    if os.path.exists(csv_path):
      try:
        count = _warehouse_convert_csv_to_jsonl(csv_path, jsonl_path)
        _append_log({"type": "warehouse_convert", "status": "ok", "count": count, "src": csv_path, "dst": jsonl_path})
      except Exception as e:
        _append_log({"type": "warehouse_convert", "status": "error", "error": str(e)})
        # Dönüşüm başarısızsa en azından belleğe yüklemeyi deneyelim
        _warehouse_init()
    else:
      _append_log({"type": "warehouse_source", "status": "missing", "csv": csv_path, "jsonl": jsonl_path})
  except Exception as e:
    _append_log({"type": "warehouse_prepare_error", "error": str(e)})


def _iter_warehouse_jsonl():
  """JSONL'ı satır satır okuyup kayıt döndürür."""
  jsonl_path = os.path.join(os.getcwd(), WAREHOUSE_JSONL)
  if not os.path.exists(jsonl_path):
    return
  with open(jsonl_path, "r", encoding="utf-8") as f:
    for line in f:
      line = line.strip()
      if not line:
        continue
      try:
        rec = json.loads(line)
        yield rec
      except Exception:
        continue


def _lookup_jsonl(query: Optional[str], region_id: Optional[str]) -> Optional[Dict[str, Any]]:
  jsonl_path = os.path.join(os.getcwd(), WAREHOUSE_JSONL)
  if not os.path.exists(jsonl_path):
    return None
  # ID öncelik
  if region_id:
    rid = str(region_id)
    for rec in _iter_warehouse_jsonl():
      if str(rec.get("region_id")) == rid:
        return {k: rec.get(k) for k in ["region_id", "bolge", "sehir", "adres", "ips"]}
    return None
  # İsim sorgusu
  if not query:
    return None
  q = _normalize_text(query)
  # Tam eşleşme
  for rec in _iter_warehouse_jsonl():
    if _normalize_text(str(rec.get("bolge") or "")) == q:
      return {k: rec.get(k) for k in ["region_id", "bolge", "sehir", "adres", "ips"]}
  # İçeren eşleşme
  for rec in _iter_warehouse_jsonl():
    if q in _normalize_text(str(rec.get("bolge") or "")):
      return {k: rec.get(k) for k in ["region_id", "bolge", "sehir", "adres", "ips"]}
  # Sayı verilmişse ID olarak dene
  if q.isdigit():
    for rec in _iter_warehouse_jsonl():
      if str(rec.get("region_id")) == q:
        return {k: rec.get(k) for k in ["region_id", "bolge", "sehir", "adres", "ips"]}
  return None


def _lookup_csv_stream(query: Optional[str], region_id: Optional[str]) -> Optional[Dict[str, Any]]:
  csv_path = os.path.join(os.getcwd(), WAREHOUSE_CSV)
  if not os.path.exists(csv_path):
    return None
  with open(csv_path, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    if region_id:
      rid = str(region_id)
      for row in reader:
        row_id = str(row.get("") or row.get("id") or row.get("index") or "").strip()
        if row_id == rid:
          ips_raw = row.get("ips")
          ips: List[str] = []
          try:
            parsed = ast.literal_eval(ips_raw) if ips_raw else []
            if isinstance(parsed, (list, tuple)):
              ips = [str(x) for x in parsed]
          except Exception:
            ips = [str(ips_raw)] if ips_raw else []
          return {
            "region_id": row_id,
            "bolge": str(row.get("bolge") or "").strip(),
            "sehir": str(row.get("sehir") or "").strip(),
            "adres": str(row.get("adres") or "").strip(),
            "ips": ips,
          }
      return None
    # İsim sorgusu
    if not query:
      return None
    q = _normalize_text(query)
    # Tam eşleşme ve içeren için tek geçişte bakmak adına iki aşama yapıyoruz
    rows_cache = []
    for row in reader:
      rows_cache.append(row)
      if _normalize_text(str(row.get("bolge") or "")) == q:
        ips_raw = row.get("ips")
        ips: List[str] = []
        try:
          parsed = ast.literal_eval(ips_raw) if ips_raw else []
          if isinstance(parsed, (list, tuple)):
            ips = [str(x) for x in parsed]
        except Exception:
          ips = [str(ips_raw)] if ips_raw else []
        return {
          "region_id": str(row.get("") or row.get("id") or row.get("index") or "").strip(),
          "bolge": str(row.get("bolge") or "").strip(),
          "sehir": str(row.get("sehir") or "").strip(),
          "adres": str(row.get("adres") or "").strip(),
          "ips": ips,
        }
    for row in rows_cache:
      if q in _normalize_text(str(row.get("bolge") or "")):
        ips_raw = row.get("ips")
        ips: List[str] = []
        try:
          parsed = ast.literal_eval(ips_raw) if ips_raw else []
          if isinstance(parsed, (list, tuple)):
            ips = [str(x) for x in parsed]
        except Exception:
          ips = [str(ips_raw)] if ips_raw else []
        return {
          "region_id": str(row.get("") or row.get("id") or row.get("index") or "").strip(),
          "bolge": str(row.get("bolge") or "").strip(),
          "sehir": str(row.get("sehir") or "").strip(),
          "adres": str(row.get("adres") or "").strip(),
          "ips": ips,
        }
  return None


def get_warehouse_info(query: Optional[str] = None, region_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
  """Bölge adı veya region_id ile tek kayıt döndürür. Önce JSONL, sonra CSV'e düşer.
  JSONL/CSV akış okuma kullanılır; tüm dosya belleğe yüklenmez.
  """
  # Önce JSONL'dan dene
  result = _lookup_jsonl(query=query, region_id=region_id)
  if result is not None:
    return result
  # JSONL yoksa CSV akışından dene
  result = _lookup_csv_stream(query=query, region_id=region_id)
  if result is not None:
    return result
  # Son çare: belleğe yüklenmişse kullan
  if region_id:
    rec = _warehouse_by_id.get(str(region_id))
    if rec:
      return {k: rec[k] for k in ["region_id", "bolge", "sehir", "adres", "ips"]}
  if query:
    q = _normalize_text(query)
    for rec in _warehouse_records:
      if rec.get("norm_bolge") == q or q in rec.get("norm_bolge", ""):
        return {k: rec[k] for k in ["region_id", "bolge", "sehir", "adres", "ips"]}
  return None


# Ollama chat + function calling (tools)
WAREHOUSE_TOOL_DEF = {
  "type": "function",
  "function": {
    "name": "get_warehouse_info",
    "description": "Belirtilen bölgenin IP listesi, şehir ve açık adresini getirir.",
    "parameters": {
      "type": "object",
      "properties": {
        "query": {"type": "string", "description": "Bölge adı/ifadesi. Örn: 'İstanbul Avrupa Bölge Müdürlüğü'"},
        "region_id": {"type": "string", "description": "CSV ilk sütun kimliği. Örn: '5'"}
      },
      "required": []
    }
  }
}


# Oracle: Kullanıcının e-postasından depo/bölge bilgisini getir
def _extract_single_email(value: str) -> str:
  try:
    if not value:
      return ""
    # Header biçimi: "Ad Soyad <mail@eker.com>" olabilir
    m = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", str(value))
    return m.group(0).lower() if m else str(value).strip().lower()
  except Exception:
    return str(value or "").strip().lower()


def get_user_warehouse_info(email: str) -> Optional[Dict[str, Any]]:
  """Oracle prosedürü üzerinden e-posta ile çalışanın depo/bölge bilgisini getirir.

  Dönen örnek veri:
  {"DEPOADI":"Antalya","BOLGEKOD":"ANT","LONGNAME":"Vahdet Yücel","EMAIL":"vahdetyucel@eker.com"}
  """
  try:
    e = _extract_single_email(email)
    if not e:
      return None
    url = "https://portal.eker.com/pls/gpt/GPT_FUNC.GET_USER_WAREHOUSE_INFO"
    r = httpx.get(url, params={"P_EMAIL": e}, timeout=8.0)
    if r.status_code != 200:
      _append_log({"type": "tool_error", "tool": "get_user_warehouse_info", "error": f"http_status_{r.status_code}"})
      return None
    try:
      data = r.json()
    except Exception:
      data = json.loads((r.text or "").strip() or "{}")
    # Beklenen anahtarlar: DEPOADI, BOLGEKOD, LONGNAME, EMAIL
    if isinstance(data, dict) and (data.get("DEPOADI") or data.get("BOLGEKOD")):
      return {
        "DEPOADI": data.get("DEPOADI"),
        "BOLGEKOD": data.get("BOLGEKOD"),
        "LONGNAME": data.get("LONGNAME"),
        "EMAIL": data.get("EMAIL") or e,
      }
    return None
  except Exception as e:
    try:
      _append_log({"type": "tool_error", "tool": "get_user_warehouse_info", "error": str(e)})
    except Exception:
      pass
    return None


USER_WAREHOUSE_BY_EMAIL_TOOL_DEF = {
  "type": "function",
  "function": {
    "name": "get_user_warehouse_info",
    "description": "E-posta adresi ile çalışanın depo/bölge bilgisini Oracle servisten getirir (DEPOADI, BOLGEKOD).",
    "parameters": {
      "type": "object",
      "properties": {
        "email": {"type": "string", "description": "Çalışanın e-posta adresi. Örn: adsoyad@eker.com"}
      },
      "required": ["email"]
    }
  }
}


async def _ollama_chat_with_tools_execute(model: str, system_text: str, user_text: str, timeout: float = 90.0, ctx: Optional[Dict[str, Any]] = None) -> str:
  messages: List[Dict[str, Any]] = []
  if system_text:
    messages.append({"role": "system", "content": system_text})
  messages.append({"role": "user", "content": user_text})
  tools = [WAREHOUSE_TOOL_DEF, USER_WAREHOUSE_BY_EMAIL_TOOL_DEF]

  async with httpx.AsyncClient() as client:
    for _ in range(3):
      r = await client.post(
        "http://localhost:11435/api/chat",
        json={"model": model, "messages": messages, "tools": tools, "stream": False},
        timeout=timeout,
      )
      data = r.json()
      msg = data.get("message") or {}
      tool_calls = msg.get("tool_calls") or []
      if not tool_calls:
        content = msg.get("content", "") or data.get("content", "")
        return (content or "").strip()

      # Araç çağrılarını yürüt ve sonuçları mesaja ekle
      for call in tool_calls:
        fn = ((call.get("function") or {}).get("name") or "").strip()
        args = (call.get("function") or {}).get("arguments")
        if isinstance(args, str):
          try:
            args = json.loads(args)
          except Exception:
            args = {}
        if not isinstance(args, dict):
          args = {}

        # Log: tool çağrısı
        try:
          log_ev = {"type": "tool_call", "tool": fn, "args": args}
          if ctx:
            if ctx.get("threadId"):
              log_ev["threadId"] = ctx.get("threadId")
            if ctx.get("subject"):
              log_ev["subject"] = ctx.get("subject")
          _append_log(log_ev)
        except Exception:
          pass

        if fn == "get_warehouse_info":
          try:
            result = get_warehouse_info(query=args.get("query"), region_id=args.get("region_id"))
          except Exception as e:
            result = None
            try:
              err_ev = {"type": "tool_error", "tool": fn, "error": str(e)}
              if ctx and ctx.get("threadId"):
                err_ev["threadId"] = ctx.get("threadId")
              _append_log(err_ev)
            except Exception:
              pass
          # Log: tool sonucu (özet)
          try:
            summary = None
            if isinstance(result, dict):
              summary = {k: result.get(k) for k in ["region_id", "bolge"]}
            res_ev = {"type": "tool_result", "tool": fn, "summary": summary}
            if ctx:
              if ctx.get("threadId"):
                res_ev["threadId"] = ctx.get("threadId")
              if ctx.get("subject"):
                res_ev["subject"] = ctx.get("subject")
            _append_log(res_ev)
          except Exception:
            pass
          tool_msg = {"role": "tool", "name": "get_warehouse_info", "content": json.dumps({"ok": True, "result": result}, ensure_ascii=False)}
        elif fn == "get_user_warehouse_info":
          try:
            email_value = args.get("email") or args.get("from") or args.get("from_addr")
            result = get_user_warehouse_info(email=email_value)
          except Exception as e:
            result = None
            try:
              err_ev = {"type": "tool_error", "tool": fn, "error": str(e)}
              if ctx and ctx.get("threadId"):
                err_ev["threadId"] = ctx.get("threadId")
              _append_log(err_ev)
            except Exception:
              pass
          # Log: tool sonucu (özet)
          try:
            summary = None
            if isinstance(result, dict):
              summary = {k: result.get(k) for k in ["DEPOADI", "BOLGEKOD", "EMAIL"]}
            res_ev = {"type": "tool_result", "tool": fn, "summary": summary}
            if ctx:
              if ctx.get("threadId"):
                res_ev["threadId"] = ctx.get("threadId")
              if ctx.get("subject"):
                res_ev["subject"] = ctx.get("subject")
            _append_log(res_ev)
          except Exception:
            pass
          tool_msg = {"role": "tool", "name": "get_user_warehouse_info", "content": json.dumps({"ok": True, "result": result}, ensure_ascii=False)}
        else:
          try:
            unk_ev = {"type": "tool_unknown", "tool": fn}
            if ctx and ctx.get("threadId"):
              unk_ev["threadId"] = ctx.get("threadId")
            _append_log(unk_ev)
          except Exception:
            pass
          tool_msg = {"role": "tool", "name": fn, "content": json.dumps({"ok": False, "error": "unknown_tool"}, ensure_ascii=False)}

        messages.append(tool_msg)

  return ""

# Eski txt fonksiyonları -> SQLite fonksiyonlarına yönlendirme
def _read_last_history_id() -> Optional[int]:
    return _db_get_history_id()

def _write_last_history_id(value: int) -> None:
    _db_set_history_id(value)

def _dedup_has(message_id: str) -> bool:
    return _db_dedup_has_message(message_id)

def _dedup_add(message_id: str) -> None:
    _db_dedup_add_message(message_id)

def _thread_dedup_has(thread_id: str) -> bool:
    return _db_dedup_has_thread(thread_id)

def _thread_dedup_add(thread_id: str) -> None:
    _db_dedup_add_thread(thread_id)


def _normalize_subject(subject: str) -> str:
  if not subject:
    return ""
  s = subject.strip()
  # Remove common reply/forward prefixes (Turkish and English)
  prefixes = ["re:", "fwd:", "fw:", "ynt:", "yanıt:", "yönlendir:"]
  changed = True
  while changed and s:
    changed = False
    low = s.lower().lstrip()
    for p in prefixes:
      if low.startswith(p):
        s = s[len(s) - len(low) + len(p):].lstrip()
        changed = True
        break
  return s.strip().lower()


async def ensure_watch():
    gmail.users().watch(
        userId="me",
        body={
            "topicName": f"projects/{os.environ['GCP_PROJECT']}/topics/{os.environ['PUBSUB_TOPIC']}",
            "labelIds": ["INBOX"],
            "labelFilterBehavior": "INCLUDE",
        },
    ).execute()

async def _ollama_generate(model: str, prompt: str, timeout: float = 60.0, retries: int = 2) -> str:
    last_err = None
    for attempt in range(retries + 1):
        try:
            async with httpx.AsyncClient() as client:
                r = await client.post(
                    "http://localhost:11435/api/generate",
                    json={"model": model, "prompt": prompt, "stream": False},
                    timeout=timeout,
                )
                return r.json().get("response", "").strip()
        except (ReadTimeout, httpx.HTTPError) as e:
            last_err = e
            await asyncio.sleep(0.6 * (attempt + 1))
    raise last_err or RuntimeError("LLM call failed")


# Durable SQLite queue --------------------------------------------------------
_db_conn: Optional[sqlite3.Connection] = None
_db_lock = threading.Lock()


def _db_init() -> None:
  global _db_conn
  if _db_conn is None:
    _db_conn = sqlite3.connect(QUEUE_DB, check_same_thread=False)
    _db_conn.execute(
      """
      CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_ts INTEGER NOT NULL,
        available_ts INTEGER NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL,
        payload TEXT NOT NULL
      )
      """
    )
    _db_conn.execute(
      """
      CREATE TABLE IF NOT EXISTS kv_settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      )
      """
    )
    _db_conn.execute(
      """
      CREATE TABLE IF NOT EXISTS instructions (
        key TEXT PRIMARY KEY,
        text TEXT NOT NULL,
        updated_ts INTEGER NOT NULL
      )
      """
    )
    _db_conn.execute(
      """
      CREATE TABLE IF NOT EXISTS history_state (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      )
      """
    )
    _db_conn.execute(
      """
      CREATE TABLE IF NOT EXISTS dedup_messages (
        message_id TEXT PRIMARY KEY,
        created_ts INTEGER NOT NULL
      )
      """
    )
    _db_conn.execute(
      """
      CREATE TABLE IF NOT EXISTS dedup_threads (
        thread_id TEXT PRIMARY KEY,
        created_ts INTEGER NOT NULL
      )
      """
    )
    _db_conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_available ON jobs(status, available_ts, id)")
    _db_conn.execute("CREATE INDEX IF NOT EXISTS idx_dedup_messages_ts ON dedup_messages(created_ts)")
    _db_conn.execute("CREATE INDEX IF NOT EXISTS idx_dedup_threads_ts ON dedup_threads(created_ts)")
    _db_conn.commit()
    _db_load_seed_if_needed()
    _db_init_auth()
    _db_migrate_txt_files()

def _db_get_setting(key: str) -> Optional[str]:
  with _db_lock:
    cur = _db_conn.execute("SELECT value FROM kv_settings WHERE key=?", (key,))
    row = cur.fetchone()
    return row[0] if row else None

def _db_set_setting(key: str, value: str) -> None:
  with _db_lock:
    _db_conn.execute(
      "INSERT INTO kv_settings(key, value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
      (key, value),
    )
    _db_conn.commit()

def _db_get_instruction(key: str) -> Optional[str]:
  with _db_lock:
    cur = _db_conn.execute("SELECT text FROM instructions WHERE key=?", (key,))
    row = cur.fetchone()
    return row[0] if row else None

def _db_set_instruction(key: str, text: str) -> None:
  now = int(time.time())
  with _db_lock:
    _db_conn.execute(
      "INSERT INTO instructions(key, text, updated_ts) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET text=excluded.text, updated_ts=excluded.updated_ts",
      (key, text, now),
    )
    _db_conn.commit()
  
def _db_init_auth() -> None:
  try:
    with _db_lock:
      _db_conn.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
          username TEXT PRIMARY KEY,
          password_hash TEXT NOT NULL
        )
        """
      )
      username = "oguzhan"
      password_plain = "4140893@OGK"
      cur = _db_conn.execute("SELECT username FROM users WHERE username=?", (username,))
      row = cur.fetchone()
      if not row:
        hashv = pbkdf2_sha256.hash(password_plain)
        _db_conn.execute("INSERT INTO users(username, password_hash) VALUES(?,?)", (username, hashv))
        _db_conn.commit()
  except Exception as e:
    _append_log({"type":"auth_init_error","error":str(e)})

def _auth_check_password(username: str, password: str) -> bool:
  try:
    with _db_lock:
      cur = _db_conn.execute("SELECT password_hash FROM users WHERE username=?", (username,))
      row = cur.fetchone()
    if not row:
      return False
    return pbkdf2_sha256.verify(password, row[0])
  except Exception:
    return False

def _is_logged_in(request: FastAPIRequest) -> bool:
  try:
    return bool(request.session.get("user"))
  except Exception:
    return False

# --- SQLite-based replacements for txt files ---

def _db_get_history_id() -> Optional[int]:
  try:
    with _db_lock:
      cur = _db_conn.execute("SELECT value FROM history_state WHERE key='last_history_id'")
      row = cur.fetchone()
      return int(row[0]) if row else None
  except Exception:
    return None

def _db_set_history_id(value: int) -> None:
  try:
    with _db_lock:
      _db_conn.execute(
        "INSERT INTO history_state(key, value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        ("last_history_id", str(value))
      )
      _db_conn.commit()
  except Exception:
    pass

def _db_dedup_has_message(message_id: str) -> bool:
  try:
    with _db_lock:
      cur = _db_conn.execute("SELECT message_id FROM dedup_messages WHERE message_id=?", (message_id,))
      return cur.fetchone() is not None
  except Exception:
    return False

def _db_dedup_add_message(message_id: str) -> None:
  try:
    now = int(time.time())
    with _db_lock:
      _db_conn.execute(
        "INSERT OR IGNORE INTO dedup_messages(message_id, created_ts) VALUES(?,?)",
        (message_id, now)
      )
      _db_conn.commit()
  except Exception:
    pass

def _db_dedup_has_thread(thread_id: str) -> bool:
  try:
    with _db_lock:
      cur = _db_conn.execute("SELECT thread_id FROM dedup_threads WHERE thread_id=?", (thread_id,))
      return cur.fetchone() is not None
  except Exception:
    return False

def _db_dedup_add_thread(thread_id: str) -> None:
  try:
    now = int(time.time())
    with _db_lock:
      _db_conn.execute(
        "INSERT OR IGNORE INTO dedup_threads(thread_id, created_ts) VALUES(?,?)",
        (thread_id, now)
      )
      _db_conn.commit()
  except Exception:
    pass

def _db_migrate_txt_files() -> None:
  """Mevcut txt dosyalarından verileri SQLite'a taşır"""
  try:
    # History ID migration
    if os.path.exists(LAST_HISTORY_ID_FILE):
      try:
        with open(LAST_HISTORY_ID_FILE, "r", encoding="utf-8") as f:
          value = f.read().strip()
          if value and value.isdigit():
            _db_set_history_id(int(value))
            _append_log({"type": "migrate", "source": "history_id", "value": value})
      except Exception as e:
        _append_log({"type": "migrate_error", "source": "history_id", "error": str(e)})

    # Message dedup migration
    if os.path.exists(DEDUP_FILE):
      try:
        count = 0
        with open(DEDUP_FILE, "r", encoding="utf-8") as f:
          for line in f:
            message_id = line.strip()
            if message_id:
              _db_dedup_add_message(message_id)
              count += 1
        _append_log({"type": "migrate", "source": "dedup_messages", "count": count})
      except Exception as e:
        _append_log({"type": "migrate_error", "source": "dedup_messages", "error": str(e)})

    # Thread dedup migration
    if os.path.exists(REPLIED_THREADS_FILE):
      try:
        count = 0
        with open(REPLIED_THREADS_FILE, "r", encoding="utf-8") as f:
          for line in f:
            thread_id = line.strip()
            if thread_id:
              _db_dedup_add_thread(thread_id)
              count += 1
        _append_log({"type": "migrate", "source": "dedup_threads", "count": count})
      except Exception as e:
        _append_log({"type": "migrate_error", "source": "dedup_threads", "error": str(e)})
        
  except Exception as e:
    _append_log({"type": "migrate_general_error", "error": str(e)})

def _load_email_settings() -> Dict[str, List[str]]:
  """Load email settings from database as arrays"""
  import json

  def get_email_list(key: str) -> List[str]:
    setting = _db_get_setting(key)
    if setting:
      try:
        parsed = json.loads(setting)
        return parsed if isinstance(parsed, list) else [str(parsed)]
      except (json.JSONDecodeError, TypeError):
        return [str(setting)]
    return []

  return {
    "to": get_email_list("email_to"),
    "cc": get_email_list("email_cc"),
    "bcc": get_email_list("email_bcc"),
  }

def _db_load_seed_if_needed() -> None:
  try:
    with _db_lock:
      cur = _db_conn.execute("SELECT COUNT(*) FROM instructions")
      cnt = int(cur.fetchone()[0])
    seed_path = os.path.join(os.getcwd(), "instructions_seed.json")
    if cnt == 0 and os.path.exists(seed_path):
      with open(seed_path, "r", encoding="utf-8") as f:
        data = json.load(f)
      for k, v in (data.get("instructions") or {}).items():
        _db_set_instruction(str(k), str(v))
      for k, v in (data.get("settings") or {}).items():
        _db_set_setting(str(k), str(v))
  except Exception as e:
    _append_log({"type":"seed_error","error":str(e)})


def _db_enqueue(payload: dict) -> int:
  now = int(time.time())
  data = json.dumps(payload, ensure_ascii=False)
  with _db_lock:
    cur = _db_conn.execute(
      "INSERT INTO jobs(created_ts, available_ts, attempts, status, payload) VALUES(?,?,?,?,?)",
      (now, now, 0, "pending", data),
    )
    _db_conn.commit()
    job_id = int(cur.lastrowid)
  _append_log({"type":"enqueued","job_id":job_id})
  return job_id


def _db_fetch_and_claim() -> Optional[dict]:
  now = int(time.time())
  with _db_lock:
    cur = _db_conn.execute(
      "SELECT id, attempts, payload FROM jobs WHERE status='pending' AND available_ts<=? ORDER BY id ASC LIMIT 1",
      (now,)
    )
    row = cur.fetchone()
    if not row:
      return None
    job_id, attempts, payload = int(row[0]), int(row[1]), row[2]
    # claim
    _db_conn.execute("UPDATE jobs SET status='inprogress' WHERE id=? AND status='pending'", (job_id,))
    _db_conn.commit()
  return {"id": job_id, "attempts": attempts, "payload": json.loads(payload)}


def _db_complete(job_id: int) -> None:
  with _db_lock:
    _db_conn.execute("UPDATE jobs SET status='done' WHERE id=?", (job_id,))
    _db_conn.commit()


def _db_retry(job_id: int, attempts: int, error: str) -> None:
  next_delay = int(BACKOFF_BASE_SEC * (2 ** min(attempts, 6)))
  next_available = int(time.time()) + next_delay
  with _db_lock:
    _db_conn.execute(
      "UPDATE jobs SET status='pending', attempts=?, available_ts=? WHERE id=?",
      (attempts + 1, next_available, job_id),
    )
    _db_conn.commit()
  _append_log({"type":"retry","job_id":job_id,"attempts":attempts+1,"next_available":next_available,"error":error})


def _db_fail(job_id: int, error: str) -> None:
  with _db_lock:
    _db_conn.execute("UPDATE jobs SET status='failed' WHERE id=?", (job_id,))
    _db_conn.commit()
  _append_log({"type":"failed","job_id":job_id,"error":error})

# Structured event logging ----------------------------------------------------
LOG_FILE = "events.jsonl"
QUEUE_DB = "queue.db"
MAX_ATTEMPTS = 5
BACKOFF_BASE_SEC = 30  # exponential backoff base
WORKER_POLL_SEC = 1.0


def _append_log(event: Dict[str, Any]) -> None:
    try:
        event["ts"] = int(time.time())
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
    except Exception:
        pass


def _read_logs(offset: int = 0, limit: int = 50) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            all_lines = f.readlines()
        # newest first
        sliced = list(reversed(all_lines))
        sliced = sliced[offset: offset + limit]
        for line in sliced:
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    except FileNotFoundError:
        return []
    return rows


# Aggregated UI logs -----------------------------------------------------------

def _aggregate_events_for_ui(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Group events by thread/job and show progression.
    
    Returns newest-first list with merged event information.
    """
    # Thread/job bazında grupla
    grouped: Dict[str, Dict[str, Any]] = {}
    
    # Olayları eskiden yeniye sırala (birleştirme için)
    events_sorted = sorted(events, key=lambda e: int(e.get("ts", 0)))
    
    for ev in events_sorted:
        ev_type = str(ev.get("type") or "")
        
        # Gürültülü olayları atla
        if ev_type in {"watch_refresh", "warehouse_init", "warehouse_source", "warehouse_convert", "seed_error"}:
            continue
            
        # Gruplama anahtarı
        thread_id = ev.get("threadId", "")
        job_id = ev.get("job_id")
        key = thread_id if thread_id else f"job_{job_id}" if job_id else f"ts_{ev.get('ts', '')}"
        
        # Mevcut kaydı al veya yeni oluştur
        if key not in grouped:
            grouped[key] = {
                "type": "processing",
                "subject": "",
                "from": "",
                "to": "",
                "cc": "",
                "bcc": "",
                "threadId": thread_id,
                "job_id": job_id,
                "details": "",
                "ts": int(ev.get("ts", 0) or 0),
                "tool_calls": [],
                "body_preview": "",
                "error": "",
                "tags": [],  # Arıza/Değil gibi etiketler
                "progression": []  # Olayların ilerlemesi
            }
        
        item = grouped[key]
        item["ts"] = max(item["ts"], int(ev.get("ts", 0) or 0))  # En son zaman damgası

        # Olay tipine göre güncelle
        if ev_type == "enqueued":
            item["progression"].append("📥 Kuyruğa alındı")
            if not item["details"]:
                item["details"] = "📥 E-posta işlem kuyruğuna alındı"
            
        elif ev_type == "received":
            # E-posta bilgilerini güncelle
            item["type"] = "received"
            item["subject"] = ev.get('subject', 'Konusuz')
            item["from"] = ev.get('from', 'Bilinmeyen')
            item["progression"].append("📧 E-posta alındı")
            item["details"] = f"📧 Yeni e-posta geldi\nKonu: {item['subject']}\nGönderen: {item['from']}"
            
        elif ev_type == "non_incident":
            # Arıza değil etiketi ekle
            item["tags"].append("ℹ️ ARIZA DEĞİL")
            item["progression"].append("ℹ️ Arıza değil olarak işaretlendi")
            reason = ev.get("details") or "classification=false"
            # Mevcut detayları koru ama etiket ekle
            if item["type"] == "received":
                item["details"] = f"📧 Gelen E-posta - {item['tags'][-1]}\nKonu: {item['subject']}\nGönderen: {item['from']}\nNeden: {reason}"
            item["type"] = "non_incident"
            
        elif ev_type == "sent_incident":
            # Arıza bildirimi etiketi ekle  
            item["tags"].append("✅ ARIZA BİLDİRİMİ")
            item["type"] = "sent_incident"
            item["to"] = ev.get("to", "")
            item["cc"] = ev.get("cc", "")
            item["progression"].append("✅ Arıza kaydı açıldı")
            if ev.get("reply_text"):
                item["body_preview"] = (ev.get("reply_text") or "")[:200]
            # Mevcut bilgileri koru
            item["details"] = f"📧 Gelen E-posta - {item['tags'][-1]}\nKonu: {item['subject']}\nGönderen: {item['from']}\nArıza Kaydı: {item['to']}"
            
        elif ev_type in {"classify_timeout", "generate_timeout"}:
            item["progression"].append("⏱️ Zaman aşımı")
            item["error"] = ev.get('error', 'Zaman aşımı')
            if "from_addr" in str(item["error"]):
                item["error"] = "E-posta gönderen adresi okunamadı (from_addr parametresi eksik)"
            
        elif ev_type == "failed":
            item["type"] = "failed"
            item["tags"].append("❌ BAŞARISIZ")
            item["progression"].append("❌ İşlem başarısız")
            item["error"] = ev.get("error", "Bilinmeyen hata")
            
        elif ev_type == "tool_call":
            tool = ev.get("tool", "")
            args = ev.get("args", {})
            query = ""
            if isinstance(args, dict):
                query = (
                    args.get("query")
                    or args.get("region_id")
                    or args.get("email")
                    or args.get("from")
                    or args.get("from_addr")
                    or ""
                )
            item["tool_calls"].append({"tool": tool, "args": args, "type": "call"})
            item["progression"].append(f"🔧 Fonksiyon çağrıldı: {query}")
            
        elif ev_type == "tool_result":
            summary = ev.get("summary", {})
            tool_name = ev.get("tool", "")
            if isinstance(summary, dict):
                if tool_name == "get_warehouse_info":
                    bolge = summary.get("bolge", "Bilinmiyor")
                    item["tool_calls"].append({"tool": tool_name, "result": summary, "type": "result"})
                    item["progression"].append(f"📍 Bölge bulundu: {bolge}")
                elif tool_name == "get_user_warehouse_info":
                    depo = summary.get("DEPOADI") or summary.get("BOLGEKOD") or "Bilinmiyor"
                    item["tool_calls"].append({"tool": tool_name, "result": summary, "type": "result"})
                    item["progression"].append(f"👤 Depo bulundu: {depo}")
                else:
                    item["tool_calls"].append({"tool": tool_name or "unknown", "result": summary, "type": "result"})
                    item["progression"].append("🔧 Araç sonucu alındı")
                
        elif ev_type == "retry":
            attempts = ev.get("attempts", 0)
            item["progression"].append(f"🔄 Tekrar deneme #{attempts}")
            
        elif ev_type == "classify_parse_error":
            item["progression"].append("⚠️ Sınıflandırma hatası")
            item["error"] = "Model yanıtı ayrıştırılamadı"
    
    # Sonuçları listeye dönüştür
    result = list(grouped.values())
    
    # En yeniden en eskiye sırala
    result.sort(key=lambda x: x["ts"], reverse=True)
    return result


@app.get("/logs")
async def get_logs(request: FastAPIRequest, offset: int = 0, limit: int = 50):
    if not _is_logged_in(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    raw = _read_logs(offset, limit * 3)  # Daha fazla ham olay çek
    ui_items = _aggregate_events_for_ui(raw)
    # Limit'i uygula
    ui_items = ui_items[offset:offset + limit]
    return {"items": ui_items, "offset": offset, "limit": limit}


@app.get("/logs/stats")
async def get_log_stats(request: FastAPIRequest):
    if not _is_logged_in(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    """Günlük istatistikleri döndürür."""
    try:
        # Son 24 saatin olaylarını al
        now = int(time.time())
        day_ago = now - 86400
        all_events = []
        
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    ev = json.loads(line)
                    if ev.get("ts", 0) >= day_ago:
                        all_events.append(ev)
                except Exception:
                    continue
        
        # İstatistikleri hesapla
        stats = {
            "total_emails": 0,
            "incident_reports": 0,
            "non_incidents": 0,
            "tool_calls": 0,
            "failed": 0,
            "retries": 0,
            "regions_found": set(),
            "last_24h": True,
            "timestamp": now
        }
        
        for ev in all_events:
            ev_type = ev.get("type")
            if ev_type == "received":
                stats["total_emails"] += 1
            elif ev_type == "sent_incident":
                stats["incident_reports"] += 1
            elif ev_type == "non_incident":
                stats["non_incidents"] += 1
            elif ev_type == "tool_call":
                stats["tool_calls"] += 1
            elif ev_type == "tool_result":
                summary = ev.get("summary")
                if summary and isinstance(summary, dict):
                    bolge = summary.get("bolge")
                    if bolge:
                        stats["regions_found"].add(bolge)
            elif ev_type == "failed":
                stats["failed"] += 1
            elif ev_type == "retry":
                stats["retries"] += 1
        
        # Set'i listeye çevir
        stats["regions_found"] = list(stats["regions_found"])
        stats["unique_regions"] = len(stats["regions_found"])
        
        return stats
    except Exception as e:
        return {"error": str(e), "timestamp": int(time.time())}


@app.get("/admin/logs")
async def logs_page_admin(request: FastAPIRequest):
    if not _is_logged_in(request):
        return RedirectResponse(url="/login", status_code=302)
    return Response(content=load_template("logs.html"), media_type="text/html; charset=utf-8")


async def _process_notification(decoded_message: dict):
    notification_history_id = int(decoded_message["historyId"]) 
    print(f"[bg] Processing historyId: {notification_history_id}")

    last_processed_id = _read_last_history_id()
    if last_processed_id is None:
        start_history_id = notification_history_id - 1
    else:
        start_history_id = last_processed_id

    history = gmail.users().history().list(userId="me", startHistoryId=start_history_id).execute()
    if not history.get("history"):
        _write_last_history_id(notification_history_id)
        return

    for h in history.get("history", []):
        for added in h.get("messagesAdded", []):
            mid = added["message"]["id"]
            added_labels = added["message"].get("labelIds", [])
            if "SENT" in added_labels or "INBOX" not in added_labels:
                continue
            if _dedup_has(mid):
                continue

            m = gmail.users().messages().get(userId="me", id=mid, format="full").execute()
            headers = {x["name"]: x["value"] for x in m["payload"].get("headers", [])}
            subject = headers.get("Subject", "")
            from_addr = headers.get("From", "")
            thread_id = m.get("threadId") or ""
            _append_log({"type":"received","subject":subject,"from":from_addr,"threadId":thread_id})

            payload = m.get("payload", {})
            body_text = _extract_body_text(payload)

            try:
                classify_template = _db_get_instruction("instruction.classify")
                if not classify_template:
                    raise RuntimeError("instruction.classify bulunamadı")
                # from_addr parametresini güvenli bir şekilde ekle
                try:
                    # Yeni format: hem email hem from_addr
                    classify_prompt = classify_template.format(email=body_text, from_addr=from_addr)
                except (KeyError, ValueError):
                    try:
                        # Sadece email parametresi ile dene (eski format)
                        classify_prompt = classify_template.format(email=body_text)
                    except Exception as e:
                        _append_log({"type":"classify_error","error":f"Template format error: {str(e)}"}) 
                        classify_prompt = f"E-posta: {body_text[:500]}"
                cls_text = await _ollama_generate("gpt-oss:20b", classify_prompt, timeout=40, retries=2)
                incident = False
                reason = ""
                try:
                    cls_json = json.loads(cls_text)
                    incident = bool(cls_json.get("incident", False))
                    reason = str(cls_json.get("reason", ""))
                except Exception:
                    _append_log({"type":"classify_parse_error","raw":cls_text})
                    incident = False
            except Exception as e:
                _append_log({"type":"classify_timeout","error":str(e)})
                incident = False

            if not incident:
                _append_log({"type":"non_incident","subject":subject,"threadId":thread_id,"details":"classification=false"})
                _dedup_add(mid)
                if thread_id:
                    _thread_dedup_add(thread_id)
                continue

            if thread_id and _thread_dedup_has(thread_id):
                _dedup_add(mid)
                continue

            try:
                gen_template = _db_get_instruction("instruction.generate")
                if not gen_template:
                    raise RuntimeError("instruction.generate bulunamadı")
                gen_prompt = gen_template.format(reason=reason, body=body_text)
                system_text = (
                  "Türkçe yanıtla. Gerekirse 'get_warehouse_info' aracını çağırarak ilgili bölgenin IP listesi, şehir ve açık adresini ekle. "
                  f"Gönderen e-posta adresi: {from_addr}. Bölge/depo metinde yoksa bu adresle 'get_user_warehouse_info' aracını çağır, DEPOADI/BOLGEKOD bilgisini belirterek arıza kaydını yaz."
                )
                # Tool çağrılarının loguyla ilişkilendirmek için bağlam ekle
                ctx = {"threadId": thread_id, "subject": subject}
                reply_text = await _ollama_chat_with_tools_execute("gpt-oss:20b", system_text, gen_prompt, timeout=90.0, ctx=ctx)
                if not reply_text:
                  # Araçlı sohbet başarısız ise klasik generate'a düş
                  reply_text = await _ollama_generate("gpt-oss:20b", gen_prompt, timeout=90, retries=1)
            except Exception as e:
                reply_text = "Merhaba, Eker Süt Ürünleri olarak ilgili bölgede internet hizmetinizde kesinti/performans problemi yaşamaktayız. Arıza kaydının açılarak acil inceleme, SLA/ETA ve durum güncellemeleri rica olunur."
                _append_log({"type":"generate_timeout","error":str(e)})

            message_id = headers.get("Message-Id") or headers.get("Message-ID")
            orig_from = headers.get("From")

            em = EmailMessage()
            _emails = _load_email_settings()

            # Check if we have at least one TO address
            to_addresses = _emails.get("to", [])
            if not to_addresses:
                _append_log({"type":"missing_email_config", "details":"No TO addresses configured"})
                _dedup_add(mid)
                if thread_id:
                    _thread_dedup_add(thread_id)
                continue

            em["To"] = ", ".join(to_addresses)
            cc_addresses = _emails.get("cc", [])
            if cc_addresses:
                em["Cc"] = ", ".join(cc_addresses)
            bcc_addresses = _emails.get("bcc", [])
            if bcc_addresses:
                em["Bcc"] = ", ".join(bcc_addresses)
            em["Subject"] = f"[Arıza] {subject}" if subject else "[Arıza] İnternet Kesintisi"
            if message_id:
                em["In-Reply-To"] = message_id
                em["References"] = message_id
            em["Auto-Submitted"] = "auto-replied"
            header_info = f"İleti gönderen: {orig_from}\n\n"
            footer = "\n\n--\nBu E-posta Yapay Zeka tarafından gönderilmiştir."
            em.set_content(header_info + reply_text + footer, subtype="plain", charset="utf-8")

            encoded = base64.urlsafe_b64encode(em.as_bytes()).decode().rstrip("=")
            gmail.users().messages().send(
                userId="me",
                body={"raw": encoded, "threadId": thread_id},
            ).execute()
            # log sent email and include reply_text for UI
            _append_log({
                "type":"sent_incident",
                "subject":subject,
                "threadId":thread_id,
                "to":", ".join(to_addresses),
                "cc":", ".join(cc_addresses) if cc_addresses else "",
                "bcc":", ".join(bcc_addresses) if bcc_addresses else "",
                "reply_text": reply_text
            })
            _dedup_add(mid)
            if thread_id:
                _thread_dedup_add(thread_id)

    _write_last_history_id(notification_history_id)


async def _worker_loop():
  while True:
    try:
      job = _db_fetch_and_claim()
      if not job:
        await asyncio.sleep(WORKER_POLL_SEC)
        continue
      job_id = job["id"]
      attempts = job["attempts"]
      try:
        await _process_notification(job["payload"])
        _db_complete(job_id)
      except Exception as e:
        if attempts + 1 < MAX_ATTEMPTS:
          _db_retry(job_id, attempts, str(e))
        else:
          _db_fail(job_id, str(e))
    except Exception as loop_err:
      _append_log({"type":"worker_error","error":str(loop_err)})
      await asyncio.sleep(1.5)


async def _watch_refresher_loop():
  while True:
    try:
      # Run ensure_watch in a thread to avoid blocking the event loop
      resp = await asyncio.to_thread(ensure_watch)
      try:
        exp = None
        if isinstance(resp, dict):
          data = resp.get("data") or {}
          exp = data.get("expiration")
        _append_log({"type":"watch_refresh","expiration":exp})
      except Exception:
        pass
    except Exception as e:
      _append_log({"type":"watch_refresh_error","error":str(e)})
    # Refresh roughly günde 1 kez
    await asyncio.sleep(24 * 60 * 60)


@app.post("/pubsub/gmail")
async def handle_pubsub_push(request: FastAPIRequest):
    print("\n--- New Push Notification Received ---")
    try:
        data = await request.json()
        message = data["message"]
        data_b64 = message.get("data")
        if not data_b64:
            return {"status": "no-data"}
        decoded_message = json.loads(base64.urlsafe_b64decode(data_b64 + "==").decode())
        # Enqueue durable job and return immediately
        job_id = _db_enqueue(decoded_message)
        return {"status": "enqueued", "job_id": job_id}
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": str(e)}


@app.on_event("startup")
async def _on_startup():
  _db_init()
  # Veri kaynaklarını hazırla (JSONL varsa kullan, yoksa CSV'den üret)
  _warehouse_prepare_sources()
  # Bellek fallback için yine de CSV'yi yükle (küçük boyutlu ise hızlı erişim sağlar)
  _warehouse_init()
  asyncio.create_task(_worker_loop())
  asyncio.create_task(_watch_refresher_loop())


def ensure_watch():
    """Ensures that the Gmail account is being watched for new emails."""
    try:
        # Check if a watch is already active
        watches = gmail.users().stop(userId="me").execute()
        # No exception means a watch was active, let's just proceed.
        # It will be implicitly stopped by the new watch() call.
    except Exception:
        # No watch was active, which is fine.
        pass

    request = {
        "labelIds": ["INBOX"],
        "topicName": PUBSUB_TOPIC_NAME
    }
    
    try:
        response = gmail.users().watch(userId="me", body=request).execute()
        print("Gmail watch activated successfully.")
        print(f"History ID: {response['historyId']}")
        # Initialize our pointer to the history id returned by watch
        try:
            _write_last_history_id(int(response["historyId"]))
        except Exception:
            pass
        return {"status": "success", "data": response}
    except Exception as e:
        print(f"An error occurred while setting up Gmail watch: {e}")
        return {"status": "error", "message": str(e)}


@app.get("/admin/watch")
async def start_watch():
    """Triggers the Gmail watch setup."""
    return ensure_watch()

# Basit healthcheck
@app.get("/health")
async def health():
  try:
    _ = _load_email_settings()
    return {"ok": True}
  except Exception as e:
    return {"ok": False, "error": str(e)}

@app.get("/admin/instructions")
async def instructions_page(request: FastAPIRequest):
    """AI Talimatları sayfası"""
    if not _is_logged_in(request):
        return RedirectResponse(url="/login", status_code=302)
    return Response(content=load_template("instructions.html"), media_type="text/html; charset=utf-8")

@app.get("/admin/logs")
async def logs_page_admin(request: FastAPIRequest):
    if not _is_logged_in(request):
        return RedirectResponse(url="/login", status_code=302)
    return Response(content=load_template("logs.html"), media_type="text/html; charset=utf-8")

# Kök URL: login ya da loglara yönlendir
@app.get("/")
async def root_redirect(request: FastAPIRequest):
    if _is_logged_in(request):
        return RedirectResponse(url="/admin/logs", status_code=302)
    return RedirectResponse(url="/login", status_code=302)

# E-posta ayarları sayfası
@app.get("/admin/emails")
async def emails_page(request: FastAPIRequest):
    """E-posta Ayarları sayfası"""
    if not _is_logged_in(request):
        return RedirectResponse(url="/login", status_code=302)
    return Response(content=load_template("emails.html"), media_type="text/html; charset=utf-8")

# Eski rotalar için yönlendirme
@app.get("/admin")
async def admin_redirect():
    return RedirectResponse(url="/admin/logs", status_code=302)

@app.get("/admin/config")
async def admin_config_legacy():
    return RedirectResponse(url="/admin/instructions", status_code=302)


# --- Auth pages ---
@app.get("/login")
async def login_page():
    html = """
    <!doctype html>
    <html lang=\"tr\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n<title>Giriş</title>\n<style>body{font-family:Inter,system-ui,-apple-system,sans-serif;background:#0f172a;color:#e2e8f0;display:flex;align-items:center;justify-content:center;height:100vh;margin:0} .card{background:#111827;border:1px solid #334155;border-radius:12px;box-shadow:0 10px 20px rgba(0,0,0,.2);padding:24px;max-width:360px;width:100%} h1{font-size:20px;margin:0 0 16px} label{display:block;font-size:12px;color:#94a3b8;margin-bottom:6px} input{width:100%;padding:10px 12px;border-radius:8px;border:1px solid #334155;background:#0b1220;color:#e2e8f0;margin-bottom:12px} button{width:100%;padding:10px 12px;border-radius:8px;border:none;background:#3b82f6;color:#fff;font-weight:600;cursor:pointer} .err{color:#f87171;font-size:13px;margin-bottom:8px;min-height:18px}</style></head><body>\n<div class=card><h1>Yönetim Girişi</h1><form method=post action=/login>\n<label>Kullanıcı Adı</label><input name=username autocomplete=username required>\n<label>Şifre</label><input name=password type=password autocomplete=current-password required>\n<div class=err>{{error}}</div>\n<button type=submit>Giriş Yap</button>\n</form></div></body></html>
    """
    return Response(content=html.replace("{{error}}", ""), media_type="text/html; charset=utf-8")


@app.post("/login")
async def login_submit(request: FastAPIRequest):
    form = await request.form()
    username = str(form.get("username") or "").strip()
    password = str(form.get("password") or "").strip()
    if _auth_check_password(username, password):
        request.session["user"] = username
        return RedirectResponse(url="/admin/logs", status_code=302)
    html = await login_page()
    # basit hata göstergesi
    content = html.body.decode("utf-8").replace("{{error}}", "Hatalı kullanıcı adı veya şifre")
    return Response(content=content, media_type="text/html; charset=utf-8", status_code=401)


@app.post("/logout")
async def logout(request: FastAPIRequest):
    try:
        request.session.clear()
    except Exception:
        pass
    return RedirectResponse(url="/login", status_code=302)

@app.get("/logout")
async def logout_page():
    html = """
    <!doctype html>
    <html lang=\"tr\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n<title>Çıkış</title>\n<style>body{font-family:Inter,system-ui,-apple-system,sans-serif;background:#0f172a;color:#e2e8f0;display:flex;align-items:center;justify-content:center;height:100vh;margin:0} .card{background:#111827;border:1px solid #334155;border-radius:12px;box-shadow:0 10px 20px rgba(0,0,0,.2);padding:24px;max-width:380px;width:100%;text-align:center} h1{font-size:20px;margin:0 0 12px} p{color:#94a3b8;margin:0 0 16px} .row{display:flex;gap:8px} button, a.btn{flex:1;padding:10px 12px;border-radius:8px;border:none;background:#3b82f6;color:#fff;font-weight:600;cursor:pointer;text-decoration:none;display:inline-block;text-align:center} a.btn{background:#374151}</style></head><body>\n<div class=card><h1>Oturumu kapat</h1><p>Çıkış yapmak istediğinize emin misiniz?</p><div class=row><form method=post action=/logout style=\"flex:1\"><button type=submit>Çıkış Yap</button></form><a class=btn href=/admin/logs>İptal</a></div></div></body></html>
    """
    return Response(content=html, media_type="text/html; charset=utf-8")

@app.get("/admin/instructions/data")
async def config_data(request: FastAPIRequest):
    if not _is_logged_in(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    items_instr: Dict[str, str] = {}
    try:
        with _db_lock:
            cur = _db_conn.execute("SELECT key, text FROM instructions")
            for k, t in cur.fetchall():
                items_instr[k] = t
    except Exception:
        items_instr = {}
    settings = _load_email_settings()
    return {"instructions": items_instr, "settings": settings}

@app.post("/admin/instructions/save")
async def config_save(request: FastAPIRequest):
    if not _is_logged_in(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    data = await request.json()
    instr = data.get("instructions") or {}
    for k, v in instr.items():
        _db_set_instruction(str(k), str(v))

    settings = data.get("settings") or {}
    import json

    # Convert email arrays to JSON strings for storage
    email_settings = {}
    if "to" in settings and isinstance(settings["to"], list):
        email_settings["email_to"] = json.dumps(settings["to"])
    if "cc" in settings and isinstance(settings["cc"], list):
        email_settings["email_cc"] = json.dumps(settings["cc"])
    if "bcc" in settings and isinstance(settings["bcc"], list):
        email_settings["email_bcc"] = json.dumps(settings["bcc"])

    for k, v in email_settings.items():
        _db_set_setting(str(k), str(v))

    return {"status": "ok"}


@app.get("/warehouse/search")
async def warehouse_search(q: Optional[str] = None, id: Optional[str] = None):
  """Basit arama: q (bölge adı) veya id ile kayıt döndürür."""
  try:
    info = get_warehouse_info(query=q, region_id=id)
    return {"result": info}
  except Exception as e:
    return {"error": str(e)}

@app.post("/admin/logs/reset")
async def db_reset(request: FastAPIRequest):
    if not _is_logged_in(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    """Sadece log dosyasını temizler; ayarları ve talimatları korur."""
    try:
        # Sadece log dosyasını temizle
        with open(LOG_FILE, "w", encoding="utf-8") as f:
            f.write("")
        return {"status": "ok", "message": "Log dosyası başarıyla temizlendi"}
    except Exception as e:
        return {"status": "error", "message": f"Log dosyası temizlenirken hata: {str(e)}"}

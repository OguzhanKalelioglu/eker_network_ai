# pip install fastapi uvicorn google-api-python-client google-auth-httplib2 google-auth
from fastapi import FastAPI, Request as FastAPIRequest, Response
from pydantic import BaseModel
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import base64, os, httpx, json, time, re, asyncio, sqlite3, threading
from httpx import ReadTimeout
from email.message import EmailMessage
from typing import List, Dict, Any

# Template utilities
def load_template(filename: str) -> str:
    """HTML template dosyasını yükler"""
    template_path = os.path.join(os.path.dirname(__file__), "templates", filename)
    try:
        with open(template_path, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return f"<h1>Template bulunamadı: {filename}</h1>"

app = FastAPI()

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

def _read_last_history_id() -> int | None:
    try:
        with open(LAST_HISTORY_ID_FILE, "r", encoding="utf-8") as f:
            v = f.read().strip()
            return int(v) if v else None
    except FileNotFoundError:
        return None
    except Exception:
        return None


def _write_last_history_id(value: int) -> None:
    try:
        with open(LAST_HISTORY_ID_FILE, "w", encoding="utf-8") as f:
            f.write(str(value))
    except Exception:
        pass


def _dedup_has(message_id: str) -> bool:
    try:
        with open(DEDUP_FILE, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip() == message_id:
                    return True
    except FileNotFoundError:
        return False
    return False


def _dedup_add(message_id: str) -> None:
    try:
        with open(DEDUP_FILE, "a", encoding="utf-8") as f:
            f.write(message_id + "\n")
    except Exception:
        pass


def _thread_dedup_has(thread_id: str) -> bool:
  try:
    with open(REPLIED_THREADS_FILE, "r", encoding="utf-8") as f:
      for line in f:
        if line.strip() == thread_id:
          return True
  except FileNotFoundError:
    return False
  return False


def _thread_dedup_add(thread_id: str) -> None:
  try:
    with open(REPLIED_THREADS_FILE, "a", encoding="utf-8") as f:
      f.write(thread_id + "\n")
  except Exception:
    pass


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
_db_conn: sqlite3.Connection | None = None
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
    _db_conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_available ON jobs(status, available_ts, id)")
    _db_conn.commit()
    _db_load_seed_if_needed()

def _db_get_setting(key: str) -> str | None:
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

def _db_get_instruction(key: str) -> str | None:
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


def _db_fetch_and_claim() -> dict | None:
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


# _join_emails function removed - now using arrays directly


@app.get("/logs")
async def get_logs(offset: int = 0, limit: int = 50):
    return {"items": _read_logs(offset, limit), "offset": offset, "limit": limit}


@app.get("/")
async def logs_page():
    """Ana sayfa - logs arayüzü"""
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

            body_text = ""
            payload = m.get("payload", {})
            if payload.get("mimeType") == "text/plain" and payload.get("body", {}).get("data"):
                body_text = base64.urlsafe_b64decode(payload["body"]["data"]).decode("utf-8", errors="ignore")
            else:
                parts = payload.get("parts", [])
                text_part = next((p for p in parts if p.get("mimeType") == "text/plain" and p.get("body", {}).get("data")), None)
                if text_part:
                    body_text = base64.urlsafe_b64decode(text_part["body"]["data"]).decode("utf-8", errors="ignore")

            try:
                classify_template = _db_get_instruction("instruction.classify")
                if not classify_template:
                    raise RuntimeError("instruction.classify bulunamadı")
                classify_prompt = classify_template.format(email=body_text)
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
            _append_log({
                "type":"sent_incident",
                "subject":subject,
                "threadId":thread_id,
                "to":", ".join(to_addresses),
                "cc":", ".join(cc_addresses) if cc_addresses else "",
                "bcc":", ".join(bcc_addresses) if bcc_addresses else ""
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

@app.get("/admin/config")
async def config_page():
    """Admin arayüzü - instruction ve e-posta ayarları"""
    return Response(content=load_template("admin.html"), media_type="text/html; charset=utf-8")

@app.get("/admin/config/data")
async def config_data():
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

@app.post("/admin/config/save")
async def config_save(request: FastAPIRequest):
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

@app.post("/admin/db/reset")
async def db_reset():
    """Sadece log dosyasını temizler; ayarları ve talimatları korur."""
    try:
        # Sadece log dosyasını temizle
        with open(LOG_FILE, "w", encoding="utf-8") as f:
            f.write("")
        return {"status": "ok", "message": "Log dosyası başarıyla temizlendi"}
    except Exception as e:
        return {"status": "error", "message": f"Log dosyası temizlenirken hata: {str(e)}"}

import os
import re
import time
import html
import json
import hmac
import base64
import hashlib
import secrets
import smtplib
import ssl
import threading
import unicodedata
import requests
import pandas as pd
import urllib.parse
from datetime import datetime, timedelta
from email.message import EmailMessage
from typing import Optional
from fastapi import FastAPI, BackgroundTasks, Request
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse, JSONResponse
import uvicorn
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from social_selenium import create_selenium_driver, close_selenium_driver, fetch_social_stats

app = FastAPI()

# ==========================================
IS_VERCEL = bool(str(os.getenv("VERCEL", "") or "").strip())
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "credential.json").strip() or "credential.json"
AUTH_SETTINGS_FILE = (
    str(os.getenv("AUTH_SETTINGS_FILE", "") or "").strip()
    or ("/tmp/auth_settings.json" if IS_VERCEL else "auth_settings.json")
)
SESSION_COOKIE_NAME = "social_monitor_session"
OTP_LENGTH = 6
OTP_REQUEST_COOLDOWN_SECONDS = 30
DEFAULT_SHEET_ID = os.getenv("DEFAULT_SHEET_ID", "").strip()
ACTIVE_SHEET_ID = DEFAULT_SHEET_ID
DEFAULT_SHEET_NAME = os.getenv("DEFAULT_SHEET_NAME", "").strip()
ACTIVE_SHEET_NAME = DEFAULT_SHEET_NAME
ACTIVE_SHEET_GID = "0"
BOOTSTRAP_ADMIN_EMAIL = os.getenv("AUTH_BOOTSTRAP_ADMIN_EMAIL", "").strip()
GMAIL_SMTP_EMAIL = os.getenv("GMAIL_SMTP_EMAIL", "").strip()
GMAIL_SMTP_APP_PASSWORD = os.getenv("GMAIL_SMTP_APP_PASSWORD", "").strip()
GMAIL_SMTP_FROM_EMAIL = os.getenv("GMAIL_SMTP_FROM_EMAIL", "").strip()
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "").strip()
ROW_SCAN_DELAY_SECONDS = float(os.getenv("ROW_SCAN_DELAY_SECONDS", "0.12"))
try:
    START_ROW = max(2, int(os.getenv("START_ROW", "2")))
except Exception:
    START_ROW = 2

# Quản lý trạng thái
is_running = False
is_finished = False
current_task = "Đang chờ lệnh"
logs = []
pending_updates = []
COLUMN_OVERRIDES = {"link": None, "campaign": None, "view": None, "like": None, "share": None, "comment": None, "save": None}
COLUMN_CONFIG_APPROVAL = {
    "approved": False,
    "sheet_id": "",
    "sheet_name": "",
    "approved_at_text": "",
}
HEADER_ALIASES = {
    "date": {"date", "time", "timestamp", "ngay", "thoigian", "scanat", "updatedat"},
    "link": {"link", "url", "posturl", "postlink", "linkpost", "videolink", "contentlink"},
    "campaign": {
        "campaign", "chiendich", "camp", "tenchiendich", "campaignname",
        "tencampaign", "project", "bookingitem"
    },
    "view": {"view", "views", "viewcount", "luotxem", "luotview"},
    "like": {"like", "likes", "reaction", "reactions", "react", "reacts"},
    "share": {"share", "shares", "sharecount"},
    "comment": {"comment", "comments", "commentcount", "cmt", "reply", "replies"},
    "save": {"save", "saves", "saved", "bookmark", "bookmarks", "luu", "collect", "collectcount"},
    "platform": {"platform", "nentang"},
    "caption": {"caption", "title", "mota", "noidung", "content"},
}

# Lịch tự động
schedule_mode = "off"  # off | dail y | weekly | monthly
schedule_time = "09:00"
schedule_weekday = 0  # 0=Thu 2 ... 6=Chu nhat
schedule_monthday = 1  # 1..28
schedule_end_date = ""
schedule_sheet_id = ""
schedule_sheet_name = ""
schedule_sheet_gid = "0"
schedule_targets = []
run_started_at = None
run_source = "idle"
schedule_last_run_started_at = None
schedule_last_run_finished_at = None
schedule_last_run_duration_seconds = 0.0
schedule_last_run_status = "idle"
schedule_last_run_source = ""
schedule_last_run_sheet_name = ""
schedule_last_run_processed = 0
schedule_last_run_success = 0
schedule_last_run_failed = 0
schedule_run_history = []
OTP_STORE = {}
WEEKDAY_NAMES = [
    "Thứ hai",
    "Thứ ba",
    "Thứ tư",
    "Thứ năm",
    "Thứ sáu",
    "Thứ bảy",
    "Chủ nhật",
]
last_schedule_run_key = ""
scheduler_thread = None
scheduler_stop_event = threading.Event()

def normalize_email_address(value: str) -> str:
    return (value or "").strip().lower()

FORCED_ADMIN_EMAILS = {
    normalize_email_address("thu.phannguyenanh@fanscom.vn"),
}

def parse_bool_env(value: str, default: bool) -> bool:
    raw = str(value or "").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def build_google_credentials():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    raw_json = str(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "") or "").strip()
    raw_json_b64 = str(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_BASE64", "") or "").strip()

    if raw_json:
        return Credentials.from_service_account_info(json.loads(raw_json), scopes=scopes)

    if raw_json_b64:
        decoded = base64.b64decode(raw_json_b64).decode("utf-8")
        return Credentials.from_service_account_info(json.loads(decoded), scopes=scopes)

    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise RuntimeError(
            "Missing Google service account credentials. "
            "Set SERVICE_ACCOUNT_FILE, GOOGLE_SERVICE_ACCOUNT_JSON, or GOOGLE_SERVICE_ACCOUNT_JSON_BASE64."
        )

    return Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)

def build_default_auth_settings():
    return {
        "session_secret": secrets.token_hex(32),
        "otp_ttl_seconds": 300,
        "session_ttl_seconds": 86400,
        "users": [],
        "user_meta": {},
        "mail": {
            "smtp_host": "",
            "smtp_port": 587,
            "smtp_user": "",
            "smtp_password": "",
            "smtp_from_email": "",
            "smtp_from_name": "Social Monitor",
            "use_tls": True,
            "use_ssl": False,
        },
    }

def normalize_auth_settings(data):
    settings = build_default_auth_settings()
    if isinstance(data, dict):
        settings.update({k: v for k, v in data.items() if k in settings})
        if isinstance(data.get("mail"), dict):
            settings["mail"].update(data["mail"])

    users = []
    raw_users = data.get("users", []) if isinstance(data, dict) else []
    for item in raw_users:
        if not isinstance(item, dict):
            continue
        email = normalize_email_address(item.get("email", ""))
        if not email:
            continue
        role = "admin" if str(item.get("role", "")).strip().lower() == "admin" else "user"
        users.append({"email": email, "role": role})

    deduped = {}
    for item in users:
        deduped[item["email"]] = item

    bootstrap_email = normalize_email_address(BOOTSTRAP_ADMIN_EMAIL)
    if bootstrap_email and bootstrap_email not in deduped:
        deduped[bootstrap_email] = {"email": bootstrap_email, "role": "admin"}
    for forced_email in FORCED_ADMIN_EMAILS:
        if forced_email:
            deduped[forced_email] = {"email": forced_email, "role": "admin"}

    settings["users"] = sorted(
        deduped.values(),
        key=lambda item: (0 if item["role"] == "admin" else 1, item["email"]),
    )
    settings["user_meta"] = normalize_user_meta_map((data or {}).get("user_meta", {}) if isinstance(data, dict) else {}, settings["users"])

    try:
        settings["otp_ttl_seconds"] = max(60, int(settings.get("otp_ttl_seconds", 300)))
    except Exception:
        settings["otp_ttl_seconds"] = 300
    try:
        settings["session_ttl_seconds"] = max(3600, int(settings.get("session_ttl_seconds", 86400)))
    except Exception:
        settings["session_ttl_seconds"] = 86400

    mail = settings["mail"]
    try:
        mail["smtp_port"] = int(mail.get("smtp_port", 587) or 587)
    except Exception:
        mail["smtp_port"] = 587
    mail["smtp_host"] = str(mail.get("smtp_host", "") or "").strip()
    mail["smtp_user"] = str(mail.get("smtp_user", "") or "").strip()
    mail["smtp_password"] = str(mail.get("smtp_password", "") or "")
    mail["smtp_from_email"] = normalize_email_address(mail.get("smtp_from_email", ""))
    mail["smtp_from_name"] = str(mail.get("smtp_from_name", "") or "Social Monitor").strip() or "Social Monitor"
    mail["use_tls"] = bool(mail.get("use_tls", True))
    mail["use_ssl"] = bool(mail.get("use_ssl", False))
    gmail_smtp_email = normalize_email_address(GMAIL_SMTP_EMAIL or os.getenv("GMAIL_SMTP_EMAIL", ""))
    gmail_smtp_app_password = re.sub(r"\s+", "", str(GMAIL_SMTP_APP_PASSWORD or os.getenv("GMAIL_SMTP_APP_PASSWORD", "") or ""))
    gmail_smtp_from_email = normalize_email_address(GMAIL_SMTP_FROM_EMAIL or os.getenv("GMAIL_SMTP_FROM_EMAIL", ""))
    if gmail_smtp_from_email and "@" not in gmail_smtp_from_email:
        gmail_smtp_from_email = ""
    gmail_mode_enabled = bool(gmail_smtp_email or gmail_smtp_app_password or gmail_smtp_from_email)
    env_mail = {
        "smtp_host": str(os.getenv("AUTH_SMTP_HOST", "") or "").strip() or ("smtp.gmail.com" if gmail_mode_enabled else ""),
        "smtp_user": str(os.getenv("AUTH_SMTP_USER", "") or "").strip() or gmail_smtp_email,
        "smtp_password": str(os.getenv("AUTH_SMTP_PASSWORD", "") or "") or gmail_smtp_app_password,
        "smtp_from_email": normalize_email_address(os.getenv("AUTH_SMTP_FROM_EMAIL", "")) or gmail_smtp_from_email or normalize_email_address(gmail_smtp_email),
        "smtp_from_name": str(os.getenv("AUTH_SMTP_FROM_NAME", "") or "").strip(),
    }
    if gmail_mode_enabled:
        for key, value in env_mail.items():
            if value:
                mail[key] = value
    else:
        for key, value in env_mail.items():
            if value and not mail.get(key):
                mail[key] = value
    try:
        env_smtp_port = int(str(os.getenv("AUTH_SMTP_PORT", "") or "").strip() or ("587" if gmail_mode_enabled else "0"))
    except Exception:
        env_smtp_port = 0
    if env_smtp_port:
        mail["smtp_port"] = env_smtp_port
    elif gmail_mode_enabled:
        mail["smtp_port"] = 587
    default_tls = True if gmail_mode_enabled else mail["use_tls"]
    default_ssl = False if gmail_mode_enabled else mail["use_ssl"]
    mail["use_tls"] = parse_bool_env(os.getenv("AUTH_SMTP_USE_TLS", ""), default_tls)
    mail["use_ssl"] = parse_bool_env(os.getenv("AUTH_SMTP_USE_SSL", ""), default_ssl)
    settings["session_secret"] = str(settings.get("session_secret", "") or "").strip() or secrets.token_hex(32)
    return settings

def save_auth_settings(settings):
    target = normalize_auth_settings(settings)
    directory = os.path.dirname(AUTH_SETTINGS_FILE)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(AUTH_SETTINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(target, f, ensure_ascii=False, indent=2)

def load_auth_settings():
    if os.path.exists(AUTH_SETTINGS_FILE):
        try:
            with open(AUTH_SETTINGS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = {}
    else:
        data = {}
    settings = normalize_auth_settings(data)
    if not os.path.exists(AUTH_SETTINGS_FILE):
        try:
            save_auth_settings(settings)
        except Exception:
            # Serverless deployments may not preserve local writes; continue with in-memory defaults.
            pass
    return settings

def persist_auth_settings(settings):
    global AUTH_SETTINGS
    AUTH_SETTINGS = normalize_auth_settings(settings)
    save_auth_settings(AUTH_SETTINGS)
    return AUTH_SETTINGS

def get_auth_settings():
    return AUTH_SETTINGS

def mask_email(email: str) -> str:
    normalized = normalize_email_address(email)
    if "@" not in normalized:
        return normalized
    local, domain = normalized.split("@", 1)
    if len(local) <= 2:
        masked_local = local[:1] + "*"
    else:
        masked_local = local[:2] + "*" * max(1, len(local) - 2)
    return f"{masked_local}@{domain}"

def build_access_policy_text(users) -> str:
    return "\n".join([f'{item["email"]}, {item["role"]}' for item in users or []])

def normalize_user_items(raw_items):
    parsed = {}
    for item in raw_items or []:
        if not isinstance(item, dict):
            continue
        email = normalize_email_address(item.get("email", ""))
        if not email or "@" not in email:
            continue
        role = "admin" if str(item.get("role", "")).strip().lower() == "admin" else "user"
        parsed[email] = {"email": email, "role": role}
    return sorted(parsed.values(), key=lambda item: (0 if item["role"] == "admin" else 1, item["email"]))

def parse_access_policy_text(raw_text: str):
    parsed = {}
    for raw_line in str(raw_text or "").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "," in line:
            email_part, role_part = [part.strip() for part in line.split(",", 1)]
        else:
            parts = line.split()
            email_part = parts[0] if parts else ""
            role_part = parts[1] if len(parts) > 1 else "user"
        email = normalize_email_address(email_part)
        if not email or "@" not in email:
            continue
        role = "admin" if role_part.strip().lower() == "admin" else "user"
        parsed[email] = {"email": email, "role": role}
    return sorted(parsed.values(), key=lambda item: (0 if item["role"] == "admin" else 1, item["email"]))

def normalize_user_meta_map(raw_meta, users):
    valid_emails = {item.get("email", "") for item in users or []}
    normalized = {}
    if not isinstance(raw_meta, dict):
        return normalized
    for raw_email, raw_payload in raw_meta.items():
        email = normalize_email_address(raw_email)
        if not email or email not in valid_emails:
            continue
        payload = raw_payload if isinstance(raw_payload, dict) else {}
        last_login_at = str(payload.get("last_login_at", "") or "").strip()
        try:
            login_count = max(0, int(payload.get("login_count", 0) or 0))
        except Exception:
            login_count = 0
        normalized[email] = {
            "last_login_at": last_login_at,
            "login_count": login_count,
        }
    return normalized

def record_user_login(email: str):
    normalized_email = normalize_email_address(email)
    if not normalized_email:
        return
    settings = get_auth_settings().copy()
    user_meta = dict(settings.get("user_meta", {}) or {})
    current_meta = dict(user_meta.get(normalized_email, {}) or {})
    try:
        login_count = max(0, int(current_meta.get("login_count", 0) or 0))
    except Exception:
        login_count = 0
    current_meta["last_login_at"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    current_meta["login_count"] = login_count + 1
    user_meta[normalized_email] = current_meta
    settings["user_meta"] = user_meta
    persist_auth_settings(settings)

def get_employee_records(settings=None):
    auth_settings = settings or get_auth_settings()
    user_meta = auth_settings.get("user_meta", {}) if isinstance(auth_settings.get("user_meta", {}), dict) else {}
    records = []
    for item in auth_settings.get("users", []):
        email = normalize_email_address(item.get("email", ""))
        role = "admin" if str(item.get("role", "")).strip().lower() == "admin" else "user"
        meta = user_meta.get(email, {}) if isinstance(user_meta.get(email, {}), dict) else {}
        last_login_at = str(meta.get("last_login_at", "") or "").strip()
        try:
            login_count = max(0, int(meta.get("login_count", 0) or 0))
        except Exception:
            login_count = 0
        status_key = "verified" if last_login_at else "pending"
        records.append(
            {
                "email": email,
                "role": role,
                "role_label": "Admin" if role == "admin" else "User",
                "status_key": status_key,
                "status_label": "Đã xác thực" if status_key == "verified" else "Chờ xác thực",
                "last_login_text": last_login_at or "Chưa có",
                "login_count": login_count,
                "initial": (email[:1] or "U").upper(),
                "is_forced_admin": email in FORCED_ADMIN_EMAILS,
            }
        )
    return records

AUTH_SETTINGS = load_auth_settings()

def get_policy_user(email: str, settings=None):
    normalized = normalize_email_address(email)
    if normalized in FORCED_ADMIN_EMAILS:
        return {"email": normalized, "role": "admin"}
    for item in (settings or get_auth_settings()).get("users", []):
        if item.get("email") == normalized:
            return {"email": normalized, "role": item.get("role", "user")}
    return None

def is_mail_configured(settings=None):
    mail = (settings or get_auth_settings()).get("mail", {})
    return bool(str(mail.get("smtp_host", "") or "").strip() and str(mail.get("smtp_from_email", "") or "").strip())

def encode_token_payload(payload: dict) -> str:
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

def decode_token_payload(value: str):
    padded = value + "=" * (-len(value) % 4)
    raw = base64.urlsafe_b64decode(padded.encode("ascii"))
    return json.loads(raw.decode("utf-8"))

def sign_token_value(value: str, secret_value: str) -> str:
    return hmac.new(secret_value.encode("utf-8"), value.encode("utf-8"), hashlib.sha256).hexdigest()

def create_session_token(email: str, settings=None) -> str:
    auth_settings = settings or get_auth_settings()
    payload = {
        "email": normalize_email_address(email),
        "exp": int(time.time()) + int(auth_settings.get("session_ttl_seconds", 86400)),
        "nonce": secrets.token_hex(8),
    }
    encoded = encode_token_payload(payload)
    signature = sign_token_value(encoded, auth_settings.get("session_secret", ""))
    return f"{encoded}.{signature}"

def decode_session_token(token: str, settings=None):
    auth_settings = settings or get_auth_settings()
    raw_token = str(token or "").strip()
    if "." not in raw_token:
        return None
    encoded, provided_signature = raw_token.rsplit(".", 1)
    expected_signature = sign_token_value(encoded, auth_settings.get("session_secret", ""))
    if not hmac.compare_digest(provided_signature, expected_signature):
        return None
    try:
        payload = decode_token_payload(encoded)
    except Exception:
        return None
    if int(payload.get("exp", 0) or 0) < int(time.time()):
        return None
    return payload

def get_current_user(request: Optional[Request]):
    if not request:
        return None
    token = request.cookies.get(SESSION_COOKIE_NAME, "")
    payload = decode_session_token(token)
    if not payload:
        return None
    email = normalize_email_address(payload.get("email", ""))
    user = get_policy_user(email)
    if not user:
        return None
    return {
        "email": email,
        "role": user.get("role", "user"),
        "role_label": "Admin" if user.get("role") == "admin" else "User",
    }

def set_session_cookie(response, email: str):
    token = create_session_token(email)
    response.set_cookie(
        SESSION_COOKIE_NAME,
        token,
        max_age=int(get_auth_settings().get("session_ttl_seconds", 86400)),
        httponly=True,
        samesite="lax",
        secure=False,
        path="/",
    )

def clear_session_cookie(response):
    response.delete_cookie(SESSION_COOKIE_NAME, path="/")

def get_request_next_path(request: Request) -> str:
    target = request.url.path or "/"
    if request.url.query:
        target += f"?{request.url.query}"
    return target

def require_authenticated_user(request: Request, admin_only: bool = False):
    current_user = get_current_user(request)
    if not current_user:
        if is_fetch_request(request):
            return None, JSONResponse(
                {"ok": False, "message": "Phiên đăng nhập đã hết hạn. Vui lòng đăng nhập lại.", "level": "warning"},
                status_code=401,
            )
        next_path = urllib.parse.quote(get_request_next_path(request), safe="")
        return None, RedirectResponse(url=f"/login?next={next_path}", status_code=302)
    if admin_only and current_user.get("role") != "admin":
        if is_fetch_request(request):
            return None, JSONResponse(
                {"ok": False, "message": "Chỉ admin mới dùng được chức năng này.", "level": "error"},
                status_code=403,
            )
        return None, RedirectResponse(url="/?auth_error=role", status_code=302)
    return current_user, None

def cleanup_auth_runtime():
    now_ts = time.time()
    expired_emails = [
        email
        for email, payload in OTP_STORE.items()
        if now_ts >= float(payload.get("expires_at", 0) or 0)
    ]
    for email in expired_emails:
        OTP_STORE.pop(email, None)

def generate_otp_code() -> str:
    return f"{secrets.randbelow(10 ** OTP_LENGTH):0{OTP_LENGTH}d}"

def hash_otp_code(email: str, code: str) -> str:
    return hashlib.sha256(f"{normalize_email_address(email)}::{str(code or '').strip()}".encode("utf-8")).hexdigest()

def build_otp_email_text(brand_name: str, target_email: str, otp_code: str, ttl_minutes: int) -> str:
    return "\n".join(
        [
            "Xin chào,",
            "",
            f"Bạn vừa yêu cầu đăng nhập vào {brand_name}.",
            f"Mã OTP của bạn là: {otp_code}",
            f"Mã có hiệu lực trong {ttl_minutes} phút.",
            "",
            f"Email đăng nhập: {normalize_email_address(target_email)}",
            "Nếu bạn không yêu cầu đăng nhập, hãy bỏ qua email này.",
        ]
    )

def build_otp_email_html(brand_name: str, target_email: str, otp_code: str, ttl_minutes: int) -> str:
    safe_brand = html.escape(brand_name)
    safe_email = html.escape(normalize_email_address(target_email))
    safe_code = html.escape(otp_code)
    safe_minutes = html.escape(str(ttl_minutes))
    preview_text = html.escape(f"Mã OTP đăng nhập {brand_name}: {otp_code}")
    return f"""\
<!DOCTYPE html>
<html lang="vi">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>{safe_brand} OTP</title>
  </head>
  <body style="margin:0;padding:0;background:#eef4ff;font-family:Arial,'Helvetica Neue',Helvetica,sans-serif;color:#0f172a;">
    <div style="display:none;max-height:0;overflow:hidden;opacity:0;color:transparent;">
      {preview_text}
    </div>
    <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="background:#eef4ff;margin:0;padding:24px 0;">
      <tr>
        <td align="center">
          <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="max-width:640px;background:#ffffff;border-radius:24px;overflow:hidden;border:1px solid #dbe7ff;box-shadow:0 14px 40px rgba(15,23,42,0.08);">
            <tr>
              <td style="padding:0;background:linear-gradient(135deg,#0f172a 0%,#1d4ed8 100%);">
                <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
                  <tr>
                    <td style="padding:28px 32px 22px 32px;">
                      <div style="font-size:12px;letter-spacing:0.24em;text-transform:uppercase;font-weight:700;color:#93c5fd;">Social Monitor</div>
                      <div style="margin-top:10px;font-size:28px;line-height:1.2;font-weight:800;color:#ffffff;">Mã OTP đăng nhập</div>
                      <div style="margin-top:10px;font-size:15px;line-height:1.7;color:#dbeafe;">
                        Hệ thống đã nhận yêu cầu đăng nhập vào <strong>{safe_brand}</strong>. Nhập mã bên dưới để hoàn tất xác minh.
                      </div>
                    </td>
                  </tr>
                </table>
              </td>
            </tr>
            <tr>
              <td style="padding:32px;">
                <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="margin-bottom:24px;">
                  <tr>
                    <td style="padding:22px 24px;border-radius:20px;background:#f8fbff;border:1px solid #d9e7ff;">
                      <div style="font-size:12px;font-weight:700;letter-spacing:0.16em;text-transform:uppercase;color:#64748b;">Mã xác thực</div>
                      <div style="margin-top:12px;font-size:36px;line-height:1;font-weight:800;letter-spacing:0.28em;color:#1d4ed8;font-family:'Courier New',Courier,monospace;">
                        {safe_code}
                      </div>
                      <div style="margin-top:14px;font-size:14px;line-height:1.7;color:#475569;">
                        Mã có hiệu lực trong <strong>{safe_minutes} phút</strong> và chỉ dùng được một lần.
                      </div>
                    </td>
                  </tr>
                </table>
                <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="margin-bottom:20px;">
                  <tr>
                    <td style="padding:18px 20px;border-radius:18px;background:#f8fafc;border:1px solid #e2e8f0;">
                      <div style="font-size:12px;font-weight:700;letter-spacing:0.12em;text-transform:uppercase;color:#64748b;">Thông tin đăng nhập</div>
                      <div style="margin-top:10px;font-size:15px;line-height:1.7;color:#0f172a;">
                        Email xác minh: <strong>{safe_email}</strong>
                      </div>
                    </td>
                  </tr>
                </table>
                <div style="font-size:14px;line-height:1.8;color:#475569;">
                  Nếu bạn không thực hiện yêu cầu này, có thể bỏ qua email. Không chia sẻ mã OTP với người khác.
                </div>
              </td>
            </tr>
            <tr>
              <td style="padding:0 32px 32px 32px;">
                <div style="padding-top:18px;border-top:1px solid #e2e8f0;font-size:12px;line-height:1.8;color:#94a3b8;">
                  Email này được gửi tự động từ {safe_brand}. Vui lòng không trả lời trực tiếp vào thư này.
                </div>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>
"""

def send_otp_email(target_email: str, otp_code: str, settings=None):
    auth_settings = settings or get_auth_settings()
    mail = auth_settings.get("mail", {})
    host = str(mail.get("smtp_host", "") or "").strip()
    from_email = normalize_email_address(mail.get("smtp_from_email", ""))
    if not host or not from_email:
        raise ValueError("Chưa cấu hình mail gửi OTP. Admin cần nhập SMTP trước.")

    brand_name = str(mail.get("smtp_from_name", "") or "Social Monitor").strip() or "Social Monitor"
    msg = EmailMessage()
    msg["Subject"] = f"Mã OTP đăng nhập {brand_name}"
    msg["From"] = f"{brand_name} <{from_email}>"
    msg["To"] = normalize_email_address(target_email)
    ttl_seconds = int(auth_settings.get("otp_ttl_seconds", 300))
    ttl_minutes = max(1, ttl_seconds // 60)
    msg.set_content(build_otp_email_text(brand_name, target_email, otp_code, ttl_minutes))
    msg.add_alternative(build_otp_email_html(brand_name, target_email, otp_code, ttl_minutes), subtype="html")

    smtp_user = str(mail.get("smtp_user", "") or "").strip()
    smtp_password = str(mail.get("smtp_password", "") or "")
    smtp_port = int(mail.get("smtp_port", 587) or 587)
    use_ssl = bool(mail.get("use_ssl", False))
    use_tls = bool(mail.get("use_tls", True))
    context = ssl.create_default_context()

    if use_ssl:
        with smtplib.SMTP_SSL(host, smtp_port, timeout=20, context=context) as server:
            if smtp_user:
                server.login(smtp_user, smtp_password)
            server.send_message(msg)
        return

    with smtplib.SMTP(host, smtp_port, timeout=20) as server:
        server.ehlo()
        if use_tls:
            server.starttls(context=context)
            server.ehlo()
        if smtp_user:
            server.login(smtp_user, smtp_password)
        server.send_message(msg)

def describe_smtp_error(exc: Exception) -> str:
    if isinstance(exc, smtplib.SMTPAuthenticationError):
        return (
            "Gmail không chấp nhận đăng nhập SMTP. Kiểm tra lại GMAIL_SMTP_EMAIL và "
            "GMAIL_SMTP_APP_PASSWORD, đồng thời chắc là tài khoản Gmail đã bật xác minh 2 bước "
            "và App Password còn hiệu lực."
        )
    return str(exc)

def build_auth_notice_html(message: str, level: str = "info") -> str:
    tone_map = {
        "success": "bg-emerald-500/15 text-emerald-300 border-emerald-500/30",
        "warning": "bg-amber-500/15 text-amber-300 border-amber-500/30",
        "error": "bg-red-500/15 text-red-300 border-red-500/30",
        "info": "bg-cyan-500/15 text-cyan-200 border-cyan-500/30",
    }
    return f'<div class="mb-4 rounded-xl border px-4 py-3 text-sm font-bold {tone_map.get(level, tone_map["info"])}">{html.escape(message)}</div>'

def render_login_page_html(request: Request, notice_text: str = "", notice_level: str = "info"):
    settings = get_auth_settings()
    allowed_count = len(settings.get("users", []))
    next_path = request.query_params.get("next", "/") or "/"
    helper_parts = []
    if not allowed_count:
        helper_parts.append("Chưa có email được cấp quyền.")
    if not is_mail_configured(settings):
        helper_parts.append("Mail OTP chưa sẵn sàng.")
    helper_text = " ".join(helper_parts) if helper_parts else "Nhập email để nhận OTP."
    notice_html = build_auth_notice_html(notice_text, notice_level) if notice_text else ""
    return HTMLResponse(
        f"""
        <!DOCTYPE html>
        <html lang="vi">
        <head>
            <meta charset="utf-8" />
            <meta name="viewport" content="width=device-width, initial-scale=1" />
            <title>Đăng nhập Social Monitor</title>
            <link rel="preconnect" href="https://fonts.googleapis.com">
            <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
            <link href="https://fonts.googleapis.com/css2?family=Be+Vietnam+Pro:wght@400;500;700;800;900&display=swap" rel="stylesheet">
            <style>
                :root {{ color-scheme: dark; }}
                * {{ box-sizing: border-box; }}
                body {{
                    margin: 0;
                    min-height: 100vh;
                    font-family: "Be Vietnam Pro", sans-serif;
                    background:
                        radial-gradient(circle at top right, rgba(56, 189, 248, 0.12), transparent 28%),
                        radial-gradient(circle at bottom left, rgba(16, 185, 129, 0.1), transparent 24%),
                        linear-gradient(180deg, #08101f, #0f172a 60%, #131c31);
                    color: #e2e8f0;
                    display: grid;
                    place-items: center;
                    padding: 24px;
                }}
                .auth-shell {{
                    width: min(100%, 1040px);
                    display: grid;
                    grid-template-columns: minmax(280px, 1.1fr) minmax(320px, 0.9fr);
                    overflow: hidden;
                    border-radius: 28px;
                    border: 1px solid rgba(148, 163, 184, 0.14);
                    background: rgba(9, 15, 28, 0.92);
                    box-shadow: 0 24px 70px rgba(2, 6, 23, 0.45);
                }}
                .auth-hero {{
                    padding: 40px;
                    display: flex;
                    flex-direction: column;
                    justify-content: center;
                    background:
                        radial-gradient(circle at top left, rgba(14, 165, 233, 0.18), transparent 36%),
                        linear-gradient(180deg, rgba(15, 23, 42, 0.98), rgba(12, 18, 31, 0.94));
                    border-right: 1px solid rgba(148, 163, 184, 0.12);
                }}
                .auth-kicker {{
                    font-size: 12px;
                    font-weight: 900;
                    letter-spacing: 0.28em;
                    text-transform: uppercase;
                    color: #38bdf8;
                }}
                .auth-title {{
                    margin: 16px 0 12px;
                    font-size: clamp(32px, 4vw, 48px);
                    line-height: 1.04;
                    font-weight: 900;
                    color: #f8fafc;
                }}
                .auth-subtitle {{
                    margin: 0;
                    color: #94a3b8;
                    line-height: 1.65;
                    font-size: 15px;
                    max-width: 440px;
                }}
                .auth-panel {{ padding: 34px; }}
                .auth-card {{
                    border-radius: 24px;
                    padding: 24px;
                    background: rgba(15, 23, 42, 0.74);
                    border: 1px solid rgba(148, 163, 184, 0.14);
                }}
                .auth-card-title {{
                    margin: 0 0 6px;
                    font-size: 24px;
                    font-weight: 900;
                    color: #f8fafc;
                }}
                .auth-card-sub {{
                    margin: 0 0 20px;
                    font-size: 14px;
                    color: #94a3b8;
                    line-height: 1.65;
                }}
                .auth-field {{ margin-bottom: 14px; }}
                .auth-label {{
                    display: block;
                    margin-bottom: 8px;
                    font-size: 12px;
                    font-weight: 800;
                    letter-spacing: 0.14em;
                    text-transform: uppercase;
                    color: #94a3b8;
                }}
                .auth-input {{
                    width: 100%;
                    border-radius: 16px;
                    border: 1px solid rgba(148, 163, 184, 0.16);
                    background: rgba(8, 13, 24, 0.9);
                    color: #f8fafc;
                    padding: 14px 16px;
                    font-size: 15px;
                    outline: none;
                }}
                .auth-input:focus {{
                    border-color: rgba(56, 189, 248, 0.65);
                    box-shadow: 0 0 0 3px rgba(14, 165, 233, 0.12);
                }}
                .auth-actions {{
                    display: grid;
                    gap: 12px;
                    margin-top: 16px;
                }}
                .auth-step[hidden] {{
                    display: none !important;
                }}
                .auth-btn {{
                    border: none;
                    border-radius: 16px;
                    padding: 14px 16px;
                    font-family: inherit;
                    font-size: 14px;
                    font-weight: 900;
                    letter-spacing: 0.08em;
                    text-transform: uppercase;
                    cursor: pointer;
                    transition: transform 0.18s ease, opacity 0.18s ease;
                }}
                .auth-btn:hover {{ transform: translateY(-1px); }}
                .auth-btn-primary {{
                    background: linear-gradient(135deg, #0ea5e9, #2563eb);
                    color: white;
                }}
                .auth-btn-secondary {{
                    background: rgba(30, 41, 59, 0.84);
                    color: #e2e8f0;
                    border: 1px solid rgba(148, 163, 184, 0.14);
                }}
                .auth-status {{ min-height: 48px; }}
                .auth-inline {{
                    display: grid;
                    grid-template-columns: 1fr auto;
                    gap: 12px;
                    align-items: end;
                }}
                @media (max-width: 860px) {{
                    .auth-shell {{ grid-template-columns: 1fr; }}
                    .auth-hero {{
                        border-right: none;
                        border-bottom: 1px solid rgba(148, 163, 184, 0.12);
                    }}
                    .auth-inline {{ grid-template-columns: 1fr; }}
                }}
            </style>
        </head>
        <body>
            <div class="auth-shell">
                <section class="auth-hero">
                    <div class="auth-kicker">Social Monitor</div>
                    <h1 class="auth-title">Đăng nhập bằng OTP email</h1>
                    <p class="auth-subtitle">Nhập email, lấy OTP và đăng nhập.</p>
                </section>
                <section class="auth-panel">
                    <div class="auth-card">
                        <h2 class="auth-card-title">Đăng nhập</h2>
                        <p class="auth-card-sub">{html.escape(helper_text)}</p>
                        <div id="auth-status" class="auth-status">{notice_html}</div>
                        <div class="auth-field">
                            <label class="auth-label" for="auth-email">Email</label>
                            <input id="auth-email" class="auth-input" type="email" autocomplete="email" placeholder="ban@company.com" />
                        </div>
                        <div class="auth-actions">
                            <button id="auth-request-btn" class="auth-btn auth-btn-primary" type="button">Xác nhận email</button>
                        </div>
                        <div id="auth-otp-step" class="auth-step" hidden>
                            <div class="auth-inline">
                                <div class="auth-field" style="margin-bottom:0;">
                                    <label class="auth-label" for="auth-otp">OTP 6 số</label>
                                    <input id="auth-otp" class="auth-input" type="text" inputmode="numeric" maxlength="6" autocomplete="one-time-code" placeholder="Nhập OTP" />
                                </div>
                                <button id="auth-resend-btn" class="auth-btn auth-btn-secondary" type="button">Gửi lại OTP</button>
                            </div>
                            <div class="auth-actions" style="margin-top:0;">
                                <button id="auth-verify-btn" class="auth-btn auth-btn-primary" type="button">Xác nhận đăng nhập</button>
                            </div>
                        </div>
                    </div>
                </section>
            </div>
            <script>
                const statusBox = document.getElementById("auth-status");
                const emailInput = document.getElementById("auth-email");
                const otpStep = document.getElementById("auth-otp-step");
                const otpInput = document.getElementById("auth-otp");
                const requestBtn = document.getElementById("auth-request-btn");
                const resendBtn = document.getElementById("auth-resend-btn");
                const verifyBtn = document.getElementById("auth-verify-btn");
                const nextPath = {json.dumps(next_path)};
                let otpRequested = false;

                const renderStatus = (message, level = "info") => {{
                    const toneMap = {{
                        success: "background:rgba(16,185,129,.12);color:#6ee7b7;border:1px solid rgba(16,185,129,.25);",
                        warning: "background:rgba(245,158,11,.12);color:#fcd34d;border:1px solid rgba(245,158,11,.25);",
                        error: "background:rgba(239,68,68,.12);color:#fca5a5;border:1px solid rgba(239,68,68,.25);",
                        info: "background:rgba(6,182,212,.12);color:#a5f3fc;border:1px solid rgba(6,182,212,.25);",
                    }};
                    statusBox.innerHTML = message
                        ? `<div style="padding:12px 14px;border-radius:14px;font-size:13px;font-weight:700;${{toneMap[level] || toneMap.info}}">${{message}}</div>`
                        : "";
                }};

                const setOtpStepVisible = (visible) => {{
                    otpRequested = Boolean(visible);
                    otpStep.hidden = !visible;
                }};

                const requestOtp = async (resend = false) => {{
                    const email = (emailInput.value || "").trim();
                    renderStatus(resend ? "Đang gửi lại OTP..." : "Đang gửi OTP...", "info");
                    try {{
                        const response = await fetch("/auth/request-otp", {{
                            method: "POST",
                            headers: {{ "Content-Type": "application/json", "X-Requested-With": "fetch" }},
                            body: JSON.stringify({{ email }}),
                        }});
                        const data = await response.json();
                        if (data.ok) {{
                            renderStatus("", "info");
                            setOtpStepVisible(true);
                            emailInput.readOnly = true;
                            requestBtn.textContent = "Đổi email";
                            otpInput.focus();
                        }} else {{
                            renderStatus(
                                data.message || (resend ? "Không gửi lại được OTP." : "Không gửi được OTP."),
                                data.level || "error"
                            );
                        }}
                    }} catch (_) {{
                        renderStatus(resend ? "Không gửi lại được OTP. Vui lòng thử lại." : "Không gửi được OTP. Vui lòng thử lại.", "error");
                    }}
                }};

                requestBtn.addEventListener("click", async () => {{
                    if (otpRequested) {{
                        setOtpStepVisible(false);
                        emailInput.readOnly = false;
                        otpInput.value = "";
                        requestBtn.textContent = "Xác nhận email";
                        emailInput.focus();
                        renderStatus("", "info");
                        return;
                    }}
                    await requestOtp(false);
                }});

                resendBtn.addEventListener("click", async () => {{
                    await requestOtp(true);
                }});

                verifyBtn.addEventListener("click", async () => {{
                    const email = (emailInput.value || "").trim();
                    const otp = (otpInput.value || "").trim();
                    renderStatus("Đang xác thực OTP...", "info");
                    try {{
                        const response = await fetch("/auth/verify-otp", {{
                            method: "POST",
                            headers: {{ "Content-Type": "application/json", "X-Requested-With": "fetch" }},
                            body: JSON.stringify({{ email, otp, next: nextPath }}),
                        }});
                        const data = await response.json();
                        renderStatus(data.message || "Không xác thực được OTP.", data.level || (data.ok ? "success" : "error"));
                        if (data.ok) {{
                            window.location.href = data.redirect_url || nextPath || "/";
                        }}
                    }} catch (_) {{
                        renderStatus("Không xác thực được OTP. Vui lòng thử lại.", "error");
                    }}
                }});

                emailInput.addEventListener("input", () => {{
                    if (!otpRequested) {{
                        return;
                    }}
                    setOtpStepVisible(false);
                    otpInput.value = "";
                    emailInput.readOnly = false;
                    requestBtn.textContent = "Xác nhận email";
                }});
            </script>
        </body>
        </html>
        """
    )

def build_admin_panel_html(current_user):
    if not current_user or current_user.get("role") != "admin":
        return ""
    settings = get_auth_settings()
    policy_text = build_access_policy_text(settings.get("users", []))
    mail = settings.get("mail", {})
    mail_status = "SMTP đã sẵn sàng gửi OTP." if is_mail_configured(settings) else "Chưa cấu hình mail gửi OTP."
    mail_status_class = "text-emerald-300" if is_mail_configured(settings) else "text-amber-300"
    return f"""
        <div class="bg-black/20 rounded-3xl p-6 border border-white/5 mt-6">
            <div class="flex flex-col xl:flex-row xl:items-start xl:justify-between gap-4 mb-5">
                <div>
                    <div class="text-base font-black text-slate-100">Quản trị truy cập</div>
                    <div class="text-sm text-slate-400 mt-1">Admin quản lý whitelist email, role theo access policy và SMTP để gửi OTP.</div>
                </div>
                <div class="text-sm font-bold {mail_status_class}">{mail_status}</div>
            </div>
            <div class="grid grid-cols-1 xl:grid-cols-2 gap-5">
                <div class="bg-slate-950/40 rounded-2xl p-4 border border-white/10">
                    <div class="text-sm font-black text-slate-100 mb-2">Access Policy</div>
                    <div class="text-xs text-slate-500 mb-3">Mỗi dòng một email. Dạng <code>email, role</code>. Role chỉ nhận <b>admin</b> hoặc <b>user</b>.</div>
                    <textarea id="auth-policy-text" class="w-full min-h-[220px] bg-slate-900 text-slate-100 rounded-xl px-4 py-3 border border-white/10 outline-none focus:border-cyan-400 font-mono text-sm" spellcheck="false">{html.escape(policy_text)}</textarea>
                    <div class="text-xs text-slate-500 mt-2">Email có trong danh sách này cũng chính là whitelist được phép nhận OTP.</div>
                    <button id="save-access-policy-btn" type="button" class="mt-4 w-full py-3 bg-slate-700 hover:bg-slate-600 text-white rounded-xl font-black uppercase text-sm shadow-sm shadow-slate-900/10">Lưu access policy</button>
                </div>
                <div class="bg-slate-950/40 rounded-2xl p-4 border border-white/10">
                    <div class="text-sm font-black text-slate-100 mb-2">Cấu hình mail OTP</div>
                    <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
                        <div>
                            <label class="block text-xs text-slate-400 mb-2 uppercase tracking-wider">SMTP Host</label>
                            <input id="mail-smtp-host" value="{html.escape(str(mail.get("smtp_host", "") or ""), quote=True)}" class="w-full bg-slate-900 text-slate-100 rounded-xl px-4 py-3 border border-white/10 outline-none focus:border-cyan-400" placeholder="smtp.gmail.com" />
                        </div>
                        <div>
                            <label class="block text-xs text-slate-400 mb-2 uppercase tracking-wider">SMTP Port</label>
                            <input id="mail-smtp-port" value="{html.escape(str(mail.get("smtp_port", 587) or 587), quote=True)}" class="w-full bg-slate-900 text-slate-100 rounded-xl px-4 py-3 border border-white/10 outline-none focus:border-cyan-400" placeholder="587" />
                        </div>
                        <div>
                            <label class="block text-xs text-slate-400 mb-2 uppercase tracking-wider">SMTP User</label>
                            <input id="mail-smtp-user" value="{html.escape(str(mail.get("smtp_user", "") or ""), quote=True)}" class="w-full bg-slate-900 text-slate-100 rounded-xl px-4 py-3 border border-white/10 outline-none focus:border-cyan-400" placeholder="your@email.com" />
                        </div>
                        <div>
                            <label class="block text-xs text-slate-400 mb-2 uppercase tracking-wider">SMTP Password</label>
                            <input id="mail-smtp-password" value="{html.escape(str(mail.get("smtp_password", "") or ""), quote=True)}" type="password" class="w-full bg-slate-900 text-slate-100 rounded-xl px-4 py-3 border border-white/10 outline-none focus:border-cyan-400" placeholder="App password" />
                        </div>
                        <div>
                            <label class="block text-xs text-slate-400 mb-2 uppercase tracking-wider">From Email</label>
                            <input id="mail-from-email" value="{html.escape(str(mail.get("smtp_from_email", "") or ""), quote=True)}" class="w-full bg-slate-900 text-slate-100 rounded-xl px-4 py-3 border border-white/10 outline-none focus:border-cyan-400" placeholder="no-reply@company.com" />
                        </div>
                        <div>
                            <label class="block text-xs text-slate-400 mb-2 uppercase tracking-wider">From Name</label>
                            <input id="mail-from-name" value="{html.escape(str(mail.get("smtp_from_name", "") or ""), quote=True)}" class="w-full bg-slate-900 text-slate-100 rounded-xl px-4 py-3 border border-white/10 outline-none focus:border-cyan-400" placeholder="Social Monitor" />
                        </div>
                    </div>
                    <div class="grid grid-cols-1 md:grid-cols-2 gap-3 mt-3">
                        <label class="flex items-center gap-2 text-sm text-slate-300">
                            <input id="mail-use-tls" type="checkbox" {"checked" if bool(mail.get("use_tls", True)) else ""} />
                            <span>Dùng STARTTLS</span>
                        </label>
                        <label class="flex items-center gap-2 text-sm text-slate-300">
                            <input id="mail-use-ssl" type="checkbox" {"checked" if bool(mail.get("use_ssl", False)) else ""} />
                            <span>Dùng SSL trực tiếp</span>
                        </label>
                    </div>
                    <button id="save-mail-config-btn" type="button" class="mt-4 w-full py-3 bg-slate-700 hover:bg-slate-600 text-white rounded-xl font-black uppercase text-sm shadow-sm shadow-slate-900/10">Lưu cấu hình mail</button>
                </div>
            </div>
        </div>
    """

def build_employee_panel_html(current_user):
    if not current_user or current_user.get("role") != "admin":
        return ""
    settings = get_auth_settings()
    employees = get_employee_records(settings)
    total_employees = len(employees)
    verified_count = sum(1 for item in employees if item["status_key"] == "verified")
    pending_count = total_employees - verified_count
    admin_count = sum(1 for item in employees if item["role"] == "admin")
    employee_json = json.dumps(
        [
            {
                "email": item["email"],
                "role": item["role"],
                "status_key": item["status_key"],
                "status_label": item["status_label"],
                "last_login_text": item["last_login_text"],
                "login_count": item["login_count"],
                "is_forced_admin": item["is_forced_admin"],
            }
            for item in employees
        ],
        ensure_ascii=False,
    )
    return f"""
    <section id="nhan-vien" data-dashboard-section="nhan-vien" class="dashboard-section dashboard-panel posts-board rounded-[2rem] p-6 md:p-8 mb-6 border border-white/5">
        <div class="flex flex-col gap-5">
            <div class="posts-page-head">
                <div>
                    <div class="posts-page-kicker">Nhân viên</div>
                    <h2 class="posts-page-title">Nhân viên</h2>
                    <p class="posts-page-subtitle">Quản lý email được phép đăng nhập, role và trạng thái xác thực của từng người dùng.</p>
                </div>
                <div class="employee-summary-grid">
                    <div class="employee-summary-pill">
                        <span>Tổng</span>
                        <strong id="employee-total-count">{total_employees}</strong>
                    </div>
                    <div class="employee-summary-pill">
                        <span>Đã xác thực</span>
                        <strong id="employee-verified-count">{verified_count}</strong>
                    </div>
                    <div class="employee-summary-pill">
                        <span>Admin</span>
                        <strong id="employee-admin-count">{admin_count}</strong>
                    </div>
                </div>
            </div>

            <div class="posts-toolbar rounded-[1.5rem] p-4 md:p-5">
                <div class="posts-toolbar-row">
                    <label class="posts-search-shell">
                        <i class="fa-solid fa-magnifying-glass text-slate-400"></i>
                        <input id="employee-search-input" type="text" placeholder="Tìm email nhân viên..." class="posts-search-input posts-search-field" />
                    </label>
                    <div class="posts-toolbar-actions">
                        <select id="employee-role-filter" class="employee-filter-select">
                            <option value="all">Tất cả role</option>
                            <option value="admin">Admin</option>
                            <option value="user">User</option>
                        </select>
                        <button type="button" id="employee-import-btn" class="posts-toolbar-btn">
                            <i class="fa-solid fa-file-import"></i> Nhập file
                        </button>
                        <button type="button" id="employee-reset-btn" class="posts-toolbar-btn">
                            <i class="fa-solid fa-rotate-left"></i> Đặt lại
                        </button>
                        <input id="employee-import-input" type="file" accept=".csv,.txt" class="hidden" />
                    </div>
                </div>
                <div class="posts-filter-row">
                    <button type="button" class="posts-chip is-active employee-status-chip" data-employee-status="all">Tất cả <span id="employee-chip-all">{total_employees}</span></button>
                    <button type="button" class="posts-chip employee-status-chip" data-employee-status="pending">Chờ xác thực <span id="employee-chip-pending">{pending_count}</span></button>
                    <button type="button" class="posts-chip employee-status-chip" data-employee-status="verified">Đã xác thực <span id="employee-chip-verified">{verified_count}</span></button>
                </div>
            </div>

            <div class="employee-layout">
                <div class="posts-table-shell">
                    <div class="overflow-x-auto">
                        <table class="w-full min-w-[860px] employee-table">
                            <thead>
                                <tr>
                                    <th>Nhân viên</th>
                                    <th>Role</th>
                                    <th>Trạng thái</th>
                                    <th>Lần đăng nhập gần nhất</th>
                                    <th class="text-right">Số lần</th>
                                    <th class="text-right">Thao tác</th>
                                </tr>
                            </thead>
                            <tbody id="employee-table-body"></tbody>
                        </table>
                    </div>
                    <div id="employee-empty-panel" class="posts-empty-state posts-empty-panel hidden">
                        Không có nhân viên nào khớp bộ lọc hiện tại.
                    </div>
                </div>

                <div class="employee-form-card">
                    <div class="employee-form-title">Thêm nhanh</div>
                    <div class="employee-form-sub">Nhập email để thêm vào whitelist đăng nhập. Có thể import nhiều dòng từ file CSV hoặc TXT.</div>
                    <div class="employee-form-grid">
                        <div>
                            <label class="employee-form-label" for="employee-email-input">Email</label>
                            <input id="employee-email-input" type="email" class="employee-form-input" placeholder="nhanvien@fanscom.vn" />
                        </div>
                        <div>
                            <label class="employee-form-label" for="employee-role-input">Role</label>
                            <select id="employee-role-input" class="employee-form-input">
                                <option value="user">User</option>
                                <option value="admin">Admin</option>
                            </select>
                        </div>
                    </div>
                    <div class="employee-form-actions">
                        <button type="button" id="employee-add-btn" class="posts-toolbar-btn">
                            <i class="fa-solid fa-user-plus"></i> Thêm nhân viên
                        </button>
                        <button type="button" id="employee-save-btn" class="employee-save-btn">
                            Lưu danh sách nhân viên
                        </button>
                    </div>
                    <div class="employee-form-note">Mail đã xác thực sẽ tự chuyển trạng thái sau khi đăng nhập OTP thành công. Email admin cứng vẫn luôn giữ quyền admin.</div>
                </div>
            </div>
        </div>
        <script id="employee-users-data" type="application/json">{employee_json}</script>
    </section>
    """

def add_log(msg):
    global logs
    timestamp = datetime.now().strftime("%H:%M:%S")
    # store full messages (trim list length), useful for debugging unicode/errors
    logs.insert(0, f"[{timestamp}] {msg}")
    if len(logs) > 50: logs.pop()

def build_log_html():
    if not logs:
        return '<p class="text-center text-slate-700 text-xl mt-24 uppercase font-black">Đang chờ lệnh...</p>'
    return "".join(
        [
            f'<div class="py-3 border-b border-white/5 font-mono text-sm"><span class="text-blue-400">[{l.split("] ")[0][1:]}]</span> {l.split("] ")[1]}</div>'
            for l in logs
        ]
    )

def build_pending_html():
    pending_rows_html = "".join(
        [
            f'<tr class="border-t border-white/5"><td class="py-2 pr-3 text-cyan-300 font-bold">{it["cell"]}</td><td class="py-2 pr-3 text-amber-300">{it["field"]}</td><td class="py-2 text-slate-200 break-all">{it["value"]}</td></tr>'
            for it in pending_updates
        ]
    )
    if not pending_updates:
        return '<div class="bg-black/20 rounded-3xl p-4 mb-6 border border-white/5 text-sm text-slate-400">Chưa có dữ liệu chuẩn bị nhập.</div>'
    return f"""
        <div class="bg-black/20 rounded-3xl p-6 mb-6 border border-white/5">
            <div class="flex justify-between items-center mb-3 text-sm font-bold text-slate-500 uppercase">
                <span>Ô sắp nhập</span><span class="text-cyan-300 font-black text-lg">{len(pending_updates)} ô</span>
            </div>
            <div class="overflow-x-auto">
                <table class="w-full text-sm">
                    <thead>
                        <tr class="text-left text-slate-400 uppercase text-xs">
                            <th class="py-2 pr-3">Ô</th>
                            <th class="py-2 pr-3">Trường</th>
                            <th class="py-2">Giá trị</th>
                        </tr>
                    </thead>
                    <tbody>{pending_rows_html}</tbody>
                </table>
            </div>
        </div>
    """

def build_status_payload():
    status_badge_class = "py-4 px-8 rounded-full text-lg font-black uppercase tracking-[0.16em] bg-slate-700/60 text-slate-200 border border-slate-500/20"
    status_badge_text = "Sẵn sàng"
    if is_running:
        status_badge_class = "py-4 px-8 rounded-full text-lg font-black uppercase tracking-[0.16em] bg-sky-500/12 text-sky-200 border border-sky-300/20"
        status_badge_text = "Đang quét dữ liệu..."
    elif is_finished:
        status_badge_class = "py-4 px-8 rounded-full text-lg font-black uppercase tracking-[0.16em] bg-emerald-500/12 text-emerald-200 border border-emerald-300/20"
        status_badge_text = "Đã hoàn tất"
    elif current_task == "Đã dừng thủ công":
        status_badge_class = "py-4 px-8 rounded-full text-lg font-black uppercase tracking-[0.16em] bg-amber-400/12 text-amber-200 border border-amber-300/20"
        status_badge_text = "Đã dừng"

    primary_action_html = (
        """<a href="/stop" data-inline-action="stop" class="w-full flex items-center justify-center py-4 px-4 bg-rose-600 hover:bg-rose-500 text-white rounded-xl font-black text-base uppercase tracking-[0.18em] shadow-md shadow-rose-900/20 border-b border-rose-800 transition-all active:scale-95"><i class="fa-solid fa-circle-stop mr-3 text-lg"></i> Dừng</a>"""
        if is_running
        else """<a href="/start" data-inline-action="start" class="w-full flex items-center justify-center py-4 px-4 bg-sky-600 hover:bg-sky-500 text-white rounded-xl font-black text-base uppercase tracking-[0.18em] shadow-md shadow-sky-900/20 border-b border-sky-700 transition-all active:scale-95"><i class="fa-solid fa-play mr-3 text-lg text-amber-200"></i> Bắt đầu</a>"""
    )

    return {
        "status_badge_class": status_badge_class,
        "status_badge_text": status_badge_text,
        "current_task": current_task,
        "progress_width": "100%" if is_finished else ("50%" if is_running else "0%"),
        "primary_action_html": primary_action_html,
    }

def build_snapshot_url(sheet_id: Optional[str] = None, sheet_gid: Optional[str] = None):
    resolved_sheet_id = sheet_id if sheet_id is not None else ACTIVE_SHEET_ID
    resolved_sheet_gid = sheet_gid if sheet_gid is not None else ACTIVE_SHEET_GID
    if not resolved_sheet_id:
        return ""
    return f"https://docs.google.com/spreadsheets/d/{resolved_sheet_id}/edit#gid={resolved_sheet_gid or '0'}"

def build_column_config_payload(sheet=None):
    metric_cols = {"link": "-", "campaign": "-", "view": "-", "like": "-", "share": "-", "comment": "-", "save": "-"}
    detected_text = "Chưa có sheet để tự nhận cột."
    header_row = 1
    effective_start_row = START_ROW
    if ACTIVE_SHEET_ID and ACTIVE_SHEET_NAME:
        try:
            ws = sheet or get_worksheet(ACTIVE_SHEET_NAME)
            layout = detect_sheet_layout(ws)
            header_row = max(1, int(layout.get("header_row") or 1))
            effective_start_row = resolve_effective_start_row(header_row)
            col_map = apply_column_overrides(layout.get("columns"))
            for field in metric_cols:
                col_idx = col_map.get(field)
                metric_cols[field] = col_to_a1(col_idx) if col_idx else "Chưa thấy"
            detected_text = format_detected_columns_text(layout)
        except Exception:
            pass
    manual_inputs = {field: (col_to_a1(col_idx) if col_idx else "") for field, col_idx in COLUMN_OVERRIDES.items()}
    detected_inputs = {
        field: (value if value not in {"-", "Chưa thấy"} else "")
        for field, value in metric_cols.items()
    }
    input_values = {}
    input_sources = {}
    for field in metric_cols:
        manual_value = manual_inputs.get(field, "")
        detected_value = detected_inputs.get(field, "")
        input_values[field] = manual_value or detected_value
        if manual_value:
            input_sources[field] = "THỦ CÔNG"
        elif detected_value:
            input_sources[field] = "AUTO"
        else:
            input_sources[field] = "CHƯA THẤY"
    return {
        "manual_mode": "THỦ CÔNG" if any(COLUMN_OVERRIDES.values()) else "AUTO",
        "metric_cols": metric_cols,
        "manual_inputs": manual_inputs,
        "detected_inputs": detected_inputs,
        "input_values": input_values,
        "input_sources": input_sources,
        "start_row": effective_start_row,
        "header_row": header_row,
        "detected_text": detected_text,
    }

def build_ui_state():
    payload = build_status_payload()
    payload["pending_html"] = build_pending_html()
    payload["log_html"] = build_log_html()
    payload["active_sheet_name"] = ACTIVE_SHEET_NAME or ""
    payload["active_sheet_id"] = ACTIVE_SHEET_ID or ""
    payload["active_sheet_gid"] = ACTIVE_SHEET_GID or ""
    payload["snapshot_url"] = build_snapshot_url()
    payload["schedule_config"] = build_schedule_config_payload()
    payload["schedule_tracking"] = build_schedule_tracking_payload()
    return payload

def build_ui_json_response(message: str, level: str = "info", ok: bool = True, extra: Optional[dict] = None):
    payload = build_ui_state()
    if extra:
        payload.update(extra)
    payload.update({
        "ok": ok,
        "message": message,
        "level": level,
    })
    return JSONResponse(payload)

def is_fetch_request(request: Optional[Request]):
    if not request:
        return False
    return (request.headers.get("x-requested-with", "") or "").lower() == "fetch"

def col_to_a1(col_idx: int) -> str:
    if col_idx <= 0:
        return "?"
    letters = ""
    n = col_idx
    while n > 0:
        n, rem = divmod(n - 1, 26)
        letters = chr(65 + rem) + letters
    return letters

def parse_column_input(value: str):
    raw = (value or "").strip().upper()
    if not raw:
        return None
    if raw.isdigit():
        idx = int(raw)
        return idx if idx > 0 else None
    if not re.fullmatch(r"[A-Z]+", raw):
        return None
    col_idx = 0
    for ch in raw:
        col_idx = col_idx * 26 + (ord(ch) - ord("A") + 1)
    return col_idx if col_idx > 0 else None

def parse_start_row_input(value: Optional[str]):
    raw = (value or "").strip()
    if raw == "":
        return None
    if not raw.isdigit():
        return None
    row_idx = int(raw)
    return row_idx if row_idx >= 2 else None

def set_pending_updates(row_idx: int, updates):
    global pending_updates
    rows = []
    for field, col_idx, value in updates:
        rows.append(
            {
                "cell": f"{col_to_a1(col_idx)}{row_idx}",
                "field": field,
                "value": str(value)[:120],
            }
        )
    pending_updates = rows[:20]

def parse_schedule_time(value: str):
    try:
        parts = value.strip().split(":")
        hour = int(parts[0])
        minute = int(parts[1])
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            return hour, minute
    except Exception:
        pass
    return 9, 0

def parse_schedule_date(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    return datetime.strptime(raw, "%Y-%m-%d").strftime("%Y-%m-%d")

def format_schedule_date(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    try:
        return datetime.strptime(raw, "%Y-%m-%d").strftime("%d/%m/%Y")
    except Exception:
        return raw

def format_datetime_display(value) -> str:
    if not value:
        return "Chưa có"
    if isinstance(value, str):
        return value
    try:
        return value.strftime("%d/%m/%Y %H:%M:%S")
    except Exception:
        return str(value)

def format_duration_display(seconds: float) -> str:
    try:
        total_seconds = max(0, int(round(float(seconds))))
    except Exception:
        total_seconds = 0
    minutes, secs = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours}h {minutes}m {secs}s"
    if minutes > 0:
        return f"{minutes}m {secs}s"
    return f"{secs}s"

def compute_next_schedule_run(reference: Optional[datetime] = None):
    if schedule_mode == "off":
        return None
    binding = get_schedule_sheet_binding()
    if not binding["sheet_id"] or not binding["sheet_name"]:
        return None

    now = reference or datetime.now()
    hour, minute = parse_schedule_time(schedule_time)
    end_date = None
    if schedule_end_date:
        try:
            end_date = datetime.strptime(schedule_end_date, "%Y-%m-%d").date()
        except Exception:
            end_date = None

    if schedule_mode == "daily":
        candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if candidate <= now:
            candidate += timedelta(days=1)
    elif schedule_mode == "weekly":
        candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        days_ahead = (schedule_weekday - candidate.weekday()) % 7
        if days_ahead == 0 and candidate <= now:
            days_ahead = 7
        candidate += timedelta(days=days_ahead)
    elif schedule_mode == "monthly":
        year = now.year
        month = now.month
        candidate = datetime(year, month, schedule_monthday, hour, minute)
        if candidate <= now:
            if month == 12:
                year += 1
                month = 1
            else:
                month += 1
            candidate = datetime(year, month, schedule_monthday, hour, minute)
    else:
        return None

    if end_date and candidate.date() > end_date:
        return None
    return candidate

def push_schedule_run_history(entry: dict):
    global schedule_run_history
    schedule_run_history.insert(0, entry)
    schedule_run_history = schedule_run_history[:8]


def build_schedule_target_key(sheet_id: Optional[str], sheet_name: str, row_idx, link: str) -> str:
    normalized_sheet_id = extract_sheet_id(str(sheet_id or "").strip()) or str(sheet_id or "").strip()
    normalized_sheet_name = normalize_header(sheet_name or "")
    normalized_row_idx = parse_start_row_input(str(row_idx or "")) or 0
    normalized_link = str(link or "").strip()
    return f"{normalized_sheet_id}::{normalized_sheet_name}::{normalized_row_idx}::{normalized_link}"


def summarize_schedule_targets(targets) -> str:
    normalized = normalize_schedule_targets(targets)
    count = len(normalized)
    if not count:
        return "Chưa chọn bài nào. Lịch sẽ chạy toàn bộ tab đã lưu."
    tab_count = len({(item.get("sheet_id"), normalize_header(item.get("sheet_name") or "")) for item in normalized})
    if count == 1:
        return "Đang chọn 1 bài cho lịch tự động."
    if tab_count <= 1:
        return f"Đang chọn {count} bài cho lịch tự động."
    return f"Đang chọn {count} bài ở {tab_count} tab cho lịch tự động."


def build_schedule_targets_html(targets, limit: int = 10) -> str:
    normalized = normalize_schedule_targets(targets)
    if not normalized:
        return '<div class="schedule-target-empty">Chưa có bài nào được gắn với lịch. Vào tab Bài đăng để tích chọn bài cần chạy tự động.</div>'

    cards = []
    for item in normalized[:limit]:
        title = str(item.get("title") or item.get("link") or f"Dòng {item.get('row_idx') or '-'}").strip()
        link = str(item.get("link") or "").strip()
        sheet_name = str(item.get("sheet_name") or "Sheet").strip()
        platform = str(item.get("platform") or "Khác").strip()
        campaign = str(item.get("campaign") or "").strip()
        row_text = f"Dòng {parse_metric_number(item.get('row_idx'))}"
        safe_title = html.escape(shorten_text(title, 92))
        safe_platform = html.escape(platform)
        safe_meta = html.escape(f"{sheet_name} • {row_text} • {campaign or 'Không gắn campaign'}")
        title_html = (
            f'<a href="{html.escape(link, quote=True)}" target="_blank" rel="noreferrer" class="schedule-target-link">{safe_title}</a>'
            if link
            else f'<div class="schedule-target-link">{safe_title}</div>'
        )
        cards.append(
            f"""
            <div class="schedule-target-item">
                <div class="schedule-target-top">
                    {title_html}
                    <span class="schedule-target-pill">{safe_platform}</span>
                </div>
                <div class="schedule-target-meta">{safe_meta}</div>
            </div>
            """
        )

    remaining = len(normalized) - limit
    if remaining > 0:
        cards.append(f'<div class="schedule-target-empty">Còn {remaining} bài khác đang được áp dụng cho lịch tự động.</div>')

    return "".join(cards)

def normalize_schedule_targets(raw_targets, fallback_sheet_id: Optional[str] = None):
    normalized = []
    seen = set()
    for raw in raw_targets or []:
        if not isinstance(raw, dict):
            continue
        sheet_id_raw = str(raw.get("sheet_id") or fallback_sheet_id or "").strip()
        sheet_id = extract_sheet_id(sheet_id_raw) or sheet_id_raw
        sheet_name = str(raw.get("sheet_name") or "").strip()
        row_idx = parse_start_row_input(str(raw.get("row_idx") or ""))
        link = str(raw.get("link") or "").strip()
        title = shorten_text(str(raw.get("title") or "").strip() or link or f"Dòng {row_idx or '-'}", 120)
        platform = str(raw.get("platform") or "").strip() or detect_platform(link)
        campaign = shorten_text(str(raw.get("campaign") or "").strip(), 80)
        if not sheet_id or not sheet_name or row_idx is None:
            continue
        dedupe_key = build_schedule_target_key(sheet_id, sheet_name, row_idx, link)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        normalized.append(
            {
                "sheet_id": sheet_id,
                "sheet_name": sheet_name,
                "row_idx": row_idx,
                "link": link,
                "title": title,
                "platform": platform,
                "campaign": campaign,
            }
        )
    return normalized

def get_schedule_sheet_binding(use_active_fallback: bool = True):
    if schedule_sheet_id and schedule_sheet_name:
        return {
            "sheet_id": schedule_sheet_id,
            "sheet_name": schedule_sheet_name,
            "sheet_gid": schedule_sheet_gid or "0",
            "is_saved": True,
        }
    if use_active_fallback and ACTIVE_SHEET_ID and ACTIVE_SHEET_NAME:
        return {
            "sheet_id": ACTIVE_SHEET_ID,
            "sheet_name": ACTIVE_SHEET_NAME,
            "sheet_gid": ACTIVE_SHEET_GID or "0",
            "is_saved": False,
        }
    return {
        "sheet_id": "",
        "sheet_name": "",
        "sheet_gid": "0",
        "is_saved": False,
    }

def build_schedule_scope_text():
    binding = get_schedule_sheet_binding()
    normalized_targets = normalize_schedule_targets(schedule_targets, binding["sheet_id"] or ACTIVE_SHEET_ID)
    if normalized_targets:
        tab_count = len({(item.get("sheet_id"), normalize_header(item.get("sheet_name") or "")) for item in normalized_targets})
        if binding["is_saved"]:
            if tab_count > 1:
                return f"Lịch đang nhớ {len(normalized_targets)} bài đã chọn ở {tab_count} tab trong spreadsheet hiện tại."
            return f"Lịch đang nhớ {len(normalized_targets)} bài đã chọn và sẽ chỉ quét đúng các bài này khi đến giờ."
        return f"Lịch sẽ dùng {len(normalized_targets)} bài đã chọn nếu bạn bấm Lưu lịch ngay bây giờ."
    sheet_name = binding["sheet_name"]
    if not sheet_name:
        return "Hãy chọn sheet ở phần Cấu hình trước, rồi bấm Lưu lịch để lịch tự động nhớ tab cần chạy."
    if binding["is_saved"]:
        return f"Lịch đang nhớ tab '{sheet_name}' và sẽ tự quét lại tab này khi đến giờ, không cần bấm tay."
    return f"Lịch sẽ dùng tab '{sheet_name}' nếu bạn bấm Lưu lịch ngay bây giờ."

def build_schedule_config_payload():
    binding = get_schedule_sheet_binding()
    normalized_targets = normalize_schedule_targets(schedule_targets, binding["sheet_id"] or ACTIVE_SHEET_ID)
    return {
        "label": schedule_label(),
        "sheet_name_text": binding["sheet_name"] or "Chưa chốt tab nào",
        "sheet_id_text": binding["sheet_id"] or "Chưa có Spreadsheet ID",
        "is_saved": binding["is_saved"],
        "scope_text": build_schedule_scope_text(),
        "snapshot_url": build_snapshot_url(binding["sheet_id"], binding["sheet_gid"]) if binding["sheet_id"] else "",
        "target_count": len(normalized_targets),
        "targets_summary_text": summarize_schedule_targets(normalized_targets),
        "targets_html": build_schedule_targets_html(normalized_targets),
        "targets": normalized_targets,
    }

def build_schedule_tracking_payload():
    next_run = compute_next_schedule_run()
    history_cards = []
    status_map = {
        "success": ("Thành công", "text-emerald-300"),
        "running": ("Đang chạy", "text-sky-300"),
        "error": ("Lỗi", "text-rose-300"),
        "stopped": ("Đã dừng", "text-amber-300"),
        "idle": ("Chưa chạy", "text-slate-400"),
    }

    for item in schedule_run_history[:5]:
        label, tone_class = status_map.get(item.get("status"), ("Không rõ", "text-slate-400"))
        history_cards.append(
            f"""
            <div class="schedule-history-item">
                <div>
                    <div class="schedule-history-title">{html.escape(item.get("sheet_name") or "Sheet")}</div>
                    <div class="schedule-history-meta">{html.escape(item.get("started_text") or "Chưa có thời gian")} • {html.escape(item.get("source_label") or "Không rõ")}</div>
                </div>
                <div class="text-right">
                    <div class="schedule-history-status {tone_class}">{label}</div>
                    <div class="schedule-history-meta">{html.escape(item.get("duration_text") or "0s")} • {int(item.get("processed", 0))} link</div>
                </div>
            </div>
            """
        )

    active_binding = get_schedule_sheet_binding()
    current_sheet_name = schedule_last_run_sheet_name or active_binding["sheet_name"] or "Chưa có"
    return {
        "next_run_text": format_datetime_display(next_run),
        "last_started_text": format_datetime_display(schedule_last_run_started_at),
        "last_finished_text": format_datetime_display(schedule_last_run_finished_at),
        "last_duration_text": format_duration_display(schedule_last_run_duration_seconds),
        "last_status_text": status_map.get(schedule_last_run_status, ("Chưa chạy", ""))[0],
        "last_source_text": "Tự động" if schedule_last_run_source == "schedule" else ("Thủ công" if schedule_last_run_source == "manual" else "Chưa có"),
        "last_sheet_text": current_sheet_name,
        "last_processed_text": str(int(schedule_last_run_processed or 0)),
        "last_success_text": str(int(schedule_last_run_success or 0)),
        "last_failed_text": str(int(schedule_last_run_failed or 0)),
        "is_running_text": format_datetime_display(run_started_at) if is_running and run_started_at else "Đang chờ",
        "history_html": "".join(history_cards) if history_cards else '<div class="schedule-history-empty">Chưa có lần chạy nào để theo dõi.</div>',
    }

def schedule_label():
    binding = get_schedule_sheet_binding()
    end_suffix = f" • đến {format_schedule_date(schedule_end_date)}" if schedule_end_date else ""
    sheet_suffix = f" • {binding['sheet_name']}" if binding["sheet_name"] else ""
    if schedule_mode == "daily":
        return f"Hằng ngày lúc {schedule_time}{sheet_suffix}{end_suffix}"
    if schedule_mode == "weekly":
        weekday_name = WEEKDAY_NAMES[schedule_weekday] if 0 <= schedule_weekday <= 6 else f"Thứ {schedule_weekday + 2}"
        return f"Hằng tuần ({weekday_name}) lúc {schedule_time}{sheet_suffix}{end_suffix}"
    if schedule_mode == "monthly":
        return f"Hằng tháng (ngày {schedule_monthday}) lúc {schedule_time}{sheet_suffix}{end_suffix}"
    return "Chưa bật"

def should_run_schedule(now: datetime):
    global last_schedule_run_key
    if schedule_mode == "off":
        return False
    if schedule_end_date:
        try:
            end_date = datetime.strptime(schedule_end_date, "%Y-%m-%d").date()
            if now.date() > end_date:
                return False
        except Exception:
            pass

    hour, minute = parse_schedule_time(schedule_time)
    if now.hour != hour or now.minute != minute:
        return False

    key = ""
    if schedule_mode == "daily":
        key = f"daily-{now.strftime('%Y-%m-%d')}"
    elif schedule_mode == "weekly":
        if now.weekday() != schedule_weekday:
            return False
        year, week, _ = now.isocalendar()
        key = f"weekly-{year}-{week}"
    elif schedule_mode == "monthly":
        if now.day != schedule_monthday:
            return False
        key = f"monthly-{now.strftime('%Y-%m')}"
    else:
        return False

    if key == last_schedule_run_key:
        return False
    last_schedule_run_key = key
    return True

def schedule_worker():
    while not scheduler_stop_event.is_set():
        try:
            if schedule_mode != "off" and not is_running:
                now = datetime.now()
                if should_run_schedule(now):
                    add_log(f"Kích hoạt lịch tự động: {schedule_label()}")
                    binding = get_schedule_sheet_binding()
                    selected_targets = normalize_schedule_targets(
                        [item for item in schedule_targets if (item.get("sheet_id") or "") == (binding["sheet_id"] or "")],
                        binding["sheet_id"],
                    )
                    if not binding["sheet_id"] or not binding["sheet_name"]:
                        add_log("Bỏ qua lịch tự động vì chưa có sheet/tab được lưu cho lịch.")
                    else:
                        run_scraper_logic(
                            sheet_id=binding["sheet_id"],
                            sheet_name=binding["sheet_name"],
                            targets=selected_targets,
                            source="schedule",
                        )
        except Exception as e:
            add_log(f"Lỗi lịch tự động: {str(e)}")
        scheduler_stop_event.wait(20)

def ensure_scheduler_thread():
    if IS_VERCEL:
        return
    global scheduler_thread
    if scheduler_thread and scheduler_thread.is_alive():
        return
    scheduler_stop_event.clear()
    scheduler_thread = threading.Thread(target=schedule_worker, daemon=True)
    scheduler_thread.start()

def get_gspread_client():
    creds = build_google_credentials()
    return gspread.authorize(creds)

def extract_sheet_id(sheet_input: str) -> Optional[str]:
    if not sheet_input:
        return None

    candidate = sheet_input.strip()

    # Common Sheets URL formats:
    # - /spreadsheets/d/<id>
    # - /spreadsheets/u/0/d/<id>
    m = re.search(r"/spreadsheets/(?:u/\d+/)?d/([a-zA-Z0-9-_]+)", candidate)
    if m:
        return m.group(1)

    # Fallback for links that put id in query params (?id=... or ?key=...)
    parsed = urllib.parse.urlparse(candidate)
    qs = urllib.parse.parse_qs(parsed.query)
    for key in ("id", "key"):
        val = qs.get(key, [None])[0]
        if val and re.fullmatch(r"[a-zA-Z0-9-_]{20,}", val):
            return val

    # User may paste the raw spreadsheet ID directly
    if re.fullmatch(r"[a-zA-Z0-9-_]{20,}", candidate):
        return candidate
    return None

def get_worksheet(sheet_name, sheet_id: Optional[str] = None):
    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(sheet_id or ACTIVE_SHEET_ID)
    return spreadsheet.worksheet(sheet_name)

def list_spreadsheet_tabs(sheet_input: str):
    sheet_id = extract_sheet_id(sheet_input or "")
    if not sheet_id:
        raise ValueError("Link/ID spreadsheet không hợp lệ.")
    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(sheet_id)
    return [
        {
            "title": ws.title,
            "gid": str(ws.id),
        }
        for ws in spreadsheet.worksheets()
    ]

def set_active_sheet(sheet_name, sheet_id: Optional[str] = None):
    global ACTIVE_SHEET_ID, ACTIVE_SHEET_NAME, ACTIVE_SHEET_GID
    target_sheet_id = sheet_id or ACTIVE_SHEET_ID
    ws = get_worksheet(sheet_name, target_sheet_id)
    ACTIVE_SHEET_ID = target_sheet_id
    ACTIVE_SHEET_NAME = sheet_name
    ACTIVE_SHEET_GID = str(ws.id)
    add_log(f"Đã lưu sheet: {sheet_name} | Spreadsheet ID: {ACTIVE_SHEET_ID}")

def retry_with_backoff(func, max_retries=3, base_delay=2):
    """Retry a function with exponential backoff on connection errors"""
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            error_msg = str(e).lower()
            if any(keyword in error_msg for keyword in ['connection', 'reset', 'aborted', 'timeout', '10054', 'forcibly closed']):
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt) + attempt  # Add attempt number to avoid exact multiples
                    add_log(f"Connection error, retrying in {delay}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(delay)
                    continue
            # Re-raise if not a connection error or max retries reached
            raise e

# --- Xá»­ lÃ½ link Facebook Watch / RÃºt gá»n ---
def resolve_fb_url(url):
    # Normalize Facebook URLs (mobile subdomains, wrapper redirects) and follow redirects
    try:
        if not url:
            return url
        # remove URL fragment
        url = url.split('#')[0]

        parsed = urllib.parse.urlparse(url)
        netloc = parsed.netloc.lower()

        # Normalize common mobile subdomains to www.facebook.com
        if netloc.startswith('m.') or netloc.startswith('mobile.') or netloc.startswith('mbasic.'):
            parsed = parsed._replace(netloc='www.facebook.com')
            url = urllib.parse.urlunparse(parsed)

        # l.facebook.com redirect wrapper (contains u= original URL)
        if 'l.facebook.com' in url:
            try:
                qs = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
                if 'u' in qs and qs['u']:
                    return urllib.parse.unquote(qs['u'][0]).split('#')[0]
            except Exception:
                pass

        # For fb.watch and facebook.com/watch, follow redirects (HEAD -> GET fallback)
        try:
            resp = requests.head(url, allow_redirects=True, timeout=10)
            final = resp.url
            if final:
                return final.split('#')[0]
        except Exception:
            try:
                resp = requests.get(url, allow_redirects=True, timeout=10, stream=True)
                final = resp.url
                resp.close()
                if final:
                    return final.split('#')[0]
            except Exception as e:
                add_log(f"resolve_fb_url request error: {e}")
                return url

    except Exception as e:
        add_log(f"resolve_fb_url error: {e}")
    return url

# --- Nháº­n diá»‡n ná»n táº£ng ---
def detect_platform(url):
    url_lower = url.lower()
    if "facebook.com" in url_lower or "fb.watch" in url_lower: return "Facebook"
    if "tiktok.com" in url_lower: return "TikTok"
    if "youtube.com" in url_lower or "youtu.be" in url_lower: return "YouTube"
    if "instagram.com" in url_lower: return "Instagram"
    return "Khác"

def normalize_header(text: str) -> str:
    value = unicodedata.normalize("NFD", (text or "").strip().lower())
    value = "".join(ch for ch in value if unicodedata.category(ch) != "Mn")
    value = re.sub(r"[^a-z0-9]+", "", value)
    return value

def header_matches_alias(key: str, aliases) -> bool:
    if not key:
        return False
    if key in aliases:
        return True
    for alias in aliases:
        if not alias:
            continue
        if key.startswith(alias) or key.endswith(alias) or alias in key:
            return True
    return False

def detect_columns_from_headers(headers):
    columns = {}
    for idx, header in enumerate(headers or [], start=1):
        key = normalize_header(header)
        if not key:
            continue
        for field, names in HEADER_ALIASES.items():
            if field in columns:
                continue
            if header_matches_alias(key, names):
                columns[field] = idx
                break
    return columns

def detect_sheet_layout(sheet, sample_rows: int = 5):
    best_row = 1
    best_headers = []
    best_columns = {}
    best_score = -1
    max_rows = max(1, int(sample_rows or 1))

    for row_idx in range(1, max_rows + 1):
        headers = sheet.row_values(row_idx)
        if not any(str(cell or "").strip() for cell in headers):
            continue
        columns = detect_columns_from_headers(headers)
        score = len(columns)
        if "link" in columns:
            score += 3
        if any(metric in columns for metric in ("view", "like", "share", "comment", "save")):
            score += 2
        if score > best_score:
            best_row = row_idx
            best_headers = headers
            best_columns = columns
            best_score = score

    if best_score < 0:
        best_headers = sheet.row_values(1)
        best_columns = detect_columns_from_headers(best_headers)

    return {
        "header_row": best_row,
        "headers": best_headers,
        "columns": best_columns,
    }

def detect_sheet_columns(sheet):
    return detect_sheet_layout(sheet).get("columns", {})

def get_sheet_records(sheet, layout=None):
    resolved_layout = layout or detect_sheet_layout(sheet)
    header_row = max(1, int(resolved_layout.get("header_row") or 1))
    records = sheet.get_all_records(head=header_row)
    return records, header_row, list(resolved_layout.get("headers") or [])

def resolve_effective_start_row(header_row: int) -> int:
    return max(2, START_ROW, int(header_row or 1) + 1)

def format_detected_columns_text(layout) -> str:
    if not layout:
        return ""
    col_map = apply_column_overrides(layout.get("columns") or {})
    ordered_fields = ["link", "campaign", "view", "like", "share", "comment", "save"]
    parts = []
    for field in ordered_fields:
        col_idx = col_map.get(field)
        if col_idx:
            parts.append(f"{field.upper()}={col_to_a1(col_idx)}")
    if not parts:
        return f"AUTO chưa nhận được cột nào ở header dòng {layout.get('header_row') or 1}"
    return f"AUTO nhận header dòng {layout.get('header_row') or 1}: " + ", ".join(parts)

def apply_column_overrides(columns):
    merged = dict(columns or {})
    for field, col_idx in COLUMN_OVERRIDES.items():
        if col_idx:
            merged[field] = col_idx
    return merged

def first_nonempty_value(record, *keys):
    for key in keys:
        value = record.get(key)
        if value is None:
            continue
        try:
            if pd.isna(value):
                continue
        except Exception:
            pass
        if str(value).strip() == "":
            continue
        return value
    return ""

def resolve_header_from_column(headers, col_idx: Optional[int]) -> str:
    if not headers or not col_idx or col_idx <= 0 or col_idx > len(headers):
        return ""
    return str(headers[col_idx - 1] or "").strip()

def read_record_value_from_header(record, normalized_record, header_name: str):
    if not header_name:
        return ""
    for source, key in ((record or {}, header_name), (normalized_record or {}, normalize_header(header_name))):
        value = source.get(key)
        if value is None:
            continue
        try:
            if pd.isna(value):
                continue
        except Exception:
            pass
        if str(value).strip() == "":
            continue
        return value
    return ""

def parse_metric_number(value) -> int:
    if value is None:
        return 0
    try:
        if pd.isna(value):
            return 0
    except Exception:
        pass
    if isinstance(value, (int, float)):
        try:
            return int(float(value))
        except Exception:
            return 0
    raw = str(value).strip()
    if not raw or raw.lower() == "nan":
        return 0
    digits = re.sub(r"[^\d]", "", raw)
    return int(digits) if digits else 0

def format_metric_number(value) -> str:
    return f"{parse_metric_number(value):,}".replace(",", ".")

def format_table_metric(value) -> str:
    number = parse_metric_number(value)
    return format_metric_number(number) if number > 0 else "-"

def shorten_text(text, limit: int = 88) -> str:
    value = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 1)].rstrip() + "..."

def infer_creator_name(link: str, platform: str) -> str:
    parsed = urllib.parse.urlparse(link or "")
    path_parts = [part for part in parsed.path.split("/") if part]
    host = (parsed.netloc or "").replace("www.", "")
    if platform == "TikTok":
        handle = next((part for part in path_parts if part.startswith("@")), "")
        return handle or "TikTok creator"
    if platform == "Instagram":
        return f"@{path_parts[0]}" if path_parts else "Instagram creator"
    if platform == "YouTube":
        handle = next((part for part in path_parts if part.startswith("@")), "")
        return handle or "YouTube channel"
    if platform == "Facebook":
        return "Facebook page"
    return host or platform

def infer_post_title(link: str, platform: str) -> str:
    parsed = urllib.parse.urlparse(link or "")
    if platform == "TikTok":
        creator = infer_creator_name(link, platform)
        return f"Bài TikTok của {creator}"
    if platform == "Instagram":
        return "Bài đăng Instagram"
    if platform == "YouTube":
        return "Video YouTube"
    if platform == "Facebook":
        return "Bài đăng Facebook"
    return shorten_text(parsed.geturl() or link, 72) or "Bài đăng mạng xã hội"

def infer_creator_handle(link: str, creator: str, platform: str) -> str:
    creator_name = str(creator or "").strip()
    if creator_name.startswith("@"):
        return creator_name

    parsed = urllib.parse.urlparse(link or "")
    host = (parsed.netloc or "").replace("www.", "")
    path_parts = [part for part in parsed.path.split("/") if part]

    if platform == "TikTok":
        handle = next((part for part in path_parts if part.startswith("@")), "")
        return handle or "@tiktok"
    if platform == "Instagram":
        handle = path_parts[0] if path_parts else ""
        return f"@{handle}" if handle and not handle.startswith("@") else (handle or "@instagram")
    if platform == "YouTube":
        handle = next((part for part in path_parts if part.startswith("@")), "")
        return handle or "@youtube"
    if platform == "Facebook":
        slug = path_parts[0] if path_parts else host
        return slug or "facebook.com"
    return host or platform

def format_compact_metric(value) -> str:
    number = parse_metric_number(value)
    if number >= 1_000_000_000:
        compact = f"{number / 1_000_000_000:.1f}".rstrip("0").rstrip(".")
        return compact.replace(".", ",") + " B"
    if number >= 1_000_000:
        compact = f"{number / 1_000_000:.1f}".rstrip("0").rstrip(".")
        return compact.replace(".", ",") + " M"
    if number >= 1_000:
        compact = f"{number / 1_000:.1f}".rstrip("0").rstrip(".")
        return compact.replace(".", ",") + " K"
    return format_metric_number(number)

def build_dom_slug(value: str, fallback: str = "item") -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", normalize_header(str(value or ""))).strip("-")
    return slug or fallback

def collect_posts_dataset_for_worksheet(ws, tab_index: int = 0):
    sheet_title = str(getattr(ws, "title", "") or f"Sheet {tab_index + 1}")
    sheet_slug = f"{build_dom_slug(sheet_title, 'sheet')}-{tab_index}"
    platform_counts = {"tiktok": 0, "facebook": 0, "instagram": 0, "youtube": 0, "khac": 0}
    selected_target_keys = {
        build_schedule_target_key(item.get("sheet_id"), item.get("sheet_name"), item.get("row_idx"), item.get("link"))
        for item in normalize_schedule_targets(schedule_targets, ACTIVE_SHEET_ID)
    }
    rows_html = []
    total_posts = 0
    total_views = 0
    total_engagement = 0
    creators = set()
    campaigns = set()
    error = ""

    try:
        layout = detect_sheet_layout(ws)
        col_map = apply_column_overrides(layout.get("columns"))
        records, header_row, headers = get_sheet_records(ws, layout)
        campaign_header = resolve_header_from_column(headers, col_map.get("campaign"))
        start_row = resolve_effective_start_row(header_row)
    except Exception as exc:
        error = str(exc)
        return {
            "sheet_title": sheet_title,
            "sheet_slug": sheet_slug,
            "sheet_gid": str(getattr(ws, "id", "") or "0"),
            "total_posts": 0,
            "total_views": 0,
            "total_engagement": 0,
            "creator_count": 0,
            "campaign_count": 0,
            "platform_counts": platform_counts,
            "rows_html": "",
            "error": error,
        }

    for row_idx, record in enumerate(records, start=header_row + 1):
        if row_idx < start_row:
            continue

        normalized_record = {normalize_header(str(key)): value for key, value in (record or {}).items()}
        link = str(first_nonempty_value(normalized_record, "link", "url", "posturl")).strip()
        if not link:
            continue

        platform = detect_platform(link)
        platform_key = normalize_header(platform)
        if platform_key not in platform_counts:
            platform_key = "khac"
        platform_counts[platform_key] += 1

        creator = str(
            first_nonempty_value(normalized_record, "kol", "creator", "author", "username", "account", "channel")
        ).strip() or infer_creator_name(link, platform)
        campaign = str(
            read_record_value_from_header(record, normalized_record, campaign_header)
            or first_nonempty_value(normalized_record, "campaign", "chiendich", "camp")
        ).strip() or sheet_title
        title = str(
            first_nonempty_value(normalized_record, "caption", "title", "content", "noidung", "post", "mota")
        ).strip() or infer_post_title(link, platform)
        date_text = str(first_nonempty_value(normalized_record, "date", "time", "timestamp", "ngay", "thoigian")).strip() or "-"
        view = parse_metric_number(first_nonempty_value(normalized_record, "view", "views", "luotxem"))
        reaction = parse_metric_number(first_nonempty_value(normalized_record, "like", "likes", "reaction", "reactions"))
        share = parse_metric_number(first_nonempty_value(normalized_record, "share", "shares"))
        comment = parse_metric_number(first_nonempty_value(normalized_record, "comment", "comments", "cmt"))
        save = parse_metric_number(first_nonempty_value(normalized_record, "save", "saves", "saved", "bookmark", "bookmarks", "luu"))
        engagement = reaction + share + comment + save
        status_label = "Đã quét" if any([view, reaction, share, comment, save]) or date_text != "-" else "Chờ quét"
        status_class = "posts-status-done" if status_label == "Đã quét" else "posts-status-pending"
        avatar = html.escape((creator or platform)[:1].upper())
        safe_title = html.escape(shorten_text(title, 88))
        safe_link = html.escape(link, quote=True)
        safe_creator = html.escape(shorten_text(creator, 24))
        creator_handle = infer_creator_handle(link, creator, platform)
        safe_creator_handle = html.escape(shorten_text(creator_handle, 24))
        safe_platform = html.escape(platform)
        safe_campaign = html.escape(shorten_text(campaign, 28))
        safe_date = html.escape(date_text)
        safe_content_meta = html.escape(f"{platform} • Dòng {row_idx} • Tab {sheet_title}")
        safe_sheet_name_attr = html.escape(sheet_title, quote=True)
        safe_title_attr = html.escape(title, quote=True)
        safe_platform_attr = html.escape(platform, quote=True)
        safe_campaign_attr = html.escape(campaign, quote=True)
        schedule_target_key = build_schedule_target_key(ACTIVE_SHEET_ID, sheet_title, row_idx, link)
        is_schedule_target = schedule_target_key in selected_target_keys
        row_schedule_class = " is-schedule-target" if is_schedule_target else ""
        checked_attr = " checked" if is_schedule_target else ""
        search_blob = html.escape(
            " ".join([title, creator, campaign, platform, link, date_text, sheet_title]).lower(),
            quote=True,
        )

        rows_html.append(
            f"""
            <tr class="post-row posts-table-row{row_schedule_class}" data-platform="{platform_key}" data-search="{search_blob}">
                <td class="posts-cell posts-cell-check">
                    <input
                        type="checkbox"
                        class="posts-table-check post-select-check"
                        data-post-select
                        data-sheet-id="{html.escape(ACTIVE_SHEET_ID or '', quote=True)}"
                        data-sheet-name="{safe_sheet_name_attr}"
                        data-row-idx="{row_idx}"
                        data-link="{safe_link}"
                        data-title="{safe_title_attr}"
                        data-platform-name="{safe_platform_attr}"
                        data-campaign-name="{safe_campaign_attr}"
                        data-target-key="{html.escape(schedule_target_key, quote=True)}"
                        aria-label="Chọn dòng {row_idx}"
                        {checked_attr}
                    />
                </td>
                <td class="posts-cell posts-cell-content">
                    <div class="post-content-wrap">
                        <a href="{safe_link}" target="_blank" rel="noreferrer" class="post-title-link">{safe_title}</a>
                        <div class="post-content-meta">{safe_content_meta}</div>
                    </div>
                </td>
                <td class="posts-cell">
                    <div class="flex items-center gap-3">
                        <div class="post-avatar post-avatar-{platform_key}">{avatar}</div>
                        <div>
                            <div class="post-creator-name">{safe_creator}</div>
                            <div class="post-creator-handle">{safe_creator_handle}</div>
                        </div>
                    </div>
                </td>
                <td class="posts-cell"><span class="post-status-pill {status_class}">{status_label}</span></td>
                <td class="posts-cell posts-cell-date">{safe_date}</td>
                <td class="posts-cell posts-cell-metric">{format_table_metric(view)}</td>
                <td class="posts-cell posts-cell-metric">{format_table_metric(reaction)}</td>
                <td class="posts-cell posts-cell-metric">{format_table_metric(share)}</td>
                <td class="posts-cell posts-cell-metric">{format_table_metric(comment)}</td>
                <td class="posts-cell posts-cell-metric posts-cell-metric-strong">{format_table_metric(engagement)}</td>
                <td class="posts-cell posts-cell-campaign">
                    <div class="posts-campaign-main">{safe_campaign}</div>
                    <div class="posts-campaign-sub">{safe_platform}</div>
                </td>
            </tr>
            """
        )

        total_posts += 1
        total_views += view
        total_engagement += engagement
        if creator:
            creators.add(creator)
        if campaign:
            campaigns.add(campaign)

    return {
        "sheet_title": sheet_title,
        "sheet_slug": sheet_slug,
        "sheet_gid": str(getattr(ws, "id", "") or "0"),
        "total_posts": total_posts,
        "total_views": total_views,
        "total_engagement": total_engagement,
        "creator_count": len(creators),
        "campaign_count": len(campaigns),
        "platform_counts": platform_counts,
        "rows_html": "".join(rows_html),
        "error": error,
    }

def parse_dashboard_date(value):
    raw = str(value or "").strip()
    if not raw or raw == "-":
        return None
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt)
        except Exception:
            continue
    return None

def build_overview_panel_html(sheet, snapshot_url: str, status_payload, schedule_text: str):
    records = []
    overview_error = ""
    campaign_header = ""
    header_row = 1
    start_row = max(2, START_ROW)
    if ACTIVE_SHEET_ID and ACTIVE_SHEET_NAME:
        try:
            ws = sheet or get_worksheet(ACTIVE_SHEET_NAME)
            layout = detect_sheet_layout(ws)
            col_map = apply_column_overrides(layout.get("columns"))
            records, header_row, headers = get_sheet_records(ws, layout)
            campaign_header = resolve_header_from_column(headers, col_map.get("campaign"))
            start_row = resolve_effective_start_row(header_row)
        except Exception as exc:
            overview_error = str(exc)

    total_posts = 0
    total_views = 0
    total_engagement = 0
    creators = set()
    campaigns = {}

    for row_idx, record in enumerate(records, start=header_row + 1):
        if row_idx < start_row:
            continue
        normalized_record = {normalize_header(str(key)): value for key, value in (record or {}).items()}
        link = str(first_nonempty_value(normalized_record, "link", "url", "posturl")).strip()
        if not link:
            continue

        platform = detect_platform(link)
        creator = str(
            first_nonempty_value(normalized_record, "kol", "creator", "author", "username", "account", "channel")
        ).strip() or infer_creator_name(link, platform)
        campaign_name = str(
            read_record_value_from_header(record, normalized_record, campaign_header)
            or first_nonempty_value(normalized_record, "campaign", "chiendich", "camp")
        ).strip() or (ACTIVE_SHEET_NAME or "Campaign hiện tại")
        view = parse_metric_number(first_nonempty_value(normalized_record, "view", "views", "luotxem"))
        reaction = parse_metric_number(first_nonempty_value(normalized_record, "like", "likes", "reaction", "reactions"))
        share = parse_metric_number(first_nonempty_value(normalized_record, "share", "shares"))
        comment = parse_metric_number(first_nonempty_value(normalized_record, "comment", "comments", "cmt"))
        save = parse_metric_number(first_nonempty_value(normalized_record, "save", "saves", "saved", "bookmark", "bookmarks", "luu"))
        engagement = reaction + share + comment + save
        scanned_at = parse_dashboard_date(first_nonempty_value(normalized_record, "date", "time", "timestamp", "ngay", "thoigian"))

        total_posts += 1
        total_views += view
        total_engagement += engagement
        creators.add(creator.strip().lower())

        campaign_bucket = campaigns.setdefault(
            campaign_name,
            {
                "posts": 0,
                "views": 0,
                "engagement": 0,
                "creators": set(),
                "platforms": set(),
                "started_at": None,
            },
        )
        campaign_bucket["posts"] += 1
        campaign_bucket["views"] += view
        campaign_bucket["engagement"] += engagement
        campaign_bucket["creators"].add(creator.strip().lower())
        campaign_bucket["platforms"].add(platform)
        if scanned_at and (campaign_bucket["started_at"] is None or scanned_at < campaign_bucket["started_at"]):
            campaign_bucket["started_at"] = scanned_at

    total_campaigns = len(campaigns) if campaigns else (1 if ACTIVE_SHEET_NAME else 0)
    total_creators = len([item for item in creators if item])
    featured_campaign_name = ACTIVE_SHEET_NAME or "Campaign hiện tại"
    featured_campaign = {
        "posts": total_posts,
        "views": total_views,
        "engagement": total_engagement,
        "creators": total_creators,
        "platforms": set(),
        "started_at": None,
    }
    if campaigns:
        featured_campaign_name, featured_campaign = sorted(
            campaigns.items(),
            key=lambda item: (item[1]["posts"], item[1]["views"], item[1]["engagement"]),
            reverse=True,
        )[0]
    featured_campaign_creators = featured_campaign.get("creators", total_creators)
    if isinstance(featured_campaign_creators, set):
        featured_campaign_creators = len([item for item in featured_campaign_creators if item])
    featured_campaign_creators = parse_metric_number(featured_campaign_creators)

    started_text = (
        featured_campaign["started_at"].strftime("%d/%m/%Y")
        if featured_campaign.get("started_at")
        else datetime.now().strftime("%d/%m/%Y")
    )
    platforms_text = ", ".join(sorted(featured_campaign.get("platforms") or [])) or "Đa nền tảng"
    status_chip_class = "overview-status-live" if is_running else ("overview-status-done" if is_finished else "overview-status-waiting")
    status_chip_text = "Đang diễn ra" if is_running else ("Đã hoàn tất" if is_finished else "Sẵn sàng")
    overview_error_html = (
        f'<div class="mt-4 rounded-2xl border border-amber-500/20 bg-amber-500/10 px-4 py-3 text-sm text-amber-200">Không tải được toàn bộ dữ liệu overview: {html.escape(overview_error)}</div>'
        if overview_error
        else ""
    )

    return f"""
    <section id="tong-quan" data-dashboard-section="tong-quan" class="dashboard-section dashboard-panel is-active mb-6">
        <div class="overview-shell">
            <div class="overview-header">
                <div class="overview-kicker">Tổng quan</div>
            </div>

            <div class="overview-stat-grid">
                <div class="overview-stat-card">
                    <div class="overview-stat-icon icon-campaign"><i class="fa-regular fa-flag"></i></div>
                    <div>
                        <div class="overview-stat-label">Tổng số chiến dịch</div>
                        <div class="overview-stat-value">{format_metric_number(total_campaigns)}</div>
                    </div>
                </div>
                <div class="overview-stat-card">
                    <div class="overview-stat-icon icon-post"><i class="fa-solid fa-photo-film"></i></div>
                    <div>
                        <div class="overview-stat-label">Tổng số bài đăng</div>
                        <div class="overview-stat-value">{format_metric_number(total_posts)}</div>
                    </div>
                </div>
                <div class="overview-stat-card">
                    <div class="overview-stat-icon icon-view"><i class="fa-regular fa-eye"></i></div>
                    <div>
                        <div class="overview-stat-label">Tổng lượt xem</div>
                        <div class="overview-stat-value">{format_compact_metric(total_views)}</div>
                    </div>
                </div>
                <div class="overview-stat-card">
                    <div class="overview-stat-icon icon-engagement"><i class="fa-solid fa-chart-line"></i></div>
                    <div>
                        <div class="overview-stat-label">Tổng tương tác</div>
                        <div class="overview-stat-value">{format_compact_metric(total_engagement)}</div>
                    </div>
                </div>
                <div class="overview-stat-card">
                    <div class="overview-stat-icon icon-creator"><i class="fa-solid fa-user-group"></i></div>
                    <div>
                        <div class="overview-stat-label">Tổng creators</div>
                        <div class="overview-stat-value">{format_metric_number(total_creators)}</div>
                    </div>
                </div>
            </div>

            <div class="overview-section-title">Chiến dịch đang diễn ra</div>
            <div class="overview-campaign-card">
                <div class="flex flex-col gap-6">
                    <div>
                        <h3 class="overview-campaign-title">Chiến dịch - {html.escape(featured_campaign_name)}</h3>
                        <div class="overview-campaign-meta">
                            <span class="overview-campaign-pill {status_chip_class}">{status_chip_text}</span>
                            <span class="overview-campaign-pill overview-campaign-pill-secondary">{html.escape(platforms_text)}</span>
                            <span class="overview-campaign-start">Bắt đầu {started_text}</span>
                        </div>
                    </div>
                </div>

                <div class="overview-campaign-metrics">
                    <div class="overview-campaign-metric metric-posts">
                        <div class="overview-campaign-metric-label">Bài đăng</div>
                        <div class="overview-campaign-metric-value">{format_metric_number(featured_campaign["posts"])}</div>
                    </div>
                    <div class="overview-campaign-metric metric-views">
                        <div class="overview-campaign-metric-label">Lượt xem</div>
                        <div class="overview-campaign-metric-value">{format_metric_number(featured_campaign["views"])}</div>
                    </div>
                    <div class="overview-campaign-metric metric-engagement">
                        <div class="overview-campaign-metric-label">Tương tác</div>
                        <div class="overview-campaign-metric-value">{format_metric_number(featured_campaign["engagement"])}</div>
                    </div>
                    <div class="overview-campaign-metric metric-creators">
                        <div class="overview-campaign-metric-label">Creators</div>
                        <div class="overview-campaign-metric-value">{format_metric_number(featured_campaign_creators)}</div>
                    </div>
                </div>
                {overview_error_html}
            </div>
        </div>
    </section>
    """

def build_posts_panel_html(sheet=None):
    if not ACTIVE_SHEET_ID:
        return """
        <section id="bai-dang" data-dashboard-section="bai-dang" class="dashboard-section dashboard-panel posts-board rounded-[2rem] p-6 md:p-8 mb-6 border border-white/5">
            <div class="posts-empty-card rounded-[1.5rem] p-8 text-center">
                <div class="text-sm uppercase tracking-[0.32em] text-slate-500 font-bold">Bài đăng</div>
                <div class="mt-3 text-2xl font-black text-slate-100">Chưa có dữ liệu để hiển thị</div>
                <p class="mt-2 text-sm text-slate-400">Hãy chọn Google Sheet hợp lệ rồi tải lại trang để xem danh sách bài đăng.</p>
            </div>
        </section>
        """

    try:
        gc = get_gspread_client()
        spreadsheet = gc.open_by_key(ACTIVE_SHEET_ID)
        worksheets = spreadsheet.worksheets()
    except Exception as exc:
        return f"""
        <section id="bai-dang" data-dashboard-section="bai-dang" class="dashboard-section dashboard-panel posts-board rounded-[2rem] p-6 md:p-8 mb-6 border border-white/5">
            <div class="posts-empty-card rounded-[1.5rem] p-8 text-center">
                <div class="text-sm uppercase tracking-[0.32em] text-slate-500 font-bold">Bài đăng</div>
                <div class="mt-3 text-2xl font-black text-slate-100">Không tải được danh sách bài đăng</div>
                <p class="mt-2 text-sm text-slate-400">{html.escape(str(exc))}</p>
            </div>
        </section>
        """

    datasets = []
    for tab_index, ws in enumerate(worksheets):
        dataset = collect_posts_dataset_for_worksheet(sheet if sheet is not None and ws.title == ACTIVE_SHEET_NAME else ws, tab_index)
        datasets.append(dataset)

    if not datasets:
        return """
        <section id="bai-dang" data-dashboard-section="bai-dang" class="dashboard-section dashboard-panel posts-board rounded-[2rem] p-6 md:p-8 mb-6 border border-white/5">
            <div class="posts-empty-card rounded-[1.5rem] p-8 text-center">
                <div class="text-sm uppercase tracking-[0.32em] text-slate-500 font-bold">Bài đăng</div>
                <div class="mt-3 text-2xl font-black text-slate-100">Spreadsheet chưa có tab nào để hiển thị</div>
            </div>
        </section>
        """

    active_sheet_slug = next(
        (item["sheet_slug"] for item in datasets if item["sheet_title"] == ACTIVE_SHEET_NAME),
        datasets[0]["sheet_slug"],
    )
    active_sheet_title = next(
        (item["sheet_title"] for item in datasets if item["sheet_slug"] == active_sheet_slug),
        datasets[0]["sheet_title"],
    )
    active_total_posts = next(
        (item["total_posts"] for item in datasets if item["sheet_slug"] == active_sheet_slug),
        datasets[0]["total_posts"],
    )
    schedule_selected_count = len(normalize_schedule_targets(schedule_targets, ACTIVE_SHEET_ID))
    spreadsheet_snapshot_url = build_snapshot_url(ACTIVE_SHEET_ID, ACTIVE_SHEET_GID)

    summary_cards_html = []
    detail_panels_html = []
    for dataset in datasets:
        safe_sheet_title = html.escape(dataset["sheet_title"])
        is_active = dataset["sheet_slug"] == active_sheet_slug
        card_class = " is-active" if is_active else ""
        error_html = (
            f'<div class="posts-sheet-card-error">{html.escape(shorten_text(dataset["error"], 88))}</div>'
            if dataset["error"] else ""
        )
        summary_cards_html.append(
            f"""
            <button type="button" class="posts-sheet-card{card_class}" data-posts-tab-trigger="{dataset["sheet_slug"]}" data-posts-tab-title="{safe_sheet_title}">
                <div class="posts-sheet-card-head">
                    <div>
                        <div class="posts-sheet-card-kicker">Tab sheet</div>
                        <div class="posts-sheet-card-title">{safe_sheet_title}</div>
                        <div class="posts-sheet-card-meta">{format_metric_number(dataset["creator_count"])} creator • {format_metric_number(dataset["campaign_count"])} campaign</div>
                    </div>
                    <div class="posts-sheet-card-badge">{format_metric_number(dataset["total_posts"])} bài</div>
                </div>
                <div class="posts-sheet-card-stats">
                    <div class="posts-sheet-card-stat"><span>View</span><strong>{format_compact_metric(dataset["total_views"])}</strong></div>
                    <div class="posts-sheet-card-stat"><span>Engage</span><strong>{format_compact_metric(dataset["total_engagement"])}</strong></div>
                    <div class="posts-sheet-card-stat"><span>TikTok</span><strong>{format_metric_number(dataset["platform_counts"]["tiktok"])}</strong></div>
                </div>
                {error_html}
            </button>
            """
        )

        filter_definitions = [
            ("all", "Tất cả", dataset["total_posts"]),
            ("tiktok", "TikTok", dataset["platform_counts"]["tiktok"]),
            ("facebook", "Facebook", dataset["platform_counts"]["facebook"]),
            ("instagram", "Instagram", dataset["platform_counts"]["instagram"]),
            ("youtube", "YouTube", dataset["platform_counts"]["youtube"]),
            ("khac", "Khác", dataset["platform_counts"]["khac"]),
        ]
        filter_chips_html = "".join(
            [
                f'<button type="button" class="posts-chip{" is-active" if key == "all" else ""}" data-platform="{key}">{label} <span>{count}</span></button>'
                for key, label, count in filter_definitions
                if key == "all" or count > 0
            ]
        )
        panel_active_class = " is-active" if is_active else ""
        detail_panels_html.append(
            f"""
            <div class="posts-tab-panel{panel_active_class}" data-posts-tab-panel="{dataset["sheet_slug"]}" data-posts-tab-title="{safe_sheet_title}" data-posts-platform="all">
                <div class="posts-tab-panel-head">
                    <div>
                        <div class="posts-tab-panel-kicker">Đang xem tab</div>
                        <div class="posts-tab-panel-title">{safe_sheet_title}</div>
                        <div class="posts-tab-panel-sub">Bấm các card phía trên để chuyển nhanh giữa các trang trong spreadsheet hiện tại.</div>
                    </div>
                    <a href="{html.escape(build_snapshot_url(ACTIVE_SHEET_ID, dataset["sheet_gid"]), quote=True)}" target="_blank" rel="noreferrer" class="posts-toolbar-btn">
                        <i class="fa-solid fa-up-right-from-square"></i> Mở tab này
                    </a>
                </div>
                <div class="posts-toolbar rounded-[1.5rem] p-4 md:p-5">
                    <div class="posts-toolbar-row">
                        <label class="posts-search-shell">
                            <i class="fa-solid fa-magnifying-glass text-slate-400"></i>
                            <input type="text" placeholder="Tìm kiếm bài đăng, creator hoặc chiến dịch..." class="posts-search-input posts-search-field" />
                        </label>
                        <div class="posts-toolbar-actions">
                            <button type="button" class="posts-toolbar-btn posts-reset-btn"><i class="fa-solid fa-rotate-left"></i> Đặt lại</button>
                        </div>
                    </div>
                    <div class="posts-filter-row">
                        {filter_chips_html}
                    </div>
                </div>
                <div class="posts-table-shell">
                    <div class="overflow-x-auto">
                        <table class="w-full min-w-[1180px] posts-table">
                            <thead>
                                <tr>
                                    <th class="posts-check-col"><input type="checkbox" class="posts-table-check posts-select-all" data-select-all-posts aria-label="Chọn tất cả" /></th>
                                    <th>Nội dung</th>
                                    <th>Creator</th>
                                    <th>Trạng thái</th>
                                    <th>Ngày quét</th>
                                    <th class="text-right">View</th>
                                    <th class="text-right">Reaction</th>
                                    <th class="text-right">Share</th>
                                    <th class="text-right">Comment</th>
                                    <th class="text-right">Engagement</th>
                                    <th>Chiến dịch</th>
                                </tr>
                            </thead>
                            <tbody>
                                {dataset["rows_html"] if dataset["rows_html"] else '<tr><td colspan="11" class="posts-empty-state">Tab này chưa có link nào hợp lệ để hiển thị.</td></tr>'}
                            </tbody>
                        </table>
                    </div>
                    <div class="posts-empty-state posts-empty-panel hidden">
                        Không có bài đăng nào khớp bộ lọc hiện tại.
                    </div>
                </div>
            </div>
            """
        )

    return f"""
    <section id="bai-dang" data-dashboard-section="bai-dang" class="dashboard-section dashboard-panel posts-board rounded-[2rem] p-6 md:p-8 mb-6 border border-white/5">
        <div class="flex flex-col gap-5">
            <div class="posts-page-head">
                <div>
                    <div class="posts-page-kicker">Bài đăng</div>
                    <h2 class="posts-page-title">Bài đăng</h2>
                    <p class="posts-page-subtitle">Xem tổng theo từng tab trong spreadsheet hiện tại, rồi bấm vào tab muốn xem để mở toàn bộ link và thông tin chi tiết. Đang xem: <span id="posts-active-tab-label" class="text-slate-200 font-bold">{html.escape(active_sheet_title)}</span>.</p>
                </div>
                <div class="posts-page-actions">
                    <div class="posts-counter-pill">
                        <div class="posts-counter-label">Đang hiển thị</div>
                        <div class="posts-counter-value" id="posts-visible-count">{active_total_posts} bài</div>
                    </div>
                    <div class="posts-counter-pill">
                        <div class="posts-counter-label">Đang chọn cho lịch</div>
                        <div class="posts-counter-value" id="schedule-selected-count">{schedule_selected_count} bài</div>
                    </div>
                    <button type="button" id="save-schedule-targets-btn" class="posts-toolbar-btn">
                        <i class="fa-regular fa-calendar-check"></i> Lưu bài cho lịch
                    </button>
                    <button type="button" id="clear-schedule-targets-btn" class="posts-toolbar-btn">
                        <i class="fa-solid fa-eraser"></i> Xóa bài lịch
                    </button>
                </div>
            </div>

            <div class="posts-sheet-summary-shell">
                <div class="posts-sheet-summary-head">
                    <div>
                        <div class="posts-sheet-summary-kicker">Spreadsheet hiện tại</div>
                        <div class="posts-sheet-summary-title">{html.escape(ACTIVE_SHEET_NAME or active_sheet_title)}</div>
                        <div class="posts-sheet-summary-sub">{html.escape(ACTIVE_SHEET_ID)}</div>
                    </div>
                    <a href="{html.escape(spreadsheet_snapshot_url, quote=True)}" target="_blank" rel="noreferrer" class="posts-toolbar-btn">
                        <i class="fa-solid fa-sheet-plastic"></i> Mở Google Sheet
                    </a>
                </div>
                <div class="posts-sheet-summary-grid">
                    {"".join(summary_cards_html)}
                </div>
            </div>

            <div class="posts-tab-panels">
                {"".join(detail_panels_html)}
            </div>
        </div>
    </section>
    """

def build_row_updates(col_map, platform, now, stats):
    row_updates = []
    if col_map.get("date"):
        row_updates.append(("date", col_map["date"], now))
    if col_map.get("caption"):
        row_updates.append(("caption", col_map["caption"], str(stats.get("cap", ""))))
    if col_map.get("view"):
        row_updates.append(("view", col_map["view"], int(stats.get("v", 0))))
    if col_map.get("like"):
        row_updates.append(("like", col_map["like"], int(stats.get("l", 0))))
    if col_map.get("share"):
        row_updates.append(("share", col_map["share"], int(stats.get("s", 0))))
    if col_map.get("comment"):
        row_updates.append(("comment", col_map["comment"], int(stats.get("c", 0))))
    if col_map.get("save") and "save" in stats:
        row_updates.append(("save", col_map["save"], int(stats.get("save", 0))))
    return row_updates

def normalize_cell_value(field, value):
    if field in {"view", "like", "share", "comment", "save"}:
        try:
            return int(str(value).strip())
        except Exception:
            return value
    return value


# --- Xá»­ lÃ½ YouTube ---
def get_youtube_stats(url):
    try:
        if not YOUTUBE_API_KEY:
            return None
        video_id_match = re.search(r"(?:v=|\/)([0-9A-Za-z_-]{11}).*", url)
        if not video_id_match: return None
        youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
        item = youtube.videos().list(part="statistics,snippet", id=video_id_match.group(1)).execute()['items'][0]
        return {
            "v": item['statistics'].get("viewCount", 0),
            "l": item['statistics'].get("likeCount", 0),
            "s": 0, "c": item['statistics'].get("commentCount", 0),
            "cap": item['snippet'].get('title', '')
        }
    except: return None

# --- Xá»­ lÃ½ Äa ná»n táº£ng (Facebook, TikTok, IG) ---
def get_social_stats(url, platform_name, driver=None):
    return fetch_social_stats(url, platform_name, driver=driver, logger=add_log)

def resolve_target_row_index(target, urls, min_row: int = 2):
    fallback_row = parse_start_row_input(str(target.get("row_idx") or ""))
    stored_link = str(target.get("link") or "").strip()
    if fallback_row and 0 < fallback_row <= len(urls):
        current_link = str(urls[fallback_row - 1] or "").strip()
        if not stored_link or current_link == stored_link:
            return fallback_row
    if stored_link:
        for idx, current_link in enumerate(urls, start=1):
            if idx < max(2, min_row):
                continue
            if str(current_link or "").strip() == stored_link:
                return idx
    return fallback_row

# --- Logic quet du lieu ---
def run_scraper_logic(sheet_id: Optional[str] = None, sheet_name: Optional[str] = None, targets=None, source: str = "manual"):
    global current_task, is_finished, is_running, pending_updates
    global run_started_at, run_source
    global schedule_last_run_started_at, schedule_last_run_finished_at, schedule_last_run_duration_seconds
    global schedule_last_run_status, schedule_last_run_source, schedule_last_run_sheet_name
    global schedule_last_run_processed, schedule_last_run_success, schedule_last_run_failed
    social_driver = None
    social_driver_failed = False
    started_at = datetime.now()
    run_started_at = started_at
    run_source = (source or "manual").strip().lower() or "manual"
    run_status = "success"
    processed_count = 0
    success_count = 0
    failed_count = 0
    try:
        normalized_targets = normalize_schedule_targets(targets or [])
        use_target_mode = len(normalized_targets) > 0

        if use_target_mode:
            grouped_targets = {}
            for item in normalized_targets:
                key = (item["sheet_id"], item["sheet_name"])
                grouped_targets.setdefault(key, []).append(item)
            scan_groups = [
                (sheet_id, sheet_name, sorted(items, key=lambda entry: entry["row_idx"]))
                for (sheet_id, sheet_name), items in grouped_targets.items()
            ]
            current_task = f"Chuẩn bị quét {len(normalized_targets)} bài theo lịch"
            add_log(f"Bắt đầu lịch tự động cho {len(normalized_targets)} bài đã chọn")
        else:
            resolved_sheet_id = (sheet_id or ACTIVE_SHEET_ID or "").strip()
            resolved_sheet_name = (sheet_name or ACTIVE_SHEET_NAME or "").strip()
            if not resolved_sheet_id or not resolved_sheet_name:
                raise ValueError("Chưa cài đặt Google Sheet. Vui lòng nhập sheet trước khi chạy.")
            scan_groups = [(resolved_sheet_id, resolved_sheet_name, None)]
            current_task = f"Chuẩn bị quét ({resolved_sheet_name})"

        schedule_last_run_started_at = started_at
        schedule_last_run_finished_at = None
        schedule_last_run_duration_seconds = 0.0
        schedule_last_run_status = "running"
        schedule_last_run_source = run_source
        schedule_last_run_sheet_name = scan_groups[0][1] if len(scan_groups) == 1 else f"{len(scan_groups)} tab"
        schedule_last_run_processed = 0
        schedule_last_run_success = 0
        schedule_last_run_failed = 0
        is_running, is_finished = True, False
        now = datetime.now().strftime("%d/%m/%Y %H:%M")
        for sheet_id, selected_sheet, selected_targets in scan_groups:
            add_log(f"Đang kết nối Google Sheets: {selected_sheet}")
            sheet = get_worksheet(selected_sheet, sheet_id)
            layout = detect_sheet_layout(sheet)
            col_map = apply_column_overrides(layout.get("columns"))
            link_col = col_map.get("link", 4)
            if "link" not in col_map:
                add_log("Không tìm thấy cột 'link', tạm dùng cột D để quét")
            urls = sheet.col_values(link_col)
            header_row = max(1, int(layout.get("header_row") or 1))
            start_row = resolve_effective_start_row(header_row)
            add_log(format_detected_columns_text(layout))

            if selected_targets:
                row_plan = []
                seen_rows = set()
                for target in selected_targets:
                    resolved_row = resolve_target_row_index(target, urls, min_row=start_row)
                    if resolved_row is None or resolved_row < start_row or resolved_row in seen_rows:
                        continue
                    seen_rows.add(resolved_row)
                    row_plan.append((resolved_row, target))
                row_plan.sort(key=lambda item: item[0])
                add_log(f"Tab '{selected_sheet}': quét {len(row_plan)} bài đã chọn")
            else:
                row_plan = [(row_idx, None) for row_idx in range(start_row, len(urls) + 1)]
                add_log(f"Bắt đầu quét từ dòng {start_row} (header ở dòng {header_row})")

            for i, target in row_plan:
                if not is_running:
                    run_status = "stopped"
                    current_task = "Đã dừng thủ công"
                    add_log("Hệ thống đã dừng thủ công.")
                    return
                url = str(urls[i - 1] if i - 1 < len(urls) else "").strip()
                if (not url or "http" not in url) and target:
                    url = str(target.get("link") or "").strip()
                if not url or "http" not in url:
                    continue
                processed_count += 1
                platform = detect_platform(url)
                current_task = f"Dòng {i}: {platform} ({selected_sheet})"
                add_log(f"Đang quét {platform}...")
                if platform == "YouTube":
                    stats = get_youtube_stats(url)
                else:
                    if social_driver is None and not social_driver_failed:
                        try:
                            social_driver = create_selenium_driver(logger=add_log)
                        except Exception as driver_error:
                            social_driver_failed = True
                            add_log(str(driver_error))
                    stats = None if social_driver_failed else get_social_stats(url, platform, driver=social_driver)
                if stats and is_running:
                    row_updates = build_row_updates(col_map, platform, now, stats)
                    set_pending_updates(i, row_updates)
                    for field, col_idx, value in row_updates:
                        value = normalize_cell_value(field, value)
                        sheet.update_cell(i, col_idx, value)
                    add_log(f"Dòng {i}: Cập nhật thành công")
                    success_count += 1
                elif is_running:
                    add_log(f"Dòng {i}: Không lấy được số liệu")
                    failed_count += 1
                time.sleep(max(0.0, ROW_SCAN_DELAY_SECONDS))
        pending_updates = []
        current_task, is_finished, is_running = "HOÀN TẤT", True, False
        add_log("=== ĐÃ QUÉT XONG ===")
    except Exception as e:
        run_status = "error"
        pending_updates = []
        is_running, current_task = False, f"Lỗi: {str(e)[:20]}"
        add_log(f"Lỗi hệ thống: {str(e)}")
    finally:
        finished_at = datetime.now()
        duration_seconds = max(0.0, (finished_at - started_at).total_seconds())
        if run_status == "success" and not is_finished and not is_running:
            run_status = "stopped" if current_task == "Đã dừng thủ công" else "success"
        schedule_last_run_finished_at = finished_at
        schedule_last_run_duration_seconds = duration_seconds
        schedule_last_run_status = run_status
        schedule_last_run_processed = processed_count
        schedule_last_run_success = success_count
        schedule_last_run_failed = failed_count
        push_schedule_run_history(
            {
                "sheet_name": schedule_last_run_sheet_name,
                "started_text": format_datetime_display(schedule_last_run_started_at),
                "finished_text": format_datetime_display(schedule_last_run_finished_at),
                "duration_text": format_duration_display(duration_seconds),
                "source_label": "Tự động" if schedule_last_run_source == "schedule" else "Thủ công",
                "status": run_status,
                "processed": processed_count,
                "success": success_count,
                "failed": failed_count,
            }
        )
        run_started_at = None
        run_source = "idle"
        close_selenium_driver(social_driver)

# --- API & UI ---
@app.on_event("startup")
def on_startup():
    ensure_scheduler_thread()

@app.get("/healthz")
def healthz():
    return {"ok": True, "service": "social-performance"}

@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    current_user = get_current_user(request)
    if current_user:
        return RedirectResponse(url="/", status_code=302)
    logged_out = request.query_params.get("logged_out", "")
    auth_error = request.query_params.get("auth_error", "")
    if logged_out == "1":
        return render_login_page_html(request, "Đã đăng xuất khỏi hệ thống.", "info")
    if auth_error == "expired":
        return render_login_page_html(request, "Phiên đăng nhập đã hết hạn. Vui lòng đăng nhập lại.", "warning")
    return render_login_page_html(request)

@app.get("/logout")
def logout(request: Request):
    response = RedirectResponse(url="/login?logged_out=1", status_code=302)
    clear_session_cookie(response)
    return response

@app.post("/auth/request-otp")
async def auth_request_otp(request: Request):
    cleanup_auth_runtime()
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    email = normalize_email_address((payload or {}).get("email", ""))
    if not email or "@" not in email:
        return JSONResponse({"ok": False, "message": "Email không hợp lệ.", "level": "error"}, status_code=400)
    user = get_policy_user(email)
    if not user:
        return JSONResponse({"ok": False, "message": "Email này chưa có trong whitelist/access policy.", "level": "error"}, status_code=403)
    last_payload = OTP_STORE.get(email, {})
    now_ts = time.time()
    if last_payload and now_ts - float(last_payload.get("sent_at", 0) or 0) < OTP_REQUEST_COOLDOWN_SECONDS:
        wait_seconds = max(1, OTP_REQUEST_COOLDOWN_SECONDS - int(now_ts - float(last_payload.get("sent_at", 0) or 0)))
        return JSONResponse({"ok": False, "message": f"OTP vừa được gửi. Chờ thêm {wait_seconds}s rồi thử lại.", "level": "warning"}, status_code=429)

    otp_code = generate_otp_code()
    try:
        send_otp_email(email, otp_code)
    except Exception as exc:
        error_message = describe_smtp_error(exc)
        add_log(f"Gửi OTP thất bại cho {mask_email(email)}: {error_message}")
        return JSONResponse({"ok": False, "message": error_message, "level": "error"}, status_code=500)

    ttl_seconds = int(get_auth_settings().get("otp_ttl_seconds", 300))
    OTP_STORE[email] = {
        "code_hash": hash_otp_code(email, otp_code),
        "expires_at": now_ts + ttl_seconds,
        "sent_at": now_ts,
    }
    add_log(f"Đã gửi OTP cho {mask_email(email)}")
    return {"ok": True, "message": f"Đã gửi OTP 6 số tới {mask_email(email)}.", "level": "success"}

@app.post("/auth/verify-otp")
async def auth_verify_otp(request: Request):
    cleanup_auth_runtime()
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    email = normalize_email_address((payload or {}).get("email", ""))
    otp = str((payload or {}).get("otp", "") or "").strip()
    next_path = str((payload or {}).get("next", "") or "").strip() or "/"
    if not email or "@" not in email:
        return JSONResponse({"ok": False, "message": "Email không hợp lệ.", "level": "error"}, status_code=400)
    if not re.fullmatch(r"\d{6}", otp):
        return JSONResponse({"ok": False, "message": "OTP phải gồm đúng 6 số.", "level": "error"}, status_code=400)

    user = get_policy_user(email)
    if not user:
        return JSONResponse({"ok": False, "message": "Email này không còn trong access policy.", "level": "error"}, status_code=403)

    saved_payload = OTP_STORE.get(email)
    if not saved_payload:
        return JSONResponse({"ok": False, "message": "OTP không tồn tại hoặc đã hết hạn. Hãy gửi lại OTP.", "level": "warning"}, status_code=400)
    if float(saved_payload.get("expires_at", 0) or 0) < time.time():
        OTP_STORE.pop(email, None)
        return JSONResponse({"ok": False, "message": "OTP đã hết hạn. Hãy gửi lại OTP.", "level": "warning"}, status_code=400)
    if hash_otp_code(email, otp) != str(saved_payload.get("code_hash", "")):
        return JSONResponse({"ok": False, "message": "OTP không đúng. Kiểm tra lại mã 6 số.", "level": "error"}, status_code=400)

    OTP_STORE.pop(email, None)
    record_user_login(email)
    response = JSONResponse(
        {
            "ok": True,
            "message": f"Đăng nhập thành công với quyền {user.get('role', 'user')}.",
            "level": "success",
            "redirect_url": next_path,
        }
    )
    set_session_cookie(response, email)
    add_log(f"{mask_email(email)} đăng nhập với quyền {user.get('role', 'user')}")
    return response

@app.post("/admin/save-access-policy")
async def admin_save_access_policy(request: Request):
    current_user, auth_response = require_authenticated_user(request, admin_only=True)
    if auth_response:
        return auth_response
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    users = parse_access_policy_text((payload or {}).get("policy_text", ""))
    if not users:
        return JSONResponse({"ok": False, "message": "Access policy phải có ít nhất 1 email hợp lệ.", "level": "error"}, status_code=400)
    if not any(item.get("role") == "admin" for item in users):
        return JSONResponse({"ok": False, "message": "Access policy cần ít nhất 1 admin.", "level": "error"}, status_code=400)
    settings = get_auth_settings().copy()
    settings["users"] = users
    persist_auth_settings(settings)
    add_log(f"{mask_email(current_user['email'])} đã cập nhật access policy ({len(users)} email)")
    return {"ok": True, "message": "Đã lưu access policy.", "level": "success", "policy_text": build_access_policy_text(users)}

@app.post("/admin/save-users")
async def admin_save_users(request: Request):
    current_user, auth_response = require_authenticated_user(request, admin_only=True)
    if auth_response:
        return auth_response
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    users = normalize_user_items((payload or {}).get("users", []))
    if not users:
        return JSONResponse({"ok": False, "message": "Danh sách nhân viên phải có ít nhất 1 email hợp lệ.", "level": "error"}, status_code=400)
    if not any(item.get("role") == "admin" for item in users):
        return JSONResponse({"ok": False, "message": "Danh sách nhân viên cần ít nhất 1 admin.", "level": "error"}, status_code=400)
    settings = get_auth_settings().copy()
    settings["users"] = users
    persist_auth_settings(settings)
    add_log(f"{mask_email(current_user['email'])} đã cập nhật danh sách nhân viên ({len(users)} email)")
    return {
        "ok": True,
        "message": "Đã lưu danh sách nhân viên.",
        "level": "success",
        "users": get_employee_records(),
    }

@app.post("/admin/save-mail-config")
async def admin_save_mail_config(request: Request):
    current_user, auth_response = require_authenticated_user(request, admin_only=True)
    if auth_response:
        return auth_response
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    try:
        smtp_port = int(str((payload or {}).get("smtp_port", "") or "587").strip() or "587")
    except Exception:
        return JSONResponse({"ok": False, "message": "SMTP Port không hợp lệ.", "level": "error"}, status_code=400)

    mail_config = {
        "smtp_host": str((payload or {}).get("smtp_host", "") or "").strip(),
        "smtp_port": smtp_port,
        "smtp_user": str((payload or {}).get("smtp_user", "") or "").strip(),
        "smtp_password": str((payload or {}).get("smtp_password", "") or ""),
        "smtp_from_email": normalize_email_address((payload or {}).get("smtp_from_email", "")),
        "smtp_from_name": str((payload or {}).get("smtp_from_name", "") or "Social Monitor").strip() or "Social Monitor",
        "use_tls": bool((payload or {}).get("use_tls", True)),
        "use_ssl": bool((payload or {}).get("use_ssl", False)),
    }
    if not mail_config["smtp_host"] or not mail_config["smtp_from_email"]:
        return JSONResponse({"ok": False, "message": "SMTP Host và From Email là bắt buộc.", "level": "error"}, status_code=400)
    settings = get_auth_settings().copy()
    settings["mail"] = mail_config
    persist_auth_settings(settings)
    add_log(f"{mask_email(current_user['email'])} đã cập nhật cấu hình mail OTP")
    return {"ok": True, "message": "Đã lưu cấu hình mail OTP.", "level": "success"}

@app.get("/start")
def start_task(request: Request, background_tasks: BackgroundTasks, sheet_name: Optional[str] = None, sheet_url: Optional[str] = None):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return auth_response
    global is_running, is_finished, pending_updates, current_task
    if is_running:
        add_log("Hệ thống đang chạy, không thể bắt đầu thêm")
        if is_fetch_request(request):
            return build_ui_json_response("Hệ thống đang chạy rồi, chưa thể bắt đầu thêm.", level="warning", ok=False)
        return HTMLResponse("<html><script>window.location.href='/';</script></html>")

    requested_sheet_id = extract_sheet_id(sheet_url or "") if sheet_url else None
    if sheet_url and not requested_sheet_id:
        add_log("Link/ID spreadsheet không hợp lệ")
        if is_fetch_request(request):
            return build_ui_json_response("Link/ID spreadsheet không hợp lệ.", level="error", ok=False)
        return HTMLResponse("<html><script>window.location.href='/?sheet_error=4';</script></html>")

    requested_sheet = (sheet_name or "").strip()
    if requested_sheet and not requested_sheet_id and not ACTIVE_SHEET_ID:
        add_log("Bạn cần nhập link hoặc ID spreadsheet ở lần đầu tiên")
        if is_fetch_request(request):
            return build_ui_json_response("Bạn cần nhập link hoặc ID spreadsheet ở lần đầu tiên.", level="warning", ok=False)
        return HTMLResponse("<html><script>window.location.href='/?sheet_error=4';</script></html>")

    if requested_sheet_id and not requested_sheet:
        if requested_sheet_id == ACTIVE_SHEET_ID and ACTIVE_SHEET_NAME:
            requested_sheet = ACTIVE_SHEET_NAME
        else:
            add_log("Cần nhập tên tab sheet cùng với link/ID spreadsheet")
            if is_fetch_request(request):
                return build_ui_json_response("Thiếu tên tab sheet. Hãy chọn tab rồi bấm Bắt đầu.", level="warning", ok=False)
            return HTMLResponse("<html><script>window.location.href='/?sheet_error=3';</script></html>")

    if requested_sheet:
        try:
            set_active_sheet(requested_sheet, requested_sheet_id)
        except Exception as e:
            add_log(f"Không tìm thấy sheet: {requested_sheet} ({str(e)[:60]})")
            if is_fetch_request(request):
                return build_ui_json_response("Không tìm thấy tab sheet. Kiểm tra lại tên tab và quyền truy cập.", level="error", ok=False)
            return HTMLResponse("<html><script>window.location.href='/?sheet_error=1';</script></html>")

    if not ACTIVE_SHEET_ID or not ACTIVE_SHEET_NAME:
        add_log("Chưa cài đặt sheet. Vui lòng nhập link/ID và tên tab trước khi chạy.")
        if is_fetch_request(request):
            return build_ui_json_response("Chưa có sheet hợp lệ. Nhập link/ID và tên tab rồi bấm Bắt đầu.", level="warning", ok=False)
        return HTMLResponse("<html><script>window.location.href='/?sheet_error=3';</script></html>")

    is_running = True
    is_finished = False
    pending_updates = []
    current_task = f"Chuẩn bị quét ({ACTIVE_SHEET_NAME})"
    add_log(f"Bắt đầu quét lại dữ liệu trên tab '{ACTIVE_SHEET_NAME}'")
    background_tasks.add_task(run_scraper_logic)
    if is_fetch_request(request):
        return build_ui_json_response(
            "Đã bắt đầu quét dữ liệu.",
            level="success",
            extra={"column_config": build_column_config_payload()},
        )
    return HTMLResponse("<html><script>window.location.href='/';</script></html>")


@app.get("/set-sheet")
def set_sheet(request: Request, sheet_name: str = "", sheet_url: str = ""):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return auth_response
    global is_running
    requested_sheet = sheet_name.strip()
    if is_running:
        add_log("Không thể nhập sheet khi hệ thống đang chạy")
        if is_fetch_request(request):
            return build_ui_json_response("Đang quét dữ liệu nên chưa nhập sheet được.", level="warning", ok=False)
        return HTMLResponse("<html><script>window.location.href='/?sheet_error=2';</script></html>")
    if not requested_sheet:
        if is_fetch_request(request):
            return build_ui_json_response("Thiếu tên tab sheet. Vui lòng nhập tên tab.", level="warning", ok=False)
        return HTMLResponse("<html><script>window.location.href='/?sheet_error=3';</script></html>")
    requested_sheet_id = extract_sheet_id(sheet_url) if sheet_url else None
    if not requested_sheet_id and not ACTIVE_SHEET_ID:
        add_log("Bạn cần nhập link hoặc ID spreadsheet ở lần đầu tiên")
        if is_fetch_request(request):
            return build_ui_json_response("Bạn cần nhập link hoặc ID spreadsheet ở lần đầu tiên.", level="warning", ok=False)
        return HTMLResponse("<html><script>window.location.href='/?sheet_error=4';</script></html>")
    if sheet_url and not requested_sheet_id:
        if is_fetch_request(request):
            return build_ui_json_response("Link/ID spreadsheet không hợp lệ.", level="error", ok=False)
        return HTMLResponse("<html><script>window.location.href='/?sheet_error=4';</script></html>")
    try:
        set_active_sheet(requested_sheet, requested_sheet_id)
        ws = get_worksheet(ACTIVE_SHEET_NAME, ACTIVE_SHEET_ID)
        detected_text = format_detected_columns_text(detect_sheet_layout(ws))
        add_log(detected_text)
        if is_fetch_request(request):
            return build_ui_json_response(
                f"Đã nhập sheet thành công. {detected_text}",
                level="success",
                extra={
                    "column_config": build_column_config_payload(ws),
                    "posts_html": build_posts_panel_html(ws),
                },
            )
        return HTMLResponse("<html><script>window.location.href='/?sheet_ok=1';</script></html>")
    except Exception as e:
        add_log(f"Không tìm thấy sheet: {requested_sheet} ({str(e)[:60]})")
        if is_fetch_request(request):
            return build_ui_json_response("Không tìm thấy tab sheet. Kiểm tra lại tên tab và quyền truy cập.", level="error", ok=False)
        return HTMLResponse("<html><script>window.location.href='/?sheet_error=1';</script></html>")


@app.post("/set-schedule-targets")
async def set_schedule_targets(request: Request):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return auth_response
    global schedule_targets
    try:
        payload = await request.json()
    except Exception:
        return build_ui_json_response("Không đọc được danh sách bài đã chọn.", level="error", ok=False)

    raw_targets = payload.get("targets", []) if isinstance(payload, dict) else []
    fallback_sheet_id = payload.get("sheet_id") if isinstance(payload, dict) else None
    schedule_targets = normalize_schedule_targets(raw_targets, fallback_sheet_id)
    ensure_scheduler_thread()

    if schedule_targets:
        add_log(f"Đã lưu {len(schedule_targets)} bài cho lịch tự động")
        return build_ui_json_response(
            f"Đã áp dụng {len(schedule_targets)} bài cho lịch tự động.",
            level="success",
            extra={"schedule_config": build_schedule_config_payload()},
        )

    add_log("Đã xóa chọn lọc bài cho lịch tự động, quay về chạy toàn tab.")
    return build_ui_json_response(
        "Đã xóa chọn lọc bài. Lịch tự động sẽ quay về chạy toàn tab đang dùng.",
        level="info",
        extra={"schedule_config": build_schedule_config_payload()},
    )

@app.get("/set-schedule")
def set_schedule(request: Request, mode: str = "off", at: str = "09:00", weekday: int = 0, monthday: int = 1, monthdate: str = "", enddate: str = ""):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return auth_response
    global schedule_mode, schedule_time, schedule_weekday, schedule_monthday, schedule_end_date, last_schedule_run_key
    global schedule_sheet_id, schedule_sheet_name, schedule_sheet_gid, schedule_targets
    mode = (mode or "off").strip().lower()
    if mode not in ["off", "daily", "weekly", "monthly"]:
        if is_fetch_request(request):
            return build_ui_json_response("Chế độ lịch không hợp lệ.", level="error", ok=False)
        return HTMLResponse("<html><script>window.location.href='/?schedule_error=1';</script></html>")

    if not ACTIVE_SHEET_ID or not ACTIVE_SHEET_NAME:
        if is_fetch_request(request):
            return build_ui_json_response("Hãy chọn sheet/tab ở phần Cấu hình trước khi lưu lịch tự động.", level="warning", ok=False)
        return HTMLResponse("<html><script>window.location.href='/?schedule_error=1';</script></html>")

    hour, minute = parse_schedule_time(at)
    safe_time = f"{hour:02d}:{minute:02d}"
    safe_weekday = max(0, min(6, int(weekday)))
    try:
        if (monthdate or "").strip():
            picked_date = datetime.strptime(monthdate.strip(), "%Y-%m-%d")
            safe_monthday = max(1, min(28, picked_date.day))
        else:
            safe_monthday = max(1, min(28, int(monthday)))
    except Exception:
        if is_fetch_request(request):
            return build_ui_json_response("Ngày chạy không hợp lệ.", level="error", ok=False)
        return HTMLResponse("<html><script>window.location.href='/?schedule_error=2';</script></html>")
    try:
        safe_end_date = parse_schedule_date(enddate)
    except Exception:
        if is_fetch_request(request):
            return build_ui_json_response("Ngày kết thúc không hợp lệ. Dùng định dạng YYYY-MM-DD.", level="error", ok=False)
        return HTMLResponse("<html><script>window.location.href='/?schedule_error=3';</script></html>")

    schedule_mode = mode
    schedule_time = safe_time
    schedule_weekday = safe_weekday
    schedule_monthday = safe_monthday
    schedule_end_date = safe_end_date
    schedule_sheet_id = ACTIVE_SHEET_ID
    schedule_sheet_name = ACTIVE_SHEET_NAME
    schedule_sheet_gid = ACTIVE_SHEET_GID or "0"
    schedule_targets = [
        item
        for item in normalize_schedule_targets(schedule_targets, ACTIVE_SHEET_ID)
        if (item.get("sheet_id") or "") == (ACTIVE_SHEET_ID or "")
    ]
    last_schedule_run_key = ""
    ensure_scheduler_thread()
    target_suffix = f" • {len(schedule_targets)} bài đã chọn" if schedule_targets else " • chạy toàn tab"
    add_log(f"Cập nhật lịch cho tab '{schedule_sheet_name}': {schedule_label()}{target_suffix}")
    if is_fetch_request(request):
        return build_ui_json_response(
            "Đã cập nhật lịch tự động.",
            level="success",
            extra={"schedule_config": build_schedule_config_payload()},
        )
    return HTMLResponse("<html><script>window.location.href='/?schedule_ok=1';</script></html>")

@app.get("/set-columns")
def set_columns(request: Request, link: str = "", campaign: str = "", view: str = "", like: str = "", share: str = "", comment: str = "", save: str = "", start_row: Optional[str] = None):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return auth_response
    global COLUMN_OVERRIDES, START_ROW
    if is_running:
        if is_fetch_request(request):
            return build_ui_json_response("Đang quét dữ liệu nên chưa lưu cấu hình được.", level="warning", ok=False)
        return HTMLResponse("<html><script>window.location.href='/?sheet_error=2';</script></html>")

    candidates = {"link": link, "campaign": campaign, "view": view, "like": like, "share": share, "comment": comment, "save": save}
    parsed = {}
    for field, val in candidates.items():
        if (val or "").strip() == "":
            parsed[field] = None
            continue
        col_idx = parse_column_input(val)
        if not col_idx:
            if is_fetch_request(request):
                return build_ui_json_response("Cột không hợp lệ. Nhập dạng A/B/C... hoặc số 1/2/3...", level="error", ok=False)
            return HTMLResponse("<html><script>window.location.href='/?col_error=1';</script></html>")
        parsed[field] = col_idx

    if start_row is not None:
        parsed_start_row = parse_start_row_input(start_row)
        if parsed_start_row is None:
            if is_fetch_request(request):
                return build_ui_json_response("Dòng bắt đầu không hợp lệ. Nhập số từ 2 trở lên.", level="error", ok=False)
            return HTMLResponse("<html><script>window.location.href='/?col_error=2';</script></html>")
        START_ROW = parsed_start_row

    COLUMN_OVERRIDES = parsed
    add_log(
        "Cập nhật cấu hình nhập liệu: "
        + ", ".join([f"{k.upper()}={col_to_a1(v) if v else 'AUTO'}" for k, v in COLUMN_OVERRIDES.items()])
        + f", START_ROW={START_ROW}"
    )
    if is_fetch_request(request):
        return build_ui_json_response(
            "Đã lưu cấu hình nhập liệu thành công.",
            level="success",
            extra={"column_config": build_column_config_payload()},
        )
    return HTMLResponse("<html><script>window.location.href='/?col_ok=1';</script></html>")


@app.get("/stop")
def stop_task(request: Request):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return auth_response
    global is_running, pending_updates, current_task
    is_running = False
    pending_updates = []
    current_task = "Đã dừng thủ công"
    add_log("Đã gửi lệnh dừng quét.")
    if is_fetch_request(request):
        return build_ui_json_response("Đã dừng quét dữ liệu.", level="info")
    return HTMLResponse("<html><script>window.location.href='/';</script></html>")


@app.get("/download")
def download_excel(request: Request):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return auth_response
    if not ACTIVE_SHEET_ID or not ACTIVE_SHEET_NAME:
        return RedirectResponse(url="/?download_error=2", status_code=302)
    try:
        sheet = get_worksheet(ACTIVE_SHEET_NAME)
        records, _, _ = get_sheet_records(sheet)
        df = pd.DataFrame(records)
        path = "Social_Export.xlsx"
        df.to_excel(path, index=False, engine="openpyxl")
        safe_sheet_name = re.sub(r"[^A-Za-z0-9_-]+", "_", ACTIVE_SHEET_NAME).strip("_") or "sheet"
        return FileResponse(path, filename=f"Data_{safe_sheet_name}_{datetime.now().strftime('%H%M')}.xlsx")
    except ImportError as e:
        add_log(f"Lỗi export Excel: thiếu thư viện ({str(e)})")
        return RedirectResponse(url="/?download_error=1", status_code=302)
    except Exception as e:
        add_log(f"Lỗi export Excel: {str(e)}")
        return RedirectResponse(url="/?download_error=3", status_code=302)

@app.get("/download-all")
def download_excel_all(request: Request):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return auth_response
    if not ACTIVE_SHEET_ID:
        return RedirectResponse(url="/?download_error=2", status_code=302)
    try:
        gc = get_gspread_client()
        spreadsheet = gc.open_by_key(ACTIVE_SHEET_ID)
        path = "Social_Export_All.xlsx"
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            for ws in spreadsheet.worksheets():
                records, _, _ = get_sheet_records(ws)
                df = pd.DataFrame(records)
                safe_ws_name = (ws.title or "Sheet")[:31]
                df.to_excel(writer, sheet_name=safe_ws_name, index=False)
        return FileResponse(path, filename=f"Data_all_tabs_{datetime.now().strftime('%H%M')}.xlsx")
    except ImportError as e:
        add_log(f"Lỗi export all tabs: thiếu thư viện ({str(e)})")
        return RedirectResponse(url="/?download_error=1", status_code=302)
    except Exception as e:
        add_log(f"Lỗi export all tabs: {str(e)}")
        return RedirectResponse(url="/?download_error=4", status_code=302)


@app.get("/status")
def status(request: Request):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return auth_response
    return build_ui_state()


@app.get("/sheet-tabs")
def sheet_tabs(request: Request, sheet_url: str = ""):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return auth_response
    requested_sheet_id = extract_sheet_id(sheet_url or "")
    if not requested_sheet_id:
        return {
            "ok": False,
            "sheet_id": "",
            "tabs": [],
            "message": "Dán link Google Sheet hoặc Sheet ID hợp lệ để tải danh sách tab.",
        }
    try:
        tabs = list_spreadsheet_tabs(requested_sheet_id)
        return {
            "ok": True,
            "sheet_id": requested_sheet_id,
            "tabs": tabs,
            "message": f"Tìm thấy {len(tabs)} tab trong spreadsheet.",
        }
    except Exception as exc:
        return {
            "ok": False,
            "sheet_id": requested_sheet_id,
            "tabs": [],
            "message": f"Không tải được danh sách tab: {str(exc)}",
        }


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return auth_response
    log_html = build_log_html()
    snapshot_url = build_snapshot_url()
    schedule_text = schedule_label()
    mode_selected = {
        "off": "selected" if schedule_mode == "off" else "",
        "daily": "selected" if schedule_mode == "daily" else "",
        "weekly": "selected" if schedule_mode == "weekly" else "",
        "monthly": "selected" if schedule_mode == "monthly" else "",
    }
    weekday_options = "".join(
        [
            f'<option value="{idx}" {"selected" if schedule_weekday == idx else ""}>{name}</option>'
            for idx, name in enumerate(WEEKDAY_NAMES)
        ]
    )

    sheet_error = request.query_params.get("sheet_error", "")
    sheet_ok = request.query_params.get("sheet_ok", "")
    schedule_error = request.query_params.get("schedule_error", "")
    today = datetime.now()
    schedule_date_value = f"{today.year:04d}-{today.month:02d}-{max(1, min(28, schedule_monthday)):02d}"
    schedule_end_value = schedule_end_date
    schedule_config = build_schedule_config_payload()
    schedule_tracking = build_schedule_tracking_payload()
    ws = None
    if ACTIVE_SHEET_ID and ACTIVE_SHEET_NAME:
        try:
            ws = get_worksheet(ACTIVE_SHEET_NAME)
        except Exception:
            pass
    column_config = build_column_config_payload(ws)
    manual_mode = column_config["manual_mode"]
    metric_manual_inputs = column_config["manual_inputs"]
    metric_detected_inputs = column_config["detected_inputs"]
    metric_input_values = column_config["input_values"]
    metric_input_sources = column_config["input_sources"]
    current_user_email = html.escape(current_user.get("email", ""))
    current_user_role = html.escape(current_user.get("role_label", "User"))
    status_payload = build_status_payload()
    employee_panel_html = build_employee_panel_html(current_user)
    employee_sidebar_link = (
        '<a href="#nhan-vien" class="sidebar-link" data-nav-link="nhan-vien"><span class="sidebar-link-icon"><i class="fa-solid fa-users"></i></span><span>Nhân viên</span></a>'
        if current_user.get("role") == "admin"
        else ""
    )
    metric_cols_html = f"""
        <div class="bg-black/20 rounded-3xl p-6 mb-6 border border-white/5">
            <div class="mb-3 text-sm font-bold text-slate-500 uppercase">
                <span>Thiết lập quét</span>
            </div>
            <div class="grid grid-cols-1 xl:grid-cols-[minmax(0,1.16fr)_minmax(380px,0.84fr)] gap-4 items-start">
                <div class="space-y-4">
            <div class="bg-slate-950/40 rounded-2xl p-4 border border-white/10 mb-5">
                <div class="mb-4">
                    <div>
                        <div class="text-base font-black text-slate-100">Sheet và cấu hình quét</div>
                    </div>
                </div>
                <div class="space-y-4">
                    <div>
                        <div class="text-sm text-slate-300 mb-2">Sheet đang dùng: <span class="text-emerald-400 font-bold" data-active-sheet-name>{ACTIVE_SHEET_NAME or 'Chưa cài đặt'}</span></div>
                        <div class="text-xs text-slate-400">Spreadsheet ID: <span class="text-blue-300" data-active-sheet-id>{ACTIVE_SHEET_ID or 'Chưa cài đặt'}</span></div>
                        <div class="text-[11px] text-slate-500 mt-2">Nhập sheet trước để nhận diện cột. Sau đó bạn thao tác ở cột trái, còn phần log sẽ luôn nằm ở cột phải để theo dõi xuyên suốt.</div>
                    </div>
                    <form id="set-sheet-form" action="/set-sheet" method="get" class="flex flex-col gap-3">
                        <input id="sheet-url-input" name="sheet_url" value="{snapshot_url}" placeholder="Nhập link Google Sheet hoặc Sheet ID" class="w-full bg-slate-900 text-slate-100 rounded-xl px-4 py-3 border border-white/10 outline-none focus:border-blue-400" />
                        <div class="grid grid-cols-1 md:grid-cols-[minmax(0,1fr)_220px] gap-3">
                            <input id="sheet-name-input" name="sheet_name" value="{ACTIVE_SHEET_NAME}" list="sheet-name-options" autocomplete="off" placeholder="Nhập tên tab sheet" class="w-full bg-slate-900 text-slate-100 rounded-xl px-4 py-3 border border-white/10 outline-none focus:border-blue-400" />
                            <button type="submit" class="w-full px-6 py-3 bg-slate-700 hover:bg-slate-600 text-white rounded-xl font-black uppercase text-sm shadow-sm shadow-slate-900/10">Nhập Sheet</button>
                        </div>
                        <datalist id="sheet-name-options"></datalist>
                        <div id="sheet-tabs-state" class="text-xs text-slate-500">Dán link Google Sheet để hiện danh sách tab có trong file.</div>
                        <div id="sheet-tabs-list" class="sheet-tabs-list hidden"></div>
                    </form>
                </div>
            </div>
            <div class="flex justify-between items-center mb-3 text-sm font-bold text-slate-500 uppercase">
                <span>Cột nhập liệu</span><span class="text-cyan-300 font-black text-lg" data-config-mode>{manual_mode}</span>
            </div>
            <div class="grid grid-cols-1 md:grid-cols-2 gap-3 mb-4">
                <div>
                    <div class="flex items-center justify-between gap-3 mb-2">
                        <label class="block text-xs text-slate-400 uppercase tracking-wider">Cột Link</label>
                        <span class="text-[11px] text-slate-500 font-black uppercase tracking-[0.18em]" data-column-source="link">{metric_input_sources["link"]}</span>
                    </div>
                    <input name="link" form="set-columns-form" data-column-input="link" data-detected-value="{metric_detected_inputs['link']}" data-manual-value="{metric_manual_inputs['link']}" value="{metric_input_values['link']}" placeholder="VD: D hoặc 4" class="w-full bg-slate-900 text-slate-100 rounded-xl px-4 py-3 border border-white/10 outline-none focus:border-cyan-400" />
                </div>
                <div>
                    <div class="flex items-center justify-between gap-3 mb-2">
                        <label class="block text-xs text-slate-400 uppercase tracking-wider">Cột Chiến dịch</label>
                        <span class="text-[11px] text-slate-500 font-black uppercase tracking-[0.18em]" data-column-source="campaign">{metric_input_sources["campaign"]}</span>
                    </div>
                    <input name="campaign" form="set-columns-form" data-column-input="campaign" data-detected-value="{metric_detected_inputs['campaign']}" data-manual-value="{metric_manual_inputs['campaign']}" value="{metric_input_values['campaign']}" placeholder="VD: C hoặc 3" class="w-full bg-slate-900 text-slate-100 rounded-xl px-4 py-3 border border-white/10 outline-none focus:border-cyan-400" />
                    <p class="mt-1 text-[11px] text-slate-500">Chỉ đọc để phân loại trên web, không ghi vào sheet khi quét.</p>
                </div>
                <div>
                    <div class="flex items-center justify-between gap-3 mb-2">
                        <label class="block text-xs text-slate-400 uppercase tracking-wider">Cột View</label>
                        <span class="text-[11px] text-slate-500 font-black uppercase tracking-[0.18em]" data-column-source="view">{metric_input_sources["view"]}</span>
                    </div>
                    <input name="view" form="set-columns-form" data-column-input="view" data-detected-value="{metric_detected_inputs['view']}" data-manual-value="{metric_manual_inputs['view']}" value="{metric_input_values['view']}" placeholder="VD: E hoặc 5" class="w-full bg-slate-900 text-slate-100 rounded-xl px-4 py-3 border border-white/10 outline-none focus:border-cyan-400" />
                </div>
                <div>
                    <div class="flex items-center justify-between gap-3 mb-2">
                        <label class="block text-xs text-slate-400 uppercase tracking-wider">Cột Like</label>
                        <span class="text-[11px] text-slate-500 font-black uppercase tracking-[0.18em]" data-column-source="like">{metric_input_sources["like"]}</span>
                    </div>
                    <input name="like" form="set-columns-form" data-column-input="like" data-detected-value="{metric_detected_inputs['like']}" data-manual-value="{metric_manual_inputs['like']}" value="{metric_input_values['like']}" placeholder="VD: F hoặc 6" class="w-full bg-slate-900 text-slate-100 rounded-xl px-4 py-3 border border-white/10 outline-none focus:border-cyan-400" />
                </div>
                <div>
                    <div class="flex items-center justify-between gap-3 mb-2">
                        <label class="block text-xs text-slate-400 uppercase tracking-wider">Cột Share</label>
                        <span class="text-[11px] text-slate-500 font-black uppercase tracking-[0.18em]" data-column-source="share">{metric_input_sources["share"]}</span>
                    </div>
                    <input name="share" form="set-columns-form" data-column-input="share" data-detected-value="{metric_detected_inputs['share']}" data-manual-value="{metric_manual_inputs['share']}" value="{metric_input_values['share']}" placeholder="VD: G hoặc 7" class="w-full bg-slate-900 text-slate-100 rounded-xl px-4 py-3 border border-white/10 outline-none focus:border-cyan-400" />
                </div>
                <div>
                    <div class="flex items-center justify-between gap-3 mb-2">
                        <label class="block text-xs text-slate-400 uppercase tracking-wider">Cột Comment</label>
                        <span class="text-[11px] text-slate-500 font-black uppercase tracking-[0.18em]" data-column-source="comment">{metric_input_sources["comment"]}</span>
                    </div>
                    <input name="comment" form="set-columns-form" data-column-input="comment" data-detected-value="{metric_detected_inputs['comment']}" data-manual-value="{metric_manual_inputs['comment']}" value="{metric_input_values['comment']}" placeholder="VD: H hoặc 8" class="w-full bg-slate-900 text-slate-100 rounded-xl px-4 py-3 border border-white/10 outline-none focus:border-cyan-400" />
                </div>
                <div>
                    <div class="flex items-center justify-between gap-3 mb-2">
                        <label class="block text-xs text-slate-400 uppercase tracking-wider">Cột Save</label>
                        <span class="text-[11px] text-slate-500 font-black uppercase tracking-[0.18em]" data-column-source="save">{metric_input_sources["save"]}</span>
                    </div>
                    <input name="save" form="set-columns-form" data-column-input="save" data-detected-value="{metric_detected_inputs['save']}" data-manual-value="{metric_manual_inputs['save']}" value="{metric_input_values['save']}" placeholder="VD: I hoặc 9" class="w-full bg-slate-900 text-slate-100 rounded-xl px-4 py-3 border border-white/10 outline-none focus:border-cyan-400" />
                </div>
                <div>
                    <div class="flex items-center justify-between gap-3 mb-2">
                        <label class="block text-xs text-slate-400 uppercase tracking-wider">Dòng bắt đầu</label>
                        <span class="text-[11px] text-slate-500 font-black uppercase tracking-[0.18em]">ĐANG DÙNG</span>
                    </div>
                    <input name="start_row" form="set-columns-form" value="{START_ROW}" inputmode="numeric" placeholder="VD: 2" class="w-full bg-slate-900 text-slate-100 rounded-xl px-4 py-3 border border-white/10 outline-none focus:border-cyan-400" />
                </div>
            </div>
            <form id="set-columns-form" action="/set-columns" method="get" class="mb-4">
                <button type="submit" class="w-full py-3 bg-slate-700 hover:bg-slate-600 text-white rounded-xl font-black uppercase text-sm shadow-sm shadow-slate-900/10">Lưu cấu hình nhập liệu</button>
            </form>
            <div class="bg-slate-900/55 rounded-2xl p-4 border border-white/10">
                <div class="flex justify-between items-center mb-2 text-xs font-bold text-slate-500 uppercase">
                    <span>Quét dữ liệu</span><span class="text-blue-300">Thao tác nhanh</span>
                </div>
                <div class="text-xs uppercase tracking-[0.22em] font-bold text-slate-500 mb-2">Tiến trình hiện tại</div>
                <div id="current-task" class="text-lg font-black text-slate-100">{status_payload["current_task"]}</div>
                <div class="w-full bg-slate-800/80 rounded-full h-3 overflow-hidden mt-4 mb-4">
                    <div id="progress-bar" class="bg-blue-500 h-full transition-all duration-1000" style="width: {status_payload["progress_width"]}"></div>
                </div>
                <div id="primary-action">{status_payload["primary_action_html"]}</div>
            </div>
                </div>
                <div class="bg-slate-950/40 rounded-2xl p-4 border border-white/10 xl:sticky xl:top-24">
                    <div class="flex justify-between items-center mb-3 text-sm font-bold text-slate-500 uppercase">
                        <span>Nhật ký hệ thống</span><button type="submit" form="set-sheet-form" class="log-save-btn">Lưu thông tin sheet</button>
                    </div>
                    <div id="log-section" class="bg-black/40 rounded-2xl p-5 h-[36rem] overflow-y-auto border border-white/5 shadow-inner font-mono italic text-sm">
                        {log_html}
                    </div>
                </div>
            </div>
        </div>
    """
    overview_html = build_overview_panel_html(ws, snapshot_url, status_payload, schedule_text)
    posts_html = build_posts_panel_html(ws)
    return f"""
    <!DOCTYPE html>
    <html lang="vi">
    <head>
        <meta charset="UTF-8"><title>Social Scraper v2.2</title>
        <script>
            (() => {{
                try {{
                    const savedTheme = localStorage.getItem("dashboard_theme");
                    const normalizedTheme = savedTheme === "light" || savedTheme === "dark"
                        ? savedTheme
                        : "light";
                    document.documentElement.dataset.theme = normalizedTheme;
                }} catch (_) {{
                    document.documentElement.dataset.theme = "dark";
                }}
            }})();
        </script>
        <script src="https://cdn.tailwindcss.com"></script>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
        <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/flatpickr/dist/flatpickr.min.css">
        <script src="https://cdn.jsdelivr.net/npm/flatpickr"></script>
        <script src="https://cdn.jsdelivr.net/npm/flatpickr/dist/l10n/vn.js"></script>
        <style>
            .date-shell {{
                display: flex;
                gap: 10px;
                padding: 6px;
                border-radius: 14px;
                border: 1px solid rgba(255, 255, 255, 0.12);
                background: linear-gradient(145deg, rgba(15, 23, 42, 0.92), rgba(10, 15, 30, 0.92));
                box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.04);
            }}
            .date-shell input {{
                border: 0 !important;
                background: transparent !important;
                padding: 10px 12px !important;
            }}
            .date-picker-btn {{
                width: 44px;
                min-width: 44px;
                border-radius: 10px;
                border: 1px solid rgba(34, 211, 238, 0.35);
                background: linear-gradient(160deg, rgba(14, 116, 144, 0.35), rgba(6, 182, 212, 0.12));
                color: #67e8f9;
                transition: all 0.2s ease;
            }}
            .date-picker-btn:hover {{
                transform: translateY(-1px);
                border-color: rgba(34, 211, 238, 0.7);
                box-shadow: 0 8px 18px rgba(6, 182, 212, 0.2);
            }}
            .schedule-history-item {{
                display: flex;
                align-items: flex-start;
                justify-content: space-between;
                gap: 14px;
                padding: 14px 16px;
                border-radius: 16px;
                border: 1px solid rgba(255, 255, 255, 0.08);
                background: rgba(15, 23, 42, 0.58);
            }}
            .schedule-history-title {{
                font-size: 14px;
                font-weight: 900;
                color: #f8fafc;
            }}
            .schedule-history-meta {{
                margin-top: 4px;
                font-size: 12px;
                color: #94a3b8;
            }}
            .schedule-history-status {{
                font-size: 12px;
                font-weight: 900;
                text-transform: uppercase;
                letter-spacing: 0.12em;
            }}
            .schedule-history-empty {{
                padding: 16px;
                border-radius: 16px;
                border: 1px dashed rgba(148, 163, 184, 0.18);
                background: rgba(15, 23, 42, 0.32);
                color: #94a3b8;
                font-size: 13px;
            }}
            .schedule-target-item {{
                padding: 14px 16px;
                border-radius: 16px;
                border: 1px solid rgba(148, 163, 184, 0.12);
                background: rgba(15, 23, 42, 0.48);
            }}
            .schedule-target-top {{
                display: flex;
                align-items: center;
                justify-content: space-between;
                gap: 12px;
            }}
            .schedule-target-link {{
                color: #f8fafc;
                font-size: 14px;
                font-weight: 800;
                line-height: 1.45;
                text-decoration: none;
                word-break: break-word;
            }}
            .schedule-target-link:hover {{
                color: #bfdbfe;
            }}
            .schedule-target-pill {{
                display: inline-flex;
                align-items: center;
                justify-content: center;
                padding: 7px 10px;
                border-radius: 999px;
                background: rgba(37, 99, 235, 0.14);
                border: 1px solid rgba(96, 165, 250, 0.2);
                color: #dbeafe;
                font-size: 11px;
                font-weight: 900;
                text-transform: uppercase;
                letter-spacing: 0.08em;
                white-space: nowrap;
            }}
            .schedule-target-meta {{
                margin-top: 8px;
                font-size: 12px;
                color: #94a3b8;
                line-height: 1.6;
            }}
            .schedule-target-empty {{
                padding: 16px;
                border-radius: 16px;
                border: 1px dashed rgba(148, 163, 184, 0.18);
                background: rgba(15, 23, 42, 0.24);
                color: #94a3b8;
                font-size: 13px;
            }}
            .sheet-tabs-list {{
                display: flex;
                flex-wrap: wrap;
                gap: 10px;
            }}
            .sheet-tab-chip {{
                display: inline-flex;
                align-items: center;
                gap: 8px;
                padding: 10px 14px;
                border-radius: 999px;
                background: rgba(15, 23, 42, 0.72);
                border: 1px solid rgba(148, 163, 184, 0.16);
                color: #cbd5e1;
                font-size: 13px;
                font-weight: 800;
                transition: all 0.18s ease;
            }}
            .sheet-tab-chip:hover {{
                background: rgba(37, 99, 235, 0.16);
                border-color: rgba(96, 165, 250, 0.28);
                color: #ffffff;
            }}
            .sheet-tab-chip.is-active {{
                background: rgba(16, 185, 129, 0.16);
                border-color: rgba(52, 211, 153, 0.26);
                color: #a7f3d0;
            }}
            .flatpickr-calendar {{
                background: #0b1222;
                border: 1px solid rgba(148, 163, 184, 0.25);
                box-shadow: 0 20px 55px rgba(2, 6, 23, 0.55);
                border-radius: 14px;
                color: #e2e8f0;
            }}
            .flatpickr-months .flatpickr-month,
            .flatpickr-current-month .flatpickr-monthDropdown-months,
            .flatpickr-current-month input.cur-year {{
                color: #e2e8f0;
                fill: #e2e8f0;
            }}
            .flatpickr-weekday {{
                color: #93c5fd;
                font-weight: 700;
            }}
            .flatpickr-day {{
                color: #cbd5e1;
                border-radius: 9px;
            }}
            .flatpickr-day:hover {{
                background: rgba(6, 182, 212, 0.2);
                border-color: rgba(34, 211, 238, 0.2);
            }}
            .flatpickr-day.selected,
            .flatpickr-day.startRange,
            .flatpickr-day.endRange {{
                background: linear-gradient(145deg, #0ea5e9, #06b6d4);
                border-color: #06b6d4;
                color: #ffffff;
                box-shadow: 0 6px 16px rgba(14, 165, 233, 0.35);
            }}
            .flatpickr-day.today {{
                border-color: rgba(96, 165, 250, 0.9);
            }}
            .flatpickr-day.schedule-weekday-match:not(.selected):not(.startRange):not(.endRange) {{
                background: rgba(6, 182, 212, 0.18);
                border-color: rgba(34, 211, 238, 0.9);
                color: #a5f3fc;
                box-shadow: inset 0 0 0 1px rgba(34, 211, 238, 0.34), 0 0 0 1px rgba(8, 145, 178, 0.18);
            }}
            .flatpickr-day.nextMonthDay,
            .flatpickr-day.prevMonthDay {{
                color: #64748b;
            }}
            .overview-shell {{
                display: grid;
                gap: 18px;
            }}
            .overview-header {{
                display: flex;
                align-items: flex-start;
                justify-content: space-between;
                gap: 20px;
                padding: 2px 2px 4px;
            }}
            .overview-kicker {{
                font-size: 13px;
                font-weight: 900;
                letter-spacing: 0.12em;
                color: #f8fafc;
            }}
            .overview-title {{
                margin-top: 8px;
                font-size: clamp(2rem, 4vw, 3rem);
                line-height: 1.04;
                font-weight: 900;
                letter-spacing: -0.04em;
                color: #f8fafc;
            }}
            .overview-subtitle {{
                margin-top: 10px;
                max-width: 740px;
                font-size: 15px;
                line-height: 1.65;
                color: #94a3b8;
            }}
            .overview-actions {{
                display: flex;
                align-items: center;
                justify-content: flex-end;
                flex-wrap: wrap;
                gap: 12px;
            }}
            .overview-action-pill {{
                display: inline-flex;
                align-items: center;
                gap: 10px;
                padding: 12px 16px;
                border-radius: 14px;
                background: rgba(30, 41, 59, 0.7);
                border: 1px solid rgba(148, 163, 184, 0.12);
                color: #dbe2ee;
                font-size: 14px;
                font-weight: 800;
                text-decoration: none;
            }}
            .overview-action-pill-live {{
                color: #22c55e;
                border-color: rgba(34, 197, 94, 0.24);
                box-shadow: inset 0 0 0 1px rgba(34, 197, 94, 0.12);
            }}
            .overview-action-link:hover {{
                background: rgba(30, 41, 59, 0.94);
                color: #ffffff;
            }}
            .overview-stat-grid {{
                display: grid;
                grid-template-columns: repeat(3, minmax(0, 1fr));
                gap: 16px;
            }}
            .overview-stat-card {{
                display: flex;
                align-items: center;
                gap: 16px;
                min-height: 104px;
                padding: 18px 20px;
                border-radius: 22px;
                background: linear-gradient(180deg, rgba(23, 31, 48, 0.9), rgba(28, 37, 58, 0.88));
                border: 1px solid rgba(148, 163, 184, 0.12);
                box-shadow: 0 12px 28px rgba(15, 23, 42, 0.18);
            }}
            .overview-stat-icon {{
                width: 54px;
                height: 54px;
                border-radius: 18px;
                display: inline-flex;
                align-items: center;
                justify-content: center;
                font-size: 21px;
                flex: 0 0 auto;
            }}
            .icon-campaign {{
                color: #fb923c;
                background: rgba(251, 146, 60, 0.14);
            }}
            .icon-post {{
                color: #a5b4fc;
                background: rgba(129, 140, 248, 0.14);
            }}
            .icon-view {{
                color: #60a5fa;
                background: rgba(59, 130, 246, 0.14);
            }}
            .icon-engagement {{
                color: #38bdf8;
                background: rgba(56, 189, 248, 0.14);
            }}
            .icon-creator {{
                color: #c084fc;
                background: rgba(168, 85, 247, 0.14);
            }}
            .overview-stat-label {{
                font-size: 12px;
                font-weight: 800;
                letter-spacing: 0.1em;
                text-transform: uppercase;
                color: #93a4bf;
            }}
            .overview-stat-value {{
                margin-top: 10px;
                font-size: clamp(1.7rem, 3vw, 2.2rem);
                line-height: 1;
                font-weight: 900;
                color: #f8fafc;
            }}
            .overview-section-title {{
                margin-top: 8px;
                font-size: 15px;
                font-weight: 900;
                letter-spacing: 0.04em;
                text-transform: uppercase;
                color: #f8fafc;
            }}
            .overview-campaign-card {{
                padding: 20px;
                border-radius: 24px;
                background: linear-gradient(180deg, rgba(20, 28, 45, 0.92), rgba(26, 35, 55, 0.9));
                border: 1px solid rgba(148, 163, 184, 0.12);
                box-shadow: 0 14px 32px rgba(15, 23, 42, 0.18);
            }}
            .overview-campaign-title {{
                font-size: clamp(1.35rem, 2vw, 1.8rem);
                font-weight: 900;
                color: #f8fafc;
            }}
            .overview-campaign-meta {{
                display: flex;
                align-items: center;
                flex-wrap: wrap;
                gap: 10px;
                margin-top: 12px;
            }}
            .overview-campaign-pill {{
                display: inline-flex;
                align-items: center;
                padding: 8px 12px;
                border-radius: 999px;
                font-size: 13px;
                font-weight: 800;
            }}
            .overview-status-live {{
                background: rgba(37, 99, 235, 0.16);
                color: #93c5fd;
                border: 1px solid rgba(96, 165, 250, 0.24);
            }}
            .overview-status-done {{
                background: rgba(16, 185, 129, 0.14);
                color: #6ee7b7;
                border: 1px solid rgba(110, 231, 183, 0.22);
            }}
            .overview-status-waiting {{
                background: rgba(148, 163, 184, 0.14);
                color: #cbd5e1;
                border: 1px solid rgba(203, 213, 225, 0.18);
            }}
            .overview-campaign-pill-secondary {{
                background: rgba(16, 185, 129, 0.12);
                color: #99f6e4;
                border: 1px solid rgba(45, 212, 191, 0.16);
            }}
            .overview-campaign-start {{
                font-size: 13px;
                color: #94a3b8;
                font-weight: 700;
            }}
            .overview-progress-box {{
                min-width: min(100%, 340px);
                padding: 16px 18px;
                border-radius: 20px;
                background: rgba(15, 23, 42, 0.72);
                border: 1px solid rgba(148, 163, 184, 0.12);
            }}
            .overview-progress-label {{
                font-size: 11px;
                font-weight: 800;
                letter-spacing: 0.18em;
                text-transform: uppercase;
                color: #64748b;
            }}
            .overview-progress-value {{
                margin-top: 10px;
                font-size: 22px;
                font-weight: 900;
                color: #f8fafc;
            }}
            .overview-campaign-metrics {{
                display: grid;
                grid-template-columns: repeat(4, minmax(0, 1fr));
                gap: 14px;
                margin-top: 18px;
            }}
            .overview-campaign-metric {{
                padding: 16px 18px;
                border-radius: 18px;
                border: 1px solid rgba(148, 163, 184, 0.1);
            }}
            .metric-posts {{
                background: rgba(99, 102, 241, 0.16);
            }}
            .metric-views {{
                background: rgba(71, 85, 105, 0.28);
            }}
            .metric-engagement {{
                background: rgba(14, 116, 144, 0.22);
            }}
            .metric-creators {{
                background: rgba(88, 28, 135, 0.24);
            }}
            .overview-campaign-metric-label {{
                font-size: 12px;
                font-weight: 800;
                letter-spacing: 0.08em;
                text-transform: uppercase;
                color: #bfd2ea;
            }}
            .overview-campaign-metric-value {{
                margin-top: 12px;
                font-size: clamp(1.5rem, 2.2vw, 2rem);
                line-height: 1;
                font-weight: 900;
                color: #f8fafc;
            }}
            .overview-control-card {{
                padding: 20px;
                border-radius: 24px;
                background: linear-gradient(180deg, rgba(9, 16, 32, 0.98), rgba(12, 24, 45, 0.94));
                border: 1px solid rgba(148, 163, 184, 0.12);
            }}
            .overview-control-header {{
                display: flex;
                align-items: flex-start;
                justify-content: space-between;
                gap: 18px;
                margin-bottom: 16px;
            }}
            .overview-control-title {{
                font-size: 15px;
                font-weight: 900;
                color: #f8fafc;
            }}
            .overview-control-subtitle {{
                margin-top: 6px;
                font-size: 13px;
                line-height: 1.55;
                color: #94a3b8;
            }}
            .posts-board {{
                background: transparent;
            }}
            .posts-page-head {{
                display: flex;
                align-items: flex-start;
                justify-content: space-between;
                gap: 20px;
            }}
            .posts-page-actions {{
                display: flex;
                align-items: stretch;
                justify-content: flex-end;
                gap: 12px;
                flex-wrap: wrap;
            }}
            .posts-page-kicker {{
                font-size: 13px;
                font-weight: 900;
                letter-spacing: 0.08em;
                color: #f8fafc;
            }}
            .posts-page-title {{
                margin-top: 8px;
                font-size: clamp(2rem, 4vw, 3rem);
                line-height: 1.04;
                font-weight: 900;
                letter-spacing: -0.04em;
                color: #f8fafc;
            }}
            .posts-page-subtitle {{
                margin-top: 8px;
                font-size: 15px;
                color: #94a3b8;
            }}
            .posts-counter-pill {{
                min-width: 180px;
                padding: 16px 18px;
                border-radius: 18px;
                background: linear-gradient(180deg, rgba(23, 31, 48, 0.9), rgba(28, 37, 58, 0.88));
                border: 1px solid rgba(148, 163, 184, 0.12);
                box-shadow: 0 12px 28px rgba(15, 23, 42, 0.18);
            }}
            .posts-counter-label {{
                font-size: 11px;
                font-weight: 800;
                letter-spacing: 0.18em;
                text-transform: uppercase;
                color: #64748b;
            }}
            .posts-counter-value {{
                margin-top: 10px;
                font-size: 24px;
                font-weight: 900;
                color: #f8fafc;
            }}
            .posts-table-row.is-schedule-target {{
                box-shadow: inset 4px 0 0 rgba(34, 197, 94, 0.82);
                background: rgba(15, 118, 110, 0.08);
            }}
            .posts-sheet-summary-shell {{
                display: grid;
                gap: 16px;
            }}
            .posts-sheet-summary-head {{
                display: flex;
                align-items: flex-start;
                justify-content: space-between;
                gap: 16px;
                flex-wrap: wrap;
            }}
            .posts-sheet-summary-kicker {{
                font-size: 11px;
                font-weight: 800;
                letter-spacing: 0.18em;
                text-transform: uppercase;
                color: #64748b;
            }}
            .posts-sheet-summary-title {{
                margin-top: 8px;
                font-size: 22px;
                font-weight: 900;
                color: #f8fafc;
            }}
            .posts-sheet-summary-sub {{
                margin-top: 6px;
                font-size: 12px;
                color: #94a3b8;
                word-break: break-all;
            }}
            .posts-sheet-summary-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(230px, 1fr));
                gap: 14px;
            }}
            .posts-sheet-card {{
                width: 100%;
                text-align: left;
                padding: 18px;
                border-radius: 20px;
                background: linear-gradient(180deg, rgba(23, 31, 48, 0.9), rgba(28, 37, 58, 0.88));
                border: 1px solid rgba(148, 163, 184, 0.12);
                box-shadow: 0 12px 28px rgba(15, 23, 42, 0.18);
                transition: all 0.2s ease;
            }}
            .posts-sheet-card:hover {{
                transform: translateY(-1px);
                border-color: rgba(148, 163, 184, 0.2);
                box-shadow: 0 16px 32px rgba(15, 23, 42, 0.16);
            }}
            .posts-sheet-card.is-active {{
                border-color: rgba(148, 163, 184, 0.24);
                background: linear-gradient(180deg, rgba(51, 65, 85, 0.88), rgba(30, 41, 59, 0.9));
                box-shadow: inset 0 0 0 1px rgba(148, 163, 184, 0.12), 0 16px 32px rgba(15, 23, 42, 0.18);
            }}
            .posts-sheet-card-head {{
                display: flex;
                align-items: flex-start;
                justify-content: space-between;
                gap: 14px;
            }}
            .posts-sheet-card-kicker {{
                font-size: 11px;
                font-weight: 800;
                letter-spacing: 0.18em;
                text-transform: uppercase;
                color: #64748b;
            }}
            .posts-sheet-card-title {{
                margin-top: 8px;
                font-size: 18px;
                font-weight: 900;
                color: #f8fafc;
            }}
            .posts-sheet-card-meta {{
                margin-top: 6px;
                font-size: 12px;
                color: #94a3b8;
            }}
            .posts-sheet-card-badge {{
                padding: 8px 11px;
                border-radius: 999px;
                background: rgba(148, 163, 184, 0.12);
                color: #e2e8f0;
                font-size: 12px;
                font-weight: 900;
                white-space: nowrap;
            }}
            .posts-sheet-card-stats {{
                display: grid;
                grid-template-columns: repeat(3, minmax(0, 1fr));
                gap: 10px;
                margin-top: 16px;
            }}
            .posts-sheet-card-stat {{
                padding: 10px 12px;
                border-radius: 14px;
                background: rgba(30, 41, 59, 0.56);
                border: 1px solid rgba(148, 163, 184, 0.08);
            }}
            .posts-sheet-card-stat span {{
                display: block;
                font-size: 11px;
                color: #94a3b8;
                letter-spacing: 0.08em;
                text-transform: uppercase;
                font-weight: 800;
            }}
            .posts-sheet-card-stat strong {{
                display: block;
                margin-top: 8px;
                font-size: 15px;
                color: #f8fafc;
                font-weight: 900;
            }}
            .posts-sheet-card-error {{
                margin-top: 12px;
                font-size: 12px;
                color: #fca5a5;
                line-height: 1.5;
            }}
            .posts-tab-panels {{
                display: grid;
                gap: 16px;
            }}
            .posts-tab-panel {{
                display: none;
                gap: 14px;
            }}
            .posts-tab-panel.is-active {{
                display: grid;
                animation: dashboardPanelReveal 0.22s ease;
            }}
            .posts-tab-panel-head {{
                display: flex;
                align-items: flex-start;
                justify-content: space-between;
                gap: 16px;
                flex-wrap: wrap;
            }}
            .posts-tab-panel-kicker {{
                font-size: 11px;
                font-weight: 800;
                letter-spacing: 0.18em;
                text-transform: uppercase;
                color: #64748b;
            }}
            .posts-tab-panel-title {{
                margin-top: 8px;
                font-size: 24px;
                font-weight: 900;
                color: #f8fafc;
            }}
            .posts-tab-panel-sub {{
                margin-top: 6px;
                font-size: 13px;
                color: #94a3b8;
            }}
            .posts-empty-card {{
                background: linear-gradient(180deg, rgba(23, 31, 48, 0.9), rgba(28, 37, 58, 0.88));
                border: 1px solid rgba(148, 163, 184, 0.12);
            }}
            .posts-toolbar {{
                background: linear-gradient(180deg, rgba(23, 31, 48, 0.9), rgba(28, 37, 58, 0.88));
                border: 1px solid rgba(148, 163, 184, 0.12);
            }}
            .posts-toolbar-row {{
                display: flex;
                align-items: center;
                justify-content: space-between;
                flex-wrap: wrap;
                gap: 14px;
            }}
            .posts-toolbar-actions {{
                display: flex;
                align-items: center;
                gap: 12px;
            }}
            .posts-toolbar-btn {{
                display: inline-flex;
                align-items: center;
                gap: 10px;
                padding: 12px 15px;
                border-radius: 14px;
                background: rgba(30, 41, 59, 0.72);
                border: 1px solid rgba(148, 163, 184, 0.16);
                color: #e2e8f0;
                font-size: 14px;
                font-weight: 800;
                transition: all 0.2s ease;
            }}
            .posts-toolbar-btn:hover {{
                background: rgba(51, 65, 85, 0.82);
                color: #ffffff;
            }}
            .posts-search-shell {{
                display: flex;
                align-items: center;
                gap: 12px;
                width: 100%;
                max-width: 560px;
                background: rgba(30, 41, 59, 0.72);
                border: 1px solid rgba(148, 163, 184, 0.16);
                border-radius: 14px;
                padding: 12px 16px;
            }}
            .posts-search-input {{
                width: 100%;
                background: transparent;
                border: 0;
                outline: none;
                color: #f8fafc;
            }}
            .posts-search-input::placeholder {{
                color: #64748b;
            }}
            .posts-filter-row {{
                display: flex;
                flex-wrap: wrap;
                gap: 10px;
                margin-top: 14px;
            }}
            .posts-chip {{
                display: inline-flex;
                align-items: center;
                gap: 8px;
                border: 1px solid rgba(148, 163, 184, 0.18);
                border-radius: 999px;
                padding: 9px 14px;
                background: rgba(30, 41, 59, 0.64);
                color: #cbd5e1;
                font-size: 13px;
                font-weight: 700;
                transition: all 0.2s ease;
            }}
            .posts-chip span {{
                color: #64748b;
                font-size: 12px;
            }}
            .posts-chip.is-active {{
                background: rgba(148, 163, 184, 0.16);
                border-color: rgba(148, 163, 184, 0.22);
                color: #ffffff;
                box-shadow: 0 10px 20px rgba(15, 23, 42, 0.12);
            }}
            .posts-chip.is-active span {{
                color: rgba(255, 255, 255, 0.78);
            }}
            .posts-table-shell {{
                background: linear-gradient(180deg, rgba(20, 28, 45, 0.92), rgba(24, 33, 52, 0.92));
                border: 1px solid rgba(148, 163, 184, 0.12);
                border-radius: 24px;
                overflow: hidden;
            }}
            .employee-layout {{
                display: grid;
                grid-template-columns: minmax(0, 1.5fr) minmax(280px, 0.9fr);
                gap: 18px;
            }}
            .employee-summary-grid {{
                display: inline-grid;
                grid-template-columns: repeat(3, minmax(92px, 1fr));
                gap: 12px;
            }}
            .employee-summary-pill {{
                min-width: 0;
                padding: 12px 14px;
                border-radius: 18px;
                background: rgba(30, 41, 59, 0.64);
                border: 1px solid rgba(148, 163, 184, 0.14);
                text-align: left;
            }}
            .employee-summary-pill span {{
                display: block;
                font-size: 11px;
                letter-spacing: 0.12em;
                text-transform: uppercase;
                color: #94a3b8;
                font-weight: 800;
            }}
            .employee-summary-pill strong {{
                display: block;
                margin-top: 8px;
                font-size: 22px;
                line-height: 1;
                font-weight: 900;
                color: #f8fafc;
            }}
            .employee-filter-select,
            .employee-form-input,
            .employee-role-select {{
                width: 100%;
                border-radius: 14px;
                border: 1px solid rgba(148, 163, 184, 0.16);
                background: rgba(15, 23, 42, 0.9);
                color: #f8fafc;
                padding: 11px 14px;
                font-size: 14px;
                outline: none;
            }}
            .employee-filter-select:focus,
            .employee-form-input:focus,
            .employee-role-select:focus {{
                border-color: rgba(56, 189, 248, 0.44);
            }}
            .employee-table {{
                border-collapse: separate;
                border-spacing: 0;
            }}
            .employee-table thead th {{
                position: sticky;
                top: 0;
                z-index: 1;
                background: rgba(51, 65, 85, 0.9);
                padding: 16px 20px;
                color: #cbd5e1;
                font-size: 12px;
                font-weight: 800;
                letter-spacing: 0.12em;
                text-transform: uppercase;
                text-align: left;
                white-space: nowrap;
            }}
            .employee-table tbody td {{
                border-top: 1px solid rgba(148, 163, 184, 0.12);
                padding: 14px 16px;
                color: #e2e8f0;
                vertical-align: top;
            }}
            .employee-row-user {{
                display: flex;
                align-items: center;
                gap: 12px;
                min-width: 220px;
            }}
            .employee-avatar {{
                width: 40px;
                height: 40px;
                border-radius: 14px;
                display: inline-flex;
                align-items: center;
                justify-content: center;
                background: linear-gradient(135deg, rgba(14, 165, 233, 0.16), rgba(59, 130, 246, 0.16));
                color: #bae6fd;
                font-weight: 900;
                flex: 0 0 auto;
            }}
            .employee-email {{
                font-size: 14px;
                font-weight: 800;
                color: #f8fafc;
                word-break: break-all;
            }}
            .employee-meta {{
                margin-top: 3px;
                font-size: 12px;
                color: #94a3b8;
            }}
            .employee-status-badge {{
                display: inline-flex;
                align-items: center;
                justify-content: center;
                padding: 8px 12px;
                border-radius: 999px;
                font-size: 12px;
                font-weight: 800;
                white-space: nowrap;
            }}
            .employee-status-badge.is-verified {{
                background: rgba(16, 185, 129, 0.14);
                color: #6ee7b7;
                border: 1px solid rgba(110, 231, 183, 0.22);
            }}
            .employee-status-badge.is-pending {{
                background: rgba(245, 158, 11, 0.14);
                color: #fcd34d;
                border: 1px solid rgba(252, 211, 77, 0.2);
            }}
            .employee-table-actions {{
                display: flex;
                justify-content: flex-end;
            }}
            .employee-icon-btn {{
                display: inline-flex;
                align-items: center;
                justify-content: center;
                width: 38px;
                height: 38px;
                border-radius: 12px;
                border: 1px solid rgba(148, 163, 184, 0.14);
                background: rgba(30, 41, 59, 0.68);
                color: #e2e8f0;
                transition: all 0.18s ease;
            }}
            .employee-icon-btn:hover {{
                background: rgba(51, 65, 85, 0.9);
            }}
            .employee-icon-btn:disabled {{
                opacity: 0.4;
                cursor: not-allowed;
            }}
            .employee-form-card {{
                padding: 20px;
                border-radius: 24px;
                background: linear-gradient(180deg, rgba(20, 28, 45, 0.92), rgba(24, 33, 52, 0.92));
                border: 1px solid rgba(148, 163, 184, 0.12);
            }}
            .employee-form-title {{
                font-size: 18px;
                font-weight: 900;
                color: #f8fafc;
            }}
            .employee-form-sub {{
                margin-top: 8px;
                font-size: 13px;
                line-height: 1.7;
                color: #94a3b8;
            }}
            .employee-form-grid {{
                display: grid;
                gap: 14px;
                margin-top: 18px;
            }}
            .employee-form-label {{
                display: block;
                margin-bottom: 8px;
                font-size: 12px;
                font-weight: 800;
                letter-spacing: 0.12em;
                text-transform: uppercase;
                color: #94a3b8;
            }}
            .employee-form-actions {{
                display: grid;
                gap: 12px;
                margin-top: 18px;
            }}
            .employee-save-btn {{
                width: 100%;
                padding: 13px 16px;
                border-radius: 14px;
                border: 1px solid rgba(96, 165, 250, 0.28);
                background: linear-gradient(135deg, rgba(14, 165, 233, 0.92), rgba(37, 99, 235, 0.94));
                color: #ffffff;
                font-size: 13px;
                font-weight: 900;
                letter-spacing: 0.08em;
                text-transform: uppercase;
            }}
            .employee-form-note {{
                margin-top: 14px;
                font-size: 12px;
                line-height: 1.7;
                color: #94a3b8;
            }}
            .posts-table {{
                border-collapse: separate;
                border-spacing: 0;
            }}
            .posts-table thead th {{
                position: sticky;
                top: 0;
                z-index: 1;
                background: rgba(51, 65, 85, 0.9);
                padding: 16px 20px;
                color: #cbd5e1;
                font-size: 12px;
                font-weight: 800;
                letter-spacing: 0.12em;
                text-transform: uppercase;
                text-align: left;
                white-space: nowrap;
            }}
            .posts-check-col {{
                width: 44px;
            }}
            .posts-table-row {{
                transition: background 0.2s ease;
            }}
            .posts-table-row:hover {{
                background: rgba(148, 163, 184, 0.06);
            }}
            .posts-table tbody td {{
                border-top: 1px solid rgba(148, 163, 184, 0.12);
            }}
            .posts-cell {{
                padding: 14px 16px;
                vertical-align: top;
                color: #e2e8f0;
            }}
            .posts-cell-check {{
                width: 44px;
                text-align: center;
            }}
            .posts-cell-content {{
                min-width: 200px;
                max-width: 320px;
            }}
            .posts-cell-date {{
                color: #cbd5e1;
                white-space: nowrap;
            }}
            .posts-cell-metric {{
                text-align: right;
                white-space: nowrap;
                font-weight: 800;
                color: #f8fafc;
            }}
            .posts-cell-metric-strong {{
                color: #dbeafe;
            }}
            .posts-cell-campaign {{
                min-width: 180px;
            }}
            .posts-table-check {{
                width: 15px;
                height: 15px;
                accent-color: #3b82f6;
                cursor: pointer;
            }}
            .post-title-link {{
                color: #f8fafc;
                font-weight: 800;
                line-height: 1.45;
                text-decoration: none;
            }}
            .post-title-link:hover {{
                color: #93c5fd;
            }}
            .post-content-meta {{
                margin-top: 8px;
                font-size: 12px;
                color: #64748b;
            }}
            .post-avatar {{
                width: 42px;
                height: 42px;
                border-radius: 999px;
                display: inline-flex;
                align-items: center;
                justify-content: center;
                font-weight: 900;
                color: #ffffff;
                box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.24);
            }}
            .post-creator-name {{
                font-size: 15px;
                font-weight: 800;
                color: #f8fafc;
            }}
            .post-creator-handle {{
                margin-top: 4px;
                font-size: 12px;
                color: #93c5fd;
            }}
            .post-avatar-tiktok {{ background: linear-gradient(135deg, #111827, #ec4899); }}
            .post-avatar-facebook {{ background: linear-gradient(135deg, #1d4ed8, #38bdf8); }}
            .post-avatar-instagram {{ background: linear-gradient(135deg, #f97316, #ec4899); }}
            .post-avatar-youtube {{ background: linear-gradient(135deg, #ef4444, #f97316); }}
            .post-avatar-khac {{ background: linear-gradient(135deg, #475569, #94a3b8); }}
            .post-status-pill {{
                display: inline-flex;
                align-items: center;
                border-radius: 999px;
                padding: 7px 12px;
                font-size: 12px;
                font-weight: 800;
                white-space: nowrap;
            }}
            .posts-status-done {{
                background: rgba(37, 99, 235, 0.16);
                color: #93c5fd;
                border: 1px solid rgba(96, 165, 250, 0.2);
            }}
            .posts-status-pending {{
                background: rgba(245, 158, 11, 0.14);
                color: #fcd34d;
                border: 1px solid rgba(245, 158, 11, 0.2);
            }}
            .posts-campaign-main {{
                font-size: 15px;
                font-weight: 800;
                color: #dbeafe;
            }}
            .posts-campaign-sub {{
                margin-top: 5px;
                font-size: 12px;
                color: #94a3b8;
            }}
            .posts-empty-state {{
                padding: 28px 20px;
                text-align: center;
                font-size: 14px;
                color: #94a3b8;
                border-top: 1px solid rgba(148, 163, 184, 0.12);
            }}
            .link-history-shell {{
                padding: 24px;
                border-radius: 28px;
                background: linear-gradient(180deg, rgba(23, 31, 48, 0.9), rgba(28, 37, 58, 0.88));
                border: 1px solid rgba(148, 163, 184, 0.12);
                box-shadow: 0 14px 32px rgba(15, 23, 42, 0.18);
            }}
            .link-history-header {{
                display: flex;
                align-items: flex-start;
                justify-content: space-between;
                gap: 18px;
                margin-bottom: 18px;
            }}
            .link-history-title-main {{
                font-size: 24px;
                font-weight: 900;
                color: #f8fafc;
            }}
            .link-history-sub {{
                margin-top: 6px;
                font-size: 13px;
                color: #94a3b8;
            }}
            .link-history-summary {{
                display: flex;
                flex-wrap: wrap;
                gap: 10px;
            }}
            .link-history-stat {{
                min-width: 92px;
                padding: 11px 14px;
                border-radius: 16px;
                background: rgba(30, 41, 59, 0.68);
                border: 1px solid rgba(148, 163, 184, 0.12);
            }}
            .link-history-stat span {{
                display: block;
                font-size: 11px;
                letter-spacing: 0.12em;
                text-transform: uppercase;
                color: #64748b;
                font-weight: 800;
            }}
            .link-history-stat strong {{
                display: block;
                margin-top: 8px;
                font-size: 20px;
                color: #f8fafc;
                font-weight: 900;
            }}
            .link-history-table-shell {{
                border-radius: 22px;
                overflow: hidden;
                border: 1px solid rgba(148, 163, 184, 0.12);
                background: rgba(20, 28, 45, 0.78);
            }}
            .link-history-table {{
                border-collapse: collapse;
            }}
            .link-history-table thead th {{
                background: rgba(51, 65, 85, 0.9);
                color: #cbd5e1;
                padding: 14px 16px;
                font-size: 12px;
                font-weight: 800;
                letter-spacing: 0.12em;
                text-transform: uppercase;
                text-align: left;
                white-space: nowrap;
            }}
            .link-history-row {{
                transition: background 0.18s ease;
            }}
            .link-history-row:hover {{
                background: rgba(148, 163, 184, 0.06);
            }}
            .link-history-cell {{
                padding: 14px 16px;
                border-top: 1px solid rgba(148, 163, 184, 0.12);
                vertical-align: top;
                color: #e2e8f0;
            }}
            .link-history-cell-row {{
                font-weight: 900;
                color: #93c5fd;
                white-space: nowrap;
            }}
            .link-history-title {{
                font-size: 14px;
                font-weight: 800;
                color: #f8fafc;
                line-height: 1.45;
            }}
            .link-history-link {{
                display: inline-block;
                margin-top: 6px;
                color: #93c5fd;
                font-size: 12px;
                word-break: break-all;
            }}
            .link-history-link:hover {{
                color: #bfdbfe;
            }}
            .link-history-platform {{
                display: inline-flex;
                align-items: center;
                padding: 8px 12px;
                border-radius: 999px;
                background: rgba(37, 99, 235, 0.14);
                border: 1px solid rgba(96, 165, 250, 0.18);
                color: #dbeafe;
                font-size: 12px;
                font-weight: 800;
            }}
            .link-history-campaign {{
                font-size: 14px;
                font-weight: 800;
                color: #f8fafc;
            }}
            .link-history-sub {{
                margin-top: 6px;
                font-size: 12px;
                color: #94a3b8;
            }}
            .link-history-cell-date {{
                white-space: nowrap;
                color: #cbd5e1;
            }}
            .link-history-empty,
            .link-history-empty-row {{
                padding: 22px;
                text-align: center;
                color: #94a3b8;
                font-size: 14px;
            }}
            html {{
                scroll-behavior: smooth;
            }}
            .dashboard-shell {{
                display: grid;
                grid-template-columns: 280px minmax(0, 1fr);
                gap: 16px;
                align-items: start;
                width: 100%;
                max-width: none;
                min-height: calc(100vh - 16px);
            }}
            .dashboard-sidebar {{
                position: sticky;
                top: 8px;
                min-height: calc(100vh - 16px);
                padding: 22px 18px;
                border-radius: 28px;
                background:
                    radial-gradient(circle at top, rgba(148, 163, 184, 0.1), transparent 36%),
                    linear-gradient(180deg, rgba(18, 24, 37, 0.96), rgba(24, 33, 52, 0.94));
                border: 1px solid rgba(148, 163, 184, 0.12);
                box-shadow: 0 18px 40px rgba(15, 23, 42, 0.2);
            }}
            .sidebar-brand {{
                display: flex;
                align-items: center;
                justify-content: space-between;
                gap: 12px;
                margin-bottom: 22px;
            }}
            .sidebar-brand-title {{
                font-size: 28px;
                font-weight: 900;
                line-height: 1;
                color: #f8fafc;
            }}
            .sidebar-brand-subtitle {{
                margin-top: 6px;
                font-size: 12px;
                letter-spacing: 0.24em;
                text-transform: uppercase;
                color: #64748b;
                font-weight: 800;
            }}
            .sidebar-pulse {{
                width: 42px;
                height: 42px;
                border-radius: 14px;
                display: inline-flex;
                align-items: center;
                justify-content: center;
                background: linear-gradient(135deg, rgba(71, 85, 105, 0.95), rgba(100, 116, 139, 0.92));
                color: #eff6ff;
                box-shadow: 0 12px 24px rgba(51, 65, 85, 0.18);
            }}
            .sidebar-status-card {{
                border-radius: 22px;
                padding: 16px 18px;
                background: rgba(30, 41, 59, 0.64);
                border: 1px solid rgba(148, 163, 184, 0.14);
                margin-bottom: 18px;
            }}
            .sidebar-schedule-card {{
                border-radius: 22px;
                padding: 16px 18px;
                background: rgba(15, 23, 42, 0.72);
                border: 1px solid rgba(148, 163, 184, 0.12);
            }}
            .sidebar-schedule-kicker {{
                font-size: 11px;
                letter-spacing: 0.2em;
                text-transform: uppercase;
                color: #64748b;
                font-weight: 800;
            }}
            .sidebar-schedule-name {{
                margin-top: 10px;
                font-size: 16px;
                line-height: 1.35;
                font-weight: 900;
                color: #f8fafc;
            }}
            .sidebar-schedule-id {{
                margin-top: 6px;
                font-size: 11px;
                line-height: 1.6;
                color: #94a3b8;
                word-break: break-all;
            }}
            .sidebar-schedule-scope {{
                margin-top: 10px;
                font-size: 12px;
                line-height: 1.65;
                color: #cbd5e1;
            }}
            .sidebar-schedule-link {{
                margin-top: 12px;
                display: inline-flex;
                align-items: center;
                justify-content: center;
                width: 100%;
                padding: 10px 12px;
                border-radius: 14px;
                background: rgba(30, 41, 59, 0.84);
                border: 1px solid rgba(148, 163, 184, 0.14);
                color: #f8fafc;
                font-size: 12px;
                font-weight: 900;
                letter-spacing: 0.08em;
                text-transform: uppercase;
                text-decoration: none;
                transition: all 0.18s ease;
            }}
            .sidebar-schedule-link:hover {{
                background: rgba(51, 65, 85, 0.92);
            }}
            .theme-toggle-btn {{
                width: 42px;
                height: 42px;
                display: flex;
                align-items: center;
                justify-content: center;
                gap: 0;
                padding: 0;
                margin-bottom: 0;
                border-radius: 14px;
                border: 1px solid rgba(148, 163, 184, 0.16);
                background: rgba(15, 23, 42, 0.82);
                color: #e2e8f0;
                transition: all 0.2s ease;
            }}
            .theme-toggle-btn:hover {{
                transform: translateY(-1px);
                border-color: rgba(148, 163, 184, 0.22);
                background: rgba(30, 41, 59, 0.9);
            }}
            .theme-toggle-icon {{
                width: 100%;
                height: 100%;
                border-radius: inherit;
                display: inline-flex;
                align-items: center;
                justify-content: center;
                background: transparent;
                color: #f8fafc;
                flex: 0 0 auto;
                box-shadow: none;
            }}
            .theme-toggle-copy {{
                display: none;
            }}
            .theme-toggle-label {{
                font-size: 13px;
                font-weight: 900;
                color: #f8fafc;
                line-height: 1.1;
            }}
            .theme-toggle-meta {{
                display: none;
            }}
            .dashboard-utilitybar {{
                display: flex;
                justify-content: flex-end;
                align-items: center;
                gap: 12px;
                padding: 12px 24px;
                border-bottom: 1px solid rgba(148, 163, 184, 0.1);
                background: rgba(12, 18, 31, 0.72);
            }}
            .utility-userbar {{
                display: inline-flex;
                align-items: center;
                gap: 10px;
                min-width: 0;
            }}
            .utility-user-pill {{
                display: inline-flex;
                align-items: center;
                gap: 10px;
                padding: 8px 12px;
                border-radius: 999px;
                background: rgba(15, 23, 42, 0.82);
                border: 1px solid rgba(148, 163, 184, 0.14);
                color: #e2e8f0;
                min-width: 0;
            }}
            .utility-user-avatar {{
                width: 28px;
                height: 28px;
                border-radius: 999px;
                display: inline-flex;
                align-items: center;
                justify-content: center;
                background: linear-gradient(135deg, rgba(14, 165, 233, 0.9), rgba(59, 130, 246, 0.92));
                color: #eff6ff;
                font-size: 12px;
                font-weight: 900;
                flex: 0 0 auto;
            }}
            .utility-user-copy {{
                min-width: 0;
                display: flex;
                flex-direction: column;
            }}
            .utility-user-email {{
                font-size: 13px;
                font-weight: 800;
                color: #f8fafc;
                line-height: 1.1;
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
                max-width: 240px;
            }}
            .utility-user-role {{
                margin-top: 2px;
                font-size: 11px;
                font-weight: 800;
                letter-spacing: 0.12em;
                text-transform: uppercase;
                color: #94a3b8;
            }}
            .utility-logout {{
                display: inline-flex;
                align-items: center;
                justify-content: center;
                gap: 8px;
                padding: 10px 12px;
                border-radius: 12px;
                background: rgba(15, 23, 42, 0.82);
                border: 1px solid rgba(148, 163, 184, 0.14);
                color: #e2e8f0;
                font-size: 13px;
                font-weight: 800;
                transition: all 0.2s ease;
            }}
            .utility-logout:hover {{
                background: rgba(30, 41, 59, 0.92);
                color: #ffffff;
            }}
            .sidebar-status-label {{
                font-size: 11px;
                letter-spacing: 0.26em;
                text-transform: uppercase;
                color: #64748b;
                font-weight: 800;
            }}
            .sidebar-status-value {{
                margin-top: 10px;
                font-size: 16px;
                font-weight: 900;
                color: #f8fafc;
            }}
            .sidebar-status-meta {{
                margin-top: 6px;
                font-size: 12px;
                color: #94a3b8;
            }}
            .sidebar-nav {{
                display: flex;
                flex-direction: column;
                gap: 8px;
                margin-bottom: 18px;
            }}
            .sidebar-link {{
                display: flex;
                align-items: center;
                gap: 12px;
                padding: 12px 14px;
                border-radius: 16px;
                color: #cbd5e1;
                text-decoration: none;
                font-weight: 700;
                transition: all 0.18s ease;
            }}
            .sidebar-link:hover {{
                background: rgba(51, 65, 85, 0.56);
                color: #ffffff;
            }}
            .sidebar-link.is-active {{
                background: linear-gradient(135deg, rgba(71, 85, 105, 0.52), rgba(100, 116, 139, 0.42));
                color: #ffffff;
                box-shadow: inset 0 0 0 1px rgba(148, 163, 184, 0.16);
            }}
            .sidebar-link-icon {{
                width: 34px;
                height: 34px;
                border-radius: 11px;
                display: inline-flex;
                align-items: center;
                justify-content: center;
                background: rgba(15, 23, 42, 0.84);
                border: 1px solid rgba(148, 163, 184, 0.12);
                color: #93c5fd;
                flex: 0 0 auto;
            }}
            .dashboard-main {{
                min-width: 0;
                min-height: calc(100vh - 16px);
                border-radius: 32px;
                background:
                    radial-gradient(circle at top left, rgba(148, 163, 184, 0.06), transparent 24%),
                    linear-gradient(180deg, rgba(18, 24, 37, 0.94), rgba(24, 33, 52, 0.94));
                border: 1px solid rgba(148, 163, 184, 0.1);
                box-shadow: 0 22px 50px rgba(15, 23, 42, 0.2);
                overflow: hidden;
            }}
            .dashboard-main-inner {{
                padding: 20px 24px 28px;
            }}
            .dashboard-section {{
                scroll-margin-top: 24px;
            }}
            .dashboard-panel {{
                display: none;
            }}
            .dashboard-panel.is-active {{
                display: block;
                animation: dashboardPanelReveal 0.22s ease;
            }}
            .dashboard-section-title {{
                margin-bottom: 14px;
                padding-left: 4px;
                font-size: 12px;
                letter-spacing: 0.28em;
                text-transform: uppercase;
                color: #64748b;
                font-weight: 900;
            }}
            .dashboard-card-title {{
                margin-bottom: 14px;
                font-size: 12px;
                letter-spacing: 0.28em;
                text-transform: uppercase;
                color: #64748b;
                font-weight: 900;
            }}
            .log-save-btn {{
                display: inline-flex;
                align-items: center;
                justify-content: center;
                padding: 10px 14px;
                border-radius: 12px;
                border: 1px solid rgba(96, 165, 250, 0.24);
                background: linear-gradient(135deg, rgba(37, 99, 235, 0.22), rgba(59, 130, 246, 0.16));
                color: #dbeafe;
                font-size: 12px;
                font-weight: 900;
                letter-spacing: 0.08em;
                text-transform: uppercase;
                transition: all 0.18s ease;
            }}
            .log-save-btn:hover {{
                background: linear-gradient(135deg, rgba(37, 99, 235, 0.34), rgba(59, 130, 246, 0.24));
                border-color: rgba(147, 197, 253, 0.38);
                color: #ffffff;
                transform: translateY(-1px);
            }}
            html[data-theme="light"] body {{
                background:
                    radial-gradient(circle at top left, rgba(96, 165, 250, 0.12), transparent 26%),
                    radial-gradient(circle at 85% 12%, rgba(14, 165, 233, 0.08), transparent 22%),
                    linear-gradient(180deg, #eef4fa 0%, #f8fbfd 48%, #edf3f8 100%) !important;
                color: #0f172a !important;
            }}
            html[data-theme="light"] .dashboard-sidebar {{
                background:
                    radial-gradient(circle at top, rgba(125, 211, 252, 0.12), transparent 34%),
                    linear-gradient(180deg, rgba(248, 251, 255, 0.98), rgba(237, 244, 250, 0.96));
                border-color: rgba(191, 219, 254, 0.5);
                box-shadow: 0 18px 40px rgba(148, 163, 184, 0.12);
            }}
            html[data-theme="light"] .dashboard-main {{
                background:
                    radial-gradient(circle at top left, rgba(191, 219, 254, 0.16), transparent 26%),
                    linear-gradient(180deg, rgba(253, 254, 255, 0.98), rgba(242, 247, 251, 0.98));
                border-color: rgba(203, 213, 225, 0.72);
                box-shadow: 0 22px 50px rgba(148, 163, 184, 0.12);
            }}
            html[data-theme="light"] .sidebar-brand-title,
            html[data-theme="light"] .sidebar-status-value,
            html[data-theme="light"] .overview-kicker,
            html[data-theme="light"] .overview-title,
            html[data-theme="light"] .overview-section-title,
            html[data-theme="light"] .overview-stat-value,
            html[data-theme="light"] .overview-campaign-title,
            html[data-theme="light"] .overview-campaign-metric-value,
            html[data-theme="light"] .posts-page-kicker,
            html[data-theme="light"] .posts-page-title,
            html[data-theme="light"] .posts-counter-value,
            html[data-theme="light"] .posts-sheet-summary-title,
            html[data-theme="light"] .posts-sheet-card-title,
            html[data-theme="light"] .posts-sheet-card-stat strong,
            html[data-theme="light"] .posts-tab-panel-title,
            html[data-theme="light"] .employee-summary-pill strong,
            html[data-theme="light"] .employee-form-title,
            html[data-theme="light"] .employee-email,
            html[data-theme="light"] .link-history-title-main,
            html[data-theme="light"] .link-history-title,
            html[data-theme="light"] .schedule-history-title,
            html[data-theme="light"] .post-title-link,
            html[data-theme="light"] .post-creator-name,
            html[data-theme="light"] .posts-cell-metric,
            html[data-theme="light"] .posts-campaign-main,
            html[data-theme="light"] .sidebar-schedule-name,
            html[data-theme="light"] .sidebar-schedule-link,
            html[data-theme="light"] .theme-toggle-label,
            html[data-theme="light"] .theme-toggle-icon,
            html[data-theme="light"] .utility-user-email,
            html[data-theme="light"] .utility-logout {{
                color: #0f172a;
            }}
            html[data-theme="light"] .sidebar-brand-subtitle,
            html[data-theme="light"] .sidebar-status-label,
            html[data-theme="light"] .sidebar-status-meta,
            html[data-theme="light"] .sidebar-schedule-kicker,
            html[data-theme="light"] .sidebar-schedule-id,
            html[data-theme="light"] .sidebar-schedule-scope,
            html[data-theme="light"] .employee-summary-pill span,
            html[data-theme="light"] .employee-form-sub,
            html[data-theme="light"] .employee-form-label,
            html[data-theme="light"] .employee-form-note,
            html[data-theme="light"] .employee-meta,
            html[data-theme="light"] .dashboard-section-title,
            html[data-theme="light"] .dashboard-card-title,
            html[data-theme="light"] .overview-subtitle,
            html[data-theme="light"] .overview-stat-label,
            html[data-theme="light"] .overview-campaign-start,
            html[data-theme="light"] .overview-control-subtitle,
            html[data-theme="light"] .posts-page-subtitle,
            html[data-theme="light"] .posts-counter-label,
            html[data-theme="light"] .posts-sheet-summary-kicker,
            html[data-theme="light"] .posts-sheet-summary-sub,
            html[data-theme="light"] .posts-sheet-card-kicker,
            html[data-theme="light"] .posts-sheet-card-meta,
            html[data-theme="light"] .posts-sheet-card-stat span,
            html[data-theme="light"] .posts-tab-panel-kicker,
            html[data-theme="light"] .posts-tab-panel-sub,
            html[data-theme="light"] .posts-content-meta,
            html[data-theme="light"] .posts-campaign-sub,
            html[data-theme="light"] .link-history-sub,
            html[data-theme="light"] .link-history-stat span,
            html[data-theme="light"] .schedule-history-meta,
            html[data-theme="light"] .theme-toggle-meta,
            html[data-theme="light"] .utility-user-role,
            html[data-theme="light"] .text-slate-400,
            html[data-theme="light"] .text-slate-500 {{
                color: #64748b !important;
            }}
            html[data-theme="light"] .sidebar-status-card,
            html[data-theme="light"] .sidebar-schedule-card,
            html[data-theme="light"] .dashboard-utilitybar,
            html[data-theme="light"] .theme-toggle-btn,
            html[data-theme="light"] .utility-user-pill,
            html[data-theme="light"] .utility-logout,
            html[data-theme="light"] .overview-stat-card,
            html[data-theme="light"] .overview-campaign-card,
            html[data-theme="light"] .posts-counter-pill,
            html[data-theme="light"] .employee-summary-pill,
            html[data-theme="light"] .posts-sheet-card,
            html[data-theme="light"] .posts-sheet-card-stat,
            html[data-theme="light"] .posts-empty-card,
            html[data-theme="light"] .posts-toolbar,
            html[data-theme="light"] .posts-table-shell,
            html[data-theme="light"] .employee-form-card,
            html[data-theme="light"] .link-history-shell,
            html[data-theme="light"] .link-history-table-shell,
            html[data-theme="light"] .link-history-stat,
            html[data-theme="light"] .schedule-history-item,
            html[data-theme="light"] .schedule-history-empty,
            html[data-theme="light"] .schedule-target-item,
            html[data-theme="light"] .schedule-target-empty,
            html[data-theme="light"] .bg-black\\/20,
            html[data-theme="light"] .bg-slate-950\\/40,
            html[data-theme="light"] .bg-slate-900\\/60,
            html[data-theme="light"] .bg-slate-900\\/70,
            html[data-theme="light"] .bg-slate-900\\/55 {{
                background: linear-gradient(180deg, rgba(255, 255, 255, 0.94), rgba(243, 247, 251, 0.96)) !important;
                border-color: rgba(203, 213, 225, 0.7) !important;
                box-shadow: 0 10px 24px rgba(148, 163, 184, 0.08);
            }}
            html[data-theme="light"] .metric-posts {{
                background: rgba(99, 102, 241, 0.1);
            }}
            html[data-theme="light"] .metric-views {{
                background: rgba(148, 163, 184, 0.16);
            }}
            html[data-theme="light"] .metric-engagement {{
                background: rgba(14, 116, 144, 0.1);
            }}
            html[data-theme="light"] .metric-creators {{
                background: rgba(168, 85, 247, 0.1);
            }}
            html[data-theme="light"] .overview-action-pill,
            html[data-theme="light"] .posts-toolbar-btn,
            html[data-theme="light"] .posts-chip,
            html[data-theme="light"] .sheet-tab-chip,
            html[data-theme="light"] .posts-search-shell,
            html[data-theme="light"] .employee-filter-select,
            html[data-theme="light"] .employee-form-input,
            html[data-theme="light"] .employee-role-select,
            html[data-theme="light"] .employee-icon-btn,
            html[data-theme="light"] .date-shell,
            html[data-theme="light"] .sidebar-link-icon,
            html[data-theme="light"] .schedule-target-pill {{
                background: rgba(243, 248, 252, 0.96) !important;
                border-color: rgba(191, 219, 254, 0.46) !important;
                color: #0f172a;
            }}
            html[data-theme="light"] .sidebar-link {{
                color: #334155;
            }}
            html[data-theme="light"] .sidebar-link:hover {{
                background: rgba(226, 232, 240, 0.8);
                color: #0f172a;
            }}
            html[data-theme="light"] .log-save-btn {{
                background: linear-gradient(135deg, rgba(226, 239, 255, 0.98), rgba(211, 227, 253, 0.96));
                border-color: rgba(191, 219, 254, 0.9);
                color: #1d4ed8;
                box-shadow: 0 8px 18px rgba(148, 163, 184, 0.12);
            }}
            html[data-theme="light"] .log-save-btn:hover {{
                background: linear-gradient(135deg, rgba(214, 232, 255, 0.98), rgba(196, 219, 252, 0.96));
                color: #1e3a8a;
                border-color: rgba(96, 165, 250, 0.6);
            }}
            html[data-theme="light"] .sidebar-link.is-active {{
                background: linear-gradient(135deg, rgba(226, 232, 240, 0.82), rgba(241, 245, 249, 0.88));
                color: #0f172a;
                box-shadow: inset 0 0 0 1px rgba(148, 163, 184, 0.18);
            }}
            html[data-theme="light"] .posts-table thead th,
            html[data-theme="light"] .link-history-table thead th {{
                background: rgba(241, 245, 249, 0.96);
                color: #475569;
            }}
            html[data-theme="light"] .posts-cell,
            html[data-theme="light"] .posts-cell-date,
            html[data-theme="light"] .link-history-cell,
            html[data-theme="light"] .link-history-cell-date,
            html[data-theme="light"] .text-slate-100,
            html[data-theme="light"] .text-slate-200,
            html[data-theme="light"] .text-slate-300,
            html[data-theme="light"] .schedule-target-link {{
                color: #0f172a !important;
            }}
            html[data-theme="light"] .schedule-target-meta {{
                color: #64748b !important;
            }}
            html[data-theme="light"] .posts-table-row.is-schedule-target {{
                background: rgba(16, 185, 129, 0.08) !important;
                box-shadow: inset 4px 0 0 rgba(16, 185, 129, 0.55);
            }}
            html[data-theme="light"] .post-title-link:hover,
            html[data-theme="light"] .link-history-link,
            html[data-theme="light"] .post-creator-handle {{
                color: #2563eb;
            }}
            html[data-theme="light"] .link-history-link:hover {{
                color: #1d4ed8;
            }}
            html[data-theme="light"] input:not([type="checkbox"]),
            html[data-theme="light"] select,
            html[data-theme="light"] textarea,
            html[data-theme="light"] .posts-search-input {{
                background: #f4f8fc !important;
                color: #0f172a !important;
                border-color: rgba(191, 219, 254, 0.52) !important;
            }}
            html[data-theme="light"] #status-badge {{
                background: linear-gradient(135deg, rgba(226, 232, 240, 0.96), rgba(203, 213, 225, 0.92)) !important;
                color: #334155 !important;
                border-color: rgba(148, 163, 184, 0.28) !important;
                box-shadow: none !important;
            }}
            html[data-theme="light"] .bg-slate-800\\/80 {{
                background: rgba(203, 213, 225, 0.78) !important;
            }}
            html[data-theme="light"] #log-section,
            html[data-theme="light"] .bg-black\\/40 {{
                background: linear-gradient(180deg, rgba(239, 244, 249, 0.98), rgba(228, 236, 245, 0.98)) !important;
                border-color: rgba(203, 213, 225, 0.8) !important;
                box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.82), inset 0 -1px 0 rgba(148, 163, 184, 0.08) !important;
            }}
            html[data-theme="light"] #current-task {{
                color: #1e293b !important;
            }}
            html[data-theme="light"] .date-shell input {{
                background: transparent !important;
            }}
            html[data-theme="light"] input::placeholder,
            html[data-theme="light"] textarea::placeholder,
            html[data-theme="light"] .posts-search-input::placeholder {{
                color: #94a3b8 !important;
            }}
            html[data-theme="light"] .flatpickr-calendar {{
                background: #ffffff;
                border-color: rgba(148, 163, 184, 0.24);
                box-shadow: 0 20px 55px rgba(148, 163, 184, 0.18);
                color: #0f172a;
            }}
            html[data-theme="light"] .flatpickr-months .flatpickr-month,
            html[data-theme="light"] .flatpickr-current-month .flatpickr-monthDropdown-months,
            html[data-theme="light"] .flatpickr-current-month input.cur-year,
            html[data-theme="light"] .flatpickr-day {{
                color: #0f172a;
                fill: #0f172a;
            }}
            html[data-theme="light"] .flatpickr-day.nextMonthDay,
            html[data-theme="light"] .flatpickr-day.prevMonthDay {{
                color: #94a3b8;
            }}
            @keyframes dashboardPanelReveal {{
                from {{
                    opacity: 0;
                    transform: translateY(6px);
                }}
                to {{
                    opacity: 1;
                    transform: translateY(0);
                }}
            }}
            @media (max-width: 1180px) {{
                .dashboard-shell {{
                    grid-template-columns: 1fr;
                    min-height: auto;
                }}
                .dashboard-sidebar {{
                    position: static;
                    min-height: auto;
                    padding: 18px;
                }}
                .employee-layout {{
                    grid-template-columns: 1fr;
                }}
                .sidebar-nav {{
                    flex-direction: row;
                    flex-wrap: wrap;
                }}
                .sidebar-link {{
                    flex: 1 1 180px;
                }}
                .overview-header {{
                    flex-direction: column;
                }}
                .overview-actions {{
                    justify-content: flex-start;
                }}
            }}
            @media (max-width: 768px) {{
                .dashboard-utilitybar {{
                    padding: 10px 16px;
                    justify-content: space-between;
                    flex-wrap: wrap;
                }}
                .dashboard-main-inner {{
                    padding: 18px;
                }}
                .employee-summary-grid {{
                    width: 100%;
                    grid-template-columns: repeat(3, minmax(0, 1fr));
                }}
                .utility-user-email {{
                    max-width: 160px;
                }}
                .posts-page-head,
                .posts-toolbar-row {{
                    flex-direction: column;
                    align-items: stretch;
                }}
                .posts-page-actions {{
                    justify-content: stretch;
                }}
                .overview-stat-grid,
                .overview-campaign-metrics {{
                    grid-template-columns: 1fr;
                }}
                .overview-control-header {{
                    flex-direction: column;
                }}
            }}
        </style>
        <script>
            document.addEventListener("DOMContentLoaded", () => {{
                const sheetUrlInput = document.getElementById("sheet-url-input");
                const sheetNameInput = document.getElementById("sheet-name-input");
                const setSheetForm = document.querySelector("form[action='/set-sheet']");
                const setColumnsForm = document.getElementById("set-columns-form");
                const sheetTabsState = document.getElementById("sheet-tabs-state");
                const sheetTabsList = document.getElementById("sheet-tabs-list");
                const sheetNameOptions = document.getElementById("sheet-name-options");
                const scheduleForm = document.querySelector("form[action='/set-schedule']");
                const scheduleModeSelect = document.getElementById("schedule-mode-select");
                const weekdaySelect = document.getElementById("schedule-weekday-select");
                const monthDateInput = document.getElementById("schedule-monthdate-input");
                const monthDayInput = document.querySelector("input[name='monthday']");
                const endDateInput = document.getElementById("schedule-enddate-input");
                const monthDateBtn = document.getElementById("monthdate-picker-btn");
                const endDateBtn = document.getElementById("enddate-picker-btn");
                const scheduleMonthdateHelp = document.getElementById("schedule-monthdate-help");
                const scheduleBoundSheetName = document.getElementById("schedule-bound-sheet-name");
                const scheduleBoundSheetId = document.getElementById("schedule-bound-sheet-id");
                const scheduleBoundScope = document.getElementById("schedule-bound-scope");
                const scheduleBoundLink = document.getElementById("schedule-bound-link");
                const scheduleTrackNext = document.getElementById("schedule-track-next");
                const scheduleTrackStarted = document.getElementById("schedule-track-started");
                const scheduleTrackFinished = document.getElementById("schedule-track-finished");
                const scheduleTrackDuration = document.getElementById("schedule-track-duration");
                const scheduleTrackRunning = document.getElementById("schedule-track-running");
                const scheduleTrackStatus = document.getElementById("schedule-track-status");
                const scheduleTrackSource = document.getElementById("schedule-track-source");
                const scheduleTrackSheet = document.getElementById("schedule-track-sheet");
                const scheduleTrackProcessed = document.getElementById("schedule-track-processed");
                const scheduleTrackSuccess = document.getElementById("schedule-track-success");
                const scheduleTrackFailed = document.getElementById("schedule-track-failed");
                const scheduleTrackHistory = document.getElementById("schedule-track-history");
                const scheduleTargetSummary = document.getElementById("schedule-target-summary");
                const scheduleTargetList = document.getElementById("schedule-target-list");
                const themeToggle = document.getElementById("theme-toggle");
                const themeToggleIcon = document.getElementById("theme-toggle-icon");
                const themeToggleLabel = document.getElementById("theme-toggle-label");
                const themeToggleMeta = document.getElementById("theme-toggle-meta");
                const authPolicyText = document.getElementById("auth-policy-text");
                const saveAccessPolicyBtn = document.getElementById("save-access-policy-btn");
                const mailSmtpHost = document.getElementById("mail-smtp-host");
                const mailSmtpPort = document.getElementById("mail-smtp-port");
                const mailSmtpUser = document.getElementById("mail-smtp-user");
                const mailSmtpPassword = document.getElementById("mail-smtp-password");
                const mailFromEmail = document.getElementById("mail-from-email");
                const mailFromName = document.getElementById("mail-from-name");
                const mailUseTls = document.getElementById("mail-use-tls");
                const mailUseSsl = document.getElementById("mail-use-ssl");
                const saveMailConfigBtn = document.getElementById("save-mail-config-btn");
                const employeeUsersData = document.getElementById("employee-users-data");
                const employeeSearchInput = document.getElementById("employee-search-input");
                const employeeRoleFilter = document.getElementById("employee-role-filter");
                const employeeStatusChips = Array.from(document.querySelectorAll(".employee-status-chip"));
                const employeeTableBody = document.getElementById("employee-table-body");
                const employeeEmptyPanel = document.getElementById("employee-empty-panel");
                const employeeEmailInput = document.getElementById("employee-email-input");
                const employeeRoleInput = document.getElementById("employee-role-input");
                const employeeAddBtn = document.getElementById("employee-add-btn");
                const employeeSaveBtn = document.getElementById("employee-save-btn");
                const employeeImportBtn = document.getElementById("employee-import-btn");
                const employeeImportInput = document.getElementById("employee-import-input");
                const employeeResetBtn = document.getElementById("employee-reset-btn");
                const employeeTotalCount = document.getElementById("employee-total-count");
                const employeeVerifiedCount = document.getElementById("employee-verified-count");
                const employeeAdminCount = document.getElementById("employee-admin-count");
                const employeeChipAll = document.getElementById("employee-chip-all");
                const employeeChipPending = document.getElementById("employee-chip-pending");
                const employeeChipVerified = document.getElementById("employee-chip-verified");
                const activeSheetNameEls = Array.from(document.querySelectorAll("[data-active-sheet-name]"));
                const activeSheetIdEls = Array.from(document.querySelectorAll("[data-active-sheet-id]"));
                const configModeEls = Array.from(document.querySelectorAll("[data-config-mode]"));
                const columnDetectedTextEls = Array.from(document.querySelectorAll("[data-column-detected-text]"));
                const columnInputEls = Object.fromEntries(
                    ["link", "campaign", "view", "like", "share", "comment", "save"].map((field) => [
                        field,
                        document.querySelector(`[data-column-input="${{field}}"]`),
                    ])
                );
                const columnSourceEls = Object.fromEntries(
                    ["link", "campaign", "view", "like", "share", "comment", "save"].map((field) => [
                        field,
                        document.querySelector(`[data-column-source="${{field}}"]`),
                    ])
                );
                let monthPicker = null;
                let endPicker = null;
                let sheetTabsRequestId = 0;
                let sheetTabsDebounce = null;
                let employeeUsersState = [];
                let employeeStatusFilter = "all";
                let postsScheduleCount = document.getElementById("schedule-selected-count");
                let saveScheduleTargetsBtn = document.getElementById("save-schedule-targets-btn");
                let clearScheduleTargetsBtn = document.getElementById("clear-schedule-targets-btn");
                let savedScheduleTargets = {json.dumps(schedule_config["targets"], ensure_ascii=False)};
                let scheduleSelectionDirty = false;

                const applyTheme = (theme) => {{
                    const normalizedTheme = theme === "light" ? "light" : "dark";
                    document.documentElement.dataset.theme = normalizedTheme;
                    if (themeToggleIcon) {{
                        themeToggleIcon.innerHTML = normalizedTheme === "light"
                            ? '<i class="fa-solid fa-sun"></i>'
                            : '<i class="fa-solid fa-moon"></i>';
                    }}
                    if (themeToggleLabel) {{
                        themeToggleLabel.textContent = normalizedTheme === "light" ? "Sáng" : "Tối";
                    }}
                    if (themeToggleMeta) {{
                        themeToggleMeta.textContent = normalizedTheme === "light"
                            ? "Nhấn để đổi sang tối"
                            : "Nhấn để đổi sang sáng";
                    }}
                    if (themeToggle) {{
                        const nextThemeText = normalizedTheme === "light" ? "Đổi sang tối" : "Đổi sang sáng";
                        themeToggle.setAttribute("title", nextThemeText);
                        themeToggle.setAttribute("aria-label", nextThemeText);
                    }}
                }};

                applyTheme(document.documentElement.dataset.theme || "dark");
                if (themeToggle) {{
                    themeToggle.addEventListener("click", () => {{
                        const nextTheme = document.documentElement.dataset.theme === "light" ? "dark" : "light";
                        applyTheme(nextTheme);
                        try {{
                            localStorage.setItem("dashboard_theme", nextTheme);
                        }} catch (_) {{
                        }}
                    }});
                }}

                const restoreDraft = (el, key) => {{
                    if (!el) return;
                    const saved = sessionStorage.getItem(key);
                    if (saved && (!el.value || el.value.trim() === "")) {{
                        el.value = saved;
                    }}
                    el.addEventListener("input", () => sessionStorage.setItem(key, el.value));
                }};

                restoreDraft(sheetUrlInput, "draft_sheet_url");
                restoreDraft(sheetNameInput, "draft_sheet_name");

                const setSheetTabsMessage = (message = "", tone = "muted") => {{
                    if (!sheetTabsState) return;
                    const toneMap = {{
                        muted: "text-slate-500",
                        loading: "text-cyan-300",
                        success: "text-emerald-300",
                        error: "text-amber-300",
                    }};
                    sheetTabsState.className = `text-xs ${{
                        toneMap[tone] || toneMap.muted
                    }}`;
                    sheetTabsState.textContent = message;
                }};

                const clearSheetTabs = () => {{
                    if (sheetNameOptions) {{
                        sheetNameOptions.innerHTML = "";
                    }}
                    if (sheetTabsList) {{
                        sheetTabsList.innerHTML = "";
                        sheetTabsList.classList.add("hidden");
                    }}
                }};

                const renderSheetTabs = (tabs) => {{
                    if (sheetNameOptions) {{
                        sheetNameOptions.innerHTML = tabs
                            .map((tab) => `<option value="${{tab.title}}"></option>`)
                            .join("");
                    }}
                    if (!sheetTabsList) return;
                    if (!tabs.length) {{
                        sheetTabsList.innerHTML = "";
                        sheetTabsList.classList.add("hidden");
                        return;
                    }}
                    const selectedTitle = (sheetNameInput?.value || "").trim();
                    sheetTabsList.innerHTML = tabs
                        .map((tab) => {{
                            const activeClass = tab.title === selectedTitle ? " is-active" : "";
                            return `<button type="button" class="sheet-tab-chip${{activeClass}}" data-sheet-tab="${{tab.title}}">${{tab.title}}</button>`;
                        }})
                        .join("");
                    sheetTabsList.classList.remove("hidden");
                    sheetTabsList.querySelectorAll("[data-sheet-tab]").forEach((button) => {{
                        button.addEventListener("click", () => {{
                            if (sheetNameInput) {{
                                sheetNameInput.value = button.dataset.sheetTab || "";
                                sessionStorage.setItem("draft_sheet_name", sheetNameInput.value);
                                renderSheetTabs(tabs);
                                sheetNameInput.focus();
                            }}
                        }});
                    }});
                }};

                const shouldLookupSheetTabs = (value) => {{
                    const trimmed = (value || "").trim();
                    return trimmed.length >= 20 || trimmed.includes("/spreadsheets/");
                }};

                const fetchSheetTabs = async (value, silent = false) => {{
                    const rawValue = (value || "").trim();
                    if (!rawValue) {{
                        clearSheetTabs();
                        setSheetTabsMessage("Dán link Google Sheet để hiện danh sách tab có trong file.");
                        return;
                    }}
                    if (!shouldLookupSheetTabs(rawValue)) {{
                        clearSheetTabs();
                        setSheetTabsMessage("Tiếp tục nhập link hoặc Sheet ID để tải danh sách tab.");
                        return;
                    }}

                    const requestId = ++sheetTabsRequestId;
                    if (!silent) {{
                        setSheetTabsMessage("Đang tải danh sách tab...", "loading");
                    }}

                    try {{
                        const response = await fetch(`/sheet-tabs?sheet_url=${{encodeURIComponent(rawValue)}}`, {{
                            headers: {{ "X-Requested-With": "fetch" }},
                            cache: "no-store",
                        }});
                        if (!response.ok) {{
                            throw new Error("Không gọi được API danh sách tab.");
                        }}
                        const data = await response.json();
                        if (requestId !== sheetTabsRequestId) return;

                        if (!data.ok) {{
                            clearSheetTabs();
                            setSheetTabsMessage(data.message || "Không tải được danh sách tab.", "error");
                            return;
                        }}

                        renderSheetTabs(Array.isArray(data.tabs) ? data.tabs : []);
                        setSheetTabsMessage(data.message || "Đã tải danh sách tab.", "success");
                    }} catch (_) {{
                        if (requestId !== sheetTabsRequestId) return;
                        clearSheetTabs();
                        setSheetTabsMessage("Không tải được danh sách tab. Kiểm tra link sheet và quyền truy cập.", "error");
                    }}
                }};

                const scheduleSheetTabsFetch = () => {{
                    if (sheetTabsDebounce) {{
                        clearTimeout(sheetTabsDebounce);
                    }}
                    sheetTabsDebounce = setTimeout(() => {{
                        fetchSheetTabs(sheetUrlInput?.value || "");
                    }}, 450);
                }};

                if (sheetUrlInput) {{
                    sheetUrlInput.addEventListener("input", scheduleSheetTabsFetch);
                    sheetUrlInput.addEventListener("blur", () => fetchSheetTabs(sheetUrlInput.value, true));
                }}
                if (sheetNameInput) {{
                    sheetNameInput.addEventListener("input", () => {{
                        if (sheetTabsList && !sheetTabsList.classList.contains("hidden")) {{
                            sheetTabsList.querySelectorAll("[data-sheet-tab]").forEach((button) => {{
                                button.classList.toggle("is-active", (button.dataset.sheetTab || "") === sheetNameInput.value.trim());
                            }});
                        }}
                    }});
                }}
                if (sheetUrlInput?.value) {{
                    fetchSheetTabs(sheetUrlInput.value, true);
                }}

                if (setSheetForm) {{
                    setSheetForm.addEventListener("submit", async (event) => {{
                        event.preventDefault();
                        const params = new URLSearchParams(new FormData(setSheetForm));
                        try {{
                            const response = await fetch(`/set-sheet?${{params.toString()}}`, {{
                                headers: {{ "X-Requested-With": "fetch" }},
                                cache: "no-store",
                            }});
                            const data = await response.json();
                            if (data.ok) {{
                                sessionStorage.removeItem("draft_sheet_url");
                                sessionStorage.removeItem("draft_sheet_name");
                                applyActiveSheetMeta(data, true);
                                applyColumnConfigState(data);
                                applyScheduleConfigState(data);
                                applyScheduleTrackingState(data);
                                if (typeof data.posts_html === "string") {{
                                    replacePostsPanelHtml(data.posts_html);
                                    setActivePanel("bai-dang");
                                }}
                                if (sheetUrlInput?.value) {{
                                    fetchSheetTabs(sheetUrlInput.value, true);
                                }}
                            }}
                            applyStatusState(data);
                            showNotice(
                                data.message || (data.ok ? "Đã nhập sheet thành công." : "Không nhập được sheet."),
                                data.level || (data.ok ? "success" : "error")
                            );
                        }} catch (_) {{
                            showNotice("Không nhập được sheet. Vui lòng thử lại.", "error");
                        }}
                    }});
                }}

                if (setColumnsForm) {{
                    setColumnsForm.addEventListener("submit", async (event) => {{
                        event.preventDefault();
                        const params = new URLSearchParams();
                        document.querySelectorAll("[form='set-columns-form'][name]").forEach((field) => {{
                            const rawValue = (field.value || "").trim();
                            if (field.matches("[data-column-input]")) {{
                                const detectedValue = String(field.dataset.detectedValue || "").trim().toUpperCase();
                                const manualValue = String(field.dataset.manualValue || "").trim().toUpperCase();
                                const normalizedRawValue = rawValue.toUpperCase();
                                if (!rawValue) {{
                                    params.set(field.name, "");
                                }} else if (!manualValue && detectedValue && normalizedRawValue === detectedValue) {{
                                    params.set(field.name, "");
                                }} else {{
                                    params.set(field.name, rawValue);
                                }}
                                return;
                            }}
                            params.set(field.name, rawValue);
                        }});
                        try {{
                            const response = await fetch(`/set-columns?${{params.toString()}}`, {{
                                headers: {{ "X-Requested-With": "fetch" }},
                                cache: "no-store",
                            }});
                            const data = await response.json();
                            applyStatusState(data);
                            applyScheduleConfigState(data);
                            applyScheduleTrackingState(data);
                            if (data.ok) {{
                                applyColumnConfigState(data);
                            }}
                            showNotice(
                                data.message || (data.ok ? "Đã lưu cấu hình nhập liệu thành công." : "Không lưu được cấu hình nhập liệu."),
                                data.level || (data.ok ? "success" : "error")
                            );
                        }} catch (_) {{
                            showNotice("Không lưu được cấu hình nhập liệu. Vui lòng thử lại.", "error");
                        }}
                    }});
                }}

                if (saveAccessPolicyBtn && authPolicyText) {{
                    saveAccessPolicyBtn.addEventListener("click", async () => {{
                        try {{
                            const response = await fetch("/admin/save-access-policy", {{
                                method: "POST",
                                headers: {{ "Content-Type": "application/json", "X-Requested-With": "fetch" }},
                                body: JSON.stringify({{ policy_text: authPolicyText.value || "" }}),
                            }});
                            const data = await response.json();
                            if (data.ok && typeof data.policy_text === "string") {{
                                authPolicyText.value = data.policy_text;
                            }}
                            showNotice(
                                data.message || (data.ok ? "Đã lưu access policy." : "Không lưu được access policy."),
                                data.level || (data.ok ? "success" : "error")
                            );
                        }} catch (_) {{
                            showNotice("Không lưu được access policy. Vui lòng thử lại.", "error");
                        }}
                    }});
                }}

                if (saveMailConfigBtn) {{
                    saveMailConfigBtn.addEventListener("click", async () => {{
                        const payload = {{
                            smtp_host: mailSmtpHost?.value || "",
                            smtp_port: mailSmtpPort?.value || "",
                            smtp_user: mailSmtpUser?.value || "",
                            smtp_password: mailSmtpPassword?.value || "",
                            smtp_from_email: mailFromEmail?.value || "",
                            smtp_from_name: mailFromName?.value || "",
                            use_tls: Boolean(mailUseTls?.checked),
                            use_ssl: Boolean(mailUseSsl?.checked),
                        }};
                        try {{
                            const response = await fetch("/admin/save-mail-config", {{
                                method: "POST",
                                headers: {{ "Content-Type": "application/json", "X-Requested-With": "fetch" }},
                                body: JSON.stringify(payload),
                            }});
                            const data = await response.json();
                            showNotice(
                                data.message || (data.ok ? "Đã lưu cấu hình mail." : "Không lưu được cấu hình mail."),
                                data.level || (data.ok ? "success" : "error")
                            );
                        }} catch (_) {{
                            showNotice("Không lưu được cấu hình mail. Vui lòng thử lại.", "error");
                        }}
                    }});
                }}

                document.addEventListener("click", async (event) => {{
                    const actionLink = event.target.closest("[data-inline-action]");
                    if (!actionLink) return;
                    event.preventDefault();

                    const action = actionLink.dataset.inlineAction || "";
                    const baseUrl = actionLink.getAttribute("href") || (action === "stop" ? "/stop" : "/start");
                    let requestUrl = baseUrl;
                    if (action === "start") {{
                        const params = new URLSearchParams();
                        const draftSheetUrl = (sheetUrlInput?.value || "").trim();
                        const draftSheetName = (sheetNameInput?.value || "").trim();
                        if (draftSheetUrl) {{
                            params.set("sheet_url", draftSheetUrl);
                        }}
                        if (draftSheetName) {{
                            params.set("sheet_name", draftSheetName);
                        }}
                        if (params.toString()) {{
                            requestUrl += `?${{params.toString()}}`;
                        }}
                    }}

                    try {{
                        const response = await fetch(requestUrl, {{
                            headers: {{ "X-Requested-With": "fetch" }},
                            cache: "no-store",
                        }});
                        const data = await response.json();
                        applyStatusState(data);
                        applyScheduleConfigState(data);
                        applyScheduleTrackingState(data);
                        if (action === "start" && data.ok) {{
                            sessionStorage.removeItem("draft_sheet_url");
                            sessionStorage.removeItem("draft_sheet_name");
                            applyActiveSheetMeta(data, true);
                            applyColumnConfigState(data);
                            if (sheetUrlInput?.value) {{
                                fetchSheetTabs(sheetUrlInput.value, true);
                            }}
                        }}
                        showNotice(
                            data.message || (data.ok ? "Đã cập nhật tác vụ." : "Không thực hiện được tác vụ."),
                            data.level || (data.ok ? "success" : "error")
                        );
                    }} catch (_) {{
                        showNotice(
                            action === "stop"
                                ? "Không dừng được tác vụ. Vui lòng thử lại."
                                : "Không bắt đầu được tác vụ. Vui lòng thử lại.",
                            "error"
                        );
                    }}
                }});

                const getWeeklyJsDay = () => ((parseInt(weekdaySelect?.value || "0", 10) + 1) % 7);
                const updateSchedulePreview = () => {{
                    const mode = scheduleModeSelect?.value || "off";
                    if (scheduleMonthdateHelp) {{
                        scheduleMonthdateHelp.textContent = mode === "weekly"
                            ? "Mở lịch để xem toàn bộ ngày đúng thứ đã chọn được khoanh sẵn."
                            : "Hằng tháng: chọn ngày chạy. Hằng tuần: mở lịch để xem các ngày của thứ đã chọn được khoanh sẵn.";
                    }}
                }};
                const syncScheduleWeekdayHighlights = () => {{
                    if (!monthPicker?.calendarContainer) return;
                    const mode = scheduleModeSelect?.value || "off";
                    const activeMonth = monthPicker.currentMonth;
                    const activeYear = monthPicker.currentYear;
                    const targetWeekday = getWeeklyJsDay();
                    monthPicker.calendarContainer.querySelectorAll(".flatpickr-day").forEach((dayElem) => {{
                        dayElem.classList.remove("schedule-weekday-match");
                        if (mode !== "weekly" || !dayElem.dateObj) return;
                        if (dayElem.dateObj.getFullYear() !== activeYear || dayElem.dateObj.getMonth() !== activeMonth) return;
                        if (dayElem.dateObj.getDay() === targetWeekday) {{
                            dayElem.classList.add("schedule-weekday-match");
                        }}
                    }});
                }};
                const redrawScheduleCalendar = () => {{
                    if (monthPicker && typeof monthPicker.redraw === "function") {{
                        monthPicker.redraw();
                    }}
                    requestAnimationFrame(syncScheduleWeekdayHighlights);
                    updateSchedulePreview();
                }};

                if (monthDateInput && typeof flatpickr === "function") {{
                    monthPicker = flatpickr(monthDateInput, {{
                        dateFormat: "Y-m-d",
                        altInput: true,
                        altFormat: "d/m/Y",
                        locale: (window.flatpickr && flatpickr.l10ns && flatpickr.l10ns.vn) ? "vn" : "default",
                        disableMobile: true,
                        allowInput: true,
                        onDayCreate: (_, __, fp, dayElem) => {{
                            dayElem.classList.remove("schedule-weekday-match");
                            const mode = scheduleModeSelect?.value || "off";
                            if (mode !== "weekly" || !dayElem.dateObj) return;
                            if (dayElem.dateObj.getFullYear() !== fp.currentYear || dayElem.dateObj.getMonth() !== fp.currentMonth) return;
                            if (dayElem.dateObj.getDay() === getWeeklyJsDay()) {{
                                dayElem.classList.add("schedule-weekday-match");
                            }}
                        }},
                        onMonthChange: () => redrawScheduleCalendar(),
                        onYearChange: () => redrawScheduleCalendar(),
                        onOpen: () => redrawScheduleCalendar(),
                        onReady: () => redrawScheduleCalendar(),
                        onValueUpdate: () => redrawScheduleCalendar(),
                    }});
                }}

                if (endDateInput && typeof flatpickr === "function") {{
                    endPicker = flatpickr(endDateInput, {{
                        dateFormat: "Y-m-d",
                        altInput: true,
                        altFormat: "d/m/Y",
                        locale: (window.flatpickr && flatpickr.l10ns && flatpickr.l10ns.vn) ? "vn" : "default",
                        disableMobile: true,
                        allowInput: true,
                        onChange: () => updateSchedulePreview(),
                    }});
                }}

                if (scheduleForm && monthDateInput && monthDayInput) {{
                    const syncMonthday = () => {{
                        if (!monthDateInput.value) return;
                        const parts = monthDateInput.value.split("-");
                        const day = parseInt(parts[2], 10);
                        if (!Number.isNaN(day)) {{
                            monthDayInput.value = Math.max(1, Math.min(28, day));
                        }}
                    }};
                    monthDateInput.addEventListener("change", syncMonthday);
                    scheduleForm.addEventListener("submit", syncMonthday);
                    syncMonthday();

                    scheduleForm.addEventListener("submit", async (event) => {{
                        event.preventDefault();
                        syncMonthday();
                        const params = new URLSearchParams(new FormData(scheduleForm));
                        try {{
                            const response = await fetch(`/set-schedule?${{params.toString()}}`, {{
                                headers: {{ "X-Requested-With": "fetch" }},
                                cache: "no-store",
                            }});
                            const data = await response.json();
                            applyStatusState(data);
                            applyScheduleConfigState(data);
                            applyScheduleTrackingState(data);
                            showNotice(
                                data.message || (data.ok ? "Đã cập nhật lịch tự động." : "Không lưu được lịch tự động."),
                                data.level || (data.ok ? "success" : "error")
                            );
                        }} catch (_) {{
                            showNotice("Không lưu được lịch tự động. Vui lòng thử lại.", "error");
                        }}
                    }});
                }}
                if (scheduleModeSelect) {{
                    scheduleModeSelect.addEventListener("change", redrawScheduleCalendar);
                }}
                if (weekdaySelect) {{
                    weekdaySelect.addEventListener("change", redrawScheduleCalendar);
                }}
                if (endDateInput) {{
                    endDateInput.addEventListener("change", updateSchedulePreview);
                }}

                if (monthDateInput && monthDateBtn) {{
                    monthDateBtn.addEventListener("click", () => {{
                        try {{
                            if (monthPicker && typeof monthPicker.open === "function") {{
                                monthPicker.open();
                            }} else if (typeof monthDateInput.showPicker === "function") {{
                                monthDateInput.showPicker();
                            }} else {{
                                monthDateInput.focus();
                                monthDateInput.click();
                            }}
                        }} catch (_) {{
                            monthDateInput.focus();
                        }}
                    }});
                }}
                if (endDateInput && endDateBtn) {{
                    endDateBtn.addEventListener("click", () => {{
                        try {{
                            if (endPicker && typeof endPicker.open === "function") {{
                                endPicker.open();
                            }} else if (typeof endDateInput.showPicker === "function") {{
                                endDateInput.showPicker();
                            }} else {{
                                endDateInput.focus();
                                endDateInput.click();
                            }}
                        }} catch (_) {{
                            endDateInput.focus();
                        }}
                    }});
                }}
                updateSchedulePreview();
                redrawScheduleCalendar();

                const statusBadge = document.getElementById("status-badge");
                const currentTaskLabel = document.getElementById("current-task");
                const progressBar = document.getElementById("progress-bar");
                const logSection = document.getElementById("log-section");
                const primaryAction = document.getElementById("primary-action");
                const scheduleLabelEls = Array.from(document.querySelectorAll("[data-schedule-label]"));
                const sidebarStatusText = document.getElementById("sidebar-status-text");
                const sidebarStatusTask = document.getElementById("sidebar-status-task");
                let postsVisibleCount = document.getElementById("posts-visible-count");
                let postsActiveTabLabel = document.getElementById("posts-active-tab-label");
                let postsTabCards = Array.from(document.querySelectorAll("[data-posts-tab-trigger]"));
                let postsTabPanels = Array.from(document.querySelectorAll("[data-posts-tab-panel]"));
                const sidebarLinks = Array.from(document.querySelectorAll("[data-nav-link]"));
                const dashboardSections = Array.from(document.querySelectorAll("[data-dashboard-section]"));
                let refreshInFlight = false;

                const showNotice = (_message = "", _level = "info") => {{}};
                const syncPostsDomRefs = () => {{
                    postsVisibleCount = document.getElementById("posts-visible-count");
                    postsActiveTabLabel = document.getElementById("posts-active-tab-label");
                    postsTabCards = Array.from(document.querySelectorAll("[data-posts-tab-trigger]"));
                    postsTabPanels = Array.from(document.querySelectorAll("[data-posts-tab-panel]"));
                    postsScheduleCount = document.getElementById("schedule-selected-count");
                    saveScheduleTargetsBtn = document.getElementById("save-schedule-targets-btn");
                    clearScheduleTargetsBtn = document.getElementById("clear-schedule-targets-btn");
                }};

                const normalizeScheduleKeyPart = (value) => String(value || "")
                    .trim()
                    .toLowerCase()
                    .normalize("NFD")
                    .replace(/[\\u0300-\\u036f]/g, "")
                    .replace(/[^a-z0-9]+/g, "");

                const getScheduleTargetKey = (item) => {{
                    const providedKey = String(item?.target_key || "").trim();
                    if (providedKey) return providedKey;
                    const sheetId = String(item?.sheet_id || "").trim();
                    const sheetName = normalizeScheduleKeyPart(item?.sheet_name || "");
                    const rowIdx = Number.parseInt(String(item?.row_idx || "0"), 10) || 0;
                    const link = String(item?.link || "").trim();
                    return `${{sheetId}}::${{sheetName}}::${{rowIdx}}::${{link}}`;
                }};

                const getTargetFromCheckbox = (checkbox) => {{
                    const rowIdx = Number.parseInt(String(checkbox?.dataset?.rowIdx || "0"), 10) || 0;
                    return {{
                        sheet_id: String(checkbox?.dataset?.sheetId || "").trim(),
                        sheet_name: String(checkbox?.dataset?.sheetName || "").trim(),
                        row_idx: rowIdx,
                        link: String(checkbox?.dataset?.link || "").trim(),
                        title: String(checkbox?.dataset?.title || "").trim(),
                        platform: String(checkbox?.dataset?.platformName || "").trim(),
                        campaign: String(checkbox?.dataset?.campaignName || "").trim(),
                        target_key: String(checkbox?.dataset?.targetKey || "").trim(),
                    }};
                }};

                const collectCheckedScheduleTargets = () => {{
                    const seen = new Set();
                    return Array.from(document.querySelectorAll("[data-post-select]:checked"))
                        .map((checkbox) => getTargetFromCheckbox(checkbox))
                        .filter((item) => item.sheet_id && item.sheet_name && item.row_idx > 0)
                        .filter((item) => {{
                            const key = getScheduleTargetKey(item);
                            if (seen.has(key)) return false;
                            seen.add(key);
                            return true;
                        }});
                }};

                const updateScheduleRowVisuals = () => {{
                    document.querySelectorAll("[data-post-select]").forEach((checkbox) => {{
                        const row = checkbox.closest(".post-row");
                        if (!row) return;
                        row.classList.toggle("is-schedule-target", Boolean(checkbox.checked));
                    }});
                }};

                const updateScheduleSelectionCounter = () => {{
                    const checkedCount = collectCheckedScheduleTargets().length;
                    if (postsScheduleCount) {{
                        postsScheduleCount.textContent = `${{checkedCount}} bài`;
                    }}
                    updateScheduleRowVisuals();
                }};

                const applySavedScheduleTargetsToPosts = (force = false) => {{
                    if (scheduleSelectionDirty && !force) return;
                    const targetKeys = new Set((Array.isArray(savedScheduleTargets) ? savedScheduleTargets : []).map((item) => getScheduleTargetKey(item)));
                    document.querySelectorAll("[data-post-select]").forEach((checkbox) => {{
                        const item = getTargetFromCheckbox(checkbox);
                        checkbox.checked = targetKeys.has(getScheduleTargetKey(item));
                    }});
                    updateScheduleSelectionCounter();
                }};

                const parseEmployeeUsersData = () => {{
                    if (!employeeUsersData) return [];
                    try {{
                        const parsed = JSON.parse(employeeUsersData.textContent || "[]");
                        return Array.isArray(parsed) ? parsed : [];
                    }} catch (_) {{
                        return [];
                    }}
                }};

                const normalizeEmployeeItem = (item) => {{
                    const email = String(item?.email || "").trim().toLowerCase();
                    if (!email || !email.includes("@")) return null;
                    const role = String(item?.role || "user").trim().toLowerCase() === "admin" ? "admin" : "user";
                    const lastLoginText = String(item?.last_login_text || "").trim();
                    const statusKey = String(item?.status_key || (lastLoginText && lastLoginText !== "Chưa có" ? "verified" : "pending")).trim() === "verified" ? "verified" : "pending";
                    const loginCount = Math.max(0, Number.parseInt(String(item?.login_count || "0"), 10) || 0);
                    return {{
                        email,
                        role,
                        role_label: role === "admin" ? "Admin" : "User",
                        status_key: statusKey,
                        status_label: statusKey === "verified" ? "Đã xác thực" : "Chờ xác thực",
                        last_login_text: lastLoginText || "Chưa có",
                        login_count: loginCount,
                        is_forced_admin: Boolean(item?.is_forced_admin),
                    }};
                }};

                const dedupeEmployees = (items) => {{
                    const map = new Map();
                    (Array.isArray(items) ? items : []).forEach((item) => {{
                        const normalized = normalizeEmployeeItem(item);
                        if (!normalized) return;
                        map.set(normalized.email, normalized);
                    }});
                    return Array.from(map.values()).sort((a, b) => {{
                        const roleCompare = (a.role === "admin" ? 0 : 1) - (b.role === "admin" ? 0 : 1);
                        if (roleCompare !== 0) return roleCompare;
                        return a.email.localeCompare(b.email);
                    }});
                }};

                const updateEmployeeSummary = (items) => {{
                    const rows = Array.isArray(items) ? items : [];
                    const verified = rows.filter((item) => item.status_key === "verified").length;
                    const admins = rows.filter((item) => item.role === "admin").length;
                    const pending = Math.max(0, rows.length - verified);
                    if (employeeTotalCount) employeeTotalCount.textContent = String(rows.length);
                    if (employeeVerifiedCount) employeeVerifiedCount.textContent = String(verified);
                    if (employeeAdminCount) employeeAdminCount.textContent = String(admins);
                    if (employeeChipAll) employeeChipAll.textContent = String(rows.length);
                    if (employeeChipPending) employeeChipPending.textContent = String(pending);
                    if (employeeChipVerified) employeeChipVerified.textContent = String(verified);
                }};

                const renderEmployeeRows = () => {{
                    if (!employeeTableBody) return;
                    const searchValue = String(employeeSearchInput?.value || "").trim().toLowerCase();
                    const roleValue = String(employeeRoleFilter?.value || "all").trim().toLowerCase();
                    const rows = employeeUsersState.filter((item) => {{
                        const matchesSearch = !searchValue || item.email.toLowerCase().includes(searchValue);
                        const matchesRole = roleValue === "all" || item.role === roleValue;
                        const matchesStatus = employeeStatusFilter === "all" || item.status_key === employeeStatusFilter;
                        return matchesSearch && matchesRole && matchesStatus;
                    }});
                    employeeTableBody.innerHTML = rows.map((item) => {{
                        const forcedHint = item.is_forced_admin ? '<div class="employee-meta">Admin cứng</div>' : `<div class="employee-meta">${{item.role_label}}</div>`;
                        return `
                            <tr>
                                <td>
                                    <div class="employee-row-user">
                                        <span class="employee-avatar">${{item.email.charAt(0).toUpperCase()}}</span>
                                        <div>
                                            <div class="employee-email">${{item.email}}</div>
                                            ${{forcedHint}}
                                        </div>
                                    </div>
                                </td>
                                <td>
                                    <select class="employee-role-select" data-employee-role="${{item.email}}" ${{item.is_forced_admin ? "disabled" : ""}}>
                                        <option value="user" ${{item.role === "user" ? "selected" : ""}}>User</option>
                                        <option value="admin" ${{item.role === "admin" ? "selected" : ""}}>Admin</option>
                                    </select>
                                </td>
                                <td><span class="employee-status-badge ${{item.status_key === "verified" ? "is-verified" : "is-pending"}}">${{item.status_label}}</span></td>
                                <td>${{item.last_login_text || "Chưa có"}}</td>
                                <td class="text-right font-black">${{item.login_count || 0}}</td>
                                <td>
                                    <div class="employee-table-actions">
                                        <button type="button" class="employee-icon-btn" data-employee-remove="${{item.email}}" title="Xóa nhân viên" ${{item.is_forced_admin ? "disabled" : ""}}>
                                            <i class="fa-regular fa-trash-can"></i>
                                        </button>
                                    </div>
                                </td>
                            </tr>
                        `;
                    }}).join("");
                    const hasRows = rows.length > 0;
                    if (employeeEmptyPanel) employeeEmptyPanel.classList.toggle("hidden", hasRows);

                    employeeTableBody.querySelectorAll("[data-employee-role]").forEach((select) => {{
                        select.addEventListener("change", () => {{
                            const email = select.getAttribute("data-employee-role") || "";
                            employeeUsersState = employeeUsersState.map((item) => (
                                item.email === email ? {{ ...item, role: select.value === "admin" ? "admin" : "user", role_label: select.value === "admin" ? "Admin" : "User" }} : item
                            ));
                            updateEmployeeSummary(employeeUsersState);
                            renderEmployeeRows();
                        }});
                    }});

                    employeeTableBody.querySelectorAll("[data-employee-remove]").forEach((button) => {{
                        button.addEventListener("click", () => {{
                            const email = button.getAttribute("data-employee-remove") || "";
                            employeeUsersState = employeeUsersState.filter((item) => item.email !== email);
                            updateEmployeeSummary(employeeUsersState);
                            renderEmployeeRows();
                        }});
                    }});
                }};

                const saveEmployeeUsers = async () => {{
                    try {{
                        const response = await fetch("/admin/save-users", {{
                            method: "POST",
                            headers: {{ "Content-Type": "application/json", "X-Requested-With": "fetch" }},
                            body: JSON.stringify({{ users: employeeUsersState.map((item) => ({{ email: item.email, role: item.role }})) }}),
                        }});
                        const data = await response.json();
                        if (data.ok && Array.isArray(data.users)) {{
                            employeeUsersState = dedupeEmployees(data.users);
                            updateEmployeeSummary(employeeUsersState);
                            renderEmployeeRows();
                        }}
                        showNotice(
                            data.message || (data.ok ? "Đã lưu danh sách nhân viên." : "Không lưu được danh sách nhân viên."),
                            data.level || (data.ok ? "success" : "error")
                        );
                    }} catch (_) {{
                        showNotice("Không lưu được danh sách nhân viên. Vui lòng thử lại.", "error");
                    }}
                }};

                if (employeeUsersData) {{
                    employeeUsersState = dedupeEmployees(parseEmployeeUsersData());
                    updateEmployeeSummary(employeeUsersState);
                    renderEmployeeRows();
                }}

                if (employeeStatusChips.length) {{
                    employeeStatusChips.forEach((chip) => {{
                        chip.addEventListener("click", () => {{
                            employeeStatusFilter = chip.getAttribute("data-employee-status") || "all";
                            employeeStatusChips.forEach((item) => item.classList.toggle("is-active", item === chip));
                            renderEmployeeRows();
                        }});
                    }});
                }}

                if (employeeSearchInput) {{
                    employeeSearchInput.addEventListener("input", renderEmployeeRows);
                }}

                if (employeeRoleFilter) {{
                    employeeRoleFilter.addEventListener("change", renderEmployeeRows);
                }}

                if (employeeAddBtn) {{
                    employeeAddBtn.addEventListener("click", () => {{
                        const email = String(employeeEmailInput?.value || "").trim().toLowerCase();
                        const role = String(employeeRoleInput?.value || "user").trim().toLowerCase() === "admin" ? "admin" : "user";
                        if (!email || !email.includes("@")) {{
                            showNotice("Email nhân viên không hợp lệ.", "error");
                            return;
                        }}
                        const existing = employeeUsersState.find((item) => item.email === email);
                        if (existing) {{
                            employeeUsersState = employeeUsersState.map((item) => (
                                item.email === email ? {{ ...item, role, role_label: role === "admin" ? "Admin" : "User" }} : item
                            ));
                            showNotice("Đã cập nhật role cho email đã tồn tại.", "info");
                        }} else {{
                            employeeUsersState = dedupeEmployees([
                                ...employeeUsersState,
                                {{
                                    email,
                                    role,
                                    role_label: role === "admin" ? "Admin" : "User",
                                    status_key: "pending",
                                    status_label: "Chờ xác thực",
                                    last_login_text: "Chưa có",
                                    login_count: 0,
                                    is_forced_admin: false,
                                }},
                            ]);
                            showNotice("Đã thêm nhân viên vào danh sách chờ lưu.", "success");
                        }}
                        if (employeeEmailInput) employeeEmailInput.value = "";
                        if (employeeRoleInput) employeeRoleInput.value = "user";
                        updateEmployeeSummary(employeeUsersState);
                        renderEmployeeRows();
                    }});
                }}

                if (employeeSaveBtn) {{
                    employeeSaveBtn.addEventListener("click", saveEmployeeUsers);
                }}

                if (employeeResetBtn) {{
                    employeeResetBtn.addEventListener("click", () => {{
                        if (employeeSearchInput) employeeSearchInput.value = "";
                        if (employeeRoleFilter) employeeRoleFilter.value = "all";
                        employeeStatusFilter = "all";
                        employeeStatusChips.forEach((chip) => chip.classList.toggle("is-active", (chip.getAttribute("data-employee-status") || "all") === "all"));
                        renderEmployeeRows();
                    }});
                }}

                if (employeeImportBtn && employeeImportInput) {{
                    employeeImportBtn.addEventListener("click", () => employeeImportInput.click());
                    employeeImportInput.addEventListener("change", async () => {{
                        const file = employeeImportInput.files?.[0];
                        if (!file) return;
                        try {{
                            const raw = await file.text();
                            const imported = raw
                                .split("\\r").join("")
                                .split("\\n")
                                .map((line) => line.trim())
                                .filter(Boolean)
                                .map((line) => {{
                                    const parts = line
                                        .split("\\t").join(",")
                                        .split(";").join(",")
                                        .split(",")
                                        .map((part) => part.trim())
                                        .filter(Boolean);
                                    return {{
                                        email: parts[0] || "",
                                        role: String(parts[1] || "user").toLowerCase() === "admin" ? "admin" : "user",
                                    }};
                                }});
                            employeeUsersState = dedupeEmployees([...employeeUsersState, ...imported]);
                            updateEmployeeSummary(employeeUsersState);
                            renderEmployeeRows();
                            showNotice(`Đã nhập ${{imported.length}} dòng nhân viên từ file.`, "success");
                        }} catch (_) {{
                            showNotice("Không đọc được file nhân viên. Dùng CSV hoặc TXT đơn giản.", "error");
                        }} finally {{
                            employeeImportInput.value = "";
                        }}
                    }});
                }}

                const applyActiveSheetMeta = (data, syncInputs = false) => {{
                    const sheetName = (data?.active_sheet_name || "").trim() || "Chưa cài đặt";
                    const sheetId = (data?.active_sheet_id || "").trim() || "Chưa cài đặt";
                    activeSheetNameEls.forEach((el) => {{
                        el.textContent = sheetName;
                    }});
                    activeSheetIdEls.forEach((el) => {{
                        el.textContent = sheetId;
                    }});
                    if (syncInputs && sheetNameInput && typeof data?.active_sheet_name === "string") {{
                        sheetNameInput.value = data.active_sheet_name;
                    }}
                    if (syncInputs && sheetUrlInput && typeof data?.snapshot_url === "string") {{
                        sheetUrlInput.value = data.snapshot_url;
                    }}
                }};

                const applyColumnConfigState = (data) => {{
                    const columnConfig = data?.column_config;
                    if (!columnConfig) return;
                    const manualMode = columnConfig.manual_mode || "AUTO";
                    configModeEls.forEach((el) => {{
                        el.textContent = manualMode;
                    }});
                    const inputValues = columnConfig.input_values || {{}};
                    const detectedInputs = columnConfig.detected_inputs || {{}};
                    const manualInputs = columnConfig.manual_inputs || {{}};
                    const inputSources = columnConfig.input_sources || {{}};
                    Object.entries(columnInputEls).forEach(([field, el]) => {{
                        if (!el) return;
                        const nextValue = inputValues[field] || "";
                        if (document.activeElement !== el) {{
                            el.value = nextValue;
                        }}
                        el.dataset.detectedValue = detectedInputs[field] || "";
                        el.dataset.manualValue = manualInputs[field] || "";
                    }});
                    Object.entries(columnSourceEls).forEach(([field, el]) => {{
                        if (!el) return;
                        el.textContent = inputSources[field] || "CHƯA THẤY";
                    }});
                    const startRowField = setColumnsForm?.querySelector("[name='start_row']");
                    if (startRowField && document.activeElement !== startRowField) {{
                        startRowField.value = `${{columnConfig.start_row || 2}}`;
                    }}
                    const detectedText = columnConfig.detected_text || "";
                    columnDetectedTextEls.forEach((el) => {{
                        el.textContent = detectedText;
                    }});
                }};

                const applyScheduleConfigState = (data) => {{
                    const scheduleConfig = data?.schedule_config;
                    if (!scheduleConfig) return;
                    const label = scheduleConfig.label || "Chưa bật";
                    if (Array.isArray(scheduleConfig.targets)) {{
                        savedScheduleTargets = scheduleConfig.targets;
                    }}
                    scheduleLabelEls.forEach((el) => {{
                        el.textContent = label;
                    }});
                    if (scheduleBoundSheetName) {{
                        scheduleBoundSheetName.textContent = scheduleConfig.sheet_name_text || "Chưa chốt tab nào";
                    }}
                    if (scheduleBoundSheetId) {{
                        scheduleBoundSheetId.textContent = scheduleConfig.sheet_id_text || "Chưa có Spreadsheet ID";
                    }}
                    if (scheduleBoundScope) {{
                        scheduleBoundScope.textContent = scheduleConfig.scope_text || "";
                    }}
                    if (scheduleBoundLink) {{
                        const hasLink = Boolean(scheduleConfig.snapshot_url);
                        scheduleBoundLink.classList.toggle("hidden", !hasLink);
                        scheduleBoundLink.href = hasLink ? scheduleConfig.snapshot_url : "#";
                    }}
                    if (scheduleTargetSummary) {{
                        scheduleTargetSummary.textContent = scheduleConfig.targets_summary_text || "Chưa chọn bài nào. Lịch sẽ chạy toàn bộ tab đã lưu.";
                    }}
                    if (scheduleTargetList && typeof scheduleConfig.targets_html === "string") {{
                        scheduleTargetList.innerHTML = scheduleConfig.targets_html;
                    }}
                    applySavedScheduleTargetsToPosts();
                }};

                const applyScheduleTrackingState = (data) => {{
                    const tracking = data?.schedule_tracking;
                    if (!tracking) return;
                    if (scheduleTrackNext) {{
                        scheduleTrackNext.textContent = tracking.next_run_text || "Chưa có";
                    }}
                    if (scheduleTrackStarted) {{
                        scheduleTrackStarted.textContent = tracking.last_started_text || "Chưa có";
                    }}
                    if (scheduleTrackFinished) {{
                        scheduleTrackFinished.textContent = tracking.last_finished_text || "Chưa có";
                    }}
                    if (scheduleTrackDuration) {{
                        scheduleTrackDuration.textContent = tracking.last_duration_text || "0s";
                    }}
                    if (scheduleTrackRunning) {{
                        scheduleTrackRunning.textContent = tracking.is_running_text || "Đang chờ";
                    }}
                    if (scheduleTrackStatus) {{
                        scheduleTrackStatus.textContent = tracking.last_status_text || "Chưa chạy";
                    }}
                    if (scheduleTrackSource) {{
                        scheduleTrackSource.textContent = tracking.last_source_text || "Chưa có";
                    }}
                    if (scheduleTrackSheet) {{
                        scheduleTrackSheet.textContent = tracking.last_sheet_text || "Chưa có";
                    }}
                    if (scheduleTrackProcessed) {{
                        scheduleTrackProcessed.textContent = tracking.last_processed_text || "0";
                    }}
                    if (scheduleTrackSuccess) {{
                        scheduleTrackSuccess.textContent = tracking.last_success_text || "0";
                    }}
                    if (scheduleTrackFailed) {{
                        scheduleTrackFailed.textContent = tracking.last_failed_text || "0";
                    }}
                    if (scheduleTrackHistory && typeof tracking.history_html === "string") {{
                        scheduleTrackHistory.innerHTML = tracking.history_html;
                    }}
                }};

                const applyStatusState = (data) => {{
                    if (!data) return;
                    if (statusBadge) {{
                        statusBadge.className = data.status_badge_class;
                        statusBadge.textContent = data.status_badge_text;
                    }}
                    if (sidebarStatusText) {{
                        sidebarStatusText.textContent = data.status_badge_text;
                    }}
                    if (sidebarStatusTask) {{
                        sidebarStatusTask.textContent = data.current_task;
                    }}
                    if (currentTaskLabel) {{
                        currentTaskLabel.textContent = data.current_task;
                    }}
                    if (progressBar) {{
                        progressBar.style.width = data.progress_width;
                    }}
                    if (logSection && typeof data.log_html === "string") {{
                        logSection.innerHTML = data.log_html;
                    }}
                    if (primaryAction && typeof data.primary_action_html === "string") {{
                        primaryAction.innerHTML = data.primary_action_html;
                    }}
                }};

                const refreshDashboard = async () => {{
                    if (document.hidden || refreshInFlight) return;
                    refreshInFlight = true;
                    try {{
                        const response = await fetch("/status", {{
                            headers: {{ "X-Requested-With": "fetch" }},
                            cache: "no-store",
                        }});
                        if (!response.ok) return;
                        const data = await response.json();
                        applyStatusState(data);
                        applyActiveSheetMeta(data);
                        applyColumnConfigState(data);
                        applyScheduleConfigState(data);
                        applyScheduleTrackingState(data);
                    }} catch (_) {{
                    }} finally {{
                        refreshInFlight = false;
                    }}
                }};

                const getVisibleRowChecks = (panel) => Array.from(panel.querySelectorAll(".post-row"))
                    .filter((row) => !row.classList.contains("hidden"))
                    .map((row) => row.querySelector("[data-post-select]"))
                    .filter(Boolean);

                const updatePanelSelectAllState = (panel) => {{
                    if (!panel) return;
                    const selectAll = panel.querySelector("[data-select-all-posts]");
                    if (!selectAll) return;
                    const rowChecks = getVisibleRowChecks(panel);
                    const checkedCount = rowChecks.filter((item) => item.checked).length;
                    selectAll.checked = rowChecks.length > 0 && checkedCount === rowChecks.length;
                    selectAll.indeterminate = checkedCount > 0 && checkedCount < rowChecks.length;
                }};

                const syncPostsSelectionState = () => {{
                    postsTabPanels.forEach((panel) => updatePanelSelectAllState(panel));
                    updateScheduleSelectionCounter();
                }};

                const persistScheduleTargets = async (targets, emptyMessage = "Đã xóa chọn lọc bài cho lịch tự động.") => {{
                    try {{
                        const response = await fetch("/set-schedule-targets", {{
                            method: "POST",
                            headers: {{ "Content-Type": "application/json", "X-Requested-With": "fetch" }},
                            body: JSON.stringify({{
                                sheet_id: String(activeSheetIdEls[0]?.textContent || "").trim(),
                                targets,
                            }}),
                        }});
                        const data = await response.json();
                        if (data.ok) {{
                            scheduleSelectionDirty = false;
                            applyScheduleConfigState(data);
                            updateScheduleSelectionCounter();
                        }}
                        showNotice(
                            data.message || (targets.length ? `Đã lưu ${{targets.length}} bài cho lịch tự động.` : emptyMessage),
                            data.level || (data.ok ? "success" : "error")
                        );
                    }} catch (_) {{
                        showNotice("Không lưu được danh sách bài cho lịch tự động. Vui lòng thử lại.", "error");
                    }}
                }};

                const getActivePostsPanel = () => postsTabPanels.find((panel) => panel.classList.contains("is-active")) || null;

                const applyPostFilters = (panel = getActivePostsPanel()) => {{
                    if (!panel) {{
                        if (postsVisibleCount) {{
                            postsVisibleCount.textContent = "0 bài";
                        }}
                        return;
                    }}
                    const searchInput = panel.querySelector(".posts-search-field");
                    const emptyState = panel.querySelector(".posts-empty-panel");
                    const term = (searchInput?.value || "").trim().toLowerCase();
                    const activePlatform = panel.dataset.postsPlatform || "all";
                    const rows = Array.from(panel.querySelectorAll(".post-row"));
                    let visible = 0;

                    rows.forEach((row) => {{
                        const platform = row.dataset.platform || "khac";
                        const haystack = row.dataset.search || "";
                        const matchesPlatform = activePlatform === "all" || platform === activePlatform;
                        const matchesTerm = !term || haystack.includes(term);
                        const shouldShow = matchesPlatform && matchesTerm;
                        row.classList.toggle("hidden", !shouldShow);
                        if (shouldShow) {{
                            visible += 1;
                        }}
                    }});

                    if (emptyState) {{
                        emptyState.classList.toggle("hidden", visible !== 0);
                    }}
                    if (postsVisibleCount) {{
                        postsVisibleCount.textContent = `${{visible}} bài`;
                    }}
                    if (postsActiveTabLabel) {{
                        postsActiveTabLabel.textContent = panel.dataset.postsTabTitle || "Chưa chọn";
                    }}
                    updatePanelSelectAllState(panel);
                }};

                const setActivePostsTab = (tabSlug) => {{
                    const safeSlug = postsTabCards.some((card) => card.dataset.postsTabTrigger === tabSlug)
                        ? tabSlug
                        : (postsTabCards[0]?.dataset.postsTabTrigger || "");
                    postsTabCards.forEach((card) => {{
                        card.classList.toggle("is-active", card.dataset.postsTabTrigger === safeSlug);
                    }});
                    postsTabPanels.forEach((panel) => {{
                        panel.classList.toggle("is-active", panel.dataset.postsTabPanel === safeSlug);
                    }});
                    applyPostFilters(getActivePostsPanel());
                }};

                const initializePostsPanel = () => {{
                    postsTabCards.forEach((card) => {{
                        card.addEventListener("click", () => {{
                            setActivePostsTab(card.dataset.postsTabTrigger || "");
                        }});
                    }});

                    postsTabPanels.forEach((panel) => {{
                        const searchField = panel.querySelector(".posts-search-field");
                        const resetButton = panel.querySelector(".posts-reset-btn");
                        const chips = Array.from(panel.querySelectorAll(".posts-chip"));
                        const selectAll = panel.querySelector("[data-select-all-posts]");
                        const rowChecks = Array.from(panel.querySelectorAll("[data-post-select]"));

                        chips.forEach((chip) => {{
                            chip.addEventListener("click", () => {{
                                panel.dataset.postsPlatform = chip.dataset.platform || "all";
                                chips.forEach((item) => item.classList.toggle("is-active", item === chip));
                                applyPostFilters(panel);
                            }});
                        }});

                        if (searchField) {{
                            searchField.addEventListener("input", () => applyPostFilters(panel));
                        }}

                        if (resetButton) {{
                            resetButton.addEventListener("click", () => {{
                                panel.dataset.postsPlatform = "all";
                                if (searchField) {{
                                    searchField.value = "";
                                }}
                                chips.forEach((chip) => chip.classList.toggle("is-active", (chip.dataset.platform || "all") === "all"));
                                applyPostFilters(panel);
                            }});
                        }}

                        if (selectAll) {{
                            selectAll.addEventListener("change", () => {{
                                getVisibleRowChecks(panel).forEach((checkbox) => {{
                                    checkbox.checked = selectAll.checked;
                                }});
                                syncPostsSelectionState();
                            }});
                        }}

                        rowChecks.forEach((checkbox) => {{
                            checkbox.addEventListener("change", () => {{
                                scheduleSelectionDirty = true;
                                syncPostsSelectionState();
                            }});
                        }});
                    }});

                    if (saveScheduleTargetsBtn) {{
                        saveScheduleTargetsBtn.addEventListener("click", async () => {{
                            const targets = collectCheckedScheduleTargets();
                            await persistScheduleTargets(
                                targets,
                                "Đã xóa chọn lọc bài. Lịch tự động sẽ quay về chạy toàn tab đang dùng."
                            );
                        }});
                    }}

                    if (clearScheduleTargetsBtn) {{
                        clearScheduleTargetsBtn.addEventListener("click", async () => {{
                            document.querySelectorAll("[data-post-select]").forEach((checkbox) => {{
                                checkbox.checked = false;
                            }});
                            scheduleSelectionDirty = true;
                            syncPostsSelectionState();
                            await persistScheduleTargets(
                                [],
                                "Đã xóa chọn lọc bài. Lịch tự động sẽ quay về chạy toàn tab đang dùng."
                            );
                        }});
                    }}

                    if (postsTabCards.length) {{
                        const initialPostsTab = postsTabCards.find((card) => card.classList.contains("is-active"))?.dataset.postsTabTrigger
                            || postsTabCards[0].dataset.postsTabTrigger;
                        setActivePostsTab(initialPostsTab || "");
                    }} else {{
                        if (postsVisibleCount) postsVisibleCount.textContent = "0 bài";
                        if (postsActiveTabLabel) postsActiveTabLabel.textContent = "Chưa chọn";
                    }}
                    syncPostsSelectionState();
                    applySavedScheduleTargetsToPosts(true);
                }};

                const replacePostsPanelHtml = (postsHtml) => {{
                    if (typeof postsHtml !== "string" || !postsHtml.trim()) return;
                    const currentPostsSection = document.getElementById("bai-dang");
                    if (!currentPostsSection) return;
                    const template = document.createElement("template");
                    template.innerHTML = postsHtml.trim();
                    const nextPostsSection = template.content.firstElementChild;
                    if (!nextPostsSection) return;
                    currentPostsSection.innerHTML = nextPostsSection.innerHTML;
                    syncPostsDomRefs();
                    initializePostsPanel();
                    applySavedScheduleTargetsToPosts(true);
                }};

                const setActivePanel = (sectionId) => {{
                    const availableIds = dashboardSections.map((section) => section.dataset.dashboardSection);
                    const targetId = availableIds.includes(sectionId) ? sectionId : "tong-quan";

                    sidebarLinks.forEach((link) => {{
                        link.classList.toggle("is-active", link.dataset.navLink === targetId);
                    }});
                    dashboardSections.forEach((section) => {{
                        section.classList.toggle("is-active", section.dataset.dashboardSection === targetId);
                    }});

                    if (window.location.hash !== `#${{targetId}}`) {{
                        history.replaceState(null, "", `#${{targetId}}`);
                    }}
                }};

                sidebarLinks.forEach((link) => {{
                    link.addEventListener("click", (event) => {{
                        event.preventDefault();
                        setActivePanel(link.dataset.navLink || "tong-quan");
                    }});
                }});

                window.addEventListener("hashchange", () => {{
                    setActivePanel((window.location.hash || "").replace("#", ""));
                }});

                initializePostsPanel();
                setActivePanel((window.location.hash || "").replace("#", "") || "tong-quan");
                setInterval(refreshDashboard, 4000);
            }});
        </script>
    </head>
    <body class="bg-[#0b0f1a] text-slate-200 min-h-screen p-2 md:p-3">
        <div class="dashboard-shell">
            <aside class="dashboard-sidebar">
                <div class="sidebar-brand">
                    <div>
                        <div class="sidebar-brand-title">Social Monitor</div>
                        <div class="sidebar-brand-subtitle">Dashboard phân loại</div>
                    </div>
                    <div class="sidebar-pulse"><i class="fa-solid fa-compass-drafting"></i></div>
                </div>

                <div class="sidebar-status-card">
                    <div class="sidebar-status-label">Trạng thái hiện tại</div>
                    <div id="sidebar-status-text" class="sidebar-status-value">{status_payload["status_badge_text"]}</div>
                    <div id="sidebar-status-task" class="sidebar-status-meta">{status_payload["current_task"]}</div>
                </div>

                <nav class="sidebar-nav">
                    <a href="#tong-quan" class="sidebar-link is-active" data-nav-link="tong-quan"><span class="sidebar-link-icon"><i class="fa-solid fa-gauge-high"></i></span><span>Tổng quan</span></a>
                    <a href="#cau-hinh" class="sidebar-link" data-nav-link="cau-hinh"><span class="sidebar-link-icon"><i class="fa-solid fa-sliders"></i></span><span>Cấu hình</span></a>
                    <a href="#bai-dang" class="sidebar-link" data-nav-link="bai-dang"><span class="sidebar-link-icon"><i class="fa-regular fa-newspaper"></i></span><span>Bài đăng</span></a>
                    {employee_sidebar_link}
                    <a href="#lich-tu-dong" class="sidebar-link" data-nav-link="lich-tu-dong"><span class="sidebar-link-icon"><i class="fa-regular fa-calendar-days"></i></span><span>Lịch tự động</span></a>
                    <a href="#theo-doi-lan-chay" class="sidebar-link" data-nav-link="theo-doi-lan-chay"><span class="sidebar-link-icon"><i class="fa-solid fa-clock-rotate-left"></i></span><span>Theo dõi lần chạy</span></a>
                </nav>
            </aside>

            <main class="dashboard-main">
                <div class="dashboard-utilitybar">
                    <button type="button" id="theme-toggle" class="theme-toggle-btn">
                        <span id="theme-toggle-icon" class="theme-toggle-icon"><i class="fa-solid fa-moon"></i></span>
                        <span class="theme-toggle-copy">
                            <span id="theme-toggle-label" class="theme-toggle-label">Tối</span>
                            <span id="theme-toggle-meta" class="theme-toggle-meta">Nhấn để đổi sang sáng</span>
                        </span>
                    </button>
                    <div class="utility-userbar">
                        <div class="utility-user-pill" title="{current_user_email}">
                            <span class="utility-user-avatar">{html.escape((current_user.get("email", "U")[:1] or "U").upper())}</span>
                            <span class="utility-user-copy">
                                <span class="utility-user-email">{current_user_email}</span>
                                <span class="utility-user-role">{current_user_role}</span>
                            </span>
                        </div>
                        <a href="/logout" class="utility-logout" title="Đăng xuất">
                            <i class="fa-solid fa-arrow-right-from-bracket"></i>
                        </a>
                    </div>
                </div>
                <div class="dashboard-main-inner">
                    {overview_html}

                    <section id="cau-hinh" data-dashboard-section="cau-hinh" class="dashboard-section dashboard-panel mb-6">
                        <div class="dashboard-section-title">Cấu hình</div>
                        {metric_cols_html}
                    </section>

                    {posts_html}
                    {employee_panel_html}

                    <section id="lich-tu-dong" data-dashboard-section="lich-tu-dong" class="dashboard-section dashboard-panel mb-6">
                        <div class="dashboard-section-title">Lịch tự động</div>
                        <div class="bg-black/20 rounded-3xl p-6 border border-white/5">
                            <div class="flex justify-between items-center mb-3 text-sm font-bold text-slate-500 uppercase">
                                <span>Lịch tự động</span><span class="text-cyan-300 font-black text-lg" data-schedule-label>{schedule_text}</span>
                            </div>
                            <form action="/set-schedule" method="get" class="flex flex-col gap-3">
                                <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
                                    <div>
                                        <label class="block text-xs text-slate-400 mb-2 uppercase tracking-wider">Chế độ chạy</label>
                                        <select id="schedule-mode-select" name="mode" class="w-full bg-slate-900 text-slate-100 rounded-xl px-4 py-3 border border-white/10 outline-none focus:border-cyan-400">
                                            <option value="off" {mode_selected["off"]}>Chưa bật</option>
                                            <option value="daily" {mode_selected["daily"]}>Hằng ngày</option>
                                            <option value="weekly" {mode_selected["weekly"]}>Hằng tuần</option>
                                            <option value="monthly" {mode_selected["monthly"]}>Hằng tháng</option>
                                        </select>
                                    </div>
                                    <div>
                                        <label class="block text-xs text-slate-400 mb-2 uppercase tracking-wider">Giờ chạy (HH:MM)</label>
                                        <input name="at" value="{schedule_time}" placeholder="VD: 09:00" class="w-full bg-slate-900 text-slate-100 rounded-xl px-4 py-3 border border-white/10 outline-none focus:border-cyan-400" />
                                    </div>
                                </div>
                                <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
                                    <div>
                                        <label class="block text-xs text-slate-400 mb-2 uppercase tracking-wider">Thứ trong tuần</label>
                                        <select id="schedule-weekday-select" name="weekday" class="w-full bg-slate-900 text-slate-100 rounded-xl px-4 py-3 border border-white/10 outline-none focus:border-cyan-400">
                                            {weekday_options}
                                        </select>
                                        <p class="mt-1 text-[11px] text-slate-500">Dùng cho chế độ hằng tuần.</p>
                                    </div>
                                    <div>
                                        <label class="block text-xs text-slate-400 mb-2 uppercase tracking-wider">Ngày trong tháng / lịch xem trước</label>
                                        <div class="date-shell">
                                            <input id="schedule-monthdate-input" name="monthdate" type="text" value="{schedule_date_value}" placeholder="YYYY-MM-DD" class="flex-1 bg-slate-900 text-slate-100 rounded-xl px-4 py-3 border border-white/10 outline-none focus:border-cyan-400" />
                                            <button id="monthdate-picker-btn" type="button" class="date-picker-btn" title="Mở lịch">
                                                <i class="fa-solid fa-calendar-days"></i>
                                            </button>
                                        </div>
                                        <input name="monthday" type="hidden" value="{schedule_monthday}" />
                                        <p id="schedule-monthdate-help" class="mt-1 text-[11px] text-slate-500">Hằng tháng: chọn ngày chạy. Hằng tuần: mở lịch để xem các ngày của thứ đã chọn được khoanh sẵn.</p>
                                    </div>
                                </div>
                                <div class="grid grid-cols-1 gap-3">
                                    <div>
                                        <label class="block text-xs text-slate-400 mb-2 uppercase tracking-wider">Ngày kết thúc vòng lặp</label>
                                        <div class="date-shell">
                                            <input id="schedule-enddate-input" name="enddate" type="text" value="{schedule_end_value}" placeholder="YYYY-MM-DD" class="flex-1 bg-slate-900 text-slate-100 rounded-xl px-4 py-3 border border-white/10 outline-none focus:border-cyan-400" />
                                            <button id="enddate-picker-btn" type="button" class="date-picker-btn" title="Mở lịch kết thúc">
                                                <i class="fa-solid fa-calendar-check"></i>
                                            </button>
                                        </div>
                                        <p class="mt-1 text-[11px] text-slate-500">Để trống nếu muốn lặp vô thời hạn. Nếu có ngày này thì lịch sẽ tự dừng sau ngày đã chọn.</p>
                                    </div>
                                </div>
                                <div class="text-xs text-cyan-200/80 bg-cyan-500/10 border border-cyan-500/20 rounded-xl px-3 py-2">
                                    Gợi ý: Chọn <b>Hằng ngày</b> nếu chỉ cần giờ chạy. Với <b>Hằng tuần</b>, lịch sẽ khoanh toàn bộ ngày đúng thứ bạn chọn. Với <b>Hằng tháng</b>, hệ thống lấy ngày 1-28 từ ô lịch.
                                </div>
                                <div class="rounded-2xl border border-white/10 bg-slate-950/35 px-4 py-4">
                                    <div class="flex items-center justify-between gap-3 mb-3">
                                        <div>
                                            <div class="text-[11px] uppercase tracking-[0.22em] text-slate-400 font-black">Bài đang set lịch</div>
                                            <div id="schedule-target-summary" class="mt-2 text-sm font-black text-slate-100">{html.escape(schedule_config["targets_summary_text"])}</div>
                                        </div>
                                        <a href="#bai-dang" class="posts-toolbar-btn" data-nav-link="bai-dang">
                                            <i class="fa-regular fa-newspaper"></i> Chọn từ bài đăng
                                        </a>
                                    </div>
                                    <div id="schedule-target-list" class="grid gap-2">
                                        {schedule_config["targets_html"]}
                                    </div>
                                </div>
                                <button type="submit" class="w-full py-3 bg-slate-700 hover:bg-slate-600 text-white rounded-xl font-black uppercase text-sm shadow-sm shadow-slate-900/10">Lưu lịch</button>
                            </form>
                        </div>
                    </section>

                    <section id="theo-doi-lan-chay" data-dashboard-section="theo-doi-lan-chay" class="dashboard-section dashboard-panel mb-6">
                        <div class="dashboard-section-title">Theo dõi lần chạy</div>
                        <div class="bg-black/20 rounded-3xl p-6 border border-white/5">
                            <div class="rounded-2xl border border-white/10 bg-slate-950/35 px-4 py-4">
                                <div class="flex items-center justify-between gap-3 mb-3">
                                    <div>
                                        <div class="text-[11px] uppercase tracking-[0.22em] text-slate-400 font-black">Theo dõi lần chạy</div>
                                        <div class="mt-2 text-sm text-slate-400">Tự cập nhật theo lịch và khi bấm chạy tay, để bạn biết lần gần nhất hệ thống đã xử lý tab nào.</div>
                                    </div>
                                    <a href="#lich-tu-dong" class="posts-toolbar-btn" data-nav-link="lich-tu-dong">
                                        <i class="fa-regular fa-calendar-days"></i> Mở lịch tự động
                                    </a>
                                </div>
                                <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-5 gap-3">
                                    <div class="bg-slate-900/60 rounded-xl px-3 py-3 border border-white/8">
                                        <div class="text-[11px] uppercase tracking-[0.18em] text-slate-500 font-bold">Lần kế tiếp</div>
                                        <div id="schedule-track-next" class="mt-2 text-sm font-black text-slate-100">{html.escape(schedule_tracking["next_run_text"])}</div>
                                    </div>
                                    <div class="bg-slate-900/60 rounded-xl px-3 py-3 border border-white/8">
                                        <div class="text-[11px] uppercase tracking-[0.18em] text-slate-500 font-bold">Bắt đầu gần nhất</div>
                                        <div id="schedule-track-started" class="mt-2 text-sm font-black text-slate-100">{html.escape(schedule_tracking["last_started_text"])}</div>
                                    </div>
                                    <div class="bg-slate-900/60 rounded-xl px-3 py-3 border border-white/8">
                                        <div class="text-[11px] uppercase tracking-[0.18em] text-slate-500 font-bold">Kết thúc gần nhất</div>
                                        <div id="schedule-track-finished" class="mt-2 text-sm font-black text-slate-100">{html.escape(schedule_tracking["last_finished_text"])}</div>
                                    </div>
                                    <div class="bg-slate-900/60 rounded-xl px-3 py-3 border border-white/8">
                                        <div class="text-[11px] uppercase tracking-[0.18em] text-slate-500 font-bold">Thời lượng</div>
                                        <div id="schedule-track-duration" class="mt-2 text-sm font-black text-cyan-200">{html.escape(schedule_tracking["last_duration_text"])}</div>
                                    </div>
                                    <div class="bg-slate-900/60 rounded-xl px-3 py-3 border border-white/8">
                                        <div class="text-[11px] uppercase tracking-[0.18em] text-slate-500 font-bold">Đang chạy từ</div>
                                        <div id="schedule-track-running" class="mt-2 text-sm font-black text-slate-100">{html.escape(schedule_tracking["is_running_text"])}</div>
                                    </div>
                                </div>
                                <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-5 gap-3 mt-3">
                                    <div class="bg-slate-900/60 rounded-xl px-3 py-3 border border-white/8">
                                        <div class="text-[11px] uppercase tracking-[0.18em] text-slate-500 font-bold">Trạng thái</div>
                                        <div id="schedule-track-status" class="mt-2 text-sm font-black text-slate-100">{html.escape(schedule_tracking["last_status_text"])}</div>
                                    </div>
                                    <div class="bg-slate-900/60 rounded-xl px-3 py-3 border border-white/8">
                                        <div class="text-[11px] uppercase tracking-[0.18em] text-slate-500 font-bold">Nguồn chạy</div>
                                        <div id="schedule-track-source" class="mt-2 text-sm font-black text-slate-100">{html.escape(schedule_tracking["last_source_text"])}</div>
                                    </div>
                                    <div class="bg-slate-900/60 rounded-xl px-3 py-3 border border-white/8">
                                        <div class="text-[11px] uppercase tracking-[0.18em] text-slate-500 font-bold">Tab đã chạy</div>
                                        <div id="schedule-track-sheet" class="mt-2 text-sm font-black text-slate-100">{html.escape(schedule_tracking["last_sheet_text"])}</div>
                                    </div>
                                    <div class="bg-slate-900/60 rounded-xl px-3 py-3 border border-white/8">
                                        <div class="text-[11px] uppercase tracking-[0.18em] text-slate-500 font-bold">Link đã quét</div>
                                        <div id="schedule-track-processed" class="mt-2 text-sm font-black text-slate-100">{html.escape(schedule_tracking["last_processed_text"])}</div>
                                    </div>
                                    <div class="bg-slate-900/60 rounded-xl px-3 py-3 border border-white/8">
                                        <div class="text-[11px] uppercase tracking-[0.18em] text-slate-500 font-bold">Thành công / trượt</div>
                                        <div class="mt-2 text-sm font-black text-slate-100"><span id="schedule-track-success">{html.escape(schedule_tracking["last_success_text"])}</span> / <span id="schedule-track-failed">{html.escape(schedule_tracking["last_failed_text"])}</span></div>
                                    </div>
                                </div>
                                <div class="mt-4">
                                    <div class="text-[11px] uppercase tracking-[0.22em] text-slate-500 font-black mb-2">Lịch sử gần nhất</div>
                                    <div id="schedule-track-history" class="grid gap-2">
                                        {schedule_tracking["history_html"]}
                                    </div>
                                </div>
                            </div>
                        </div>
                    </section>
                </div>
            </main>
        </div>
    </body>
    </html>
    """


if __name__ == "__main__":
    host = str(os.getenv("UVICORN_HOST", "0.0.0.0") or "0.0.0.0").strip() or "0.0.0.0"
    try:
        port = int(str(os.getenv("PORT", os.getenv("UVICORN_PORT", "8000")) or "8000").strip() or "8000")
    except Exception:
        port = 8000
    reload_enabled = parse_bool_env(os.getenv("UVICORN_RELOAD", "false"), False)
    uvicorn.run("scraper:app", host=host, port=port, reload=reload_enabled)

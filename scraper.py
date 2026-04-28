import os
import re
import time
import html
import json
import calendar
import hmac
import base64
import hashlib
import secrets
import smtplib
import socket
import ssl
import threading
import unicodedata
import requests
import pandas as pd
import urllib.parse
from contextlib import asynccontextmanager
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


def _configure_process_timezone() -> str:
    # Force server process timezone so datetime.now() follows app timezone everywhere.
    tz_name = (os.getenv("APP_TIMEZONE") or "Asia/Ho_Chi_Minh").strip() or "Asia/Ho_Chi_Minh"
    os.environ["TZ"] = tz_name
    try:
        time.tzset()
    except Exception:
        # tzset is not available on some platforms (e.g. Windows local dev)
        pass
    return tz_name


APP_TIMEZONE = _configure_process_timezone()


@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_scheduler_thread()
    yield


app = FastAPI(lifespan=lifespan)

DASHBOARD_SECTION_IDS = {
    "tong-quan",
    "cau-hinh",
    "bai-dang",
    "cai-dat",
    "lich-tu-dong",
    "theo-doi-lan-chay",
}

# ==========================================
# Cáº¤U HÃŒNH THÃ”NG Sá»
# ==========================================
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "credential.json").strip() or "credential.json"
AUTH_SETTINGS_FILE = "auth_settings.json"
SESSION_COOKIE_NAME = "social_monitor_session"
OTP_LENGTH = 6
OTP_REQUEST_COOLDOWN_SECONDS = 30
DEFAULT_SHEET_ID = os.getenv("DEFAULT_SHEET_ID", "").strip()
ACTIVE_SHEET_ID = DEFAULT_SHEET_ID
DEFAULT_SHEET_NAME = os.getenv("DEFAULT_SHEET_NAME", "").strip()
ACTIVE_SHEET_NAME = DEFAULT_SHEET_NAME
ACTIVE_SHEET_GID = "0"
ENABLE_HIGHLIGHT_ON_FAILED_SCRAPE = os.getenv("ENABLE_HIGHLIGHT_ON_FAILED_SCRAPE", "false").strip().lower() in {"1", "true", "yes", "on"}
def save_sheet_tabs_cache(cache_data):
    try:
        with open(SHEET_TABS_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache_data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def load_sheet_tabs_cache():
    if os.path.exists(SHEET_TABS_CACHE_FILE):
        try:
            with open(SHEET_TABS_CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_sheet_data_cache(cache_data):
    try:
        with open(SHEET_DATA_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache_data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def load_sheet_data_cache():
    if os.path.exists(SHEET_DATA_CACHE_FILE):
        try:
            with open(SHEET_DATA_CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_dashboard_cache(cache_data):
    try:
        with open(DASHBOARD_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache_data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def load_dashboard_cache():
    if os.path.exists(DASHBOARD_CACHE_FILE):
        try:
            with open(DASHBOARD_CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

SHEET_TABS_CACHE_FILE = "sheet_tabs_cache.json"
SHEET_TABS_CACHE = load_sheet_tabs_cache()
SHEET_TABS_CACHE_TTL_SECONDS = 600  # Increased to 10 minutes
SHEET_DATA_CACHE_FILE = "sheet_data_cache.json"
SHEET_DATA_CACHE = load_sheet_data_cache()  # Persistence for sheet.get_all_values()
SHEET_DATA_CACHE_TTL_SECONDS = 300  # 5 minutes
SHEET_LAYOUT_CACHE = {}  # {sheet_id:sheet_name -> {"updated_at": iso, "layout": {...}}}
SHEET_LAYOUT_CACHE_TTL_SECONDS = 300
DASHBOARD_CACHE_FILE = "dashboard_cache.json"
DASHBOARD_CACHE = load_dashboard_cache()  # Memory cache mirrored by file
DASHBOARD_CACHE_TTL_SECONDS = 300
SHEET_TABS_REQUEST_LIMITER = {}  # {sheet_id: last_request_time}
SHEET_TABS_MIN_INTERVAL_SECONDS = 5  # Minimum interval between requests for same sheet
BOOTSTRAP_ADMIN_EMAIL = os.getenv("AUTH_BOOTSTRAP_ADMIN_EMAIL", "").strip()
# Gmail SMTP - điền trực tiếp vào đây nếu muốn cấu hình OTP ngay trong code.
# Nếu để trống, app mới fallback sang biến môi trường cùng tên.
GMAIL_SMTP_EMAIL = "fanscom.ecom@gmail.com"
GMAIL_SMTP_APP_PASSWORD = "btqtzotpeyhnzzac"
GMAIL_SMTP_FROM_EMAIL = "fanscom.ecom@gmail.com"
YOUTUBE_API_KEY = "AIzaSyAbMDEzmIVpsVTASYhTaXI6oC7BudQWzlU"
ROW_SCAN_DELAY_SECONDS = float(os.getenv("ROW_SCAN_DELAY_SECONDS", "0.0"))
try:
    START_ROW = max(2, int(os.getenv("START_ROW", "2")))
except Exception:
    START_ROW = 2
try:
    MAX_SAVED_SHEETS_PER_USER = max(50, int(os.getenv("MAX_SAVED_SHEETS_PER_USER", "200")))
except Exception:
    MAX_SAVED_SHEETS_PER_USER = 200
try:
    MAX_CAMPAIGNS_PER_USER = max(20, int(os.getenv("MAX_CAMPAIGNS_PER_USER", "200")))
except Exception:
    MAX_CAMPAIGNS_PER_USER = 200

# Quản lý trạng thái
is_running = False
is_finished = False
current_task = "Đang chờ lệnh"
logs = []
pending_updates = []
COLUMN_OVERRIDES = {"date": None, "air_date": None, "link": None, "view": None, "like": None, "share": None, "comment": None, "buzz": None, "save": None}
COLUMN_CONFIG_APPROVAL = {
    "approved": False,
    "sheet_id": "",
    "sheet_name": "",
    "approved_at_text": "",
}
DASHBOARD_DATE_KEYS = ("date", "time", "timestamp", "ngay", "ngayquet", "thoigian", "thoigianquet")
HEADER_ALIASES = {
    "date": {
        "date", "time", "timestamp", "ngay", "ngayquet", "thoigian", "thoigianquet",
        "scanat", "scandate", "updatedat", "lastupdated", "lastscan",
        "publishdate", "createdat", "ngaydangtai", "ngaytao"
    },
    "air_date": {
        "air", "aired", "airdate", "aireddate", "ngayair", "ngaydang", "ngayairbai", "ngaydangbai",
        "postingdate", "publish", "publishedat", "ngaydangbai"
    },
    "link": {
        "link", "url", "posturl", "postlink", "linkpost", "videolink", "contentlink",
        "urlpost", "linkbaidang", "duonglink", "urlvideo"
    },
    "campaign": {
        "campaign", "chiendich", "camp", "tenchiendich", "campaignname",
        "tencampaign", "project", "bookingitem"
    },
    "view": {
        "view", "views", "viewcount", "luotxem", "luotview", "xem",
        "play", "plays", "reach", "impression", "impressions"
    },
    "like": {
        "like", "likes", "reaction", "reactions", "react", "reacts",
        "thich", "tim", "thatim", "tym", "love", "likecount"
    },
    "share": {"share", "shares", "sharecount", "chiase", "luotchiase"},
    "comment": {"comment", "comments", "commentcount", "cmt", "reply", "replies", "binhluan", "luotbinhluan"},
    "buzz": {
        "buzz", "buzzcount", "totalbuzz", "tongbuzz", "commentshare", "sharecomment",
        "commentshares", "sharecomments", "binhluanchiase", "chiasebinhluan",
        "binhluanvachiase", "chiasevabinhluan"
    },
    "save": {
        "save", "saves", "saved", "bookmark", "bookmarks", "luu", "collect", "collectcount",
        "favorite", "favourite", "luotluu"
    },
    "platform": {"platform", "nentang"},
    "caption": {"caption", "title", "mota", "noidung"},
    "plan": {"plan", "nam", "period", "fiscalyear"},
    "line_product": {"lineproduct", "sanpham", "nhanhang", "line", "product"},
    "kol_tier": {"koltier", "tier", "phanloaikol", "kolevel"},
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
WEEKDAY_SHORT_NAMES = ["T2", "T3", "T4", "T5", "T6", "T7", "CN"]
last_schedule_run_key = ""
scheduler_thread = None
scheduler_stop_event = threading.Event()
USER_RUNTIME_LOCK = threading.RLock()
USER_RUNTIME_STATES = {}


def build_default_runtime_state(owner_email: str = ""):
    return {
        "owner_email": str(owner_email or "").strip().lower(),
        "active_sheet_id": DEFAULT_SHEET_ID,
        "active_sheet_name": DEFAULT_SHEET_NAME,
        "active_sheet_gid": "0",
        "is_running": False,
        "is_finished": False,
        "current_task": "Đang chờ lệnh",
        "logs": [],
        "pending_updates": [],
        "column_overrides": dict(COLUMN_OVERRIDES),
        "column_overrides_by_tab": {},
        "column_config_approval": dict(COLUMN_CONFIG_APPROVAL),
        "start_row": START_ROW,
        "schedule_mode": "off",
        "schedule_time": "09:00",
        "schedule_weekday": 0,
        "schedule_monthday": 1,
        "schedule_end_date": "",
        "schedule_sheet_id": "",
        "schedule_sheet_name": "",
        "schedule_sheet_gid": "0",
        "schedule_targets": [],
        "schedule_entries": [],
        "active_schedule_key": "",
        "schedule_tracking_key": "",
        "run_started_at": None,
        "run_source": "idle",
        "schedule_last_run_started_at": None,
        "schedule_last_run_finished_at": None,
        "schedule_last_run_duration_seconds": 0.0,
        "schedule_last_run_status": "idle",
        "schedule_last_run_source": "",
        "schedule_last_run_sheet_name": "",
        "schedule_last_run_processed": 0,
        "schedule_last_run_success": 0,
        "schedule_last_run_failed": 0,
        "schedule_run_history": [],
        "last_schedule_run_key": "",
        "run_progress_current": 0,
        "run_progress_total": 0,
        "run_progress_phase": "idle",
        "tab_progress": {},
        "selected_tabs": [],
        "tab_config_active_key": "",
        "_schedule_hydrated": False,
    }


SYSTEM_RUNTIME_STATE = build_default_runtime_state()


def get_runtime_owner_email(user_or_email=None) -> str:
    if isinstance(user_or_email, dict):
        return normalize_email_address(user_or_email.get("email", ""))
    return normalize_email_address(str(user_or_email or ""))


def get_runtime_state(user_or_email=None):
    owner_email = get_runtime_owner_email(user_or_email)
    if not owner_email:
        ensure_runtime_state_shape(SYSTEM_RUNTIME_STATE)
        return SYSTEM_RUNTIME_STATE
    with USER_RUNTIME_LOCK:
        runtime_state = USER_RUNTIME_STATES.get(owner_email)
        if runtime_state is None:
            runtime_state = build_default_runtime_state(owner_email)
            USER_RUNTIME_STATES[owner_email] = runtime_state
        ensure_runtime_state_shape(runtime_state)
        if not runtime_state.get("_schedule_hydrated") and "hydrate_runtime_state_from_saved_schedules" in globals():
            hydrate_runtime_state_from_saved_schedules(runtime_state)
        return runtime_state


def resolve_runtime_state(state=None):
    if isinstance(state, dict) and "logs" in state and "active_sheet_id" in state:
        ensure_runtime_state_shape(state)
        return state
    return get_runtime_state(state)


def ensure_runtime_state_shape(runtime_state):
    if not isinstance(runtime_state, dict):
        return runtime_state
    column_overrides = runtime_state.get("column_overrides")
    if not isinstance(column_overrides, dict):
        runtime_state["column_overrides"] = dict(COLUMN_OVERRIDES)
    else:
        for field, default_value in COLUMN_OVERRIDES.items():
            column_overrides.setdefault(field, default_value)
    if not isinstance(runtime_state.get("column_overrides_by_tab"), dict):
        runtime_state["column_overrides_by_tab"] = {}
    column_config_approval = runtime_state.get("column_config_approval")
    if not isinstance(column_config_approval, dict):
        runtime_state["column_config_approval"] = dict(COLUMN_CONFIG_APPROVAL)
    else:
        for field, default_value in COLUMN_CONFIG_APPROVAL.items():
            column_config_approval.setdefault(field, default_value)
    runtime_state.setdefault("run_progress_current", 0)
    runtime_state.setdefault("run_progress_total", 0)
    runtime_state.setdefault("run_progress_phase", "idle")
    runtime_state.setdefault("tab_progress", {})
    return runtime_state


def iter_runtime_states():
    if "ensure_runtime_states_for_saved_schedules" in globals():
        ensure_runtime_states_for_saved_schedules()
    with USER_RUNTIME_LOCK:
        return list(USER_RUNTIME_STATES.values())

def normalize_email_address(value: str) -> str:
    return (value or "").strip().lower()


def normalize_saved_sheet_entries(raw_items):
    entries = []
    seen = set()
    for item in raw_items if isinstance(raw_items, list) else []:
        if not isinstance(item, dict):
            continue
        sheet_id = str(item.get("sheet_id", "") or "").strip()
        sheet_name = str(item.get("sheet_name", "") or "").strip()
        if not sheet_id or not sheet_name:
            continue
        key = f"{sheet_id}::{sheet_name}".lower()
        if key in seen:
            continue
        seen.add(key)
        entries.append({
            "sheet_id": sheet_id,
            "sheet_name": sheet_name,
            "sheet_gid": str(item.get("sheet_gid", "") or "0").strip() or "0",
            "saved_at_text": str(item.get("saved_at_text", "") or "").strip(),
            "campaign_label": str(item.get("campaign_label", "") or "").strip(),
            "brand_label": str(item.get("brand_label", "") or "").strip(),
            "industry_label": str(item.get("industry_label", "") or "").strip(),
            "campaign_description": str(item.get("campaign_description", "") or "").strip(),
        })
    return entries


def normalize_campaign_labels(raw_items):
    labels = []
    seen = set()
    for item in raw_items if isinstance(raw_items, list) else []:
        label = str(item or "").strip()
        if not label:
            continue
        key = label.lower()
        if key in seen:
            continue
        seen.add(key)
        labels.append(label)
    return labels


def normalize_notification_preferences_map(raw_map, users):
    normalized = {}
    source = raw_map if isinstance(raw_map, dict) else {}
    allowed_emails = {
        normalize_email_address(item.get("email", ""))
        for item in users if isinstance(item, dict)
    }
    for email, prefs in source.items():
        normalized_email = normalize_email_address(email)
        if not normalized_email or (allowed_emails and normalized_email not in allowed_emails):
            continue
        prefs = prefs if isinstance(prefs, dict) else {}
        normalized[normalized_email] = {
            "email_notifications": bool(prefs.get("email_notifications", True)),
            "system_alerts": bool(prefs.get("system_alerts", True)),
        }
    for email in allowed_emails:
        if not email:
            continue
        normalized.setdefault(email, {
            "email_notifications": True,
            "system_alerts": True,
        })
    return normalized

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

def build_default_auth_settings():
    return {
        "session_secret": secrets.token_hex(32),
        "otp_ttl_seconds": 300,
        "session_ttl_seconds": 86400,
        "users": [],
        "user_meta": {},
        "saved_sheets": [],
        "saved_sheets_by_user": {},
        "schedule_entries": [],
        "schedule_entries_by_user": {},
        "campaigns": [],
        "campaigns_by_user": {},
        "notification_preferences_by_user": {},
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
    settings["saved_sheets"] = normalize_saved_sheet_entries((data or {}).get("saved_sheets", []) if isinstance(data, dict) else [])
    raw_saved_sheets_by_user = (data or {}).get("saved_sheets_by_user", {}) if isinstance(data, dict) else {}
    normalized_saved_sheets_by_user = {}
    if isinstance(raw_saved_sheets_by_user, dict):
        for email, entries in raw_saved_sheets_by_user.items():
            normalized_email = normalize_email_address(email)
            if not normalized_email:
                continue
            normalized_saved_sheets_by_user[normalized_email] = normalize_saved_sheet_entries(entries)
    if settings["saved_sheets"] and not normalized_saved_sheets_by_user:
        migration_owner = next((item["email"] for item in settings["users"] if item.get("role") == "admin"), "")
        migration_owner = normalize_email_address(migration_owner) or next(iter(FORCED_ADMIN_EMAILS), "")
        if migration_owner:
            normalized_saved_sheets_by_user[migration_owner] = [dict(item) for item in settings["saved_sheets"]]
    settings["saved_sheets_by_user"] = normalized_saved_sheets_by_user
    settings["schedule_entries"] = normalize_persisted_schedule_entries((data or {}).get("schedule_entries", []) if isinstance(data, dict) else [])
    raw_schedule_entries_by_user = (data or {}).get("schedule_entries_by_user", {}) if isinstance(data, dict) else {}
    normalized_schedule_entries_by_user = {}
    if isinstance(raw_schedule_entries_by_user, dict):
        for email, entries in raw_schedule_entries_by_user.items():
            normalized_email = normalize_email_address(email)
            if not normalized_email:
                continue
            normalized_schedule_entries_by_user[normalized_email] = normalize_persisted_schedule_entries(entries)
    if settings["schedule_entries"] and not normalized_schedule_entries_by_user:
        migration_owner = next((item["email"] for item in settings["users"] if item.get("role") == "admin"), "")
        migration_owner = normalize_email_address(migration_owner) or next(iter(FORCED_ADMIN_EMAILS), "")
        if migration_owner:
            normalized_schedule_entries_by_user[migration_owner] = [dict(item) for item in settings["schedule_entries"]]
    settings["schedule_entries_by_user"] = normalized_schedule_entries_by_user
    settings["campaigns"] = normalize_campaign_labels((data or {}).get("campaigns", []) if isinstance(data, dict) else [])
    raw_campaigns_by_user = (data or {}).get("campaigns_by_user", {}) if isinstance(data, dict) else {}
    normalized_campaigns_by_user = {}
    if isinstance(raw_campaigns_by_user, dict):
        for email, labels in raw_campaigns_by_user.items():
            normalized_email = normalize_email_address(email)
            if not normalized_email:
                continue
            normalized_campaigns_by_user[normalized_email] = normalize_campaign_labels(labels)
    settings["campaigns_by_user"] = normalized_campaigns_by_user
    settings["notification_preferences_by_user"] = normalize_notification_preferences_map(
        (data or {}).get("notification_preferences_by_user", {}) if isinstance(data, dict) else {},
        settings["users"],
    )

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
    with open(AUTH_SETTINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(normalize_auth_settings(settings), f, ensure_ascii=False, indent=2)

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
        save_auth_settings(settings)
    return settings

def persist_auth_settings(settings):
    global AUTH_SETTINGS
    AUTH_SETTINGS = normalize_auth_settings(settings)
    save_auth_settings(AUTH_SETTINGS)
    return AUTH_SETTINGS

def background_refresh_dashboard_data(user_email, section_type):
    """
    section_type: 'overview', 'posts', 'config', or 'schedule'
    This runs in a background thread/task.
    """
    try:
        auth_data = load_auth_settings()
        user_list = auth_data.get("users", [])
        target_user = next((u for u in user_list if u.get("email") == user_email), None)
        if not target_user:
            return
        
        runtime_state = get_runtime_state(target_user)
        active_ws = None
        if runtime_state.get("active_sheet_id") and runtime_state.get("active_sheet_name"):
            try:
                time.sleep(0.5)  # Stagger API calls to stay within Google Sheets quota
                active_ws = get_worksheet(runtime_state["active_sheet_name"], runtime_state["active_sheet_id"], runtime_state)
            except Exception:
                pass
        
        now_str = datetime.now().isoformat()
        full_cache = load_dashboard_cache()
        
        if section_type == "overview":
            html_content = build_overview_panel_for_state(runtime_state, sheet=active_ws)
            full_cache[f"{user_email}:overview"] = {"updated_at": now_str, "html": html_content}
        elif section_type == "posts":
            html_content = build_posts_panel_html(active_ws, runtime_state)
            full_cache[f"{user_email}:posts"] = {"updated_at": now_str, "html": html_content}
        elif section_type == "config":
            # For the config panel, we just return the metric_cols_html block
            column_config = build_column_config_payload(active_ws, runtime_state)
            status_payload = build_status_payload(runtime_state)
            snapshot_url = build_snapshot_url(state=runtime_state)
            metric_cols_html = f"""
                <div class="bg-black/20 rounded-3xl p-6 mb-6 border border-white/5">
                    <div class="mb-3 text-sm font-bold text-slate-500 uppercase">
                        <span>Thiết lập quét</span>
                    </div>
                    <div class="grid grid-cols-1 xl:grid-cols-[minmax(0,1.16fr)_minmax(380px,0.84fr)] gap-4 items-start">
                        <div class="space-y-4">
                            <div class="bg-slate-950/40 rounded-2xl p-4 border border-white/10 mb-5">
                                <div class="space-y-4">
                                    <form action="/set-sheet" method="get" class="flex flex-col gap-3">
                                        <label class="text-xs font-black uppercase tracking-[0.16em] text-slate-400">Nhập link sheet</label>
                                        <div class="flex flex-col md:flex-row gap-2 md:items-center">
                                            <input id="sheet-url-input" name="sheet_url" value="{snapshot_url}" placeholder="Nhập link Google Sheet hoặc Sheet ID" class="w-full bg-slate-900 text-slate-100 rounded-xl px-4 py-3 border border-white/10 outline-none focus:border-blue-400" />
                                            <button type="submit" class="w-full md:w-auto md:shrink-0 px-4 py-3 rounded-xl border border-emerald-400/30 bg-emerald-500/15 hover:bg-emerald-500/25 text-emerald-200 text-xs font-black tracking-[0.12em] uppercase transition-all">
                                                Lưu sheet
                                            </button>
                                        </div>
                                        <input id="sheet-name-input" name="sheet_name" value="{runtime_state['active_sheet_name']}" list="sheet-name-options" autocomplete="off" class="hidden" />
                                        <datalist id="sheet-name-options"></datalist>
                                        <div id="sheet-tabs-state" class="text-xs text-slate-500">Dán link Google Sheet để hiện danh sách tab có trong file.</div>
                                        <div id="sheet-tabs-list" class="sheet-tabs-list hidden"></div>
                                    </form>
                                </div>
                            </div>
                            <div class="flex flex-col md:flex-row md:items-center md:justify-between gap-2 mb-3 text-sm font-bold text-slate-500 uppercase">
                                <span>Cột nhập liệu</span>
                                <div class="flex items-center gap-3">
                                    <button id="auto-fill-columns-btn" type="button" class="px-4 py-1.5 rounded-lg border border-cyan-400/30 bg-cyan-500/10 hover:bg-cyan-500/20 text-cyan-200 text-xs font-black tracking-wider transition-all normal-case">AUTO</button>
                                </div>
                            </div>
                            <div id="col-config-tab-bar" class="hidden flex-wrap gap-1 mb-0"></div>
                            <div id="col-config-apply-note" class="text-xs text-cyan-200/80 bg-cyan-500/10 border border-cyan-500/20 rounded-xl px-3 py-2 mb-3">
                                Chọn nhiều tab ở danh sách phía trên để cấu hình từng tab riêng.
                            </div>
                            <div class="grid grid-cols-1 md:grid-cols-2 gap-3 mb-4">
                                {''.join([
                                    f'''
                                    <div>
                                        <div class="flex items-center justify-between gap-3 mb-2">
                                            <label class="block text-xs text-slate-400 uppercase tracking-wider">{
                                                "Cột ngày quét" if field == "date" else
                                                "Cột ngày air bài" if field == "air_date" else
                                                "Cột Link" if field == "link" else
                                                "Cột Buzz (Buzz = Comment + Share)" if field == "buzz" else
                                                "Cột View" if field == "view" else
                                                "Cột Like" if field == "like" else
                                                "Cột Share" if field == "share" else
                                                "Cột Comment" if field == "comment" else
                                                "Cột Save" if field == "save" else
                                                "Dòng bắt đầu"
                                            }</label>
                                            <span class="text-[11px] text-slate-500 font-black uppercase tracking-[0.18em]" data-column-source="{field}">{column_config["input_sources"].get(field, "CHƯA THẤY")}</span>
                                        </div>
                                        <input name="{field}" form="set-columns-form" data-column-input="{field}" data-detected-value="{column_config["detected_inputs"].get(field, '')}" data-manual-value="{column_config["manual_inputs"].get(field, '')}" value="{column_config["input_values"].get(field, '')}" placeholder="{
                                            "VD: A hoặc 1" if field == "date" else
                                            "VD: B hoặc 2" if field == "air_date" else
                                            "VD: D hoặc 4" if field == "link" else
                                            "VD: C hoặc 3" if field == "buzz" else
                                            "VD: E hoặc 5" if field == "view" else
                                            "VD: F hoặc 6" if field == "like" else
                                            "VD: G hoặc 7" if field == "share" else
                                            "VD: H hoặc 8" if field == "comment" else
                                            "VD: I hoặc 9" if field == "save" else
                                            "VD: 2"
                                        }" class="w-full bg-slate-900 text-slate-100 rounded-xl px-4 py-3 border border-white/10 outline-none focus:border-cyan-400" />
                                    </div>
                                    ''' for field in ["date", "air_date", "link", "buzz", "view", "like", "share", "comment", "save"]
                                ])}
                                <div>
                                    <div class="flex items-center justify-between gap-3 mb-2">
                                        <label class="block text-xs text-slate-400 uppercase tracking-wider">Dòng bắt đầu</label>
                                        <span class="text-[11px] text-slate-500 font-black uppercase tracking-[0.18em]">ĐANG DÙNG</span>
                                    </div>
                                    <input name="start_row" form="set-columns-form" value="{runtime_state.get('start_row', 2)}" inputmode="numeric" placeholder="VD: 2" class="w-full bg-slate-900 text-slate-100 rounded-xl px-4 py-3 border border-white/10 outline-none focus:border-cyan-400" />
                                </div>
                            </div>
                            <div class="text-xs text-emerald-200/85 bg-emerald-500/10 border border-emerald-500/20 rounded-xl px-3 py-2 mb-3" data-column-detected-text>
                                {column_config['detected_text']}
                            </div>
                            <div class="text-xs text-cyan-200/80 bg-cyan-500/10 border border-cyan-500/20 rounded-xl px-3 py-2 mb-3">
                                Các ô đã tự hiện cột đang nhận. Bạn sửa trực tiếp ngay trong đó nếu cần. Xóa trống ô nào thì ô đó quay về AUTO theo header của sheet. Buzz được tính bằng Comment + Share. Dòng bắt đầu nhận số từ 2 trở lên.
                            </div>
                            <form id="set-columns-form" action="/set-columns" method="get" class="mb-4">
                                <input id="col-config-active-tab-input" type="hidden" name="tab_name" value="" />
                                <button type="submit" class="w-full py-3 bg-slate-700 hover:bg-slate-600 text-white rounded-xl font-black uppercase text-sm shadow-sm shadow-slate-900/10">Lưu cấu hình nhập liệu</button>
                            </form>
                        </div>
                        <div class="xl:sticky xl:top-6">
                            <div class="bg-slate-900/55 rounded-2xl p-4 border border-white/10 mb-4">
                                <div class="flex justify-between items-center mb-2 text-xs font-bold text-slate-500 uppercase">
                                    <span>Quét dữ liệu</span><span class="text-blue-300">Thao tác nhanh</span>
                                </div>
                                <div class="text-xs uppercase tracking-[0.22em] font-bold text-slate-500 mb-2">Tiến trình hiện tại</div>
                                <div id="current-task" class="text-lg font-black text-slate-100">{status_payload['current_task']}</div>
                                <div class="w-full bg-slate-800/80 rounded-full h-3 overflow-hidden mt-4 mb-4">
                                    <div id="progress-bar" class="bg-blue-500 h-full transition-all duration-1000" style="width: {status_payload['progress_width']}"></div>
                                </div>
                                <div class="flex items-center justify-between gap-3 mt-4 mb-3">
                                    <div class="text-xs uppercase tracking-[0.22em] font-black text-sky-300">Tiến trình từng tab</div>
                                    <div id="progress-text" class="text-xs text-slate-400 font-bold">{status_payload.get('progress_text', '')}</div>
                                </div>
                                <div id="tab-progress-section" class="hidden space-y-3 mb-4"></div>
                                <div id="primary-action">{status_payload['primary_action_html']}</div>
                            </div>
                            <div class="bg-slate-950/40 rounded-2xl p-4 border border-white/10">
                                <div class="flex justify-between items-center mb-3 text-sm font-bold text-slate-500 uppercase">
                                    <span>Nhật ký hệ thống</span><span class="text-slate-400">Cập nhật realtime</span>
                                </div>
                                <div id="log-section" class="bg-black/40 rounded-2xl p-4 h-[42vh] min-h-[320px] max-h-[560px] overflow-y-auto border border-white/5 shadow-inner font-mono italic text-sm">
                                    {build_log_html(runtime_state)}
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            """
            full_cache[f"{user_email}:config"] = {"updated_at": now_str, "html": metric_cols_html}
        elif section_type == "schedule":
            schedule_text = schedule_label(runtime_state)
            schedule_config = build_schedule_config_payload(runtime_state)
            mode_selected = {
                "off": "selected" if runtime_state["schedule_mode"] == "off" else "",
                "daily": "selected" if runtime_state["schedule_mode"] == "daily" else "",
                "weekly": "selected" if runtime_state["schedule_mode"] == "weekly" else "",
                "monthly": "selected" if runtime_state["schedule_mode"] == "monthly" else "",
            }
            weekday_options = "".join(
                [
                    f'<option value="{idx}" {"selected" if runtime_state["schedule_weekday"] == idx else ""}>{name}</option>'
                    for idx, name in enumerate(WEEKDAY_NAMES)
                ]
            )
            schedule_html = f"""
                <div class="dashboard-section-title">Lịch tự động</div>
                <div class="bg-black/20 rounded-3xl p-6 border border-white/5">
                    <div class="flex justify-between items-center mb-3 text-sm font-bold text-slate-500 uppercase">
                        <span>Lịch tự động</span><span class="text-cyan-300 font-black text-lg" data-schedule-label>{schedule_text}</span>
                    </div>
                    <form action="/set-schedule" method="get" class="flex flex-col gap-3">
                        <div>
                            <label class="block text-xs text-slate-400 mb-2 uppercase tracking-wider">Sheet áp dụng cho lịch</label>
                            <input id="schedule-sheet-search" type="text" list="schedule-sheet-datalist" autocomplete="off" placeholder="Gõ để tìm hoặc chọn sheet..." value="{html.escape(runtime_state.get('schedule_sheet_name', ''), quote=True)}" class="w-full bg-slate-900 text-slate-100 rounded-xl px-4 py-3 border border-white/10 outline-none focus:border-cyan-400" />
                            <datalist id="schedule-sheet-datalist"></datalist>
                            <select id="schedule-sheet-select" name="sheet_binding" class="hidden">
                                {schedule_config.get('sheet_options_html', '')}
                            </select>
                            <p class="mt-1 text-[11px] text-slate-500">Gõ tên sheet ở ngay ô này để hiện gợi ý, rồi chọn luôn trong cùng một dòng.</p>
                        </div>
                        <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
                            <div>
                                <label class="block text-xs text-slate-400 mb-2 uppercase tracking-wider">Chế độ chạy</label>
                                <select id="schedule-mode-select" name="mode" class="w-full bg-slate-900 text-slate-100 rounded-xl px-4 py-3 border border-white/10 outline-none focus:border-cyan-400">
                                    <option value="off" {mode_selected.get('off', '')}>Chưa bật</option>
                                    <option value="daily" {mode_selected.get('daily', '')}>Hằng ngày</option>
                                    <option value="weekly" {mode_selected.get('weekly', '')}>Hằng tuần</option>
                                    <option value="monthly" {mode_selected.get('monthly', '')}>Hằng tháng</option>
                                </select>
                            </div>
                            <div>
                                <label class="block text-xs text-slate-400 mb-2 uppercase tracking-wider">Giờ chạy (HH:MM)</label>
                                <input name="at" value="{runtime_state.get('schedule_at', '')}" placeholder="VD: 09:00" class="w-full bg-slate-900 text-slate-100 rounded-xl px-4 py-3 border border-white/10 outline-none focus:border-cyan-400" />
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
                                    <input id="schedule-monthdate-input" name="monthdate" type="text" value="{runtime_state.get('schedule_monthdate', '')}" placeholder="YYYY-MM-DD" class="flex-1 bg-slate-900 text-slate-100 rounded-xl px-4 py-3 border border-white/10 outline-none focus:border-cyan-400" />
                                    <button id="monthdate-picker-btn" type="button" class="date-picker-btn" title="Mở lịch">
                                        <i class="fa-solid fa-calendar-days"></i>
                                    </button>
                                </div>
                                <input name="monthday" type="hidden" value="{runtime_state.get('schedule_monthday', '1')}" />
                                <p id="schedule-monthdate-help" class="mt-1 text-[11px] text-slate-500">Hằng tháng: chọn ngày chạy. Hằng tuần: mở lịch để xem các ngày của thứ đã chọn được khoanh sẵn.</p>
                            </div>
                        </div>
                        <div class="grid grid-cols-1 gap-3">
                            <div>
                                <label class="block text-xs text-slate-400 mb-2 uppercase tracking-wider">Ngày kết thúc vòng lặp</label>
                                <div class="date-shell">
                                    <input id="schedule-enddate-input" name="enddate" type="text" value="{runtime_state.get('schedule_enddate', '')}" placeholder="YYYY-MM-DD" class="flex-1 bg-slate-900 text-slate-100 rounded-xl px-4 py-3 border border-white/10 outline-none focus:border-cyan-400" />
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
                                <div class="text-[11px] uppercase tracking-[0.22em] text-slate-400 font-black">Theo dõi lần chạy</div>
                                <div class="text-xs text-slate-500">Tự cập nhật theo lịch và khi bấm chạy tay</div>
                            </div>
                            <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-5 gap-3">
                                <div class="bg-slate-900/60 rounded-xl px-3 py-3 border border-white/8">
                                    <div class="text-[11px] uppercase tracking-[0.18em] text-slate-500 font-bold">Lần kế tiếp</div>
                                    <div id="schedule-track-next" class="mt-2 text-sm font-black text-slate-100">{schedule_config.get('next_run_text', 'Chưa có')}</div>
                                </div>
                                <div class="bg-slate-900/60 rounded-xl px-3 py-3 border border-white/8">
                                    <div class="text-[11px] uppercase tracking-[0.18em] text-slate-500 font-bold">Bắt đầu gần nhất</div>
                                    <div id="schedule-track-started" class="mt-2 text-sm font-black text-slate-100">{schedule_config.get('last_started_text', 'Chưa có')}</div>
                                </div>
                                <div class="bg-slate-900/60 rounded-xl px-3 py-3 border border-white/8">
                                    <div class="text-[11px] uppercase tracking-[0.18em] text-slate-500 font-bold">Kết thúc gần nhất</div>
                                    <div id="schedule-track-finished" class="mt-2 text-sm font-black text-slate-100">{schedule_config.get('last_finished_text', 'Chưa có')}</div>
                                </div>
                                <div class="bg-slate-900/60 rounded-xl px-3 py-3 border border-white/8">
                                    <div class="text-[11px] uppercase tracking-[0.18em] text-slate-500 font-bold">Thời lượng</div>
                                    <div id="schedule-track-duration" class="mt-2 text-sm font-black text-cyan-200">{schedule_config.get('last_duration_text', '0s')}</div>
                                </div>
                                <div class="bg-slate-900/60 rounded-xl px-3 py-3 border border-white/8">
                                    <div class="text-[11px] uppercase tracking-[0.18em] text-slate-500 font-bold">Đang chạy từ</div>
                                    <div id="schedule-track-running" class="mt-2 text-sm font-black text-slate-100">{schedule_config.get('is_running_text', 'Đang chờ')}</div>
                                </div>
                            </div>
                            <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-5 gap-3 mt-3">
                                <div class="bg-slate-900/60 rounded-xl px-3 py-3 border border-white/8">
                                    <div class="text-[11px] uppercase tracking-[0.18em] text-slate-500 font-bold">Trạng thái</div>
                                    <div id="schedule-track-status" class="mt-2 text-sm font-black text-slate-100">{schedule_config.get('last_status_text', 'Chưa chạy')}</div>
                                </div>
                                <div class="bg-slate-900/60 rounded-xl px-3 py-3 border border-white/8">
                                    <div class="text-[11px] uppercase tracking-[0.18em] text-slate-500 font-bold">Nguồn chạy</div>
                                    <div id="schedule-track-source" class="mt-2 text-sm font-black text-slate-100">{schedule_config.get('last_source_text', 'Chưa có')}</div>
                                </div>
                                <div class="bg-slate-900/60 rounded-xl px-3 py-3 border border-white/8">
                                    <div class="text-[11px] uppercase tracking-[0.18em] text-slate-500 font-bold">Tab đã chạy</div>
                                    <div id="schedule-track-sheet" class="mt-2 text-sm font-black text-slate-100">{schedule_config.get('last_sheet_text', 'Chưa có')}</div>
                                </div>
                                <div class="bg-slate-900/60 rounded-xl px-3 py-3 border border-white/8">
                                    <div class="text-[11px] uppercase tracking-[0.18em] text-slate-500 font-bold">Link đã quét</div>
                                    <div id="schedule-track-processed" class="mt-2 text-sm font-black text-slate-100">{schedule_config.get('last_processed_text', '0')}</div>
                                </div>
                                <div class="bg-slate-900/60 rounded-xl px-3 py-3 border border-white/8">
                                    <div class="text-[11px] uppercase tracking-[0.18em] text-slate-500 font-bold">Thành công / trượt</div>
                                    <div class="mt-2 text-sm font-black text-slate-100"><span id="schedule-track-success">{schedule_config.get('last_success_text', '0')}</span> / <span id="schedule-track-failed">{schedule_config.get('last_failed_text', '0')}</span></div>
                                </div>
                            </div>
                            <div class="mt-4">
                                <div class="text-[11px] uppercase tracking-[0.22em] text-slate-500 font-black mb-2">Lịch sử gần nhất</div>
                                <div id="schedule-track-history" class="grid gap-2">
                                    {schedule_config.get('history_html', '<div class="schedule-history-empty">Chưa có lần chạy nào để theo dõi.</div>')}
                                </div>
                            </div>
                        </div>
                        <button type="submit" class="w-full py-3 bg-slate-700 hover:bg-slate-600 text-white rounded-xl font-black uppercase text-sm shadow-sm shadow-slate-900/10">Lưu lịch</button>
                    </form>
                </div>
            """
            full_cache[f"{user_email}:schedule"] = {"updated_at": now_str, "html": schedule_html}
            
        save_dashboard_cache(full_cache)
        global DASHBOARD_CACHE
        DASHBOARD_CACHE.update(full_cache)
    except Exception as e:
        print(f"[REFR-ERR] Failed to background refresh {section_type} for {user_email}: {e}")



def get_auth_settings():
    return AUTH_SETTINGS


def get_saved_sheet_entries(settings=None, owner_email: Optional[str] = None):
    auth_settings = settings or get_auth_settings()
    normalized_owner = normalize_email_address(owner_email or "")
    saved_by_user = auth_settings.get("saved_sheets_by_user", {}) if isinstance(auth_settings, dict) else {}
    if normalized_owner:
        scoped_entries = saved_by_user.get(normalized_owner, []) if isinstance(saved_by_user, dict) else []
        normalized_scoped_entries = normalize_saved_sheet_entries(scoped_entries)
        if normalized_scoped_entries:
            return [dict(item) for item in normalized_scoped_entries]
        # Backward compatibility: older data may still live in shared `saved_sheets`.
        return [dict(item) for item in normalize_saved_sheet_entries(auth_settings.get("saved_sheets", []))]
    return [dict(item) for item in normalize_saved_sheet_entries(auth_settings.get("saved_sheets", []))]


def get_saved_sheet_entry(sheet_id: str, sheet_name: str, settings=None, owner_email: Optional[str] = None):
    normalized_sheet_id = str(sheet_id or "").strip()
    normalized_sheet_name = str(sheet_name or "").strip().lower()
    if not normalized_sheet_id or not normalized_sheet_name:
        return None
    for entry in get_saved_sheet_entries(settings=settings, owner_email=owner_email):
        current_sheet_id = str(entry.get("sheet_id", "") or "").strip()
        current_sheet_name = str(entry.get("sheet_name", "") or "").strip().lower()
        if current_sheet_id == normalized_sheet_id and current_sheet_name == normalized_sheet_name:
            return dict(entry)
    return None


def build_campaign_options_html(selected_label: str = "", settings=None, owner_email: Optional[str] = None) -> str:
    selected_label = str(selected_label or "").strip()
    normalized_selected = selected_label.lower()
    option_labels = list(get_saved_campaign_labels(settings=settings, owner_email=owner_email))
    option_parts = ['<option value="">Chọn chiến dịch đã tạo sẵn</option>']
    option_parts.extend(
        f'<option value="{html.escape(label, quote=True)}" {"selected" if label.lower() == normalized_selected else ""}>{html.escape(label)}</option>'
        for label in option_labels
    )
    return "".join(option_parts)


def build_sheet_metadata_payload(sheet_id: str = "", sheet_name: str = "", state=None):
    runtime_state = resolve_runtime_state(state)
    resolved_sheet_id = str(sheet_id or runtime_state.get("active_sheet_id", "") or "").strip()
    resolved_sheet_name = str(sheet_name or runtime_state.get("active_sheet_name", "") or "").strip()
    saved_entry = get_saved_sheet_entry(
        resolved_sheet_id,
        resolved_sheet_name,
        owner_email=runtime_state.get("owner_email", ""),
    )
    can_edit_metadata = bool(resolved_sheet_id and resolved_sheet_name)
    campaign_label = str((saved_entry or {}).get("campaign_label", "") or "").strip()
    brand_label = str((saved_entry or {}).get("brand_label", "") or "").strip()
    campaign_description = str((saved_entry or {}).get("campaign_description", "") or "").strip()
    return {
        "campaign_label": campaign_label,
        "brand_label": brand_label,
        "campaign_description": campaign_description,
        "campaign_options_html": build_campaign_options_html(
            campaign_label,
            owner_email=runtime_state.get("owner_email", ""),
        ),
        "campaign_has_options": bool(get_saved_campaign_labels(owner_email=runtime_state.get("owner_email", ""))),
        "has_saved_metadata": bool(saved_entry),
        "can_edit_metadata": can_edit_metadata,
    }


def get_saved_campaign_labels(settings=None, owner_email: Optional[str] = None):
    auth_settings = settings or get_auth_settings()
    normalized_owner = normalize_email_address(owner_email or "")
    campaigns_by_user = auth_settings.get("campaigns_by_user", {}) if isinstance(auth_settings, dict) else {}
    if normalized_owner:
        scoped_labels = campaigns_by_user.get(normalized_owner, []) if isinstance(campaigns_by_user, dict) else []
        return list(normalize_campaign_labels(scoped_labels))
    return list(normalize_campaign_labels(auth_settings.get("campaigns", [])))


def save_campaign_label(campaign_label: str, owner_email: Optional[str] = None):
    cleaned_label = str(campaign_label or "").strip()
    if not cleaned_label:
        return []
    normalized_owner = normalize_email_address(owner_email or "")
    settings = get_auth_settings().copy()
    existing_labels = get_saved_campaign_labels(settings, owner_email=normalized_owner)
    next_labels = [cleaned_label]
    next_labels.extend(item for item in existing_labels if item.lower() != cleaned_label.lower())
    next_labels = normalize_campaign_labels(next_labels)[:MAX_CAMPAIGNS_PER_USER]
    if normalized_owner:
        campaigns_by_user = dict(settings.get("campaigns_by_user", {}) or {})
        campaigns_by_user[normalized_owner] = next_labels
        settings["campaigns_by_user"] = campaigns_by_user
    else:
        settings["campaigns"] = next_labels
    persist_auth_settings(settings)
    return get_saved_campaign_labels(settings, owner_email=normalized_owner)


def save_sheet_entry(
    sheet_id: str,
    sheet_name: str,
    sheet_gid: str = "0",
    owner_email: Optional[str] = None,
    campaign_label: Optional[str] = None,
    brand_label: Optional[str] = None,
    industry_label: Optional[str] = None,
    campaign_description: Optional[str] = None,
):
    normalized_sheet_id = str(sheet_id or "").strip()
    normalized_sheet_name = str(sheet_name or "").strip()
    if not normalized_sheet_id or not normalized_sheet_name:
        return []
    normalized_owner = normalize_email_address(owner_email or "")
    settings = get_auth_settings().copy()
    existing_entries = get_saved_sheet_entries(settings, owner_email=normalized_owner)
    existing_entry = next(
        (
            item
            for item in existing_entries
            if str(item.get("sheet_id", "")).strip() == normalized_sheet_id
            and str(item.get("sheet_name", "")).strip().lower() == normalized_sheet_name.lower()
        ),
        None,
    )
    entry_key = f"{normalized_sheet_id}::{normalized_sheet_name}".lower()
    saved_at_text = datetime.now().strftime("%d/%m/%Y %H:%M")
    next_entries = [
        {
            "sheet_id": normalized_sheet_id,
            "sheet_name": normalized_sheet_name,
            "sheet_gid": str(sheet_gid or "0").strip() or "0",
            "saved_at_text": saved_at_text,
            "campaign_label": str(
                campaign_label if campaign_label is not None else (existing_entry or {}).get("campaign_label", "")
                or ""
            ).strip(),
            "brand_label": str(
                brand_label if brand_label is not None else (existing_entry or {}).get("brand_label", "")
                or ""
            ).strip(),
            "industry_label": str(
                industry_label if industry_label is not None else (existing_entry or {}).get("industry_label", "")
                or ""
            ).strip(),
            "campaign_description": str(
                campaign_description if campaign_description is not None else (existing_entry or {}).get("campaign_description", "")
                or ""
            ).strip(),
        }
    ]
    next_entries.extend(
        item
        for item in existing_entries
        if f"{str(item.get('sheet_id', '')).strip()}::{str(item.get('sheet_name', '')).strip()}".lower() != entry_key
    )
    if normalized_owner:
        saved_by_user = dict(settings.get("saved_sheets_by_user", {}) or {})
        saved_by_user[normalized_owner] = next_entries[:MAX_SAVED_SHEETS_PER_USER]
        settings["saved_sheets_by_user"] = saved_by_user
    else:
        settings["saved_sheets"] = next_entries[:MAX_SAVED_SHEETS_PER_USER]
    persist_auth_settings(settings)
    return get_saved_sheet_entries(settings, owner_email=normalized_owner)


def update_saved_sheet_campaign(sheet_id: str, sheet_name: str, campaign_label: str = "", owner_email: Optional[str] = None):
    normalized_sheet_id = str(sheet_id or "").strip()
    normalized_sheet_name = str(sheet_name or "").strip()
    if not normalized_sheet_id or not normalized_sheet_name:
        return []
    normalized_owner = normalize_email_address(owner_email or "")
    settings = get_auth_settings().copy()
    existing_entries = get_saved_sheet_entries(settings, owner_email=normalized_owner)
    cleaned_campaign = str(campaign_label or "").strip()
    next_entries = []
    found = False
    for item in existing_entries:
        current_sheet_id = str(item.get("sheet_id", "") or "").strip()
        current_sheet_name = str(item.get("sheet_name", "") or "").strip()
        
        normalized_current_name = unicodedata.normalize('NFC', current_sheet_name).casefold()
        normalized_target_name = unicodedata.normalize('NFC', normalized_sheet_name).casefold()
        
        if current_sheet_id == normalized_sheet_id and normalized_current_name == normalized_target_name:
            found = True
            next_entries.append(
                {
                    "sheet_id": current_sheet_id,
                    "sheet_name": current_sheet_name,
                    "sheet_gid": str(item.get("sheet_gid", "") or "0").strip() or "0",
                    "saved_at_text": str(item.get("saved_at_text", "") or "").strip(),
                    "campaign_label": cleaned_campaign,
                    "brand_label": str(item.get("brand_label", "") or "").strip(),
                    "industry_label": str(item.get("industry_label", "") or "").strip(),
                    "campaign_description": str(item.get("campaign_description", "") or "").strip(),
                }
            )
        else:
            next_entries.append(dict(item))
    if not found:
        return []
    if normalized_owner:
        saved_by_user = dict(settings.get("saved_sheets_by_user", {}) or {})
        saved_by_user[normalized_owner] = next_entries[:MAX_SAVED_SHEETS_PER_USER]
        settings["saved_sheets_by_user"] = saved_by_user
    else:
        settings["saved_sheets"] = next_entries[:MAX_SAVED_SHEETS_PER_USER]
    persist_auth_settings(settings)
    return get_saved_sheet_entries(settings, owner_email=normalized_owner)

def update_saved_sheet_metadata(
    sheet_id: str,
    sheet_name: str,
    new_sheet_name: str = "",
    campaign_label: str = "",
    brand_label: str = "",
    industry_label: str = "",
    owner_email: Optional[str] = None
):
    normalized_sheet_id = str(sheet_id or "").strip()
    normalized_sheet_name = str(sheet_name or "").strip()
    if not normalized_sheet_id or not normalized_sheet_name:
        return []
        
    normalized_owner = normalize_email_address(owner_email or "")
    settings = get_auth_settings().copy()
    existing_entries = get_saved_sheet_entries(settings, owner_email=normalized_owner)
    
    def build_updated_entry(item):
        curr_id = str(item.get("sheet_id", "") or "").strip()
        curr_name = str(item.get("sheet_name", "") or "").strip()
        return {
            "sheet_id": curr_id,
            "sheet_name": (new_sheet_name or curr_name).strip(),
            "sheet_gid": str(item.get("sheet_gid", "") or "0").strip() or "0",
            "saved_at_text": str(item.get("saved_at_text", "") or "").strip(),
            "campaign_label": str(campaign_label or "").strip(),
            "brand_label": str(brand_label or "").strip(),
            "industry_label": str(industry_label or "").strip(),
            "campaign_description": str(item.get("campaign_description", "") or "").strip(),
        }

    target_index = -1
    normalized_target_name = unicodedata.normalize('NFC', normalized_sheet_name).casefold()

    # Pass 1: match chặt theo ID + tên cũ (đúng hành vi cũ)
    for idx, item in enumerate(existing_entries):
        curr_id = str(item.get("sheet_id", "") or "").strip()
        curr_name = str(item.get("sheet_name", "") or "").strip()
        norm_curr = unicodedata.normalize('NFC', curr_name).casefold()
        if curr_id == normalized_sheet_id and norm_curr == normalized_target_name:
            target_index = idx
            break

    # Pass 2: fallback theo ID (trường hợp tên hiển thị đã lệch)
    if target_index < 0:
        for idx, item in enumerate(existing_entries):
            curr_id = str(item.get("sheet_id", "") or "").strip()
            if curr_id == normalized_sheet_id:
                target_index = idx
                break

    if target_index < 0:
        return []

    next_entries = []
    for idx, item in enumerate(existing_entries):
        if idx == target_index:
            next_entries.append(build_updated_entry(item))
        else:
            next_entries.append(dict(item))
        
    if normalized_owner:
        saved_by_user = dict(settings.get("saved_sheets_by_user", {}) or {})
        saved_by_user[normalized_owner] = next_entries[:MAX_SAVED_SHEETS_PER_USER]
        settings["saved_sheets_by_user"] = saved_by_user
    else:
        settings["saved_sheets"] = next_entries[:MAX_SAVED_SHEETS_PER_USER]
        
    persist_auth_settings(settings)
    return get_saved_sheet_entries(settings, owner_email=normalized_owner)


def build_sheet_binding_key(sheet_id: str, sheet_name: str) -> str:
    return f"{str(sheet_id or '').strip()}::{str(sheet_name or '').strip()}"

def parse_sheet_binding_key(raw_value: str):
    raw_text = str(raw_value or "").strip()
    if "::" not in raw_text:
        return "", ""
    sheet_id, sheet_name = raw_text.split("::", 1)
    return sheet_id.strip(), sheet_name.strip()


SCHEDULE_ENTRY_FIELDS = [
    "schedule_mode",
    "schedule_time",
    "schedule_weekday",
    "schedule_monthday",
    "schedule_end_date",
    "schedule_sheet_id",
    "schedule_sheet_name",
    "schedule_sheet_gid",
    "schedule_targets",
    "schedule_last_run_started_at",
    "schedule_last_run_finished_at",
    "schedule_last_run_duration_seconds",
    "schedule_last_run_status",
    "schedule_last_run_source",
    "schedule_last_run_sheet_name",
    "schedule_last_run_processed",
    "schedule_last_run_success",
    "schedule_last_run_failed",
    "schedule_run_history",
    "last_schedule_run_key",
]


def build_schedule_entry(sheet_id: str = "", sheet_name: str = "", sheet_gid: str = "0"):
    normalized_sheet_id = str(sheet_id or "").strip()
    normalized_sheet_name = str(sheet_name or "").strip()
    normalized_sheet_gid = str(sheet_gid or "0").strip() or "0"
    return {
        "key": build_sheet_binding_key(normalized_sheet_id, normalized_sheet_name) if normalized_sheet_id and normalized_sheet_name else "",
        "schedule_mode": "off",
        "schedule_time": "09:00",
        "schedule_weekday": 0,
        "schedule_monthday": 1,
        "schedule_end_date": "",
        "schedule_sheet_id": normalized_sheet_id,
        "schedule_sheet_name": normalized_sheet_name,
        "schedule_sheet_gid": normalized_sheet_gid,
        "schedule_targets": [],
        "schedule_last_run_started_at": None,
        "schedule_last_run_finished_at": None,
        "schedule_last_run_duration_seconds": 0.0,
        "schedule_last_run_status": "idle",
        "schedule_last_run_source": "",
        "schedule_last_run_sheet_name": normalized_sheet_name,
        "schedule_last_run_processed": 0,
        "schedule_last_run_success": 0,
        "schedule_last_run_failed": 0,
        "schedule_run_history": [],
        "last_schedule_run_key": "",
    }


def normalize_schedule_entry(entry: dict):
    normalized = build_schedule_entry(
        str(entry.get("schedule_sheet_id", "") or "").strip(),
        str(entry.get("schedule_sheet_name", "") or "").strip(),
        str(entry.get("schedule_sheet_gid", "") or "0").strip() or "0",
    )
    for field in SCHEDULE_ENTRY_FIELDS:
        if field not in entry:
            continue
        value = entry.get(field)
        if field == "schedule_targets":
            normalized[field] = normalize_schedule_targets(value, normalized["schedule_sheet_id"])
        elif field == "schedule_run_history":
            normalized[field] = list(value or [])[:8]
        elif field in {"schedule_last_run_processed", "schedule_last_run_success", "schedule_last_run_failed", "schedule_weekday", "schedule_monthday"}:
            try:
                normalized[field] = int(value or 0)
            except Exception:
                normalized[field] = build_schedule_entry()[field]
        elif field == "schedule_last_run_duration_seconds":
            try:
                normalized[field] = float(value or 0.0)
            except Exception:
                normalized[field] = 0.0
        else:
            normalized[field] = value
    normalized["key"] = build_sheet_binding_key(normalized["schedule_sheet_id"], normalized["schedule_sheet_name"]) if normalized["schedule_sheet_id"] and normalized["schedule_sheet_name"] else ""
    return normalized


def serialize_schedule_datetime(value):
    if not value:
        return None
    if isinstance(value, str):
        return value
    try:
        return value.strftime("%d/%m/%Y %H:%M:%S")
    except Exception:
        return str(value)


def serialize_schedule_entry(entry: dict):
    normalized = normalize_schedule_entry(entry or {})
    serialized = {"key": normalized.get("key", "")}
    for field in SCHEDULE_ENTRY_FIELDS:
        value = normalized.get(field)
        if field in {"schedule_last_run_started_at", "schedule_last_run_finished_at"}:
            serialized[field] = serialize_schedule_datetime(value)
        elif field == "schedule_targets":
            serialized[field] = [dict(item) for item in normalize_schedule_targets(value, normalized.get("schedule_sheet_id"))]
        elif field == "schedule_run_history":
            serialized[field] = [dict(item) for item in list(value or [])[:8] if isinstance(item, dict)]
        else:
            serialized[field] = value
    serialized["key"] = build_sheet_binding_key(serialized.get("schedule_sheet_id", ""), serialized.get("schedule_sheet_name", ""))
    return serialized


def normalize_persisted_schedule_entries(raw_entries):
    normalized_entries = []
    seen_keys = set()
    for raw_entry in raw_entries if isinstance(raw_entries, list) else []:
        if not isinstance(raw_entry, dict):
            continue
        entry = serialize_schedule_entry(raw_entry)
        if not entry.get("key") or entry["key"] in seen_keys:
            continue
        seen_keys.add(entry["key"])
        normalized_entries.append(entry)
    return normalized_entries


def ensure_schedule_entries_migrated(state=None):
    runtime_state = resolve_runtime_state(state)
    raw_entries = runtime_state.get("schedule_entries")
    normalized_entries = []
    seen_keys = set()
    for raw_entry in raw_entries if isinstance(raw_entries, list) else []:
        if not isinstance(raw_entry, dict):
            continue
        entry = normalize_schedule_entry(raw_entry)
        if not entry["key"] or entry["key"] in seen_keys:
            continue
        seen_keys.add(entry["key"])
        normalized_entries.append(entry)

    legacy_sheet_id = str(runtime_state.get("schedule_sheet_id", "") or "").strip()
    legacy_sheet_name = str(runtime_state.get("schedule_sheet_name", "") or "").strip()
    if legacy_sheet_id and legacy_sheet_name:
        legacy_key = build_sheet_binding_key(legacy_sheet_id, legacy_sheet_name)
        if legacy_key not in seen_keys:
            legacy_entry = build_schedule_entry(legacy_sheet_id, legacy_sheet_name, runtime_state.get("schedule_sheet_gid", "0"))
            for field in SCHEDULE_ENTRY_FIELDS:
                legacy_entry[field] = runtime_state.get(field, legacy_entry[field])
            legacy_entry["schedule_targets"] = normalize_schedule_targets(legacy_entry["schedule_targets"], legacy_sheet_id)
            legacy_entry["schedule_run_history"] = list(legacy_entry.get("schedule_run_history") or [])[:8]
            normalized_entries.insert(0, normalize_schedule_entry(legacy_entry))
            seen_keys.add(legacy_key)

    runtime_state["schedule_entries"] = normalized_entries
    active_key = str(runtime_state.get("active_schedule_key", "") or "").strip()
    if active_key not in seen_keys:
        runtime_state["active_schedule_key"] = normalized_entries[0]["key"] if normalized_entries else ""
    tracking_key = str(runtime_state.get("schedule_tracking_key", "") or "").strip()
    scheduled_keys = {entry["key"] for entry in normalized_entries if entry.get("schedule_mode") != "off"}
    if tracking_key not in scheduled_keys:
        runtime_state["schedule_tracking_key"] = ""
    return normalized_entries


def get_schedule_entry_by_key(key: str, state=None):
    runtime_state = resolve_runtime_state(state)
    lookup_key = str(key or "").strip()
    for entry in ensure_schedule_entries_migrated(runtime_state):
        if entry["key"] == lookup_key:
            return entry
    return None


def upsert_schedule_entry(sheet_id: str, sheet_name: str, sheet_gid: str = "0", state=None):
    runtime_state = resolve_runtime_state(state)
    normalized_sheet_id = str(sheet_id or "").strip()
    normalized_sheet_name = str(sheet_name or "").strip()
    normalized_key = build_sheet_binding_key(normalized_sheet_id, normalized_sheet_name)
    if not normalized_key:
        return None
    existing = get_schedule_entry_by_key(normalized_key, runtime_state)
    if existing:
        existing["schedule_sheet_gid"] = str(sheet_gid or existing.get("schedule_sheet_gid") or "0").strip() or "0"
        return existing
    entry = build_schedule_entry(normalized_sheet_id, normalized_sheet_name, sheet_gid)
    runtime_state["schedule_entries"].append(entry)
    return entry


def sync_runtime_state_from_schedule_entry(entry: Optional[dict], state=None):
    runtime_state = resolve_runtime_state(state)
    if not entry:
        return
    normalized_entry = normalize_schedule_entry(entry)
    for field in SCHEDULE_ENTRY_FIELDS:
        runtime_state[field] = normalized_entry[field]
    runtime_state["active_schedule_key"] = normalized_entry["key"]


def sync_schedule_entry_from_runtime_state(entry: Optional[dict], state=None):
    runtime_state = resolve_runtime_state(state)
    if not entry:
        return
    for field in SCHEDULE_ENTRY_FIELDS:
        if field in {"schedule_targets", "schedule_run_history"}:
            entry[field] = list(runtime_state.get(field) or [])
        else:
            entry[field] = runtime_state.get(field)
    entry["key"] = build_sheet_binding_key(entry["schedule_sheet_id"], entry["schedule_sheet_name"])


def get_active_schedule_entry(state=None, fallback_to_first: bool = False):
    runtime_state = resolve_runtime_state(state)
    entries = ensure_schedule_entries_migrated(runtime_state)
    active_key = str(runtime_state.get("active_schedule_key", "") or "").strip()
    active_entry = next((entry for entry in entries if entry["key"] == active_key), None)
    if active_entry:
        return active_entry
    if fallback_to_first and entries:
        runtime_state["active_schedule_key"] = entries[0]["key"]
        return entries[0]
    return None


def get_scheduled_entries(state=None):
    runtime_state = resolve_runtime_state(state)
    return [entry for entry in ensure_schedule_entries_migrated(runtime_state) if entry.get("schedule_mode") != "off"]


def get_schedule_tracking_entry(state=None):
    runtime_state = resolve_runtime_state(state)
    scheduled_entries = get_scheduled_entries(runtime_state)
    tracking_key = str(runtime_state.get("schedule_tracking_key", "") or "").strip()
    tracking_entry = next((entry for entry in scheduled_entries if entry["key"] == tracking_key), None)
    if tracking_entry:
        return tracking_entry
    runtime_state["schedule_tracking_key"] = ""
    return None


def get_saved_schedule_entries(settings=None, owner_email: Optional[str] = None):
    auth_settings = settings or get_auth_settings()
    normalized_owner = normalize_email_address(owner_email or "")
    schedules_by_user = auth_settings.get("schedule_entries_by_user", {}) if isinstance(auth_settings, dict) else {}
    if normalized_owner:
        scoped_entries = schedules_by_user.get(normalized_owner, []) if isinstance(schedules_by_user, dict) else []
        return [dict(item) for item in normalize_persisted_schedule_entries(scoped_entries)]
    return [dict(item) for item in normalize_persisted_schedule_entries(auth_settings.get("schedule_entries", []))]


def hydrate_runtime_state_from_saved_schedules(state=None, settings=None):
    runtime_state = resolve_runtime_state(state)
    owner_email = normalize_email_address(runtime_state.get("owner_email", ""))
    if not owner_email:
        runtime_state["_schedule_hydrated"] = True
        return runtime_state

    existing_entries = ensure_schedule_entries_migrated(runtime_state)
    saved_entries = [normalize_schedule_entry(item) for item in get_saved_schedule_entries(settings=settings, owner_email=owner_email)]
    if not saved_entries and existing_entries:
        runtime_state["_schedule_hydrated"] = True
        persist_runtime_schedule_entries(runtime_state)
        return runtime_state
    runtime_state["schedule_entries"] = saved_entries
    runtime_state["_schedule_hydrated"] = True

    if not saved_entries:
        runtime_state["active_schedule_key"] = ""
        runtime_state["schedule_tracking_key"] = ""
        return runtime_state

    first_scheduled_entry = next((entry for entry in saved_entries if entry.get("schedule_mode") != "off"), None)
    active_entry = get_active_schedule_entry(runtime_state)
    if not active_entry:
        active_entry = first_scheduled_entry or saved_entries[0]
        runtime_state["active_schedule_key"] = active_entry["key"]
    if active_entry:
        sync_runtime_state_from_schedule_entry(active_entry, runtime_state)

    tracking_key = str(runtime_state.get("schedule_tracking_key", "") or "").strip()
    if tracking_key:
        tracking_entry = next((entry for entry in saved_entries if entry["key"] == tracking_key and entry.get("schedule_mode") != "off"), None)
        if not tracking_entry:
            runtime_state["schedule_tracking_key"] = ""
    return runtime_state


def persist_runtime_schedule_entries(state=None):
    runtime_state = resolve_runtime_state(state)
    owner_email = normalize_email_address(runtime_state.get("owner_email", ""))
    settings = get_auth_settings().copy()
    if owner_email:
        schedules_by_user = dict(settings.get("schedule_entries_by_user", {}) or {})
        schedules_by_user[owner_email] = [serialize_schedule_entry(entry) for entry in ensure_schedule_entries_migrated(runtime_state)]
        settings["schedule_entries_by_user"] = schedules_by_user
    else:
        settings["schedule_entries"] = [serialize_schedule_entry(entry) for entry in ensure_schedule_entries_migrated(runtime_state)]
    persist_auth_settings(settings)
    runtime_state["_schedule_hydrated"] = True
    return get_saved_schedule_entries(settings=settings, owner_email=owner_email)


def ensure_runtime_states_for_saved_schedules():
    settings = get_auth_settings()
    schedules_by_user = settings.get("schedule_entries_by_user", {}) if isinstance(settings, dict) else {}
    if not isinstance(schedules_by_user, dict):
        return
    with USER_RUNTIME_LOCK:
        for raw_email, raw_entries in schedules_by_user.items():
            owner_email = normalize_email_address(raw_email)
            if not owner_email or not normalize_persisted_schedule_entries(raw_entries):
                continue
            runtime_state = USER_RUNTIME_STATES.get(owner_email)
            if runtime_state is None:
                runtime_state = build_default_runtime_state(owner_email)
                USER_RUNTIME_STATES[owner_email] = runtime_state
            if not runtime_state.get("_schedule_hydrated"):
                hydrate_runtime_state_from_saved_schedules(runtime_state, settings=settings)

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


def get_user_notification_preferences(user_or_email=None, settings=None):
    auth_settings = settings or get_auth_settings()
    email = get_runtime_owner_email(user_or_email)
    prefs_map = auth_settings.get("notification_preferences_by_user", {}) if isinstance(auth_settings, dict) else {}
    prefs = prefs_map.get(email, {}) if isinstance(prefs_map, dict) else {}
    return {
        "email_notifications": bool(prefs.get("email_notifications", True)),
        "system_alerts": bool(prefs.get("system_alerts", True)),
    }


def save_user_notification_preferences(user_or_email, prefs):
    owner_email = get_runtime_owner_email(user_or_email)
    if not owner_email:
        return get_user_notification_preferences(owner_email)
    settings = get_auth_settings().copy()
    prefs_map = dict(settings.get("notification_preferences_by_user", {}) or {})
    current = get_user_notification_preferences(owner_email, settings=settings)
    next_prefs = {
        "email_notifications": bool((prefs or {}).get("email_notifications", current["email_notifications"])),
        "system_alerts": bool((prefs or {}).get("system_alerts", current["system_alerts"])),
    }
    prefs_map[owner_email] = next_prefs
    settings["notification_preferences_by_user"] = prefs_map
    persist_auth_settings(settings)
    return get_user_notification_preferences(owner_email, settings=settings)

AUTH_SETTINGS = {}

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


def get_dashboard_section_from_path(path: str) -> str:
    section = str(path or "/").strip().strip("/")
    if section in {"nhan-vien", "chien-dich"}:
        return "cai-dat"
    if section in DASHBOARD_SECTION_IDS:
        return section
    return "tong-quan"

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
    mail = dict(auth_settings.get("mail", {}) or {})
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

    gmail_mail_fallback = {
        "smtp_host": "smtp.gmail.com",
        "smtp_port": 587,
        "smtp_user": normalize_email_address(GMAIL_SMTP_EMAIL or os.getenv("GMAIL_SMTP_EMAIL", "")),
        "smtp_password": re.sub(r"\s+", "", str(GMAIL_SMTP_APP_PASSWORD or os.getenv("GMAIL_SMTP_APP_PASSWORD", "") or "")),
        "smtp_from_email": normalize_email_address(GMAIL_SMTP_FROM_EMAIL or os.getenv("GMAIL_SMTP_FROM_EMAIL", "")) or normalize_email_address(GMAIL_SMTP_EMAIL or os.getenv("GMAIL_SMTP_EMAIL", "")),
        "smtp_from_name": brand_name,
        "use_tls": True,
        "use_ssl": False,
    }
    if "@" not in str(gmail_mail_fallback.get("smtp_from_email", "") or ""):
        gmail_mail_fallback["smtp_from_email"] = normalize_email_address(gmail_mail_fallback.get("smtp_user", ""))
    gmail_fallback_enabled = bool(
        gmail_mail_fallback["smtp_user"]
        and gmail_mail_fallback["smtp_password"]
        and gmail_mail_fallback["smtp_from_email"]
    )

    def _send_with_mail_config(mail_config):
        active_host = str(mail_config.get("smtp_host", "") or "").strip()
        active_port = int(mail_config.get("smtp_port", 587) or 587)
        active_user = str(mail_config.get("smtp_user", "") or "").strip()
        active_password = str(mail_config.get("smtp_password", "") or "")
        active_use_ssl = bool(mail_config.get("use_ssl", False))
        active_use_tls = bool(mail_config.get("use_tls", True))
        if active_use_ssl:
            with smtplib.SMTP_SSL(active_host, active_port, timeout=20, context=context) as server:
                if active_user:
                    server.login(active_user, active_password)
                server.send_message(msg)
            return
        with smtplib.SMTP(active_host, active_port, timeout=20) as server:
            server.ehlo()
            if active_use_tls:
                server.starttls(context=context)
                server.ehlo()
            if active_user:
                server.login(active_user, active_password)
            server.send_message(msg)

    try:
        _send_with_mail_config(mail)
        return
    except Exception as exc:
        error_text = str(exc).lower()
        current_host = str(mail.get("smtp_host", "") or "").strip().lower()
        is_host_resolution_error = isinstance(exc, socket.gaierror) or "getaddrinfo failed" in error_text
        if not (is_host_resolution_error and gmail_fallback_enabled and current_host != "smtp.gmail.com"):
            raise
        fallback_mail = dict(mail)
        fallback_mail.update(gmail_mail_fallback)
        _send_with_mail_config(fallback_mail)
        return

def describe_smtp_error(exc: Exception) -> str:
    if isinstance(exc, smtplib.SMTPAuthenticationError):
        return (
            "Gmail không chấp nhận đăng nhập SMTP. Kiểm tra lại GMAIL_SMTP_EMAIL và "
            "GMAIL_SMTP_APP_PASSWORD, đồng thời chắc là tài khoản Gmail đã bật xác minh 2 bước "
            "và App Password còn hiệu lực."
        )
    if isinstance(exc, socket.gaierror) or "getaddrinfo failed" in str(exc).lower():
        return (
            "Không kết nối được SMTP host. Nếu bạn vừa đổi cấu hình mail trong code thì hãy restart app rồi thử lại."
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

def build_employee_settings_content_html(current_user):
    if not current_user or current_user.get("role") != "admin":
        return '<div class="settings-empty-note">Chỉ admin mới xem được phần quản lý nhân viên.</div>'
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
                    <div id="employee-form-title" class="employee-form-title">Thêm nhanh</div>
                    <div id="employee-form-sub" class="employee-form-sub">Nhập email để thêm vào whitelist đăng nhập và chỉnh role ngay tại đây.</div>
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
                        <button type="button" id="employee-cancel-btn" class="posts-toolbar-btn hidden">
                            <i class="fa-solid fa-xmark"></i> Hủy sửa
                        </button>
                    </div>
                    <div class="employee-form-note">Mail đã xác thực sẽ tự chuyển trạng thái sau khi đăng nhập OTP thành công. Email admin cứng vẫn luôn giữ quyền admin.</div>
                </div>
            </div>
        </div>
        <script id="employee-users-data" type="application/json">{employee_json}</script>
    """


def build_settings_panel_html(current_user, state=None):
    is_admin = bool(current_user and current_user.get("role") == "admin")
    current_prefs = get_user_notification_preferences(current_user)
    employee_content_html = build_employee_settings_content_html(current_user) if is_admin else ""
    admin_nav_html = (
        """
                            <button type="button" class="settings-nav-item" data-settings-tab-trigger="employees">
                                <span class="settings-nav-item-icon"><i class="fa-solid fa-users"></i></span>
                                <span class="settings-nav-item-copy">
                                    <strong>Nhân viên</strong>
                                    <span>Quản lý nhân viên</span>
                                </span>
                            </button>
        """
        if is_admin
        else ""
    )
    admin_pane_html = (
        f"""
                        <div class="settings-tab-pane hidden" data-settings-tab-pane="employees">
                            <div class="settings-pane-shell">
                                {employee_content_html}
                            </div>
                        </div>
        """
        if is_admin
        else ""
    )
    return f"""
    <section id="cai-dat" data-dashboard-section="cai-dat" class="dashboard-section dashboard-panel posts-board rounded-[2rem] p-6 md:p-8 mb-6 border border-white/5">
        <div class="settings-layout">
            <aside class="settings-nav-shell">
                <div class="settings-nav-title">Cài đặt</div>
                <div class="settings-nav-list">
                    <button type="button" class="settings-nav-item is-active" data-settings-tab-trigger="notifications">
                        <span class="settings-nav-item-icon"><i class="fa-regular fa-bell"></i></span>
                        <span class="settings-nav-item-copy">
                            <strong>Thông báo</strong>
                            <span>Cấu hình thông báo</span>
                        </span>
                    </button>
                    {admin_nav_html}
                </div>
            </aside>
            <div class="settings-content-shell">
                <div class="settings-tab-pane" data-settings-tab-pane="notifications">
                    <div class="settings-pane-shell">
                        <div class="settings-pane-title">Tùy chọn thông báo</div>
                        <div class="settings-pane-sub">Bật hoặc tắt những thông báo bạn muốn nhận trong quá trình dùng hệ thống.</div>
                        <div class="settings-notify-list">
                            <label class="settings-toggle-row">
                                <span class="settings-toggle-copy">
                                    <strong>Thông báo qua Email</strong>
                                    <span>Nhận thông báo qua email</span>
                                </span>
                                <span class="settings-toggle-switch">
                                    <input id="settings-email-notifications" type="checkbox" {"checked" if current_prefs["email_notifications"] else ""} />
                                    <span class="settings-toggle-slider"></span>
                                </span>
                            </label>
                            <label class="settings-toggle-row">
                                <span class="settings-toggle-copy">
                                    <strong>Cảnh báo hệ thống</strong>
                                    <span>Hiển thị cảnh báo và thông báo hệ thống</span>
                                </span>
                                <span class="settings-toggle-switch">
                                    <input id="settings-system-alerts" type="checkbox" {"checked" if current_prefs["system_alerts"] else ""} />
                                    <span class="settings-toggle-slider"></span>
                                </span>
                            </label>
                        </div>
                        <div class="settings-pane-actions">
                            <div id="settings-notification-feedback" class="settings-inline-feedback hidden"></div>
                            <button type="button" id="save-notification-settings-btn" class="settings-save-btn">
                                <i class="fa-regular fa-floppy-disk"></i> Lưu thay đổi
                            </button>
                        </div>
                    </div>
                </div>
                {admin_pane_html}
            </div>
        </div>
    </section>
    """

def add_log(msg, state=None):
    runtime_state = resolve_runtime_state(state)
    logs = runtime_state["logs"]
    timestamp = datetime.now().strftime("%H:%M:%S")
    # store full messages (trim list length), useful for debugging unicode/errors
    logs.insert(0, f"[{timestamp}] {msg}")

def build_log_html(state=None):
    runtime_state = resolve_runtime_state(state)
    logs = runtime_state["logs"]
    if not logs:
        return '<p class="system-log-empty">Đang chờ lệnh...</p>'
    parts = []
    for item in logs:
        raw_text = str(item or "")
        timestamp_part, separator, message_part = raw_text.partition("] ")
        timestamp_text = timestamp_part[1:] if separator and timestamp_part.startswith("[") else ""
        message_text = message_part if separator else raw_text
        tab_html = ""
        tab_match = re.match(r"^\[([^\]]+)\]\s*(.*)$", message_text)
        if tab_match:
            tab_name = tab_match.group(1)
            message_text = tab_match.group(2)
            tab_html = f'<span class="system-log-tab">[{html.escape(tab_name)}]</span>'
        parts.append(
            f'<div class="system-log-line">'
            f'<span class="system-log-time">[{html.escape(timestamp_text)}]</span>'
            f'{tab_html}<span class="system-log-message">{html.escape(message_text)}</span>'
            f"</div>"
        )
    return "".join(parts)

def build_pending_html(state=None):
    runtime_state = resolve_runtime_state(state)
    pending_updates = runtime_state["pending_updates"]
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

def set_run_progress(current: Optional[int] = None, total: Optional[int] = None, phase: Optional[str] = None, state=None):
    runtime_state = resolve_runtime_state(state)
    if total is not None:
        runtime_state["run_progress_total"] = max(0, int(total or 0))
    if current is not None:
        runtime_state["run_progress_current"] = max(0, int(current or 0))
    if phase is not None:
        runtime_state["run_progress_phase"] = str(phase or "idle").strip().lower() or "idle"
    total_items = max(0, int(runtime_state.get("run_progress_total") or 0))
    current_items = max(0, int(runtime_state.get("run_progress_current") or 0))
    if total_items > 0 and current_items > total_items:
        runtime_state["run_progress_current"] = total_items
    return runtime_state


def build_run_progress_payload(state=None):
    runtime_state = resolve_runtime_state(state)
    is_running = bool(runtime_state.get("is_running"))
    is_finished = bool(runtime_state.get("is_finished"))
    current_task = str(runtime_state.get("current_task") or "")
    phase = str(runtime_state.get("run_progress_phase") or "idle").strip().lower()
    total_items = max(0, int(runtime_state.get("run_progress_total") or 0))
    current_items = max(0, int(runtime_state.get("run_progress_current") or 0))
    if total_items > 0:
        current_items = min(current_items, total_items)

    progress_text = ""
    if total_items > 0:
        progress_text = f"{total_items if is_finished else current_items}/{total_items} bài"
    elif is_running and phase == "preparing":
        progress_text = "Đang chuẩn bị danh sách bài quét"
    elif is_finished:
        progress_text = "Không có bài hợp lệ để quét"

    progress_percent = 0
    if total_items > 0:
        computed_percent = int(round((current_items / total_items) * 100))
        if is_finished:
            progress_percent = 100
        elif is_running:
            progress_percent = 98 if current_items >= total_items else max(1 if current_items > 0 else 0, min(97, computed_percent))
        elif current_task == "Đã dừng thủ công" or current_task.startswith("Lỗi:"):
            progress_percent = max(1 if current_items > 0 else 0, min(100, computed_percent))
    elif is_finished:
        progress_percent = 100

    return {
        "progress_width": f"{progress_percent}%",
        "progress_text": progress_text,
    }


def build_status_payload(state=None):
    runtime_state = resolve_runtime_state(state)
    is_running = runtime_state["is_running"]
    is_finished = runtime_state["is_finished"]
    current_task = runtime_state["current_task"]
    progress_payload = build_run_progress_payload(runtime_state)
    config_locked = (not is_running) and current_task == "Đã dừng thủ công"
    status_badge_base_class = "py-2.5 px-5 rounded-full text-sm font-black uppercase tracking-[0.14em] leading-none"
    status_badge_class = f"{status_badge_base_class} bg-slate-700/60 text-slate-200 border border-slate-500/20"
    status_badge_text = "Sẵn sàng"
    if is_running:
        status_badge_class = f"{status_badge_base_class} bg-sky-500/12 text-sky-200 border border-sky-300/20"
        status_badge_text = "Đang quét dữ liệu..."
    elif is_finished:
        status_badge_class = f"{status_badge_base_class} bg-emerald-500/12 text-emerald-200 border border-emerald-300/20"
        status_badge_text = "Đã hoàn tất"
    elif current_task == "Đã dừng thủ công":
        status_badge_class = f"{status_badge_base_class} bg-amber-400/12 text-amber-200 border border-amber-300/20"
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
        "progress_width": progress_payload["progress_width"],
        "progress_text": progress_payload["progress_text"],
        "primary_action_html": primary_action_html,
        "config_locked": config_locked,
        "config_lock_message": "Đang ở trạng thái Đã dừng. Bấm Bắt đầu để mở lại rồi hãy nhập hoặc lưu sheet." if config_locked else "",
    }

def build_snapshot_url(sheet_id: Optional[str] = None, sheet_gid: Optional[str] = None, state=None):
    runtime_state = resolve_runtime_state(state)
    resolved_sheet_id = sheet_id if sheet_id is not None else runtime_state["active_sheet_id"]
    resolved_sheet_gid = sheet_gid if sheet_gid is not None else runtime_state["active_sheet_gid"]
    if not resolved_sheet_id:
        return ""
    return f"https://docs.google.com/spreadsheets/d/{resolved_sheet_id}/edit#gid={resolved_sheet_gid or '0'}"

def build_column_config_payload(sheet=None, state=None):
    runtime_state = resolve_runtime_state(state)
    metric_cols = {"date": "-", "air_date": "-", "link": "-", "view": "-", "like": "-", "share": "-", "comment": "-", "buzz": "-", "save": "-"}
    detected_text = "Chưa có sheet để tự nhận cột."
    header_row = 1
    effective_start_row = runtime_state["start_row"]
    if runtime_state["active_sheet_id"] and runtime_state["active_sheet_name"]:
        try:
            ws = sheet or get_worksheet(runtime_state["active_sheet_name"], runtime_state["active_sheet_id"])
            layout = detect_sheet_layout(ws)
            header_row = max(1, int(layout.get("header_row") or 1))
            effective_start_row = resolve_effective_start_row(header_row, runtime_state)
            col_map = apply_column_overrides(layout.get("columns"), runtime_state["column_overrides"])
            for field in metric_cols:
                col_idx = col_map.get(field)
                metric_cols[field] = col_to_a1(col_idx) if col_idx else "Chưa thấy"
            detected_text = format_detected_columns_text(layout, runtime_state)
        except Exception:
            pass
    manual_inputs = {field: (col_to_a1(col_idx) if col_idx else "") for field, col_idx in runtime_state["column_overrides"].items()}
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
        "manual_mode": "THỦ CÔNG" if any(runtime_state["column_overrides"].values()) else "AUTO",
        "metric_cols": metric_cols,
        "manual_inputs": manual_inputs,
        "detected_inputs": detected_inputs,
        "input_values": input_values,
        "input_sources": input_sources,
        "start_row": effective_start_row,
        "header_row": header_row,
        "detected_text": detected_text,
    }

def build_ui_state(state=None):
    runtime_state = resolve_runtime_state(state)
    payload = build_status_payload(runtime_state)
    payload["pending_html"] = build_pending_html(runtime_state)
    payload["log_html"] = build_log_html(runtime_state)
    payload["active_sheet_name"] = runtime_state["active_sheet_name"] or ""
    payload["active_sheet_id"] = runtime_state["active_sheet_id"] or ""
    payload["active_sheet_gid"] = runtime_state["active_sheet_gid"] or ""
    payload["snapshot_url"] = build_snapshot_url(state=runtime_state)
    payload["sheet_metadata"] = build_sheet_metadata_payload(state=runtime_state)
    payload["schedule_config"] = build_schedule_config_payload(runtime_state)
    payload["schedule_tracking"] = build_schedule_tracking_payload(runtime_state)
    payload["tab_progress"] = dict(runtime_state.get("tab_progress") or {})
    payload["column_overrides_by_tab"] = {
        tab: {k: (col_to_a1(v) if v else "") for k, v in overrides.items()}
        for tab, overrides in (runtime_state.get("column_overrides_by_tab") or {}).items()
    }
    return payload

def build_ui_json_response(message: str, level: str = "info", ok: bool = True, extra: Optional[dict] = None, state=None):
    payload = build_ui_state(state)
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


def parse_column_override_candidates(candidates):
    parsed = {}
    for field, val in candidates.items():
        if val is None:
            continue
        if (val or "").strip() == "":
            parsed[field] = None
            continue
        col_idx = parse_column_input(val)
        if not col_idx:
            raise ValueError("Cột không hợp lệ. Nhập dạng A/B/C... hoặc số 1/2/3...")
        parsed[field] = col_idx
    return parsed


def set_pending_updates(row_idx: int, updates, state=None):
    runtime_state = resolve_runtime_state(state)
    rows = []
    for field, col_idx, value in updates:
        rows.append(
            {
                "cell": f"{col_to_a1(col_idx)}{row_idx}",
                "field": field,
                "value": str(value)[:120],
            }
        )
    runtime_state["pending_updates"] = rows[:20]

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

def compute_next_schedule_run_for_entry(entry: Optional[dict], reference: Optional[datetime] = None):
    if not entry:
        return None
    schedule_mode = entry.get("schedule_mode", "off")
    schedule_time = entry.get("schedule_time", "09:00")
    schedule_weekday = int(entry.get("schedule_weekday", 0) or 0)
    schedule_monthday = int(entry.get("schedule_monthday", 1) or 1)
    schedule_end_date = entry.get("schedule_end_date", "")
    if schedule_mode == "off":
        return None
    if not entry.get("schedule_sheet_id") or not entry.get("schedule_sheet_name"):
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


def compute_next_schedule_run(reference: Optional[datetime] = None, state=None):
    tracking_entry = get_schedule_tracking_entry(state)
    if tracking_entry:
        return compute_next_schedule_run_for_entry(tracking_entry, reference)
    active_entry = get_active_schedule_entry(state)
    if active_entry:
        return compute_next_schedule_run_for_entry(active_entry, reference)
    runtime_state = resolve_runtime_state(state)
    if runtime_state["schedule_mode"] == "off" or not runtime_state["schedule_sheet_id"] or not runtime_state["schedule_sheet_name"]:
        return None
    fallback_entry = build_schedule_entry(runtime_state["schedule_sheet_id"], runtime_state["schedule_sheet_name"], runtime_state["schedule_sheet_gid"])
    for field in SCHEDULE_ENTRY_FIELDS:
        fallback_entry[field] = runtime_state.get(field, fallback_entry[field])
    return compute_next_schedule_run_for_entry(fallback_entry, reference)

def push_schedule_run_history(entry: dict, state=None):
    runtime_state = resolve_runtime_state(state)
    runtime_state["schedule_run_history"].insert(0, entry)
    runtime_state["schedule_run_history"] = runtime_state["schedule_run_history"][:8]

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
        dedupe_key = (sheet_id, sheet_name.lower(), row_idx, link)
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

def get_schedule_sheet_binding(use_active_fallback: bool = True, state=None):
    runtime_state = resolve_runtime_state(state)
    active_entry = get_active_schedule_entry(runtime_state)
    if active_entry and active_entry["schedule_sheet_id"] and active_entry["schedule_sheet_name"]:
        return {
            "sheet_id": active_entry["schedule_sheet_id"],
            "sheet_name": active_entry["schedule_sheet_name"],
            "sheet_gid": active_entry["schedule_sheet_gid"] or "0",
            "is_saved": active_entry["schedule_mode"] != "off",
        }
    if runtime_state["schedule_sheet_id"] and runtime_state["schedule_sheet_name"]:
        return {
            "sheet_id": runtime_state["schedule_sheet_id"],
            "sheet_name": runtime_state["schedule_sheet_name"],
            "sheet_gid": runtime_state["schedule_sheet_gid"] or "0",
            "is_saved": True,
        }
    if use_active_fallback and runtime_state["active_sheet_id"] and runtime_state["active_sheet_name"]:
        return {
            "sheet_id": runtime_state["active_sheet_id"],
            "sheet_name": runtime_state["active_sheet_name"],
            "sheet_gid": runtime_state["active_sheet_gid"] or "0",
            "is_saved": False,
        }
    return {
        "sheet_id": "",
        "sheet_name": "",
        "sheet_gid": "0",
        "is_saved": False,
    }

def get_schedule_sheet_choices(include_active_fallback: bool = True, state=None):
    runtime_state = resolve_runtime_state(state)
    choices = []
    seen_keys = set()
    for entry in get_saved_sheet_entries(owner_email=runtime_state["owner_email"]):
        sheet_id = str(entry.get("sheet_id", "") or "").strip()
        sheet_name = str(entry.get("sheet_name", "") or "").strip()
        sheet_gid = str(entry.get("sheet_gid", "") or "0").strip() or "0"
        if not sheet_id or not sheet_name:
            continue
        binding_key = build_sheet_binding_key(sheet_id, sheet_name)
        if binding_key in seen_keys:
            continue
        seen_keys.add(binding_key)
        choices.append(
            {
                "key": binding_key,
                "sheet_id": sheet_id,
                "sheet_name": sheet_name,
                "sheet_gid": sheet_gid,
                "label": sheet_name,
                "is_saved": True,
            }
        )

    if include_active_fallback and runtime_state["active_sheet_id"] and runtime_state["active_sheet_name"]:
        active_key = build_sheet_binding_key(runtime_state["active_sheet_id"], runtime_state["active_sheet_name"])
        if active_key not in seen_keys:
            choices.insert(
                0,
                {
                    "key": active_key,
                    "sheet_id": runtime_state["active_sheet_id"],
                    "sheet_name": runtime_state["active_sheet_name"],
                    "sheet_gid": runtime_state["active_sheet_gid"] or "0",
                    "label": f"{runtime_state['active_sheet_name']} (đang mở)",
                    "is_saved": False,
                },
            )
    return choices

def build_schedule_sheet_options_html(state=None):
    runtime_state = resolve_runtime_state(state)
    current_binding = get_schedule_sheet_binding(state=runtime_state)
    selected_key = build_sheet_binding_key(current_binding["sheet_id"], current_binding["sheet_name"])
    choices = get_schedule_sheet_choices(state=runtime_state)
    if not choices:
        return '<option value="">Chưa có sheet nào để chọn</option>'
    option_parts = []
    for choice in choices:
        option_gid = str(choice.get("sheet_gid", "") or "0").strip() or "0"
        option_sheet_id = str(choice.get("sheet_id", "") or "").strip()
        option_sheet_name = str(choice.get("sheet_name", "") or "").strip()
        option_parts.append(
            f'<option value="{html.escape(choice["key"], quote=True)}" data-sheet-id="{html.escape(option_sheet_id, quote=True)}" data-sheet-name="{html.escape(option_sheet_name, quote=True)}" data-sheet-gid="{html.escape(option_gid, quote=True)}" {"selected" if choice["key"] == selected_key else ""}>{html.escape(choice["label"])}</option>'
        )
    return "".join(option_parts)


def get_overview_sheet_binding(state=None):
    runtime_state = resolve_runtime_state(state)
    active_sheet_id = str(runtime_state.get("active_sheet_id", "") or "").strip()
    active_sheet_name = str(runtime_state.get("active_sheet_name", "") or "").strip()
    active_sheet_gid = str(runtime_state.get("active_sheet_gid", "") or "0").strip() or "0"
    if active_sheet_id and active_sheet_name:
        return {
            "sheet_id": active_sheet_id,
            "sheet_name": active_sheet_name,
            "sheet_gid": active_sheet_gid,
            "source": "active",
        }
    saved_entries = get_saved_sheet_entries(owner_email=runtime_state["owner_email"])
    if saved_entries:
        entry = saved_entries[0]
        return {
            "sheet_id": str(entry.get("sheet_id", "") or "").strip(),
            "sheet_name": str(entry.get("sheet_name", "") or "").strip(),
            "sheet_gid": str(entry.get("sheet_gid", "") or "0").strip() or "0",
            "source": "saved",
        }
    return {
        "sheet_id": "",
        "sheet_name": "",
        "sheet_gid": "0",
        "source": "empty",
    }

def build_schedule_scope_text(state=None):
    runtime_state = resolve_runtime_state(state)
    binding = get_schedule_sheet_binding(state=runtime_state)
    sheet_name = binding["sheet_name"]
    target_count = len(runtime_state["schedule_targets"] or [])
    if not sheet_name:
        return "Hãy chọn sheet ở phần Cấu hình trước, rồi bấm Lưu lịch để lịch tự động nhớ tab cần chạy."
    if target_count:
        target_text = "1 bài đã chọn" if target_count == 1 else f"{target_count} bài đã chọn"
        if binding["is_saved"]:
            return f"Lịch đang nhớ tab '{sheet_name}' và sẽ chỉ tự quét lại {target_text} khi đến giờ."
        return f"Lịch sẽ dùng tab '{sheet_name}' và chỉ quét {target_text} nếu bạn bấm Lưu lịch ngay bây giờ."
    if binding["is_saved"]:
        return f"Lịch đang nhớ tab '{sheet_name}' và sẽ tự quét lại tab này khi đến giờ, không cần bấm tay."
    return f"Lịch sẽ dùng tab '{sheet_name}' nếu bạn bấm Lưu lịch ngay bây giờ."

def build_schedule_target_summary_text(state=None):
    runtime_state = resolve_runtime_state(state)
    target_count = len(runtime_state["schedule_targets"] or [])
    binding = get_schedule_sheet_binding(state=runtime_state)
    sheet_name = binding["sheet_name"] or "tab hiện tại"
    if target_count <= 0:
        return f"Lịch hiện đang chạy toàn bộ link trong '{sheet_name}'."
    if target_count == 1:
        return f"Lịch hiện chỉ chạy 1 bài đã chọn trong '{sheet_name}'."
    return f"Lịch hiện chỉ chạy {target_count} bài đã chọn trong '{sheet_name}'."

def build_schedule_config_payload(state=None):
    runtime_state = resolve_runtime_state(state)
    active_entry = get_active_schedule_entry(runtime_state)
    if active_entry:
        sync_runtime_state_from_schedule_entry(active_entry, runtime_state)
    binding = get_schedule_sheet_binding(state=runtime_state)
    return {
        "label": schedule_label(runtime_state),
        "sheet_name_text": binding["sheet_name"] or "Chưa chốt tab nào",
        "sheet_id_text": binding["sheet_id"] or "Chưa có Spreadsheet ID",
        "is_saved": binding["is_saved"],
        "sheet_binding_key": build_sheet_binding_key(binding["sheet_id"], binding["sheet_name"]) if binding["sheet_id"] and binding["sheet_name"] else "",
        "sheet_options_html": build_schedule_sheet_options_html(runtime_state),
        "target_count": len(runtime_state["schedule_targets"] or []),
        "target_summary_text": build_schedule_target_summary_text(runtime_state),
        "scope_text": build_schedule_scope_text(runtime_state),
        "snapshot_url": build_snapshot_url(binding["sheet_id"], binding["sheet_gid"], runtime_state) if binding["sheet_id"] else "",
    }

def schedule_label_for_entry(entry: Optional[dict], include_sheet: bool = True):
    if not entry:
        return "Chưa bật"
    end_suffix = f" • đến {format_schedule_date(entry['schedule_end_date'])}" if entry.get("schedule_end_date") else ""
    sheet_suffix = f" • {entry['schedule_sheet_name']}" if include_sheet and entry.get("schedule_sheet_name") else ""
    if entry.get("schedule_mode") == "daily":
        return f"Hằng ngày lúc {entry.get('schedule_time', '09:00')}{sheet_suffix}{end_suffix}"
    if entry.get("schedule_mode") == "weekly":
        weekday_idx = int(entry.get("schedule_weekday", 0) or 0)
        weekday_name = WEEKDAY_NAMES[weekday_idx] if 0 <= weekday_idx <= 6 else f"Thứ {weekday_idx + 2}"
        return f"Hằng tuần ({weekday_name}) lúc {entry.get('schedule_time', '09:00')}{sheet_suffix}{end_suffix}"
    if entry.get("schedule_mode") == "monthly":
        return f"Hằng tháng (ngày {entry.get('schedule_monthday', 1)}) lúc {entry.get('schedule_time', '09:00')}{sheet_suffix}{end_suffix}"
    return "Chưa bật"


def build_schedule_tracking_entries_html(state=None):
    runtime_state = resolve_runtime_state(state)
    scheduled_entries = get_scheduled_entries(runtime_state)
    tracking_entry = get_schedule_tracking_entry(runtime_state)
    active_key = tracking_entry["key"] if tracking_entry else ""
    if not scheduled_entries:
        return '<div class="text-sm text-slate-400 border border-dashed border-white/10 rounded-2xl px-4 py-4 min-w-full">Chưa có sheet nào được đặt lịch.</div>'

    status_meta_map = {
        "success": ("Thành công", "is-success"),
        "running": ("Đang chạy", "is-running"),
        "error": ("Lỗi", "is-error"),
        "stopped": ("Đã dừng", "is-stopped"),
        "idle": ("Chưa chạy", "is-idle"),
    }
    entry_rows = []
    for entry in scheduled_entries:
        next_run_text = format_datetime_display(compute_next_schedule_run_for_entry(entry))
        last_status, status_class = status_meta_map.get(entry.get("schedule_last_run_status"), ("Chưa chạy", "is-idle"))
        target_count = len(entry.get("schedule_targets") or [])
        target_text = "Cả sheet" if target_count <= 0 else ("1 bài đã chọn" if target_count == 1 else f"{target_count} bài đã chọn")
        is_active = entry["key"] == active_key
        active_class = "is-active" if is_active else ""
        entry_rows.append(
            f"""
            <button
                type="button"
                data-schedule-track-entry-key="{html.escape(entry['key'], quote=True)}"
                class="schedule-track-list-row {active_class}"
            >
                <div class="schedule-track-list-cell schedule-track-list-activity">
                    <div class="schedule-track-list-title">{html.escape(entry.get("schedule_sheet_name") or "Sheet")}</div>
                    <div class="schedule-track-list-sub">{html.escape(str(entry.get("schedule_sheet_id", "") or "").strip())}</div>
                </div>
                <div class="schedule-track-list-cell">
                    <div class="schedule-track-list-main">{html.escape(schedule_label_for_entry(entry, include_sheet=False))}</div>
                </div>
                <div class="schedule-track-list-cell">
                    <span class="schedule-track-status-pill {status_class}">{html.escape(last_status)}</span>
                </div>
                <div class="schedule-track-list-cell">
                    <div class="schedule-track-list-main">{html.escape(target_text)}</div>
                </div>
                <div class="schedule-track-list-cell">
                    <div class="schedule-track-list-main">{html.escape(next_run_text)}</div>
                </div>
            </button>
            """
        )
    return f"""
        <div class="schedule-track-list-table">
            <div class="schedule-track-list-head">
                <div class="schedule-track-list-head-cell">Hoạt động</div>
                <div class="schedule-track-list-head-cell">Lịch chạy</div>
                <div class="schedule-track-list-head-cell">Trạng thái</div>
                <div class="schedule-track-list-head-cell">Phạm vi</div>
                <div class="schedule-track-list-head-cell">Lần kế tiếp</div>
            </div>
            {''.join(entry_rows)}
        </div>
    """


def get_schedule_highlight_days_for_month(entry: Optional[dict], year: int, month: int):
    if not entry or entry.get("schedule_mode") == "off":
        return set()
    _, days_in_month = calendar.monthrange(year, month)
    end_limit = None
    if entry.get("schedule_end_date"):
        try:
            end_limit = datetime.strptime(entry["schedule_end_date"], "%Y-%m-%d").date()
        except Exception:
            end_limit = None

    highlighted_days = set()
    for day in range(1, days_in_month + 1):
        candidate_date = datetime(year, month, day).date()
        if end_limit and candidate_date > end_limit:
            continue
        if entry.get("schedule_mode") == "daily":
            highlighted_days.add(day)
        elif entry.get("schedule_mode") == "weekly":
            if candidate_date.weekday() == int(entry.get("schedule_weekday", 0) or 0):
                highlighted_days.add(day)
        elif entry.get("schedule_mode") == "monthly":
            if day == int(entry.get("schedule_monthday", 1) or 1):
                highlighted_days.add(day)
    return highlighted_days


def build_schedule_calendar_preview_payload(entry: Optional[dict], reference: Optional[datetime] = None):
    if not entry or entry.get("schedule_mode") == "off":
        return {
            "title": "Chưa có lịch",
            "subtext": "Chọn sheet đã đặt lịch để xem ngày chạy được highlight ở đây.",
            "html": '<div class="text-sm text-slate-400 border border-dashed border-white/10 rounded-2xl px-4 py-6 text-center">Chưa có ngày chạy để hiển thị.</div>',
        }

    next_run = compute_next_schedule_run_for_entry(entry, reference)
    display_dt = next_run or reference or datetime.now()
    year, month = display_dt.year, display_dt.month
    highlighted_days = get_schedule_highlight_days_for_month(entry, year, month)
    next_run_day = next_run.day if next_run and next_run.year == year and next_run.month == month else None
    today = datetime.now().date()
    month_matrix = calendar.monthcalendar(year, month)
    week_headers = "".join(
        f'<div class="text-center text-[11px] uppercase tracking-[0.18em] text-slate-500 font-black">{label}</div>'
        for label in WEEKDAY_SHORT_NAMES
    )
    day_cells = []
    for week in month_matrix:
        for day in week:
            if day == 0:
                day_cells.append('<div class="h-11 rounded-xl border border-transparent bg-transparent"></div>')
                continue
            candidate_date = datetime(year, month, day).date()
            classes = ["h-11", "rounded-xl", "border", "flex", "items-center", "justify-center", "text-sm", "font-black", "transition"]
            label_classes = ["text-slate-200"]
            if day in highlighted_days:
                classes.extend(["border-cyan-500/30", "bg-cyan-500/10"])
                label_classes = ["text-cyan-100"]
            else:
                classes.extend(["border-white/8", "bg-slate-900/50"])
            if candidate_date == today:
                classes.extend(["ring-1", "ring-white/30"])
            if next_run_day == day:
                classes = ["h-11", "rounded-xl", "border", "flex", "items-center", "justify-center", "text-sm", "font-black", "transition", "border-cyan-200", "bg-cyan-300", "text-slate-950", "shadow-lg", "shadow-cyan-500/20"]
                label_classes = ["text-slate-950"]
            day_cells.append(f'<div class="{" ".join(classes)}"><span class="{" ".join(label_classes)}">{day}</span></div>')

    subtext = (
        f"Highlight đậm: lần chạy kế tiếp {format_datetime_display(next_run)}."
        if next_run
        else "Highlight nhạt là các ngày lịch sẽ chạy trong tháng này."
    )
    return {
        "title": f"Tháng {month}/{year}",
        "subtext": subtext,
        "html": f"""
            <div class="rounded-2xl border border-white/10 bg-slate-900/50 p-4">
                <div class="grid grid-cols-7 gap-2 mb-2">{week_headers}</div>
                <div class="grid grid-cols-7 gap-2">{''.join(day_cells)}</div>
            </div>
        """,
    }


def build_schedule_tracking_payload(state=None):
    runtime_state = resolve_runtime_state(state)
    tracking_entry = get_schedule_tracking_entry(runtime_state)
    next_run = compute_next_schedule_run_for_entry(tracking_entry) if tracking_entry else None
    calendar_preview = build_schedule_calendar_preview_payload(tracking_entry)
    history_cards = []
    status_map = {
        "success": ("Thành công", "text-emerald-300"),
        "running": ("Đang chạy", "text-sky-300"),
        "error": ("Lỗi", "text-rose-300"),
        "stopped": ("Đã dừng", "text-amber-300"),
        "idle": ("Chưa chạy", "text-slate-400"),
    }

    history_items = tracking_entry.get("schedule_run_history", []) if tracking_entry else []
    for item in history_items[:5]:
        label, tone_class = status_map.get(item.get("status"), ("Không rõ", "text-slate-400"))
        started_text = html.escape(item.get("started_text") or "Chưa có")
        finished_text = html.escape(item.get("finished_text") or "Chưa có")
        source_text = html.escape(item.get("source_label") or "Không rõ")
        duration_text = html.escape(item.get("duration_text") or "0s")
        processed_count = int(item.get("processed", 0) or 0)
        success_count = int(item.get("success", 0) or 0)
        failed_count = int(item.get("failed", 0) or 0)

        history_cards.append(
            f"""
            <div class="schedule-history-item">
                <div class="schedule-history-side">
                    <div class="schedule-history-title">{html.escape(item.get("sheet_name") or "Sheet")}</div>
                    <div class="schedule-history-meta">Bắt đầu: {started_text}</div>
                    <div class="schedule-history-meta">Kết thúc: {finished_text}</div>
                    <div class="schedule-history-meta">Nguồn chạy: {source_text}</div>
                </div>
                <div class="schedule-history-side text-right">
                    <div class="schedule-history-status {tone_class}">{label}</div>
                    <div class="schedule-history-meta">Thời lượng: {duration_text}</div>
                    <div class="schedule-history-meta">Đã quét: {processed_count} link</div>
                    <div class="schedule-history-meta">Thành công / trượt: {success_count}/{failed_count}</div>
                </div>
            </div>
            """
        )

    current_sheet_name = (
        (tracking_entry or {}).get("schedule_last_run_sheet_name")
        or (tracking_entry or {}).get("schedule_sheet_name")
        or "Chưa có"
    )
    is_running_for_tracking = bool(
        tracking_entry
        and runtime_state["is_running"]
        and str(runtime_state.get("active_schedule_key", "") or "").strip() == tracking_entry["key"]
        and runtime_state["run_started_at"]
    )
    return {
        "entries_html": build_schedule_tracking_entries_html(runtime_state),
        "has_entries": bool(get_scheduled_entries(runtime_state)),
        "has_active_entry": bool(tracking_entry),
        "active_key": tracking_entry["key"] if tracking_entry else "",
        "active_sheet_name": (tracking_entry or {}).get("schedule_sheet_name", ""),
        "calendar_title": calendar_preview["title"],
        "calendar_subtext": calendar_preview["subtext"],
        "calendar_html": calendar_preview["html"],
        "next_run_text": format_datetime_display(next_run),
        "last_started_text": format_datetime_display((tracking_entry or {}).get("schedule_last_run_started_at")),
        "last_finished_text": format_datetime_display((tracking_entry or {}).get("schedule_last_run_finished_at")),
        "last_duration_text": format_duration_display((tracking_entry or {}).get("schedule_last_run_duration_seconds")),
        "last_status_text": status_map.get((tracking_entry or {}).get("schedule_last_run_status"), ("Chưa chạy", ""))[0],
        "last_source_text": "Tự động" if (tracking_entry or {}).get("schedule_last_run_source") == "schedule" else ("Thủ công" if (tracking_entry or {}).get("schedule_last_run_source") == "manual" else "Chưa có"),
        "last_sheet_text": current_sheet_name,
        "last_processed_text": str(int((tracking_entry or {}).get("schedule_last_run_processed") or 0)),
        "last_success_text": str(int((tracking_entry or {}).get("schedule_last_run_success") or 0)),
        "last_failed_text": str(int((tracking_entry or {}).get("schedule_last_run_failed") or 0)),
        "is_running_text": format_datetime_display(runtime_state["run_started_at"]) if is_running_for_tracking else "Đang chờ",
        "history_html": "".join(history_cards) if history_cards else '<div class="schedule-history-empty">Chưa có lần chạy nào để theo dõi.</div>',
    }

def schedule_label(state=None):
    active_entry = get_active_schedule_entry(state)
    if active_entry:
        return schedule_label_for_entry(active_entry)
    runtime_state = resolve_runtime_state(state)
    if runtime_state["schedule_sheet_id"] and runtime_state["schedule_sheet_name"]:
        fallback_entry = build_schedule_entry(runtime_state["schedule_sheet_id"], runtime_state["schedule_sheet_name"], runtime_state["schedule_sheet_gid"])
        for field in SCHEDULE_ENTRY_FIELDS:
            fallback_entry[field] = runtime_state.get(field, fallback_entry[field])
        return schedule_label_for_entry(fallback_entry)
    return "Chưa bật"

def should_run_schedule_entry(now: datetime, entry: Optional[dict]):
    if not entry or entry.get("schedule_mode") == "off":
        return False
    if entry.get("schedule_end_date"):
        try:
            end_date = datetime.strptime(entry["schedule_end_date"], "%Y-%m-%d").date()
            if now.date() > end_date:
                return False
        except Exception:
            pass

    hour, minute = parse_schedule_time(entry.get("schedule_time", "09:00"))
    if now.hour != hour or now.minute != minute:
        return False

    key = ""
    if entry.get("schedule_mode") == "daily":
        key = f"daily-{now.strftime('%Y-%m-%d')}"
    elif entry.get("schedule_mode") == "weekly":
        if now.weekday() != int(entry.get("schedule_weekday", 0) or 0):
            return False
        year, week, _ = now.isocalendar()
        key = f"weekly-{year}-{week}"
    elif entry.get("schedule_mode") == "monthly":
        if now.day != int(entry.get("schedule_monthday", 1) or 1):
            return False
        key = f"monthly-{now.strftime('%Y-%m')}"
    else:
        return False

    if key == entry.get("last_schedule_run_key"):
        return False
    entry["last_schedule_run_key"] = key
    return True


def should_run_schedule(now: datetime, state=None):
    active_entry = get_active_schedule_entry(state)
    if active_entry:
        return should_run_schedule_entry(now, active_entry)
    runtime_state = resolve_runtime_state(state)
    fallback_entry = build_schedule_entry(runtime_state["schedule_sheet_id"], runtime_state["schedule_sheet_name"], runtime_state["schedule_sheet_gid"])
    for field in SCHEDULE_ENTRY_FIELDS:
        fallback_entry[field] = runtime_state.get(field, fallback_entry[field])
    if should_run_schedule_entry(now, fallback_entry):
        runtime_state["last_schedule_run_key"] = fallback_entry["last_schedule_run_key"]
        return True
    return False

def schedule_worker():
    while not scheduler_stop_event.is_set():
        try:
            now = datetime.now()
            for runtime_state in iter_runtime_states():
                if runtime_state["is_running"]:
                    continue
                for entry in get_scheduled_entries(runtime_state):
                    if runtime_state["is_running"]:
                        break
                    if not should_run_schedule_entry(now, entry):
                        continue
                    runtime_state["active_schedule_key"] = entry["key"]
                    runtime_state["schedule_tracking_key"] = entry["key"]
                    sync_runtime_state_from_schedule_entry(entry, runtime_state)
                    add_log(f"Kích hoạt lịch tự động: {schedule_label_for_entry(entry)}", runtime_state)
                    binding = {
                        "sheet_id": entry["schedule_sheet_id"],
                        "sheet_name": entry["schedule_sheet_name"],
                    }
                    if not binding["sheet_id"] or not binding["sheet_name"]:
                        add_log("Bỏ qua lịch tự động vì chưa có sheet/tab được lưu cho lịch.", runtime_state)
                    else:
                        try:
                            run_scraper_logic(
                                sheet_id=binding["sheet_id"],
                                sheet_name=binding["sheet_name"],
                                targets=entry.get("schedule_targets"),
                                source="schedule",
                                state=runtime_state,
                            )
                        finally:
                            sync_schedule_entry_from_runtime_state(entry, runtime_state)
                            persist_runtime_schedule_entries(runtime_state)
        except Exception as e:
            add_log(f"Lỗi lịch tự động: {str(e)}")
        scheduler_stop_event.wait(20)

def ensure_scheduler_thread():
    global scheduler_thread
    if scheduler_thread and scheduler_thread.is_alive():
        return
    scheduler_stop_event.clear()
    scheduler_thread = threading.Thread(target=schedule_worker, daemon=True)
    scheduler_thread.start()

def get_gspread_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    service_account_json = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
    service_account_json_base64 = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_BASE64") or "").strip()

    if service_account_json:
        try:
            info = json.loads(service_account_json)
            creds = Credentials.from_service_account_info(info, scopes=scopes)
            return gspread.authorize(creds)
        except Exception as exc:
            raise ValueError(
                "GOOGLE_SERVICE_ACCOUNT_JSON không hợp lệ. "
                "Vui lòng dán nguyên JSON service account."
            ) from exc

    if service_account_json_base64:
        try:
            decoded = base64.b64decode(service_account_json_base64).decode("utf-8")
            info = json.loads(decoded)
            creds = Credentials.from_service_account_info(info, scopes=scopes)
            return gspread.authorize(creds)
        except Exception as exc:
            raise ValueError(
                "GOOGLE_SERVICE_ACCOUNT_JSON_BASE64 không hợp lệ. "
                "Vui lòng kiểm tra lại chuỗi base64."
            ) from exc

    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise FileNotFoundError(
            "Thiếu Google credentials. Hãy set GOOGLE_SERVICE_ACCOUNT_JSON "
            "(hoặc GOOGLE_SERVICE_ACCOUNT_JSON_BASE64) trên môi trường deploy."
        )

    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
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

def get_worksheet(sheet_name, sheet_id: Optional[str] = None, state=None):
    runtime_state = resolve_runtime_state(state)
    resolved_sheet_id = sheet_id or runtime_state["active_sheet_id"] or ACTIVE_SHEET_ID
    resolved_sheet_name = sheet_name
    if (
        runtime_state.get("_cached_worksheet_id") == resolved_sheet_id
        and runtime_state.get("_cached_worksheet_name") == resolved_sheet_name
        and runtime_state.get("_cached_worksheet") is not None
    ):
        return runtime_state["_cached_worksheet"]
    
    def _open_worksheet():
        gc = get_gspread_client()
        spreadsheet = gc.open_by_key(resolved_sheet_id)
        return spreadsheet.worksheet(resolved_sheet_name)
    
    worksheet = retry_with_backoff(_open_worksheet, max_retries=4, base_delay=2, handle_quota=True)
    runtime_state["_cached_worksheet_id"] = resolved_sheet_id
    runtime_state["_cached_worksheet_name"] = resolved_sheet_name
    runtime_state["_cached_worksheet"] = worksheet
    return worksheet

def list_spreadsheet_tabs(sheet_input: str):
    sheet_id = extract_sheet_id(sheet_input or "")
    if not sheet_id:
        raise ValueError("Link/ID spreadsheet không hợp lệ.")
    
    global SHEET_TABS_CACHE
    now = datetime.now()
    cache_entry = SHEET_TABS_CACHE.get(sheet_id)
    if cache_entry:
        try:
            updated_at = datetime.fromisoformat(cache_entry["updated_at"])
            if (now - updated_at).total_seconds() < SHEET_TABS_CACHE_TTL_SECONDS:
                return cache_entry["tabs"]
        except Exception:
            pass
    
    def _fetch_tabs():
        gc = get_gspread_client()
        spreadsheet = gc.open_by_key(sheet_id)
        return [
            {
                "title": ws.title,
                "gid": str(ws.id),
            }
            for ws in spreadsheet.worksheets()
        ]
    
    tabs = retry_with_backoff(_fetch_tabs, max_retries=4, base_delay=2, handle_quota=True)
    SHEET_TABS_CACHE[sheet_id] = {"updated_at": now.isoformat(), "tabs": tabs}
    save_sheet_tabs_cache(SHEET_TABS_CACHE)
    return tabs

def set_active_sheet(sheet_name, sheet_id: Optional[str] = None, state=None):
    runtime_state = resolve_runtime_state(state)
    target_sheet_id = sheet_id or runtime_state["active_sheet_id"] or ACTIVE_SHEET_ID
    if (
        target_sheet_id == runtime_state.get("active_sheet_id")
        and sheet_name == runtime_state.get("active_sheet_name")
        and runtime_state.get("_cached_worksheet") is not None
    ):
        return
    ws = get_worksheet(sheet_name, target_sheet_id, runtime_state)
    if not ws:
        raise ValueError(f"Không thể truy cập tab: {sheet_name}")
    runtime_state["active_sheet_id"] = target_sheet_id
    runtime_state["active_sheet_name"] = sheet_name
    runtime_state["active_sheet_gid"] = str(ws.id) if hasattr(ws, 'id') else "0"
    add_log(f"Đã chọn sheet: {sheet_name} | Spreadsheet ID: {runtime_state['active_sheet_id']}", runtime_state)

def is_quota_or_rate_limit_error(exc: Exception) -> bool:
    msg = str(exc or "").lower()
    return (
        "quota exceeded" in msg
        or "rate limit" in msg
        or "too many requests" in msg
        or "429" in msg
    )

def retry_with_backoff(func, max_retries=3, base_delay=2, handle_quota=True):
    """Retry a function with exponential backoff on connection errors and quota errors"""
    import time
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            error_msg = str(e).lower()
            is_quota_error = "quota exceeded" in error_msg or "429" in error_msg
            is_conn_error = any(keyword in error_msg for keyword in ['connection', 'reset', 'aborted', 'timeout', '10054', 'forcibly closed'])
            
            if (is_conn_error or (handle_quota and is_quota_error)) and attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt) + attempt
                if is_quota_error:
                    delay = max(delay, 15)  # At least 15s for quota errors
                add_log(f"{'Quota exceeded' if is_quota_error else 'Connection error'}, retrying in {delay}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(delay)
                continue
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


def is_optional_view_metric(url: str, platform: str = "") -> bool:
    url_lower = str(url or "").strip().lower()
    platform_key = str(platform or "").strip().lower()
    if not url_lower:
        return False
    if platform_key == "tiktok" and "/photo/" in url_lower:
        return True
    if platform_key == "instagram" and "/p/" in url_lower and "/reel/" not in url_lower and "/tv/" not in url_lower:
        return True
    if platform_key == "facebook":
        if any(marker in url_lower for marker in ["/reel/", "/videos/", "fb.watch"]):
            return False
        if any(marker in url_lower for marker in ["/groups/", "/posts/", "/permalink/", "/photo", "/photos/", "/share/p/"]):
            return True
    return False

def normalize_header(text: str) -> str:
    value = unicodedata.normalize("NFD", (text or "").strip().lower())
    value = "".join(ch for ch in value if unicodedata.category(ch) != "Mn")
    value = re.sub(r"[^a-z0-9]+", "", value)
    return value

def header_matches_alias(key: str, aliases, allow_partial: bool = True) -> bool:
    if not key:
        return False
    if key in aliases:
        return True
    if not allow_partial:
        return False
    for alias in aliases:
        if not alias:
            continue
        alias_len = len(alias)
        if alias_len < 3:
            continue
        if alias_len <= 4:
            if (key.startswith(alias) or key.endswith(alias)) and abs(len(key) - alias_len) <= 2:
                return True
            continue
        if key.startswith(alias) or key.endswith(alias):
            return True
        if alias_len >= 6 and alias in key:
            return True
    return False

def detect_columns_from_headers(headers, allow_partial: bool = True):
    columns = {}
    # Map each header index to its normalized key
    header_keys = []
    for idx, header in enumerate(headers or [], start=1):
        key = normalize_header(header)
        header_keys.append((idx, key))

    # Pass 1: exact alias matches only (key is directly in the alias set)
    matched_indices = set()
    for idx, key in header_keys:
        if not key or idx in matched_indices:
            continue
        for field, names in HEADER_ALIASES.items():
            if field in columns:
                continue
            if key in names:
                columns[field] = idx
                matched_indices.add(idx)
                break

    # Pass 2: substring / partial matches for remaining headers
    if allow_partial:
        for idx, key in header_keys:
            if not key or idx in matched_indices:
                continue
            for field, names in HEADER_ALIASES.items():
                if field in columns:
                    continue
                if header_matches_alias(key, names, allow_partial=True):
                    columns[field] = idx
                    matched_indices.add(idx)
                    break
    return columns

def detect_sheet_layout(sheet, sample_rows: int = 30, use_cache: bool = True):
    global SHEET_LAYOUT_CACHE
    sheet_id = getattr(getattr(sheet, "spreadsheet", None), "id", "") or ""
    sheet_name = getattr(sheet, "title", "") or ""
    cache_key = f"{sheet_id}:{sheet_name}"
    now = datetime.now()
    cached_layout_entry = SHEET_LAYOUT_CACHE.get(cache_key)
    if use_cache and cached_layout_entry:
        try:
            updated_at = datetime.fromisoformat(cached_layout_entry.get("updated_at", ""))
            if (now - updated_at).total_seconds() < SHEET_LAYOUT_CACHE_TTL_SECONDS:
                return dict(cached_layout_entry.get("layout") or {})
        except Exception:
            pass

    best_row = 1
    best_headers = []
    best_columns = {}
    best_score = -1
    max_rows = max(1, int(sample_rows or 1))

    # Fetch all sample rows in ONE API call to avoid quota exhaustion
    try:
        range_notation = f"1:{max_rows}"
        batch_rows = sheet.get(range_notation) or []
    except Exception as exc:
        if cached_layout_entry and is_quota_or_rate_limit_error(exc):
            return dict(cached_layout_entry.get("layout") or {})
        # Fallback: single call for row 1
        try:
            batch_rows = [sheet.row_values(1)]
        except Exception:
            batch_rows = []

    for row_idx, headers in enumerate(batch_rows, start=1):
        if not any(str(cell or "").strip() for cell in headers):
            continue
        strict_columns = detect_columns_from_headers(headers, allow_partial=False)
        relaxed_columns = detect_columns_from_headers(headers, allow_partial=True)
        columns = dict(strict_columns)
        for field, col_idx in relaxed_columns.items():
            columns.setdefault(field, col_idx)

        populated_cells = sum(1 for cell in headers if str(cell or "").strip())
        if populated_cells < 3 and len(columns) < 2:
            continue

        score = (len(strict_columns) * 5) + (len(columns) * 2)
        if "link" in strict_columns:
            score += 8
        elif "link" in columns:
            score += 4
        if any(metric in strict_columns for metric in ("view", "like", "share", "comment", "save", "buzz")):
            score += 4
        elif any(metric in columns for metric in ("view", "like", "share", "comment", "save", "buzz")):
            score += 2
        if "date" in strict_columns or "air_date" in strict_columns:
            score += 2

        if score > best_score:
            best_row = row_idx
            best_headers = headers
            best_columns = columns
            best_score = score

    if best_score < 0 and batch_rows:
        best_headers = batch_rows[0]
        best_columns = detect_columns_from_headers(best_headers)

    layout_result = {
        "header_row": best_row,
        "headers": best_headers,
        "columns": best_columns,
    }
    SHEET_LAYOUT_CACHE[cache_key] = {
        "updated_at": now.isoformat(),
        "layout": layout_result,
    }
    return layout_result

def detect_sheet_columns(sheet):
    return detect_sheet_layout(sheet).get("columns", {})

def build_unique_headers(headers):
    seen = {}
    unique_headers = []
    for raw_header in headers or []:
        header = str(raw_header or "").strip()
        count = seen.get(header, 0) + 1
        seen[header] = count
        if header and count > 1:
            unique_headers.append(f"{header}__dup{count}")
        else:
            unique_headers.append(header)
    return unique_headers


def get_sheet_records(sheet, layout=None, include_row_values: bool = False):
    resolved_layout = layout or detect_sheet_layout(sheet)
    header_row = max(1, int(resolved_layout.get("header_row") or 1))
    headers = list(resolved_layout.get("headers") or [])
    
    # Cache logic for all_values
    global SHEET_DATA_CACHE
    sheet_id = getattr(sheet.spreadsheet, "id", "")
    sheet_name = getattr(sheet, "title", "")
    cache_key = f"{sheet_id}:{sheet_name}"
    
    now = datetime.now()
    all_values = None
    cache_entry = SHEET_DATA_CACHE.get(cache_key)
    if cache_entry:
        try:
            updated_at = datetime.fromisoformat(cache_entry["updated_at"])
            if (now - updated_at).total_seconds() < SHEET_DATA_CACHE_TTL_SECONDS:
                all_values = cache_entry["data"]
        except Exception:
            pass
    
    if all_values is None:
        stale_values = None
        if cache_entry:
            stale_values = cache_entry.get("data")
        try:
            all_values = sheet.get_all_values()
            SHEET_DATA_CACHE[cache_key] = {
                "updated_at": now.isoformat(),
                "data": all_values
            }
            save_sheet_data_cache(SHEET_DATA_CACHE)
        except Exception as exc:
            if stale_values and is_quota_or_rate_limit_error(exc):
                all_values = stale_values
            else:
                raise
        
    if not headers and len(all_values) >= header_row:
        headers = list(all_values[header_row - 1] or [])
    unique_headers = build_unique_headers(headers)
    max_width = max(len(unique_headers), max((len(row) for row in all_values[header_row:]), default=0))
    if len(unique_headers) < max_width:
        unique_headers.extend([f"__extra_{idx}" for idx in range(len(unique_headers) + 1, max_width + 1)])

    records = []
    for row in all_values[header_row:]:
        padded_row = list(row or []) + [""] * max(0, max_width - len(row or []))
        record = {unique_headers[idx]: padded_row[idx] for idx in range(max_width)}
        if include_row_values:
            record["__row_values__"] = padded_row
        records.append(record)
    return records, header_row, headers

def resolve_effective_start_row(header_row: int, state=None) -> int:
    runtime_state = resolve_runtime_state(state)
    return max(2, runtime_state["start_row"], int(header_row or 1) + 1)

def format_detected_columns_text(layout, state=None) -> str:
    if not layout:
        return ""
    col_map = apply_column_overrides(layout.get("columns") or {}, state=state)
    ordered_fields = ["date", "air_date", "link", "view", "like", "share", "comment", "buzz", "save"]
    parts = []
    for field in ordered_fields:
        col_idx = col_map.get(field)
        if col_idx:
            parts.append(f"{field.upper()}={col_to_a1(col_idx)}")
    if not parts:
        return f"AUTO chưa nhận được cột nào ở header dòng {layout.get('header_row') or 1}"
    return f"AUTO nhận header dòng {layout.get('header_row') or 1}: " + ", ".join(parts)

def apply_column_overrides(columns, overrides=None, state=None):
    merged = dict(columns or {})
    runtime_state = resolve_runtime_state(state)
    override_map = overrides if isinstance(overrides, dict) else runtime_state["column_overrides"]
    for field, col_idx in override_map.items():
        if col_idx:
            merged[field] = col_idx
    return merged

def apply_column_overrides_for_tab(columns, tab_name: str, state=None):
    runtime_state = resolve_runtime_state(state)
    resolved_tab = str(tab_name or "").strip()
    tab_overrides = (runtime_state.get("column_overrides_by_tab") or {}).get(resolved_tab)
    if tab_overrides:
        return apply_column_overrides(columns, overrides=tab_overrides)
    return apply_column_overrides(columns, state=runtime_state)

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

def first_nonempty_value_by_alias(record, *aliases):
    if not isinstance(record, dict):
        return ""
    normalized_aliases = [normalize_header(alias) for alias in aliases if alias]
    for key, value in record.items():
        normalized_key = normalize_header(str(key or ""))
        if not normalized_key or not header_matches_alias(normalized_key, normalized_aliases):
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


def read_record_value_from_column(record, col_idx: Optional[int]):
    if not record or not col_idx or col_idx <= 0:
        return ""
    row_values = record.get("__row_values__") if isinstance(record, dict) else None
    if not isinstance(row_values, list) or col_idx > len(row_values):
        return ""
    value = row_values[col_idx - 1]
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return value if str(value).strip() else ""

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

AUTH_SETTINGS = load_auth_settings()

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
        if path_parts and path_parts[0] == "groups":
            return "Nhóm Facebook"
        if path_parts and path_parts[0] == "reel":
            return "Facebook reel"
        return "Facebook page"
    return host or platform


def resolve_post_creator_name(normalized_record, link: str, platform: str) -> str:
    normalized_record = normalized_record if isinstance(normalized_record, dict) else {}
    if platform == "Facebook":
        facebook_creator = str(
            first_nonempty_value(
                normalized_record,
                "groupprofile",
                "group",
                "groupname",
                "tennhom",
                "tennhomfb",
                "profile",
                "profilename",
                "page",
                "pagename",
                "tenpage",
                "fanpage",
                "pageprofile",
                "kol",
                "creator",
                "author",
                "username",
                "account",
                "channel",
            )
            or first_nonempty_value_by_alias(
                normalized_record,
                "group/profile",
                "group profile",
                "group",
                "group name",
                "ten nhom",
                "ten nhom fb",
                "profile",
                "profile name",
                "page",
                "page name",
                "fanpage",
                "page/profile",
            )
            or ""
        ).strip()
        return facebook_creator or infer_creator_name(link, platform)
    return str(
        first_nonempty_value(normalized_record, "kol", "creator", "author", "username", "account", "channel")
        or ""
    ).strip() or infer_creator_name(link, platform)

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
        if path_parts and path_parts[0] == "groups" and len(path_parts) > 1:
            slug = f"groups/{path_parts[1]}"
        elif path_parts and path_parts[0] == "profile.php":
            slug = f"profile/{parsed.query}" if parsed.query else (host or "facebook.com")
        elif path_parts and path_parts[0] in {"share", "reel", "watch", "posts", "photo", "photos", "permalink"}:
            slug = host or "facebook.com"
        else:
            slug = path_parts[0] if path_parts else host
        return slug or "facebook.com"
    return host or platform

def extract_brand_label_from_record(normalized_record) -> str:
    if not isinstance(normalized_record, dict):
        return ""
    return str(
        first_nonempty_value(
            normalized_record,
            "thuonghieu",
            "tenthuonghieu",
            "brand",
            "brandname",
            "nhanhang",
            "tennhanhang",
            "client",
            "clientname",
        )
        or ""
    ).strip()

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

def collect_posts_dataset_for_worksheet(
    ws,
    tab_index: int = 0,
    sheet_id: Optional[str] = None,
    sheet_slug: str = "",
    campaign_override: str = "",
    state=None,
):
    runtime_state = resolve_runtime_state(state)
    sheet_title = str(getattr(ws, "title", "") or f"Sheet {tab_index + 1}")
    resolved_sheet_id = str(sheet_id or runtime_state["active_sheet_id"] or "").strip()
    resolved_sheet_slug = sheet_slug or f"{build_dom_slug(sheet_title, 'sheet')}-{tab_index}"
    resolved_campaign_override = str(campaign_override or "").strip()
    platform_counts = {"tiktok": 0, "facebook": 0, "instagram": 0, "youtube": 0, "khac": 0}
    rows_html = []
    total_posts = 0
    total_views = 0
    total_reaction = 0
    total_share = 0
    total_comment = 0
    total_buzz = 0
    creators = set()
    campaigns = set()
    brand_counts = {}
    error = ""

    try:
        layout = detect_sheet_layout(ws, sample_rows=40, use_cache=False)
        col_map = apply_column_overrides_for_tab(layout.get("columns"), sheet_title, state=runtime_state)
        records, header_row, headers = get_sheet_records(ws, layout, include_row_values=True)
        link_header = resolve_header_from_column(headers, col_map.get("link"))
        campaign_header = resolve_header_from_column(headers, col_map.get("campaign"))
        start_row = resolve_effective_start_row(header_row, runtime_state)
    except Exception as exc:
        error = str(exc)
        return {
            "sheet_title": sheet_title,
            "sheet_slug": resolved_sheet_slug,
            "sheet_id": resolved_sheet_id,
            "sheet_gid": str(getattr(ws, "id", "") or "0"),
            "total_posts": 0,
            "total_views": 0,
            "total_reaction": 0,
            "total_share": 0,
            "total_comment": 0,
            "total_buzz": 0,
            "creator_count": 0,
            "campaign_count": 0,
            "brand_label": "",
            "platform_counts": platform_counts,
            "rows_html": "",
            "error": error,
        }

    for row_idx, record in enumerate(records, start=header_row + 1):
        if row_idx < start_row:
            continue

        normalized_record = {normalize_header(str(key)): value for key, value in (record or {}).items()}
        link = str(
            read_record_value_from_column(record, col_map.get("link"))
            or
            read_record_value_from_header(record, normalized_record, link_header)
            or first_nonempty_value(normalized_record, "link", "url", "posturl", "linkpost")
        ).strip()
        if not link:
            continue

        platform = detect_platform(link)
        platform_key = normalize_header(platform)
        if platform_key not in platform_counts:
            platform_key = "khac"
        platform_counts[platform_key] += 1

        creator = resolve_post_creator_name(normalized_record, link, platform)
        brand_label = extract_brand_label_from_record(normalized_record)
        campaign = (
            resolved_campaign_override
            or str(
                read_record_value_from_header(record, normalized_record, campaign_header)
                or first_nonempty_value(normalized_record, "campaign", "chiendich", "camp")
            ).strip()
            or sheet_title
        )
        title = str(
            first_nonempty_value(normalized_record, "caption", "title", "content", "noidung", "post", "mota")
        ).strip() or infer_post_title(link, platform)
        raw_date_text = resolve_dashboard_air_date_value(record, normalized_record, col_map) or resolve_dashboard_date_value(record, normalized_record, col_map)
        date_text = format_dashboard_date_text(raw_date_text) if raw_date_text else "-"
        date_title = format_dashboard_date_text(raw_date_text, include_time=True) if raw_date_text else "-"
        view = parse_metric_number(read_record_value_from_column(record, col_map.get("view")) or first_nonempty_value(normalized_record, "view", "views", "luotxem"))
        reaction = parse_metric_number(read_record_value_from_column(record, col_map.get("like")) or first_nonempty_value(normalized_record, "like", "likes", "reaction", "reactions"))
        share = parse_metric_number(read_record_value_from_column(record, col_map.get("share")) or first_nonempty_value(normalized_record, "share", "shares"))
        comment = parse_metric_number(read_record_value_from_column(record, col_map.get("comment")) or first_nonempty_value(normalized_record, "comment", "comments", "cmt"))
        save = parse_metric_number(read_record_value_from_column(record, col_map.get("save")) or first_nonempty_value(normalized_record, "save", "saves", "saved", "bookmark", "bookmarks", "luu"))
        buzz_raw = (
            read_record_value_from_column(record, col_map.get("buzz"))
            or first_nonempty_value(normalized_record, "buzz", "buzzcount", "totalbuzz", "tongbuzz")
        )
        buzz = parse_metric_number(buzz_raw) if str(buzz_raw or "").strip() else (share + comment)
        plan = str(first_nonempty_value(normalized_record, "plan", "nam", "period", "fiscalyear") or "-").strip()
        line_product = str(first_nonempty_value(normalized_record, "line_product", "lineproduct", "sanpham", "nhanhang", "line", "product") or "-").strip()
        kol_tier = str(first_nonempty_value(normalized_record, "kol_tier", "koltier", "tier", "phanloaikol", "kolevel") or "-").strip()
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
        safe_campaign_meta = html.escape(shorten_text(title, 28) if title else shorten_text(platform, 28))
        safe_date = html.escape(date_text)
        safe_date_title = html.escape(date_title, quote=True)
        safe_content_meta = html.escape(shorten_text(link, 76))
        safe_sheet_name_attr = html.escape(sheet_title, quote=True)
        safe_title_attr = html.escape(title, quote=True)
        safe_platform_attr = html.escape(platform, quote=True)
        safe_campaign_attr = html.escape(campaign, quote=True)
        safe_plan = html.escape(plan)
        safe_line = html.escape(line_product)
        safe_tier = html.escape(kol_tier)
        search_blob = html.escape(
            " ".join([title, creator, campaign, platform, link, date_text, sheet_title, plan, line_product, kol_tier]).lower(),
            quote=True,
        )

        rows_html.append(
            f"""
            <tr class="post-row posts-table-row" data-platform="{platform_key}" data-search="{search_blob}">
                <td class="posts-cell posts-cell-check">
                    <input
                        type="checkbox"
                        class="posts-table-check post-select-check"
                        data-post-select
                        data-sheet-id="{html.escape(resolved_sheet_id, quote=True)}"
                        data-sheet-gid="{html.escape(str(getattr(ws, 'id', '') or '0'), quote=True)}"
                        data-sheet-name="{safe_sheet_name_attr}"
                        data-row-idx="{row_idx}"
                        data-link="{safe_link}"
                        data-title="{safe_title_attr}"
                        data-platform-name="{safe_platform_attr}"
                        aria-label="Chọn dòng {row_idx}"
                    />
                </td>
                <td class="posts-cell posts-cell-content" data-post-col="content">
                    <div class="post-content-wrap">
                        <a href="{safe_link}" target="_blank" rel="noreferrer" class="post-title-link">{safe_title}</a>
                        <a href="{safe_link}" target="_blank" rel="noreferrer" class="post-content-meta">{safe_content_meta}</a>
                    </div>
                </td>
                <td class="posts-cell" data-post-col="creator">
                    <div class="flex items-center gap-3">
                        <div class="post-avatar post-avatar-{platform_key}">{avatar}</div>
                        <div>
                            <div class="post-creator-name">{safe_creator}</div>
                            <div class="post-creator-handle">{safe_creator_handle}</div>
                        </div>
                    </div>
                </td>
                <td class="posts-cell" data-post-col="status"><span class="post-status-pill {status_class}">{status_label}</span></td>
                <td class="posts-cell text-xs font-bold text-slate-400" data-post-col="plan">{safe_plan}</td>
                <td class="posts-cell text-xs font-bold text-slate-400" data-post-col="line">{safe_line}</td>
                <td class="posts-cell" data-post-col="tier">
                    {f'<span class="px-2 py-0.5 rounded-md bg-amber-500/10 text-amber-500 text-[10px] font-black border border-amber-500/20">{safe_tier}</span>' if safe_tier and safe_tier != "-" else "-"}
                </td>
                <td class="posts-cell posts-cell-date" data-post-col="date" title="{safe_date_title}">{safe_date}</td>
                <td class="posts-cell posts-cell-metric" data-post-col="view">{format_table_metric(view)}</td>
                <td class="posts-cell posts-cell-metric" data-post-col="reaction">{format_table_metric(reaction)}</td>
                <td class="posts-cell posts-cell-metric" data-post-col="share">{format_table_metric(share)}</td>
                <td class="posts-cell posts-cell-metric" data-post-col="comment">{format_table_metric(comment)}</td>
                <td class="posts-cell posts-cell-metric" data-post-col="buzz">{format_table_metric(buzz)}</td>
            </tr>
            """
        )

        total_posts += 1
        total_views += view
        total_reaction += reaction
        total_share += share
        total_comment += comment
        total_buzz += buzz
        if creator:
            creators.add(creator)
        if campaign:
            campaigns.add(campaign)
        if brand_label:
            brand_counts[brand_label] = brand_counts.get(brand_label, 0) + 1

    primary_brand_label = ""
    if brand_counts:
        primary_brand_label = sorted(
            brand_counts.items(),
            key=lambda item: (-item[1], item[0].lower()),
        )[0][0]

    return {
        "sheet_title": sheet_title,
        "sheet_slug": resolved_sheet_slug,
        "sheet_id": resolved_sheet_id,
        "sheet_gid": str(getattr(ws, "id", "") or "0"),
        "total_posts": total_posts,
        "total_views": total_views,
        "total_reaction": total_reaction,
        "total_share": total_share,
        "total_comment": total_comment,
        "total_buzz": total_buzz,
        "creator_count": len(creators),
        "campaign_count": len(campaigns),
        "brand_label": primary_brand_label,
        "platform_counts": platform_counts,
        "rows_html": "".join(rows_html),
        "error": error,
    }

def parse_dashboard_date(value):
    if isinstance(value, datetime):
        return value
    raw = str(value or "").strip()
    if not raw or raw == "-":
        return None
    normalized_raw = raw.replace("T", " ").replace("Z", "").strip()
    if re.fullmatch(r"\d+(?:\.\d+)?", normalized_raw):
        try:
            return datetime(1899, 12, 30) + timedelta(days=float(normalized_raw))
        except Exception:
            pass
    for fmt in (
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y",
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y %H:%M",
        "%d-%m-%Y",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(normalized_raw, fmt)
        except Exception:
            continue
    try:
        return datetime.fromisoformat(normalized_raw)
    except Exception:
        pass
    short_day_month = re.fullmatch(r"([0-3]?\d)\s*[-/.]\s*([01]?\d)", normalized_raw)
    if short_day_month:
        day_token = int(short_day_month.group(1))
        month_token = int(short_day_month.group(2))
        current_year = datetime.now().year
        try:
            return datetime(current_year, month_token, day_token)
        except Exception:
            return None
    return None


def resolve_dashboard_date_value(record, normalized_record, col_map) -> str:
    normalized_record = normalized_record if isinstance(normalized_record, dict) else {}
    return str(
        read_record_value_from_column(record, col_map.get("date"))
        or first_nonempty_value(normalized_record, *DASHBOARD_DATE_KEYS)
        or ""
    ).strip()

def resolve_dashboard_air_date_value(record, normalized_record, col_map) -> str:
    normalized_record = normalized_record if isinstance(normalized_record, dict) else {}
    return str(
        read_record_value_from_column(record, col_map.get("air_date"))
        or first_nonempty_value(
            normalized_record,
            "airdate",
            "aireddate",
            "ngayair",
            "ngaydang",
            "ngayairbai",
            "ngaydangbai",
            "publishdate",
            "publisheddate",
        )
        or ""
    ).strip()


def format_dashboard_date_text(value, include_time: bool = False) -> str:
    raw = str(value or "").strip()
    if not raw or raw == "-":
        return "-"
    parsed = parse_dashboard_date(raw)
    if not parsed:
        return raw
    if include_time:
        return parsed.strftime("%d/%m/%Y %H:%M")
    return parsed.strftime("%d/%m/%Y")


def format_air_date_text(value) -> str:
    parsed = parse_dashboard_date(value)
    if parsed:
        return f"{parsed.day}-{parsed.month}"
    raw = str(value or "").strip()
    token_match = re.fullmatch(r"([01]?\d)\s*[-/.]\s*([0-3]?\d)", raw)
    if token_match:
        month_token, day_token = token_match.groups()
        return f"{int(day_token)}-{int(month_token)}"
    return raw

def build_overview_panel_html(sheet, snapshot_url: str, status_payload, schedule_text: str, state=None):
    runtime_state = resolve_runtime_state(state)
    saved_entries = get_saved_sheet_entries(owner_email=runtime_state["owner_email"])
    no_campaign_label = "Chưa gắn chiến dịch"
    total_posts = 0
    total_views = 0
    total_buzz = 0
    creators = set()
    campaigns = {}
    brands_summary = {}
    timeline_rows = []
    overview_errors = []

    overview_sources = []
    if saved_entries:
        overview_sources = [dict(entry) for entry in saved_entries]
    elif runtime_state.get("active_sheet_id") and runtime_state.get("active_sheet_name"):
        overview_sources = [
            {
                "sheet_id": str(runtime_state.get("active_sheet_id", "") or "").strip(),
                "sheet_name": str(runtime_state.get("active_sheet_name", "") or "").strip(),
                "campaign_label": "",
            }
        ]

    for source_index, source_entry in enumerate(overview_sources):
        entry_sheet_id = str(source_entry.get("sheet_id", "") or "").strip()
        entry_sheet_name = str(source_entry.get("sheet_name", "") or "").strip()
        entry_campaign_label = str(source_entry.get("campaign_label", "") or "").strip()
        if not entry_sheet_id or not entry_sheet_name:
            continue
        try:
            ws = sheet if (
                sheet is not None
                and entry_sheet_id == str(runtime_state.get("active_sheet_id", "") or "").strip()
                and entry_sheet_name == str(runtime_state.get("active_sheet_name", "") or "").strip()
            ) else get_worksheet(entry_sheet_name, entry_sheet_id, runtime_state)
            layout = detect_sheet_layout(ws)
            col_map = apply_column_overrides_for_tab(layout.get("columns"), entry_sheet_name, state=runtime_state)
            records, header_row, headers = get_sheet_records(ws, layout, include_row_values=True)
            link_header = resolve_header_from_column(headers, col_map.get("link"))
            campaign_header = resolve_header_from_column(headers, col_map.get("campaign"))
            start_row = resolve_effective_start_row(header_row, runtime_state)
        except Exception as exc:
            overview_errors.append(f"{entry_sheet_name or f'Sheet {source_index + 1}'}: {exc}")
            continue

        entry_brand_label = str(source_entry.get("brand_label", "") or "").strip()
        for row_idx, record in enumerate(records, start=header_row + 1):
            if row_idx < start_row:
                continue
            normalized_record = {normalize_header(str(key)): value for key, value in (record or {}).items()}
            link = str(
                read_record_value_from_column(record, col_map.get("link"))
                or read_record_value_from_header(record, normalized_record, link_header)
                or first_nonempty_value(normalized_record, "link", "url", "posturl", "linkpost")
            ).strip()
            if not link:
                continue

            platform = detect_platform(link)
            creator = resolve_post_creator_name(normalized_record, link, platform)
            brand_name = entry_brand_label or extract_brand_label_from_record(normalized_record) or "Chưa gắn"
            campaign_name = str(
                entry_campaign_label
                or read_record_value_from_header(record, normalized_record, campaign_header)
                or first_nonempty_value(normalized_record, "campaign", "chiendich", "camp")
            ).strip()
            view = parse_metric_number(read_record_value_from_column(record, col_map.get("view")) or first_nonempty_value(normalized_record, "view", "views", "luotxem"))
            reaction = parse_metric_number(read_record_value_from_column(record, col_map.get("like")) or first_nonempty_value(normalized_record, "like", "likes", "reaction", "reactions"))
            share = parse_metric_number(read_record_value_from_column(record, col_map.get("share")) or first_nonempty_value(normalized_record, "share", "shares"))
            comment = parse_metric_number(read_record_value_from_column(record, col_map.get("comment")) or first_nonempty_value(normalized_record, "comment", "comments", "cmt"))
            save = parse_metric_number(read_record_value_from_column(record, col_map.get("save")) or first_nonempty_value(normalized_record, "save", "saves", "saved", "bookmark", "bookmarks", "luu"))
            buzz_raw = (
                read_record_value_from_column(record, col_map.get("buzz"))
                or first_nonempty_value(normalized_record, "buzz", "buzzcount", "totalbuzz", "tongbuzz")
            )
            buzz = parse_metric_number(buzz_raw) if str(buzz_raw or "").strip() else share + comment
            air_date_value = resolve_dashboard_air_date_value(record, normalized_record, col_map)
            aired_at = parse_dashboard_date(air_date_value)

            total_posts += 1
            total_views += view
            total_buzz += buzz
            creators.add(creator.strip().lower())

            campaign_bucket = campaigns.setdefault(
                campaign_name or no_campaign_label,
                {
                    "posts": 0,
                    "views": 0,
                    "creators": set(),
                    "platforms": set(),
                    "started_at": None,
                },
            )
            campaign_bucket["posts"] += 1
            campaign_bucket["views"] += view
            campaign_bucket["creators"].add(creator.strip().lower())
            campaign_bucket["platforms"].add(platform)
            if aired_at and (campaign_bucket["started_at"] is None or aired_at < campaign_bucket["started_at"]):
                campaign_bucket["started_at"] = aired_at
            if aired_at:
                timeline_rows.append(
                    {
                        "date": aired_at.strftime("%Y-%m-%d"),
                        "creator": creator.strip().lower(),
                        "view": view,
                        "buzz": buzz,
                        "brand": brand_name,
                        "sheet_name": entry_sheet_name,
                    }
                )

            brand_bucket = brands_summary.setdefault(brand_name, {"views": 0, "posts": 0, "buzz": 0})
            brand_bucket["views"] += view
            brand_bucket["posts"] += 1
            brand_bucket["buzz"] += buzz

    named_campaigns = [name for name in campaigns.keys() if name and name != no_campaign_label]
    total_campaigns = len(named_campaigns)
    total_creators = len([item for item in creators if item])
    featured_campaign_name = no_campaign_label

    sorted_campaigns = []
    if campaigns:
        preferred_campaigns = [
            item for item in campaigns.items() if item[0] and item[0] != no_campaign_label
        ] or list(campaigns.items())
        sorted_campaigns = sorted(
            preferred_campaigns,
            key=lambda item: (item[1]["posts"], item[1]["views"]),
            reverse=True,
        )
    else:
        sorted_campaigns = [(no_campaign_label, {
            "posts": total_posts,
            "views": total_views,
            "creators": total_creators,
            "platforms": set(),
            "started_at": None,
        })]

    featured_campaign_name = sorted_campaigns[0][0] if sorted_campaigns else no_campaign_label

    status_chip_class = "overview-status-live" if runtime_state["is_running"] else ("overview-status-done" if runtime_state["is_finished"] else "overview-status-waiting")
    status_chip_text = "Đang diễn ra" if runtime_state["is_running"] else ("Đã hoàn tất" if runtime_state["is_finished"] else "Sẵn sàng")

    campaign_cards_html = []
    for camp_name, camp_data in sorted_campaigns:
        camp_creators = camp_data.get("creators", 0)
        if isinstance(camp_creators, set):
            camp_creators = len([item for item in camp_creators if item])
        camp_creators = parse_metric_number(camp_creators)

        camp_started_text = (
            camp_data["started_at"].strftime("%d/%m/%Y")
            if camp_data.get("started_at")
            else datetime.now().strftime("%d/%m/%Y")
        )
        camp_platforms_text = ", ".join(sorted(camp_data.get("platforms") or [])) or "Đa nền tảng"
        camp_slug = build_dom_slug(camp_name or "khong-gan", "campaign")
        camp_title = (
            f"Chiến dịch - {camp_name}"
            if camp_name and camp_name != no_campaign_label
            else no_campaign_label
        )
        
        card_html = f"""
            <button type="button" class="overview-campaign-card overview-campaign-card-action" data-overview-open-campaign="{html.escape(camp_slug, quote=True)}">
                <div class="flex flex-col gap-6">
                    <div>
                        <h3 class="overview-campaign-title">{html.escape(camp_title)}</h3>
                        <div class="overview-campaign-meta">
                            <span class="overview-campaign-pill {status_chip_class}">{status_chip_text}</span>
                            <span class="overview-campaign-pill overview-campaign-pill-secondary">{html.escape(camp_platforms_text)}</span>
                            <span class="overview-campaign-start">Bắt đầu {camp_started_text}</span>
                        </div>
                    </div>
                </div>

                <div class="overview-campaign-metrics">
                    <div class="overview-campaign-metric metric-posts">
                        <div class="overview-campaign-metric-label">Bài đăng</div>
                        <div class="overview-campaign-metric-value">{format_metric_number(camp_data["posts"])}</div>
                    </div>
                    <div class="overview-campaign-metric metric-views">
                        <div class="overview-campaign-metric-label">Lượt xem</div>
                        <div class="overview-campaign-metric-value">{format_metric_number(camp_data["views"])}</div>
                    </div>
                    <div class="overview-campaign-metric metric-creators">
                        <div class="overview-campaign-metric-label">Creators</div>
                        <div class="overview-campaign-metric-value">{format_metric_number(camp_creators)}</div>
                    </div>
                </div>
            </button>
        """
        campaign_cards_html.append(card_html)
        
    campaigns_rendered = '<div class="flex flex-col gap-4">' + "".join(campaign_cards_html) + "</div>"
    chart_dates = sorted({item["date"] for item in timeline_rows if item.get("date")})
    chart_period_text = "Chưa có dữ liệu ngày air bài."
    if chart_dates:
        chart_period_text = (
            f"{datetime.strptime(chart_dates[0], '%Y-%m-%d').strftime('%d/%m/%Y')}"
            f" - {datetime.strptime(chart_dates[-1], '%Y-%m-%d').strftime('%d/%m/%Y')}"
        )
    source_scope_text = (
        f"{format_metric_number(len(saved_entries))} sheet đã lưu"
        if saved_entries
        else (str(runtime_state.get("active_sheet_name", "") or "").strip() or "sheet hiện tại")
    )
    chart_subtitle = (
        f"Hiệu suất theo thương hiệu từ {source_scope_text}."
        + (f" Dữ liệu tổng hợp từ {len(brands_summary)} thương hiệu." if brands_summary else " Cần có dữ liệu thương hiệu để hiện sơ đồ.")
    )
    chart_brands = []
    if brands_summary:
        for b_name, b_data in brands_summary.items():
            ratio = round((b_data["views"] / total_views) * 100, 1) if total_views > 0 else 0
            chart_brands.append({
                "name": b_name,
                "views": b_data["views"],
                "posts": b_data["posts"],
                "buzz": b_data.get("buzz", 0),
                "ratio": ratio
            })
    
    chart_brands.sort(key=lambda x: (x["buzz"], x["views"]), reverse=True)

    chart_payload_json = json.dumps(
        {
            "sheet_name": "Tổng thể" if saved_entries else (source_scope_text or featured_campaign_name),
            "entries": timeline_rows,
            "brands": chart_brands,
        },
        ensure_ascii=False,
    ).replace("</", "<\\/")
    has_quota_error = any(
        ("quota exceeded" in str(item).lower()) or ("429" in str(item).lower())
        for item in overview_errors
    )
    if has_quota_error:
        overview_error_message = (
            "Đang chạm giới hạn đọc Google Sheet (quota). "
            "Hệ thống đang dùng dữ liệu cache gần nhất, vui lòng đợi 1-2 phút rồi tải lại."
        )
    else:
        overview_error_message = "<br>".join(html.escape(item) for item in overview_errors[:3])
        if len(overview_errors) > 3:
            overview_error_message += "<br>..."

    overview_error_html = (
        '<div class="mt-4 rounded-2xl border border-amber-500/20 bg-amber-500/10 px-4 py-3 text-sm text-amber-200">'
        + "Không tải được một phần overview:<br>"
        + overview_error_message
        + "</div>"
        if overview_errors
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
            </div>

            <div class="overview-section-title">Hiệu suất tổng thể</div>
            <div class="overview-chart-card" data-overview-chart-card>
                <script type="application/json" data-overview-chart-data>{chart_payload_json}</script>
                <div class="overview-chart-head">
                    <div>
                        <div class="overview-control-title">Hiệu suất theo thương hiệu</div>
                        <div class="overview-control-subtitle">{html.escape(chart_subtitle)}</div>
                    </div>
                    <div class="overview-head-actions">
                        <button type="button" class="overview-filter-trigger" data-overview-filter-trigger aria-label="Mở bộ lọc biểu đồ thời gian">
                            <i class="fa-regular fa-calendar"></i>
                            <span>Khoảng thời gian</span>
                            <i class="fa-solid fa-chevron-down overview-filter-trigger-chevron"></i>
                        </button>
                    </div>
                </div>
                <div class="overview-chart-filter-anchor">
                    <div class="overview-chart-control-wrap hidden" data-overview-filter-panel>
                        <div class="overview-time-filter-card">
                            <div class="overview-time-filter-title">Khoảng thời gian:</div>
                            <div class="overview-time-filter-grid">
                                <button type="button" class="overview-chart-toggle is-active" data-overview-range="7d">7 ngày qua</button>
                                <button type="button" class="overview-chart-toggle" data-overview-range="30d">30 ngày qua</button>
                                <button type="button" class="overview-chart-toggle" data-overview-range="this_month">Tháng này</button>
                                <button type="button" class="overview-chart-toggle" data-overview-range="last_month">Tháng trước</button>
                                <button type="button" class="overview-chart-toggle overview-chart-toggle-full" data-overview-range="all_time">Toàn thời gian</button>
                            </div>
                            <div class="overview-time-custom-label">Hiển thị theo:</div>
                            <div class="overview-chart-segment">
                                <button type="button" class="overview-chart-toggle is-active" data-overview-granularity="day">Theo ngày</button>
                                <button type="button" class="overview-chart-toggle" data-overview-granularity="week">Theo tuần</button>
                                <button type="button" class="overview-chart-toggle" data-overview-granularity="month">Theo tháng</button>
                            </div>
                            <div class="overview-time-custom-label">Tùy chỉnh:</div>
                            <div class="overview-chart-custom-range">
                                <div class="overview-chart-custom-row">
                                    <label>Từ:</label>
                                    <input type="date" class="overview-chart-date-input" data-overview-custom-from />
                                </div>
                                <div class="overview-chart-custom-row">
                                    <label>Đến:</label>
                                    <input type="date" class="overview-chart-date-input" data-overview-custom-to />
                                </div>
                                <div class="overview-chart-custom-actions">
                                    <button type="button" class="overview-chart-apply" data-overview-apply-custom>Áp dụng</button>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="overview-chart-meta">
                    <div class="overview-chart-legend-item"><span class="overview-chart-dot" style="background:#10b981;"></span><span>Buzz (cột)</span></div>
                    <div class="overview-chart-legend-item"><span class="overview-chart-dot" style="background:#38bdf8;"></span><span>Lượt xem (đường)</span></div>
                </div>
                <div class="overview-chart-brand-legend" data-overview-brand-legend></div>
                <div class="overview-chart-frame">
                    <svg class="overview-chart-svg" data-overview-chart-svg viewBox="0 0 960 320" preserveAspectRatio="none" aria-label="Sơ đồ hiệu suất tổng thể"></svg>
                    <div class="overview-chart-single hidden" data-overview-chart-single></div>
                    <div class="overview-chart-empty hidden" data-overview-chart-empty>
                        {html.escape("Chưa có dữ liệu ngày air bài trong các sheet đã lưu để dựng sơ đồ." if saved_entries else "Sheet này chưa có dữ liệu ngày air bài để dựng sơ đồ.")}
                    </div>
                    <div class="overview-chart-tooltip hidden" data-overview-chart-tooltip></div>
                </div>
            </div>

            {overview_error_html}
        </div>
    </section>
    """


def build_overview_panel_for_state(state=None, sheet=None):
    runtime_state = resolve_runtime_state(state)
    return build_overview_panel_html(
        sheet,
        build_snapshot_url(state=runtime_state),
        build_status_payload(runtime_state),
        schedule_label(runtime_state),
        runtime_state,
    )


def format_saved_sheet_error(error: str) -> str:
    raw = str(error or "").strip()
    if not raw:
        return ""
    raw_lower = raw.lower()
    normalized = normalize_header(raw)
    if "429" in raw_lower or "quota" in raw_lower or "readrequests" in normalized:
        return "Google Sheet đang chạm giới hạn đọc dữ liệu. Thử lại sau ít phút."
    if "permission" in raw_lower or "forbidden" in raw_lower or "insufficientpermissions" in normalized:
        return "Sheet này chưa cấp đủ quyền đọc cho hệ thống."
    if "notfound" in normalized or "khongtimthay" in normalized:
        return "Không tìm thấy sheet này hoặc tab đã bị đổi tên."
    return shorten_text(raw, 84)


def summarize_campaign_groups(saved_entries):
    groups = {}
    for entry in saved_entries:
        campaign_label = str(entry.get("campaign_label", "") or "").strip()
        if not campaign_label:
            continue
        bucket = groups.setdefault(campaign_label, {"count": 0, "sheet_names": []})
        bucket["count"] += 1
        bucket["sheet_names"].append(str(entry.get("sheet_name", "") or "").strip())
    return groups


def build_campaign_panel_html(state=None, embedded: bool = False):
    runtime_state = resolve_runtime_state(state)
    saved_entries = get_saved_sheet_entries(owner_email=runtime_state["owner_email"])
    campaign_groups = summarize_campaign_groups(saved_entries)
    campaign_library = get_saved_campaign_labels(owner_email=runtime_state["owner_email"])
    known_campaign_labels = sorted({*campaign_groups.keys(), *campaign_library}, key=lambda item: item.lower())
    campaign_group_chips = "".join(
        f'<div class="posts-chip is-active"><span>{format_metric_number(campaign_groups.get(label, {}).get("count", 0))}</span> {html.escape(label)}</div>'
        for label in known_campaign_labels
    )
    wrapper_open = (
        '<section id="chien-dich" class="settings-embedded-panel">'
        if embedded
        else '<section id="chien-dich" data-dashboard-section="chien-dich" class="dashboard-section dashboard-panel posts-board rounded-[2rem] p-6 md:p-8 mb-6 border border-white/5">'
    )
    wrapper_close = "</section>"
    if not saved_entries:
        return f"""
        {wrapper_open}
            <div class="posts-empty-card rounded-[1.5rem] p-8 text-center">
                <div class="text-sm uppercase tracking-[0.32em] text-slate-500 font-bold">Chiến dịch</div>
                <div class="mt-3 text-2xl font-black text-slate-100">Chưa có sheet nào để gắn chiến dịch</div>
                <p class="mt-2 text-sm text-slate-400">Hãy lưu sheet ở phần Cấu hình trước, rồi quay lại đây để tạo chiến dịch từ sheet có sẵn.</p>
            </div>
        {wrapper_close}
        """

    rows_html = []
    for entry in saved_entries:
        sheet_id = str(entry.get("sheet_id", "") or "").strip()
        sheet_name = str(entry.get("sheet_name", "") or "").strip()
        campaign_label = str(entry.get("campaign_label", "") or "").strip()
        saved_at_text = str(entry.get("saved_at_text", "") or "").strip() or "Chưa có thời gian lưu"
        option_labels = []
        seen_labels = set()
        for candidate in [campaign_label, *known_campaign_labels]:
            normalized_candidate = str(candidate or "").strip()
            if not normalized_candidate:
                continue
            dedupe_key = normalized_candidate.lower()
            if dedupe_key in seen_labels:
                continue
            seen_labels.add(dedupe_key)
            option_labels.append(normalized_candidate)
        options_html = ['<option value="">Chưa gắn chiến dịch</option>']
        options_html.extend(
            f'<option value="{html.escape(option_label, quote=True)}" {"selected" if option_label == campaign_label else ""}>'
            + html.escape(option_label)
            + "</option>"
            for option_label in option_labels
        )
        rows_html.append(
            f"""
            <div class="campaign-sheet-row rounded-[1.2rem] border border-white/8 bg-slate-950/35 px-4 py-3" data-campaign-sheet-row>
                <div class="campaign-sheet-row-inner">
                    <div class="campaign-sheet-meta">
                        <div class="text-[11px] uppercase tracking-[0.22em] text-slate-500 font-black">Sheet có sẵn</div>
                        <div class="mt-1 text-base font-black text-slate-100 break-words">{html.escape(sheet_name)}</div>
                        <div class="mt-1 text-xs text-slate-500">Lưu lúc {html.escape(saved_at_text)}</div>
                    </div>
                    <form class="campaign-inline-form w-full lg:max-w-[34rem]" data-campaign-form>
                        <input type="hidden" name="sheet_id" value="{html.escape(sheet_id, quote=True)}" />
                        <input type="hidden" name="sheet_name" value="{html.escape(sheet_name, quote=True)}" />
                        <div class="campaign-inline-grid">
                            <select
                                name="campaign_label"
                                class="w-full bg-slate-900 text-slate-100 rounded-xl px-4 py-3 border border-white/10 outline-none focus:border-cyan-400"
                            >
                                {''.join(options_html)}
                            </select>
                            <button type="submit" class="w-full px-4 py-3 bg-slate-700 hover:bg-slate-600 text-white rounded-xl font-black uppercase text-sm shadow-sm shadow-slate-900/10">
                                Lưu
                            </button>
                        </div>
                        <div class="campaign-inline-note">
                            {html.escape(campaign_label) if campaign_label else "Chọn chiến dịch có sẵn."}
                        </div>
                    </form>
                </div>
            </div>
            """
        )

    return f"""
    {wrapper_open}
        <div class="flex flex-col gap-5">
            <div class="posts-page-head">
                <div>
                    <h2 class="posts-page-title">Chiến dịch</h2>
                </div>
                <div class="posts-counter-pill">
                    <div class="posts-counter-value">{format_metric_number(len(campaign_groups))} chiến dịch</div>
                </div>
            </div>
            <div class="posts-toolbar rounded-[1.5rem] p-4 md:p-5">
                <div class="grid gap-4">
                    <div class="rounded-[1.25rem] border border-white/8 bg-slate-950/35 px-4 py-4">
                        <div class="text-[11px] uppercase tracking-[0.22em] text-slate-500 font-black">Tạo chiến dịch mới</div>
                        <div class="mt-1 text-sm text-slate-400">Nhập tên chiến dịch ở đây rồi bấm tạo.</div>
                        <form class="mt-3 grid grid-cols-1 md:grid-cols-[minmax(0,1fr)_180px] gap-3" data-create-campaign-form>
                            <input
                                type="text"
                                name="campaign_label"
                                placeholder="Ví dụ: Mega Event tháng 4"
                                class="w-full bg-slate-900 text-slate-100 rounded-xl px-4 py-3 border border-white/10 outline-none focus:border-cyan-400"
                            />
                            <button type="submit" class="w-full px-4 py-3 bg-slate-700 hover:bg-slate-600 text-white rounded-xl font-black uppercase text-sm shadow-sm shadow-slate-900/10">
                                Tạo chiến dịch
                            </button>
                        </form>
                    </div>
                    <div class="posts-filter-row">
                        <div class="posts-chip is-active">Tất cả <span>{format_metric_number(len(saved_entries))}</span></div>
                        {campaign_group_chips}
                    </div>
                </div>
            </div>
            <div class="campaign-sheet-list" id="campaign-sheet-list">
                {''.join(rows_html)}
            </div>
        </div>
    {wrapper_close}
    """


def build_posts_panel_html(sheet=None, state=None):
    runtime_state = resolve_runtime_state(state)
    saved_entries = get_saved_sheet_entries(owner_email=runtime_state["owner_email"])
    if not saved_entries:
        return """
        <section id="bai-dang" data-dashboard-section="bai-dang" class="dashboard-section dashboard-panel posts-board rounded-[2rem] p-6 md:p-8 mb-6 border border-white/5">
            <div class="posts-empty-card rounded-[1.5rem] p-8 text-center">
                <div class="text-sm uppercase tracking-[0.32em] text-slate-500 font-bold">Bài đăng</div>
                <div class="mt-3 text-2xl font-black text-slate-100">Chưa có sheet nào được lưu</div>
                <p class="mt-2 text-sm text-slate-400">Hãy nhập sheet ở phần Cấu hình rồi bấm <b>Lưu Sheet</b> để đưa sheet đó vào đây.</p>
            </div>
        </section>
        """

    datasets = []
    for entry_index, entry in enumerate(saved_entries):
        entry_sheet_id = str(entry.get("sheet_id", "") or "").strip()
        entry_sheet_name = str(entry.get("sheet_name", "") or "").strip()
        entry_sheet_gid = str(entry.get("sheet_gid", "") or "0").strip() or "0"
        entry_saved_at_text = str(entry.get("saved_at_text", "") or "").strip()
        entry_campaign_label = str(entry.get("campaign_label", "") or "").strip()
        entry_brand_label = str(entry.get("brand_label", "") or "").strip()
        entry_industry_label = str(entry.get("industry_label", "") or "").strip()
        entry_campaign_description = str(entry.get("campaign_description", "") or "").strip()
        entry_slug = f"{build_dom_slug(entry_sheet_name, 'sheet')}-{entry_index}"
        
        try:
            # Check if this sheet is already in SHEET_DATA_CACHE (handled inside get_sheet_records)
            # but we still want to stagger the calls to get_worksheet/get_sheet_records
            if entry_index > 0:
                time.sleep(0.3)
                
            ws = sheet if (
                sheet is not None
                and entry_sheet_id == (runtime_state["active_sheet_id"] or "")
                and entry_sheet_name == (runtime_state["active_sheet_name"] or "")
            ) else get_worksheet(entry_sheet_name, entry_sheet_id, runtime_state)
            
            dataset = collect_posts_dataset_for_worksheet(
                ws,
                entry_index,
                sheet_id=entry_sheet_id,
                sheet_slug=entry_slug,
                campaign_override=entry_campaign_label,
                state=runtime_state,
            )
            dataset["saved_at_text"] = entry_saved_at_text
            dataset["sheet_id"] = entry_sheet_id
            dataset["sheet_gid"] = entry_sheet_gid or dataset.get("sheet_gid", "0")
            dataset["campaign_label"] = entry_campaign_label
            dataset["brand_label"] = entry_brand_label or str(dataset.get("brand_label", "") or "").strip()
            dataset["industry_label"] = entry_industry_label
            dataset["campaign_description"] = entry_campaign_description
        except Exception as exc:
            dataset = {
                "sheet_title": entry_sheet_name or f"Sheet {entry_index + 1}",
                "sheet_slug": entry_slug,
                "sheet_id": entry_sheet_id,
                "sheet_gid": entry_sheet_gid,
                "campaign_label": entry_campaign_label,
                "brand_label": entry_brand_label,
                "industry_label": entry_industry_label,
                "campaign_description": entry_campaign_description,
                "total_posts": 0,
                "total_views": 0,
                "total_reaction": 0,
                "total_share": 0,
                "total_comment": 0,
                "total_buzz": 0,
                "creator_count": 0,
                "campaign_count": 0,
                "brand_label": "",
                "platform_counts": {"tiktok": 0, "facebook": 0, "instagram": 0, "youtube": 0, "khac": 0},
                "rows_html": "",
                "error": str(exc),
                "saved_at_text": entry_saved_at_text,
            }
        datasets.append(dataset)

    campaign_counts = {}
    for dataset in datasets:
        campaign_label = str(dataset.get("campaign_label", "") or "").strip()
        if not campaign_label:
            continue
        campaign_counts[campaign_label] = campaign_counts.get(campaign_label, 0) + 1

    summary_rows_html = []
    detail_panels_html = []
    for dataset in datasets:
        safe_sheet_title = html.escape(dataset["sheet_title"])
        saved_at_text = html.escape(dataset.get("saved_at_text", "") or "Chưa có thời gian lưu")
        brand_label = str(dataset.get("brand_label", "") or "").strip()
        campaign_label = str(dataset.get("campaign_label", "") or "").strip()
        industry_label = str(dataset.get("industry_label", "") or "").strip()
        campaign_slug = build_dom_slug(campaign_label or "khong-gan", "campaign")
        primary_platform_key = "khac"
        primary_platform_count = 0
        for platform_key in ("tiktok", "facebook", "instagram", "youtube", "khac"):
            count = int((dataset.get("platform_counts", {}) or {}).get(platform_key) or 0)
            if count > primary_platform_count:
                primary_platform_key = platform_key
                primary_platform_count = count
        platform_label_map = {
            "tiktok": "TikTok",
            "facebook": "Facebook",
            "instagram": "Instagram",
            "youtube": "YouTube",
            "khac": "Khác",
        }
        primary_platform_label = platform_label_map.get(primary_platform_key, "Khác")
        status_label = "Lỗi đọc" if dataset["error"] else ("Đã lưu" if dataset["total_posts"] > 0 else "Chưa có bài")
        status_class = "posts-row-status-error" if dataset["error"] else ("posts-row-status-ready" if dataset["total_posts"] > 0 else "posts-row-status-empty")
        activity_title = html.escape(dataset["sheet_title"])
        activity_sub = html.escape(dataset.get("saved_at_text", "") or "Chưa có thời gian lưu")
        brand_html = (
            f"""
            <div class="posts-sheet-list-campaign-main" title="{html.escape(brand_label, quote=True)}">
                <i class="fa-solid fa-tag"></i>
                <span>{html.escape(brand_label)}</span>
            </div>
            """
            if brand_label
            else '<div class="posts-sheet-list-brand-empty">-</div>'
        )
        posts_text = format_metric_number(dataset.get("total_posts", 0))
        views_text = format_metric_number(dataset.get("total_views", 0))
        reaction_text = format_metric_number(dataset.get("total_reaction", 0))
        comment_text = format_metric_number(dataset.get("total_comment", 0))
        share_text = format_metric_number(dataset.get("total_share", 0))
        buzz_text = format_metric_number(dataset.get("total_buzz", 0))
        platform_summary = " • ".join(
            [
                f"{platform_label_map.get(platform_key, platform_key.title())} {count}"
                for platform_key, count in (dataset.get("platform_counts", {}) or {}).items()
                if int(count or 0) > 0
            ]
        )
        detail_meta_chips = []
        if brand_label:
            detail_meta_chips.append(
                f'<div class="posts-detail-summary-chip"><i class="fa-solid fa-tag"></i><span>{html.escape(brand_label)}</span></div>'
            )
        if industry_label:
            detail_meta_chips.append(
                f'<div class="posts-detail-summary-chip"><i class="fa-solid fa-shapes"></i><span>{html.escape(industry_label)}</span></div>'
            )
        detail_meta_chips_html = "".join(detail_meta_chips)
        sheet_snapshot_url = build_snapshot_url(dataset.get("sheet_id", ""), dataset["sheet_gid"], runtime_state)
        summary_rows_html.append(
            f"""
            <div class="posts-sheet-list-row" data-posts-tab-trigger="{dataset["sheet_slug"]}" data-posts-tab-title="{safe_sheet_title}" data-posts-master-campaign="all" data-posts-master-search="{html.escape((dataset['sheet_title'] + ' ' + brand_label + ' ' + industry_label).lower(), quote=True)}">
                <div class="posts-sheet-list-cell posts-sheet-list-activity">
                    <div class="posts-sheet-list-title">{activity_title}</div>
                    <div class="posts-sheet-list-sub">{activity_sub}</div>
                    {f'<div class="posts-sheet-list-error"><i class="fa-solid fa-circle-exclamation"></i><span>{html.escape(format_saved_sheet_error(dataset["error"]))}</span></div>' if dataset["error"] else ""}
                </div>
                <div class="posts-sheet-list-cell posts-sheet-list-brand">
                    {brand_html}
                </div>
                <div class="posts-sheet-list-cell">
                    <span class="posts-sheet-list-pill {status_class}">{html.escape(status_label)}</span>
                </div>
                <div class="posts-sheet-list-cell posts-sheet-list-cell-metric">{posts_text}</div>
                <div class="posts-sheet-list-cell posts-sheet-list-cell-metric">{views_text}</div>
                <div class="posts-sheet-list-cell posts-sheet-list-cell-metric">{reaction_text}</div>
                <div class="posts-sheet-list-cell posts-sheet-list-cell-metric">{comment_text}</div>
                <div class="posts-sheet-list-cell posts-sheet-list-cell-metric">{share_text}</div>
                <div class="posts-sheet-list-cell posts-sheet-list-cell-metric">{buzz_text}</div>
                <div class="posts-sheet-list-cell posts-sheet-list-actions">
                    <div class="posts-sheet-actions-menu">
                        <button type="button" class="posts-sheet-actions-toggle" data-posts-sheet-action-toggle aria-haspopup="true" aria-expanded="false">
                            <i class="fa-solid fa-ellipsis-vertical"></i>
                        </button>
                        <div class="posts-sheet-actions-dropdown hidden" data-posts-sheet-action-menu>
                            <button
                                type="button"
                                class="posts-sheet-actions-item"
                                data-posts-sheet-action="edit-metadata"
                                data-posts-sheet-id="{html.escape(dataset.get('sheet_id', ''), quote=True)}"
                                data-posts-sheet-name="{html.escape(dataset['sheet_title'], quote=True)}"
                                data-posts-sheet-brand="{html.escape(brand_label, quote=True)}"
                                data-posts-sheet-industry="{html.escape(industry_label, quote=True)}"
                            >
                                <i class="fa-regular fa-pen-to-square"></i>
                                <span>Chỉnh sửa thông tin</span>
                            </button>
                            <a href="{html.escape(sheet_snapshot_url, quote=True)}" target="_blank" rel="noreferrer" class="posts-sheet-actions-item">
                                <i class="fa-solid fa-up-right-from-square"></i>
                                <span>Mở sheet</span>
                            </a>
                        </div>
                    </div>
                </div>
            </div>
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

        detail_panels_html.append(
            f"""
            <div class="posts-tab-panel" data-posts-tab-panel="{dataset["sheet_slug"]}" data-posts-tab-title="{safe_sheet_title}" data-posts-platform="all">
                <div class="posts-tab-panel-head">
                    <div>
                        <div class="posts-tab-panel-kicker">Đang xem sheet</div>
                        <div class="posts-tab-panel-title">{safe_sheet_title}</div>
                        <div class="posts-tab-panel-sub">Sheet ID: {html.escape(dataset.get("sheet_id", ""))}</div>
                    </div>
                    <a href="{html.escape(build_snapshot_url(dataset.get("sheet_id", ""), dataset["sheet_gid"], runtime_state), quote=True)}" target="_blank" rel="noreferrer" class="posts-toolbar-btn">
                        <i class="fa-solid fa-up-right-from-square"></i> Mở sheet này
                    </a>
                </div>
                <div class="posts-detail-summary-shell">
                    <div class="posts-detail-summary-meta">
                        {detail_meta_chips_html if detail_meta_chips_html else '<div class="posts-detail-summary-chip"><i class="fa-solid fa-circle-info"></i><span>Chưa có thông tin tổng thêm</span></div>'}
                    </div>
                    <div class="posts-detail-summary-grid">
                        <div class="posts-detail-summary-card">
                            <div class="posts-detail-summary-label">Bài đăng</div>
                            <div class="posts-detail-summary-value">{posts_text}</div>
                        </div>
                        <div class="posts-detail-summary-card">
                            <div class="posts-detail-summary-label">View</div>
                            <div class="posts-detail-summary-value">{views_text}</div>
                        </div>
                        <div class="posts-detail-summary-card">
                            <div class="posts-detail-summary-label">Reaction</div>
                            <div class="posts-detail-summary-value">{reaction_text}</div>
                        </div>
                        <div class="posts-detail-summary-card">
                            <div class="posts-detail-summary-label">Comment</div>
                            <div class="posts-detail-summary-value">{comment_text}</div>
                        </div>
                        <div class="posts-detail-summary-card">
                            <div class="posts-detail-summary-label">Share</div>
                            <div class="posts-detail-summary-value">{share_text}</div>
                        </div>
                    </div>
                </div>
                <div class="posts-toolbar rounded-[1.5rem] p-4 md:p-5">
                    <div class="posts-toolbar-row">
                        <label class="posts-search-shell">
                            <i class="fa-solid fa-magnifying-glass text-slate-400"></i>
                            <input type="text" placeholder="Tìm kiếm bài đăng hoặc creator..." class="posts-search-input posts-search-field" />
                        </label>
                        <div class="posts-toolbar-actions">
                            <div class="text-xs text-slate-400 font-bold px-3 py-2 rounded-xl border border-white/10 bg-slate-900/60" data-posts-selection-count>0 bài đã chọn</div>
                            <div class="posts-columns-wrap">
                                <button type="button" class="posts-toolbar-btn posts-columns-toggle" data-post-columns-toggle>
                                    <i class="fa-solid fa-sliders"></i> Cột hiển thị
                                </button>
                                <div class="posts-columns-popover hidden" data-post-columns-menu>
                                    <div class="posts-columns-head">
                                        <div class="posts-columns-title">Cột hiển thị · <span data-post-columns-count>0/0</span></div>
                                        <button type="button" class="posts-columns-all" data-post-columns-show-all>Tất cả</button>
                                    </div>
                                    <div class="posts-columns-list" data-post-columns-list></div>
                                </div>
                            </div>
                            <button type="button" class="posts-toolbar-btn posts-rerun-btn"><i class="fa-solid fa-rotate-left"></i> Chạy lại</button>
                        </div>
                    </div>
                    <div class="posts-mini-campaign-feedback hidden" data-posts-rerun-feedback></div>
                    <div class="posts-filter-row">
                        {filter_chips_html}
                    </div>
                </div>
                <div class="posts-table-shell">
                    <div class="overflow-x-auto">
                        <table class="w-full min-w-[1450px] posts-table">
                            <thead>
                                <tr>
                                    <th class="posts-check-col"><input type="checkbox" class="posts-table-check posts-select-all" data-select-all-posts aria-label="Chọn tất cả" /></th>
                                    <th data-post-col="content">Nội dung</th>
                                    <th data-post-col="creator">Creator</th>
                                    <th data-post-col="status">Trạng thái</th>
                                    <th data-post-col="plan">Plan</th>
                                    <th data-post-col="line">Line</th>
                                    <th data-post-col="tier">Tier</th>
                                    <th data-post-col="date">Ngày quét</th>
                                    <th class="text-right" data-post-col="view">View</th>
                                    <th class="text-right" data-post-col="reaction">Reaction</th>
                                    <th class="text-right" data-post-col="share">Share</th>
                                    <th class="text-right" data-post-col="comment">Comment</th>
                                    <th class="text-right" data-post-col="buzz">Buzz</th>
                                </tr>
                            </thead>
                            <tbody>
                                {dataset["rows_html"] if dataset["rows_html"] else '<tr><td colspan="13" class="posts-empty-state">Sheet này chưa có link nào hợp lệ để hiển thị.</td></tr>'}
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

    saved_count = len(datasets)
    return f"""
    <section id="bai-dang" data-dashboard-section="bai-dang" class="dashboard-section dashboard-panel posts-board rounded-[2rem] p-6 md:p-8 mb-6 border border-white/5">
        <div class="flex flex-col gap-5">
            <div class="posts-page-head">
                <div>
                    <h2 class="posts-page-title">Bài đăng</h2>
                    <p class="posts-page-subtitle">Chọn một sheet đã lưu ở bên dưới để mở bảng chi tiết. Đang xem: <span id="posts-active-tab-label" class="text-slate-200 font-bold">Chưa chọn</span>.</p>
                </div>
                <div class="posts-counter-pill">
                    <div class="posts-counter-label">Đang hiển thị</div>
                    <div class="posts-counter-value" id="posts-visible-count">0 bài</div>
                </div>
            </div>

            <div id="posts-master-view" class="posts-master-view">
                <div class="posts-sheet-summary-shell">
                    <div class="posts-toolbar rounded-[1.5rem] p-4 md:p-5 mb-4">
                        <div class="posts-toolbar-row">
                            <label class="posts-search-shell">
                                <i class="fa-solid fa-magnifying-glass text-slate-400"></i>
                                <input type="text" placeholder="Tìm sheet hoặc thương hiệu..." class="posts-search-input posts-master-search-field" />
                            </label>
                        </div>
                    </div>
                    <div class="posts-sheet-summary-grid">
                        <div class="posts-sheet-list-table">
                            <div class="posts-sheet-list-head">
                                <div>Hoạt động</div>
                                <div>Thương hiệu</div>
                                <div>Trạng thái</div>
                                <div class="posts-sheet-list-head-metric">Bài đăng</div>
                                <div class="posts-sheet-list-head-metric">View</div>
                                <div class="posts-sheet-list-head-metric">Reaction</div>
                                <div class="posts-sheet-list-head-metric">Comment</div>
                                <div class="posts-sheet-list-head-metric">Share</div>
                                <div class="posts-sheet-list-head-metric">Buzz</div>
                                <div class="posts-sheet-list-head-action"><i class="fa-solid fa-ellipsis-vertical"></i></div>
                            </div>
                            {"".join(summary_rows_html)}
                        </div>
                    </div>
                    <div class="posts-empty-state posts-master-empty-panel hidden mt-4">
                        Không có sheet nào khớp bộ lọc hiện tại.
                    </div>
                </div>
            </div>

            <div id="posts-detail-view" class="posts-detail-view hidden">
                <div class="posts-detail-topbar">
                    <button type="button" id="posts-back-button" class="posts-toolbar-btn">
                        <i class="fa-solid fa-arrow-left"></i> Quay lại danh sách sheet
                    </button>
                </div>
                <div class="posts-tab-panels">
                    <div id="posts-selection-placeholder" class="posts-empty-card rounded-[1.5rem] p-8 text-center">
                        <div class="text-sm uppercase tracking-[0.32em] text-slate-500 font-bold">Chi tiết</div>
                        <div class="mt-3 text-2xl font-black text-slate-100">Chọn một sheet đã lưu để xem bài đăng</div>
                        <p class="mt-2 text-sm text-slate-400">Bấm vào card sheet phía trên, lúc đó bảng chi tiết sẽ hiện ra như bạn muốn.</p>
                    </div>
                    {"".join(detail_panels_html)}
                </div>
            </div>
        </div>
    </section>
    """

def build_row_updates(col_map, platform, now, stats):
    stats = stats or {}
    row_updates = []
    if col_map.get("date"):
        row_updates.append(("date", col_map["date"], now))
    air_date_value = str((stats or {}).get("air_date", "") or "").strip()
    if col_map.get("air_date") and air_date_value:
        row_updates.append(("air_date", col_map["air_date"], air_date_value))
    if col_map.get("caption"):
        row_updates.append(("caption", col_map["caption"], str(stats.get("cap", ""))))
    metric_keys = {
        "view": "v",
        "like": "l",
        "share": "s",
        "comment": "c",
        "save": "save",
    }
    for field, stat_key in metric_keys.items():
        if not col_map.get(field):
            continue
        if stat_key not in stats or stats.get(stat_key) is None:
            continue
        row_updates.append((field, col_map[field], int(stats.get(stat_key, 0))))
    if col_map.get("buzz") and ("s" in stats or "c" in stats):
        buzz_value = int(stats.get("s") or 0) + int(stats.get("c") or 0)
        row_updates.append(("buzz", col_map["buzz"], buzz_value))
    return row_updates


def ensure_dashboard_date_column(sheet, layout=None, col_map=None, state=None):
    if sheet is None:
        return dict(col_map or {})
    resolved_layout = layout or detect_sheet_layout(sheet)
    resolved_col_map = dict(col_map or apply_column_overrides(resolved_layout.get("columns"), state=state))
    return resolved_col_map

def normalize_cell_value(field, value):
    if field in {"view", "like", "share", "comment", "buzz", "save"}:
        try:
            return int(str(value).strip())
        except Exception:
            return value
    return value


def get_missing_metric_fields(col_map, stats):
    metric_keys = {
        "view": "v",
        "like": "l",
        "share": "s",
        "comment": "c",
        "save": "save",
    }
    missing_fields = []
    for field, stat_key in metric_keys.items():
        if not col_map.get(field):
            continue
        if not stats or stat_key not in stats or stats.get(stat_key) is None:
            missing_fields.append(field)
    if col_map.get("buzz") and (not stats or ("s" not in stats and "c" not in stats)):
        missing_fields.append("buzz")
    return missing_fields


def get_red_metric_fields_from_sheet(sheet, row_idx: int, col_map, url: str = "", platform: str = ""):
    if not sheet or not row_idx or not col_map:
        return []
    key_metrics = ["view", "like", "share", "comment"]
    all_zero = True
    for field in key_metrics:
        col_idx = col_map.get(field)
        if not col_idx:
            all_zero = False
            continue
        try:
            raw_value = sheet.cell(row_idx, col_idx).value
        except Exception:
            raw_value = ""
        metric_value = parse_metric_number(raw_value)
        if metric_value > 0:
            all_zero = False
            break
    return key_metrics if all_zero else []


def update_metric_highlights(sheet, row_idx: int, col_map, missing_fields):
    if not sheet or not row_idx or not col_map:
        return
    key_metrics = ["view", "like", "share", "comment"]
    requests = []
    all_zero = missing_fields == key_metrics
    for field in key_metrics:
        col_idx = col_map.get(field)
        if not col_idx:
            continue
        background = (
            {"red": 0.97, "green": 0.82, "blue": 0.82}
            if all_zero
            else {"red": 1.0, "green": 1.0, "blue": 1.0}
        )
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet.id,
                        "startRowIndex": row_idx - 1,
                        "endRowIndex": row_idx,
                        "startColumnIndex": col_idx - 1,
                        "endColumnIndex": col_idx,
                    },
                    "cell": {"userEnteredFormat": {"backgroundColor": background}},
                    "fields": "userEnteredFormat.backgroundColor",
                }
            }
        )
    if requests:
        retry_with_backoff(
            lambda: sheet.spreadsheet.batch_update({"requests": requests}),
            max_retries=3,
            handle_quota=True
        )


# --- Xá»­ lÃ½ YouTube ---
def get_youtube_stats(url):
    try:
        video_id_match = re.search(r"(?:v=|\/)([0-9A-Za-z_-]{11}).*", url)
        if not video_id_match: return None
        youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
        item = youtube.videos().list(part="statistics,snippet", id=video_id_match.group(1)).execute()['items'][0]
        return {
            "v": item['statistics'].get("viewCount", 0),
            "l": item['statistics'].get("likeCount", 0),
            "s": 0, "c": item['statistics'].get("commentCount", 0),
            "cap": item['snippet'].get('title', ''),
            "air_date": format_air_date_text(item['snippet'].get('publishedAt', '')),
        }
    except: return None

# --- Xá»­ lÃ½ Äa ná»n táº£ng (Facebook, TikTok, IG) ---
def get_social_stats(url, platform_name, driver=None, logger=None):
    return fetch_social_stats(url, platform_name, driver=driver, logger=logger or add_log)

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
def run_scraper_logic(sheet_id: Optional[str] = None, sheet_name: Optional[str] = None, targets=None, source: str = "manual", state=None, multi_tabs=None):
    runtime_state = resolve_runtime_state(state)
    add_log("⚙️ Đang khởi tạo bộ máy quét...", runtime_state)
    logger = lambda message: add_log(message, runtime_state)
    run_binding_keys = set()
    started_at = datetime.now()
    runtime_state["run_started_at"] = started_at
    runtime_state["run_source"] = (source or "manual").strip().lower() or "manual"
    run_status = "success"
    processed_count = 0
    success_count = 0
    failed_count = 0
    set_run_progress(current=0, total=0, phase="preparing", state=runtime_state)
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
            run_binding_keys = {build_sheet_binding_key(target_sheet_id, target_sheet_name) for target_sheet_id, target_sheet_name, _ in scan_groups}
            runtime_state["current_task"] = f"Chuẩn bị quét {len(normalized_targets)} bài theo lịch"
            logger(f"Bắt đầu lịch tự động cho {len(normalized_targets)} bài đã chọn")
        elif multi_tabs:
            # multi_tabs = [(sheet_id, sheet_name), ...]
            scan_groups = [(sid, sname, None) for sid, sname in multi_tabs]
            run_binding_keys = {build_sheet_binding_key(sid, sname) for sid, sname in multi_tabs}
            tab_names = ", ".join(sname for _, sname in multi_tabs)
            runtime_state["current_task"] = f"Chuẩn bị quét {len(multi_tabs)} tab song song"
            logger(f"Bắt đầu quét song song {len(multi_tabs)} tab: {tab_names}")
        else:
            resolved_sheet_id = (sheet_id or runtime_state["active_sheet_id"] or "").strip()
            resolved_sheet_name = (sheet_name or runtime_state["active_sheet_name"] or "").strip()
            if not resolved_sheet_id or not resolved_sheet_name:
                raise ValueError("Chưa cài đặt Google Sheet. Vui lòng nhập sheet trước khi chạy.")
            scan_groups = [(resolved_sheet_id, resolved_sheet_name, None)]
            run_binding_keys = {build_sheet_binding_key(resolved_sheet_id, resolved_sheet_name)}
            runtime_state["current_task"] = f"Chuẩn bị quét ({resolved_sheet_name})"

        runtime_state["schedule_last_run_started_at"] = started_at
        runtime_state["schedule_last_run_finished_at"] = None
        runtime_state["schedule_last_run_duration_seconds"] = 0.0
        runtime_state["schedule_last_run_status"] = "running"
        runtime_state["schedule_last_run_source"] = runtime_state["run_source"]
        runtime_state["schedule_last_run_sheet_name"] = scan_groups[0][1] if len(scan_groups) == 1 else f"{len(scan_groups)} tab"
        runtime_state["schedule_last_run_processed"] = 0
        runtime_state["schedule_last_run_success"] = 0
        runtime_state["schedule_last_run_failed"] = 0
        runtime_state["is_running"], runtime_state["is_finished"] = True, False
        now = datetime.now().strftime("%d/%m/%Y %H:%M")
        prepared_groups = []
        total_scannable_rows = 0
        for sheet_id, selected_sheet, selected_targets in scan_groups:
            logger(f"Đang kết nối Google Sheets: {selected_sheet}")
            sheet = get_worksheet(selected_sheet, sheet_id, runtime_state)
            layout = detect_sheet_layout(sheet)
            tab_overrides = runtime_state.get("column_overrides_by_tab", {}).get(selected_sheet)
            col_map = apply_column_overrides(layout.get("columns"), overrides=tab_overrides) if tab_overrides else apply_column_overrides(layout.get("columns"), state=runtime_state)
            had_date_column = bool(col_map.get("date"))
            col_map = ensure_dashboard_date_column(sheet, layout, col_map, runtime_state)
            if not had_date_column and col_map.get("date"):
                logger(f"Tab '{selected_sheet}': đã thêm cột Ngày quét ở {col_to_a1(col_map['date'])}")
            link_col = col_map.get("link") or 4
            # Log header names for debugging
            try:
                all_headers = sheet.row_values(max(1, int(layout.get("header_row") or 1)))
                link_header_name = all_headers[link_col - 1] if link_col - 1 < len(all_headers) else "?"
                logger(f"Tab '{selected_sheet}': Header hàng {layout.get('header_row') or 1}: {', '.join(f'{col_to_a1(i+1)}={h}' for i, h in enumerate(all_headers) if h)}")
            except Exception:
                link_header_name = "?"
                all_headers = []
            if not col_map.get("link"):
                logger(f"Tab '{selected_sheet}': Không tìm thấy cột 'link', tạm dùng cột D (4) để quét")
            else:
                logger(f"Tab '{selected_sheet}': Dùng cột link = {col_to_a1(link_col)} ({link_col}) | Header: \"{link_header_name}\"")
            urls = sheet.col_values(link_col)
            header_row = max(1, int(layout.get("header_row") or 1))
            start_row = resolve_effective_start_row(header_row, runtime_state)
            logger(f"Tab '{selected_sheet}': Đọc được {len(urls)} giá trị từ cột link, header dòng {header_row}, bắt đầu từ dòng {start_row}")

            if selected_targets:
                row_plan = []
                seen_rows = set()
                for target in selected_targets:
                    resolved_row = resolve_target_row_index(target, urls, min_row=start_row)
                    if resolved_row is None or resolved_row < start_row or resolved_row in seen_rows:
                        continue
                    url = str(urls[resolved_row - 1] if resolved_row - 1 < len(urls) else "").strip()
                    if (not url or "http" not in url.lower()) and target:
                        url = str(target.get("link") or "").strip()
                    if not url or "http" not in url.lower():
                        continue
                    seen_rows.add(resolved_row)
                    row_plan.append((resolved_row, target, url))
                row_plan.sort(key=lambda item: item[0])
                logger(f"Tab '{selected_sheet}': quét {len(row_plan)} bài đã chọn")
            else:
                row_plan = []
                for row_idx in range(start_row, len(urls) + 1):
                    url = str(urls[row_idx - 1] if row_idx - 1 < len(urls) else "").strip()
                    if not url or "http" not in url.lower():
                        continue
                    row_plan.append((row_idx, None, url))
                logger(f"Tab '{selected_sheet}': tìm thấy {len(row_plan)} bài có link hợp lệ từ dòng {start_row} (header dòng {header_row})")

            prepared_groups.append((sheet, selected_sheet, col_map, row_plan))
            total_scannable_rows += len(row_plan)

        set_run_progress(current=0, total=total_scannable_rows, phase="scanning", state=runtime_state)

        # Init per-tab progress
        runtime_state["tab_progress"] = {
            tab_name: {"current": 0, "total": len(rp), "status": "scanning"}
            for _, tab_name, _, rp in prepared_groups
        }

        # Shared mutable counters and lock for parallel scanning
        state_lock = threading.Lock()
        shared = {"processed": 0, "success": 0, "failed": 0}

        def scan_one_tab(ws, tab_name, col_map, row_plan):
            tab_driver = None
            tab_driver_failed = False
            now_str = datetime.now().strftime("%d/%m/%Y %H:%M")

            def locked_log(msg):
                with state_lock:
                    logger(msg)

            locked_log(f"[{tab_name}] ▶ Bắt đầu thread quét {len(row_plan)} bài...")
            try:
                for i, target, url in row_plan:
                    if not runtime_state["is_running"]:
                        return
                    platform = detect_platform(url)
                    with state_lock:
                        runtime_state["current_task"] = f"[{tab_name}] Dòng {i}: {platform}"
                    locked_log(f"[{tab_name}] Dòng {i}: {platform}...")
                    if platform == "YouTube":
                        stats = get_youtube_stats(url)
                    else:
                        if tab_driver is None and not tab_driver_failed:
                            try:
                                tab_driver = create_selenium_driver(logger=locked_log)
                            except Exception as driver_error:
                                # Retry once after a short delay (parallel threads may compete for resources)
                                locked_log(f"[{tab_name}] ⚠️ Driver tạo thất bại, thử lại sau 5s: {str(driver_error)[:80]}")
                                time.sleep(5)
                                try:
                                    tab_driver = create_selenium_driver(logger=locked_log)
                                except Exception as retry_err:
                                    tab_driver_failed = True
                                    locked_log(f"[{tab_name}] ❌ Không tạo được driver sau 2 lần thử. {len(row_plan)} bài sẽ bị bỏ qua. Lỗi: {str(retry_err)[:100]}")
                        if not tab_driver_failed:
                            try:
                                stats = get_social_stats(url, platform, driver=tab_driver, logger=locked_log)
                            except Exception as e:
                                error_msg = str(e).lower()
                                if "invalid session" in error_msg or "session id" in error_msg:
                                    locked_log(f"[{tab_name}] Driver session invalid, đang restart...")
                                    close_selenium_driver(tab_driver)
                                    tab_driver = None
                                    try:
                                        tab_driver = create_selenium_driver(logger=locked_log)
                                        stats = get_social_stats(url, platform, driver=tab_driver, logger=locked_log)
                                    except Exception as retry_e:
                                        locked_log(f"[{tab_name}] Retry thất bại: {str(retry_e)[:80]}")
                                        stats = None
                                else:
                                    locked_log(f"[{tab_name}] Selenium error: {str(e)[:80]}")
                                    stats = None
                        else:
                            stats = None
                    if stats and "v" not in stats and is_optional_view_metric(url, platform):
                        stats = dict(stats)
                        stats["v"] = 0
                    if stats and runtime_state["is_running"]:
                        row_updates = build_row_updates(col_map, platform, now_str, stats)
                        with state_lock:
                            set_pending_updates(i, row_updates, runtime_state)
                        sheet_requests = []
                        for field, col_idx, value in row_updates:
                            value = normalize_cell_value(field, value)
                            if isinstance(value, (int, float)):
                                cell_value = {"numberValue": value}
                                sheet_requests.append({
                                    "repeatCell": {
                                        "range": {
                                            "sheetId": ws.id,
                                            "startRowIndex": i - 1,
                                            "endRowIndex": i,
                                            "startColumnIndex": col_idx - 1,
                                            "endColumnIndex": col_idx,
                                        },
                                        "cell": {
                                            "userEnteredValue": cell_value,
                                            "userEnteredFormat": {
                                                "numberFormat": {
                                                    "type": "NUMBER",
                                                    "pattern": "#,##0",
                                                }
                                            },
                                        },
                                        "fields": "userEnteredValue,userEnteredFormat.numberFormat",
                                    }
                                })
                            else:
                                cell_value = {"stringValue": str(value)}
                                sheet_requests.append({
                                    "repeatCell": {
                                        "range": {
                                            "sheetId": ws.id,
                                            "startRowIndex": i - 1,
                                            "endRowIndex": i,
                                            "startColumnIndex": col_idx - 1,
                                            "endColumnIndex": col_idx,
                                        },
                                        "cell": {"userEnteredValue": cell_value},
                                        "fields": "userEnteredValue",
                                    }
                                })
                        if sheet_requests:
                            _reqs = sheet_requests
                            retry_with_backoff(
                                lambda: ws.spreadsheet.batch_update({"requests": _reqs}),
                                max_retries=3,
                                handle_quota=True
                            )
                        red_fields = get_red_metric_fields_from_sheet(ws, i, col_map, url, platform)
                        update_metric_highlights(ws, i, col_map, red_fields)
                        with state_lock:
                            if red_fields:
                                logger(f"[{tab_name}] Dòng {i}: {', '.join(f.upper() for f in red_fields)} đang bằng 0/trống nên đã tô đỏ")
                            logger(f"[{tab_name}] Dòng {i}: Cập nhật thành công")
                            shared["success"] += 1
                    elif runtime_state["is_running"]:
                        if ENABLE_HIGHLIGHT_ON_FAILED_SCRAPE:
                            red_fields = get_red_metric_fields_from_sheet(ws, i, col_map, url, platform)
                            update_metric_highlights(ws, i, col_map, red_fields)
                        locked_log(f"[{tab_name}] Dòng {i}: Không lấy được số liệu")
                        with state_lock:
                            shared["failed"] += 1
                    with state_lock:
                        shared["processed"] += 1
                        runtime_state["schedule_last_run_processed"] = shared["processed"]
                        runtime_state["schedule_last_run_success"] = shared["success"]
                        runtime_state["schedule_last_run_failed"] = shared["failed"]
                        set_run_progress(current=shared["processed"], total=total_scannable_rows, phase="scanning", state=runtime_state)
                        tp = runtime_state["tab_progress"].get(tab_name)
                        if tp:
                            tp["current"] = tp.get("current", 0) + 1
                    time.sleep(max(0.0, ROW_SCAN_DELAY_SECONDS))
                # Mark this tab's final status
                with state_lock:
                    tp = runtime_state["tab_progress"].get(tab_name)
                    if tp:
                        tp["status"] = "stopped" if not runtime_state["is_running"] else "completed"
            except Exception as tab_exc:
                with state_lock:
                    tp = runtime_state["tab_progress"].get(tab_name)
                    if tp:
                        tp["status"] = "error"
                    shared["failed"] += max(1, len(row_plan) - int((tp or {}).get("current", 0)))
                    logger(f"[{tab_name}] Lỗi tab: {str(tab_exc)[:100]}")
            finally:
                close_selenium_driver(tab_driver)

        if len(prepared_groups) > 1:
            tab_threads = [
                threading.Thread(target=scan_one_tab, args=(ws, tab_name, col_map, row_plan), daemon=True)
                for ws, tab_name, col_map, row_plan in prepared_groups
            ]
            for t in tab_threads:
                t.start()
            for t in tab_threads:
                t.join()
        else:
            ws, tab_name, col_map, row_plan = prepared_groups[0]
            scan_one_tab(ws, tab_name, col_map, row_plan)

        processed_count = shared["processed"]
        success_count = shared["success"]
        failed_count = shared["failed"]

        if not runtime_state["is_running"] and runtime_state["current_task"] == "Đã dừng thủ công":
            run_status = "stopped"
            set_run_progress(phase="stopped", state=runtime_state)
            return

        runtime_state["pending_updates"] = []
        set_run_progress(current=total_scannable_rows, total=total_scannable_rows, phase="completed", state=runtime_state)
        runtime_state["current_task"], runtime_state["is_finished"], runtime_state["is_running"] = "HOÀN TẤT", True, False
        logger("=== ĐÃ QUÉT XONG ===")
    except Exception as e:
        run_status = "error"
        runtime_state["pending_updates"] = []
        set_run_progress(phase="error", state=runtime_state)
        runtime_state["is_running"], runtime_state["current_task"] = False, f"Lỗi: {str(e)[:20]}"
        logger(f"Lỗi hệ thống: {str(e)}")
    finally:
        finished_at = datetime.now()
        duration_seconds = max(0.0, (finished_at - started_at).total_seconds())
        if run_status == "success" and not runtime_state["is_finished"] and not runtime_state["is_running"]:
            run_status = "stopped" if runtime_state["current_task"] == "Đã dừng thủ công" else "success"
        runtime_state["schedule_last_run_finished_at"] = finished_at
        runtime_state["schedule_last_run_duration_seconds"] = duration_seconds
        runtime_state["schedule_last_run_status"] = run_status
        runtime_state["schedule_last_run_processed"] = processed_count
        runtime_state["schedule_last_run_success"] = success_count
        runtime_state["schedule_last_run_failed"] = failed_count

        push_schedule_run_history(
            {
                "sheet_name": runtime_state["schedule_last_run_sheet_name"],
                "started_text": format_datetime_display(runtime_state["schedule_last_run_started_at"]),
                "finished_text": format_datetime_display(runtime_state["schedule_last_run_finished_at"]),
                "duration_text": format_duration_display(duration_seconds),
                "source_label": "Tự động" if runtime_state["schedule_last_run_source"] == "schedule" else "Thủ công",
                "status": run_status,
                "processed": processed_count,
                "success": success_count,
                "failed": failed_count,
            },
            runtime_state,
        )
        if len(run_binding_keys) == 1:
            matching_entry = get_schedule_entry_by_key(next(iter(run_binding_keys)), runtime_state)
            if matching_entry:
                runtime_state["active_schedule_key"] = matching_entry["key"]
                if matching_entry.get("schedule_mode") != "off":
                    runtime_state["schedule_tracking_key"] = matching_entry["key"]
                sync_schedule_entry_from_runtime_state(matching_entry, runtime_state)
                persist_runtime_schedule_entries(runtime_state)
        runtime_state["run_started_at"] = None
        runtime_state["run_source"] = "idle"
        # Drivers are closed inside scan_one_tab's finally block

# --- API & UI ---
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
def start_task(
    request: Request,
    background_tasks: BackgroundTasks,
    sheet_name: Optional[str] = None,
    sheet_names: Optional[str] = None,
    sheet_url: Optional[str] = None,
    date: Optional[str] = None,
    air_date: Optional[str] = None,
    link: Optional[str] = None,
    view: Optional[str] = None,
    like: Optional[str] = None,
    share: Optional[str] = None,
    comment: Optional[str] = None,
    buzz: Optional[str] = None,
    save: Optional[str] = None,
    start_row: Optional[str] = None,
):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return auth_response
    runtime_state = get_runtime_state(current_user)
    add_log("► Nhận lệnh Bắt đầu...", runtime_state)
    if any(val is not None for val in (date, air_date, link, view, like, share, comment, buzz, save)):
        try:
            parsed_columns = parse_column_override_candidates(
                {
                    "date": date,
                    "air_date": air_date,
                    "link": link,
                    "view": view,
                    "like": like,
                    "share": share,
                    "comment": comment,
                    "buzz": buzz,
                    "save": save,
                }
            )
            runtime_state["column_overrides"].update(parsed_columns)
        except ValueError as exc:
            return build_ui_json_response(str(exc), level="error", ok=False, state=runtime_state)
    if start_row is not None:
        parsed_start_row = parse_start_row_input(start_row)
        if parsed_start_row is None:
            return build_ui_json_response("Dòng bắt đầu không hợp lệ. Nhập số từ 2 trở lên.", level="error", ok=False, state=runtime_state)
        runtime_state["start_row"] = parsed_start_row
    if runtime_state["is_running"]:
        add_log("Hệ thống đang chạy, không thể bắt đầu thêm", runtime_state)
        if is_fetch_request(request):
            return build_ui_json_response("Hệ thống đang chạy rồi, chưa thể bắt đầu thêm.", level="warning", ok=False, state=runtime_state)
        return HTMLResponse("<html><script>window.location.href='/';</script></html>")

    requested_sheet_id = extract_sheet_id(sheet_url or "") if sheet_url else None
    if sheet_url and not requested_sheet_id:
        add_log("Link/ID spreadsheet không hợp lệ")
        if is_fetch_request(request):
            return build_ui_json_response("Link/ID spreadsheet không hợp lệ.", level="error", ok=False, state=runtime_state)
        return HTMLResponse("<html><script>window.location.href='/?sheet_error=4';</script></html>")

    requested_sheet = (sheet_name or "").strip()
    if requested_sheet and not requested_sheet_id and not runtime_state["active_sheet_id"]:
        add_log("Bạn cần nhập link hoặc ID spreadsheet ở lần đầu tiên", runtime_state)
        if is_fetch_request(request):
            return build_ui_json_response("Bạn cần nhập link hoặc ID spreadsheet ở lần đầu tiên.", level="warning", ok=False, state=runtime_state)
        return HTMLResponse("<html><script>window.location.href='/?sheet_error=4';</script></html>")

    if requested_sheet_id and not requested_sheet:
        if requested_sheet_id == runtime_state["active_sheet_id"] and runtime_state["active_sheet_name"]:
            requested_sheet = runtime_state["active_sheet_name"]
        else:
            add_log("Cần nhập tên tab sheet cùng với link/ID spreadsheet", runtime_state)
            if is_fetch_request(request):
                return build_ui_json_response("Thiếu tên tab sheet. Hãy chọn tab rồi bấm Bắt đầu.", level="warning", ok=False, state=runtime_state)
            return HTMLResponse("<html><script>window.location.href='/?sheet_error=3';</script></html>")

    if requested_sheet:
        try:
            set_active_sheet(requested_sheet, requested_sheet_id, runtime_state)
        except Exception as e:
            add_log(f"Không tìm thấy sheet: {requested_sheet} ({str(e)[:60]})", runtime_state)
            if is_fetch_request(request):
                return build_ui_json_response("Không tìm thấy tab sheet. Kiểm tra lại tên tab và quyền truy cập.", level="error", ok=False, state=runtime_state)
            return HTMLResponse("<html><script>window.location.href='/?sheet_error=1';</script></html>")

    if not runtime_state["active_sheet_id"] or not runtime_state["active_sheet_name"]:
        add_log("Chưa cài đặt sheet. Vui lòng nhập link/ID và tên tab trước khi chạy.", runtime_state)
        if is_fetch_request(request):
            return build_ui_json_response("Chưa có sheet hợp lệ. Nhập link/ID và tên tab rồi bấm Bắt đầu.", level="warning", ok=False, state=runtime_state)
        return HTMLResponse("<html><script>window.location.href='/?sheet_error=3';</script></html>")

    # Build multi-tab list if sheet_names provided (comma-separated tab names)
    resolved_sheet_id_for_run = (requested_sheet_id or runtime_state["active_sheet_id"] or "").strip()
    parsed_tab_list = [t.strip() for t in (sheet_names or "").split(",") if t.strip()] if sheet_names else []
    if len(parsed_tab_list) > 1:
        multi_tabs_for_run = [(resolved_sheet_id_for_run, t) for t in parsed_tab_list]
        task_label = f"Chuẩn bị quét {len(parsed_tab_list)} tab song song"
        log_label = f"Bắt đầu quét song song: {', '.join(parsed_tab_list)}"
    else:
        multi_tabs_for_run = None
        task_label = f"Chuẩn bị quét ({runtime_state['active_sheet_name']})"
        log_label = f"Bắt đầu quét lại dữ liệu trên tab '{runtime_state['active_sheet_name']}'"

    runtime_state["is_running"] = True
    runtime_state["is_finished"] = False
    runtime_state["pending_updates"] = []
    runtime_state["tab_progress"] = {}
    runtime_state["current_task"] = task_label
    set_run_progress(current=0, total=0, phase="preparing", state=runtime_state)
    add_log(log_label, runtime_state)
    background_tasks.add_task(run_scraper_logic, state=runtime_state, multi_tabs=multi_tabs_for_run)
    if is_fetch_request(request):
        return build_ui_json_response(
            "Đã bắt đầu quét dữ liệu.",
            level="success",
            extra={
                "column_config": build_column_config_payload(state=runtime_state),
                "sheet_metadata": build_sheet_metadata_payload(state=runtime_state),
                "campaign_html": build_campaign_panel_html(runtime_state),
                "overview_html": build_overview_panel_for_state(runtime_state),
            },
            state=runtime_state,
        )
    return HTMLResponse("<html><script>window.location.href='/';</script></html>")


@app.get("/set-sheet")
def set_sheet(request: Request, sheet_name: str = "", sheet_url: str = ""):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return auth_response
    runtime_state = get_runtime_state(current_user)
    if (not runtime_state["is_running"]) and runtime_state["current_task"] == "Đã dừng thủ công":
        if is_fetch_request(request):
            return build_ui_json_response(
                "Đang ở trạng thái Đã dừng. Bấm Bắt đầu để mở lại rồi hãy nhập sheet.",
                level="warning",
                ok=False,
                state=runtime_state,
            )
        return HTMLResponse("<html><script>window.location.href='/?sheet_error=2';</script></html>")
    requested_sheet = sheet_name.strip()
    if runtime_state["is_running"]:
        add_log("Không thể nhập sheet khi hệ thống đang chạy", runtime_state)
        if is_fetch_request(request):
            return build_ui_json_response("Đang quét dữ liệu nên chưa nhập sheet được.", level="warning", ok=False, state=runtime_state)
        return HTMLResponse("<html><script>window.location.href='/?sheet_error=2';</script></html>")
    if not requested_sheet:
        if is_fetch_request(request):
            return build_ui_json_response("Thiếu tên tab sheet. Vui lòng nhập tên tab.", level="warning", ok=False, state=runtime_state)
        return HTMLResponse("<html><script>window.location.href='/?sheet_error=3';</script></html>")
    requested_sheet_id = extract_sheet_id(sheet_url) if sheet_url else None
    if not requested_sheet_id and not runtime_state["active_sheet_id"]:
        add_log("Bạn cần nhập link hoặc ID spreadsheet ở lần đầu tiên", runtime_state)
        if is_fetch_request(request):
            return build_ui_json_response("Bạn cần nhập link hoặc ID spreadsheet ở lần đầu tiên.", level="warning", ok=False, state=runtime_state)
        return HTMLResponse("<html><script>window.location.href='/?sheet_error=4';</script></html>")
    if sheet_url and not requested_sheet_id:
        if is_fetch_request(request):
            return build_ui_json_response("Link/ID spreadsheet không hợp lệ.", level="error", ok=False, state=runtime_state)
        return HTMLResponse("<html><script>window.location.href='/?sheet_error=4';</script></html>")
    try:
        set_active_sheet(requested_sheet, requested_sheet_id, runtime_state)
        ws = get_worksheet(runtime_state["active_sheet_name"], runtime_state["active_sheet_id"], runtime_state)
        detected_text = format_detected_columns_text(detect_sheet_layout(ws), runtime_state)
        add_log(detected_text, runtime_state)
        # Invalidate cache for this sheet when actively modified
        global SHEET_DATA_CACHE, DASHBOARD_CACHE
        cache_key = f"{runtime_state['active_sheet_id']}:{runtime_state['active_sheet_name']}"
        if cache_key in SHEET_DATA_CACHE:
            del SHEET_DATA_CACHE[cache_key]
        
        # Also invalidate dashboard cache for this user to force refresh
        for section in ["overview", "posts", "config"]:
            dict_key = f"{current_user['email']}:{section}"
            if dict_key in DASHBOARD_CACHE:
                del DASHBOARD_CACHE[dict_key]
        
        save_sheet_data_cache(SHEET_DATA_CACHE)
        save_dashboard_cache(DASHBOARD_CACHE)

        if is_fetch_request(request):
            return build_ui_json_response(
                f"Đã nhập sheet thành công. {detected_text}",
                level="success",
                extra={
                    "overview_html": build_overview_panel_for_state(runtime_state, sheet=ws),
                    "column_config": build_column_config_payload(ws, runtime_state),
                    "sheet_metadata": build_sheet_metadata_payload(state=runtime_state),
                    "posts_html": build_posts_panel_html(ws, runtime_state),
                    "campaign_html": build_campaign_panel_html(runtime_state),
                },
                state=runtime_state,
            )
        return HTMLResponse("<html><script>window.location.href='/?sheet_ok=1';</script></html>")
    except Exception as e:
        add_log(f"Không tìm thấy sheet: {requested_sheet} ({str(e)[:60]})", runtime_state)
        if is_fetch_request(request):
            return build_ui_json_response("Không tìm thấy tab sheet. Kiểm tra lại tên tab và quyền truy cập.", level="error", ok=False, state=runtime_state)
        return HTMLResponse("<html><script>window.location.href='/?sheet_error=1';</script></html>")


@app.post("/save-selected-sheets")
async def save_selected_sheets(request: Request):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return auth_response
    runtime_state = get_runtime_state(current_user)
    if runtime_state["is_running"]:
        return build_ui_json_response("Đang quét dữ liệu nên chưa lưu sheet được.", level="warning", ok=False, state=runtime_state)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    sheet_url = str((payload or {}).get("sheet_url", "") or "").strip()
    requested_sheet_id = extract_sheet_id(sheet_url) if sheet_url else (runtime_state.get("active_sheet_id") or "")
    if not requested_sheet_id:
        return build_ui_json_response("Link/ID spreadsheet không hợp lệ.", level="error", ok=False, state=runtime_state)

    raw_tabs = (payload or {}).get("tabs", [])
    tab_items = []
    if isinstance(raw_tabs, list):
        for item in raw_tabs:
            if isinstance(item, dict):
                title = str(item.get("title", "") or "").strip()
                gid = str(item.get("gid", "") or "0").strip() or "0"
            else:
                title = str(item or "").strip()
                gid = "0"
            if title:
                tab_items.append({"title": title, "gid": gid})
    if not tab_items:
        return build_ui_json_response("Hãy chọn ít nhất 1 tab sheet để lưu.", level="warning", ok=False, state=runtime_state)

    saved_count = 0
    first_ws = None
    first_tab = tab_items[0]
    for item in tab_items:
        tab_name = item["title"]
        tab_gid = item["gid"]
        try:
            ws = get_worksheet(tab_name, requested_sheet_id, runtime_state)
        except Exception as exc:
            return build_ui_json_response(
                f"Không truy cập được tab '{tab_name}'. Kiểm tra lại quyền truy cập sheet.",
                level="error",
                ok=False,
                state=runtime_state,
            )
        if first_ws is None:
            first_ws = ws
        save_sheet_entry(
            requested_sheet_id,
            tab_name,
            tab_gid,
            owner_email=runtime_state["owner_email"],
        )
        saved_count += 1

    set_active_sheet(first_tab["title"], requested_sheet_id, runtime_state)
    runtime_state["active_sheet_gid"] = first_tab["gid"]
    add_log(f"Đã lưu {saved_count} tab sheet: {', '.join(item['title'] for item in tab_items)}", runtime_state)
    for item in tab_items:
        add_log(f"[{item['title']}] Đã thêm vào danh sách tab quét", runtime_state)
    return build_ui_json_response(
        f"Đã lưu {saved_count} tab sheet. Bạn có thể cấu hình từng tab rồi bấm Bắt đầu để quét cùng lúc.",
        level="success",
        extra={
            "overview_html": build_overview_panel_for_state(runtime_state, sheet=first_ws),
            "column_config": build_column_config_payload(first_ws, runtime_state),
            "sheet_metadata": build_sheet_metadata_payload(state=runtime_state),
            "posts_html": build_posts_panel_html(first_ws, runtime_state),
            "campaign_html": build_campaign_panel_html(runtime_state),
            "schedule_config": build_schedule_config_payload(runtime_state),
        },
        state=runtime_state,
    )


@app.post("/set-schedule-targets")
async def set_schedule_targets(request: Request):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return auth_response
    runtime_state = get_runtime_state(current_user)
    try:
        payload = await request.json()
    except Exception:
        return build_ui_json_response("Không đọc được danh sách bài đã chọn.", level="error", ok=False, state=runtime_state)

    raw_targets = payload.get("targets", []) if isinstance(payload, dict) else []
    fallback_sheet_id = payload.get("sheet_id") if isinstance(payload, dict) else None
    fallback_sheet_gid = str(payload.get("sheet_gid") or "0").strip() if isinstance(payload, dict) else "0"
    runtime_state["schedule_targets"] = normalize_schedule_targets(raw_targets, fallback_sheet_id)
    ensure_scheduler_thread()

    if runtime_state["schedule_targets"]:
        primary_sheet_id = runtime_state["schedule_targets"][0]["sheet_id"]
        primary_sheet_name = runtime_state["schedule_targets"][0]["sheet_name"]
        runtime_state["schedule_targets"] = [
            item
            for item in runtime_state["schedule_targets"]
            if item["sheet_id"] == primary_sheet_id and item["sheet_name"].strip().lower() == primary_sheet_name.strip().lower()
        ]
        schedule_entry = upsert_schedule_entry(primary_sheet_id, primary_sheet_name, fallback_sheet_gid, runtime_state)
        if schedule_entry:
            runtime_state["active_schedule_key"] = schedule_entry["key"]
            schedule_entry["schedule_targets"] = list(runtime_state["schedule_targets"])
            if runtime_state["schedule_mode"] != "off" and runtime_state["schedule_sheet_id"] == primary_sheet_id and runtime_state["schedule_sheet_name"].strip().lower() == primary_sheet_name.strip().lower():
                sync_schedule_entry_from_runtime_state(schedule_entry, runtime_state)
            else:
                sync_runtime_state_from_schedule_entry(schedule_entry, runtime_state)
            persist_runtime_schedule_entries(runtime_state)
        add_log(f"Đã lưu {len(runtime_state['schedule_targets'])} bài cho lịch tự động", runtime_state)
        return build_ui_json_response(
            f"Đã áp dụng {len(runtime_state['schedule_targets'])} bài cho lịch tự động.",
            level="success",
            extra={
                "schedule_config": build_schedule_config_payload(runtime_state),
                "schedule_tracking": build_schedule_tracking_payload(runtime_state),
            },
            state=runtime_state,
        )

    active_entry = get_active_schedule_entry(runtime_state)
    if active_entry:
        active_entry["schedule_targets"] = []
        sync_runtime_state_from_schedule_entry(active_entry, runtime_state)
        persist_runtime_schedule_entries(runtime_state)
    add_log("Đã xóa chọn lọc bài cho lịch tự động, quay về chạy toàn tab.", runtime_state)
    return build_ui_json_response(
        "Đã xóa chọn lọc bài. Lịch tự động sẽ quay về chạy toàn tab đang dùng.",
        level="info",
        extra={
            "schedule_config": build_schedule_config_payload(runtime_state),
            "schedule_tracking": build_schedule_tracking_payload(runtime_state),
        },
        state=runtime_state,
    )


@app.post("/start-selected")
async def start_selected_posts(request: Request, background_tasks: BackgroundTasks):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return auth_response
    runtime_state = get_runtime_state(current_user)
    if runtime_state["is_running"]:
        add_log("Hệ thống đang chạy, chưa thể quét lại các bài đã chọn", runtime_state)
        return build_ui_json_response(
            "Hệ thống đang chạy rồi, chưa thể quét thêm các bài đã chọn.",
            level="warning",
            ok=False,
            state=runtime_state,
        )

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    raw_targets = payload.get("targets", []) if isinstance(payload, dict) else []
    fallback_sheet_id = payload.get("sheet_id") if isinstance(payload, dict) else None
    normalized_targets = normalize_schedule_targets(raw_targets, fallback_sheet_id)
    if not normalized_targets:
        return build_ui_json_response(
            "Bạn chưa chọn bài nào để chạy lại.",
            level="warning",
            ok=False,
            state=runtime_state,
        )

    primary_target = normalized_targets[0]
    try:
        set_active_sheet(primary_target["sheet_name"], primary_target["sheet_id"], runtime_state)
    except Exception:
        runtime_state["active_sheet_id"] = primary_target["sheet_id"]
        runtime_state["active_sheet_name"] = primary_target["sheet_name"]
        runtime_state["active_sheet_gid"] = str(primary_target.get("sheet_gid") or "0")

    runtime_state["pending_updates"] = []
    runtime_state["is_running"] = True
    runtime_state["is_finished"] = False
    runtime_state["current_task"] = f"Chuẩn bị quét lại {len(normalized_targets)} bài đã chọn"
    set_run_progress(current=0, total=0, phase="preparing", state=runtime_state)
    add_log(f"Bắt đầu quét lại {len(normalized_targets)} bài đã chọn", runtime_state)
    background_tasks.add_task(
        run_scraper_logic,
        targets=normalized_targets,
        source="manual-selected",
        state=runtime_state,
    )
    return build_ui_json_response(
        f"Đã bắt đầu quét lại {len(normalized_targets)} bài đã chọn.",
        level="success",
        extra={"column_config": build_column_config_payload(state=runtime_state)},
        state=runtime_state,
    )

@app.get("/set-schedule")
def set_schedule(request: Request, mode: str = "off", at: str = "09:00", weekday: int = 0, monthday: int = 1, monthdate: str = "", enddate: str = "", sheet_binding: str = ""):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return auth_response
    runtime_state = get_runtime_state(current_user)
    mode = (mode or "off").strip().lower()
    if mode not in ["off", "daily", "weekly", "monthly"]:
        if is_fetch_request(request):
            return build_ui_json_response("Chế độ lịch không hợp lệ.", level="error", ok=False, state=runtime_state)
        return HTMLResponse("<html><script>window.location.href='/?schedule_error=1';</script></html>")

    selected_sheet_id, selected_sheet_name = parse_sheet_binding_key(sheet_binding)
    selected_binding = None
    if selected_sheet_id and selected_sheet_name:
        for choice in get_schedule_sheet_choices(state=runtime_state):
            if choice["sheet_id"] == selected_sheet_id and choice["sheet_name"].strip().lower() == selected_sheet_name.strip().lower():
                selected_binding = choice
                break
    if not selected_binding:
        fallback_binding = get_schedule_sheet_binding(use_active_fallback=True, state=runtime_state)
        if fallback_binding["sheet_id"] and fallback_binding["sheet_name"]:
            selected_binding = {
                "sheet_id": fallback_binding["sheet_id"],
                "sheet_name": fallback_binding["sheet_name"],
                "sheet_gid": fallback_binding["sheet_gid"],
                "is_saved": fallback_binding["is_saved"],
            }

    if not selected_binding or not selected_binding.get("sheet_id") or not selected_binding.get("sheet_name"):
        if is_fetch_request(request):
            return build_ui_json_response("Hãy chọn sheet muốn áp dụng cho lịch trước khi lưu.", level="warning", ok=False, state=runtime_state)
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
            return build_ui_json_response("Ngày chạy không hợp lệ.", level="error", ok=False, state=runtime_state)
        return HTMLResponse("<html><script>window.location.href='/?schedule_error=2';</script></html>")
    try:
        safe_end_date = parse_schedule_date(enddate)
    except Exception:
        if is_fetch_request(request):
            return build_ui_json_response("Ngày kết thúc không hợp lệ. Dùng định dạng YYYY-MM-DD.", level="error", ok=False, state=runtime_state)
        return HTMLResponse("<html><script>window.location.href='/?schedule_error=3';</script></html>")

    selected_sheet_id = str(selected_binding.get("sheet_id", "") or "").strip()
    selected_sheet_name = str(selected_binding.get("sheet_name", "") or "").strip()
    selected_sheet_gid = str(selected_binding.get("sheet_gid", "") or "0").strip() or "0"
    schedule_entry = upsert_schedule_entry(selected_sheet_id, selected_sheet_name, selected_sheet_gid, runtime_state)
    runtime_state["active_schedule_key"] = schedule_entry["key"]

    normalized_existing_targets = normalize_schedule_targets(runtime_state["schedule_targets"], selected_sheet_id)
    entry_targets = [
        item
        for item in normalized_existing_targets
        if item["sheet_id"] == selected_sheet_id and item["sheet_name"].strip().lower() == selected_sheet_name.strip().lower()
    ]
    if normalized_existing_targets and not entry_targets:
        add_log("Đã bỏ chọn lọc bài cũ vì bạn vừa đổi sang tab lịch khác.", runtime_state)
    schedule_entry["schedule_mode"] = mode
    schedule_entry["schedule_time"] = safe_time
    schedule_entry["schedule_weekday"] = safe_weekday
    schedule_entry["schedule_monthday"] = safe_monthday
    schedule_entry["schedule_end_date"] = safe_end_date
    schedule_entry["schedule_sheet_id"] = selected_sheet_id
    schedule_entry["schedule_sheet_name"] = selected_sheet_name
    schedule_entry["schedule_sheet_gid"] = selected_sheet_gid
    schedule_entry["schedule_targets"] = entry_targets
    schedule_entry["last_schedule_run_key"] = ""
    sync_runtime_state_from_schedule_entry(schedule_entry, runtime_state)
    if mode == "off" and runtime_state.get("schedule_tracking_key") == schedule_entry["key"]:
        runtime_state["schedule_tracking_key"] = ""
    persist_runtime_schedule_entries(runtime_state)
    ensure_scheduler_thread()
    add_log(f"Cập nhật lịch cho tab '{runtime_state['schedule_sheet_name']}': {schedule_label(runtime_state)}", runtime_state)
    if is_fetch_request(request):
        return build_ui_json_response(
            "Đã cập nhật lịch tự động.",
            level="success",
            extra={
                "schedule_config": build_schedule_config_payload(runtime_state),
                "schedule_tracking": build_schedule_tracking_payload(runtime_state),
            },
            state=runtime_state,
        )
    return HTMLResponse("<html><script>window.location.href='/?schedule_ok=1';</script></html>")


@app.post("/set-active-schedule")
async def set_active_schedule(request: Request):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return auth_response
    runtime_state = get_runtime_state(current_user)
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    key = str((payload or {}).get("key", "") or "").strip()
    context = str((payload or {}).get("context", "form") or "form").strip().lower()
    if context == "tracking" and not key:
        runtime_state["schedule_tracking_key"] = ""
        return build_ui_json_response(
            "Đã ẩn theo dõi lần chạy.",
            level="info",
            extra={
                "schedule_config": build_schedule_config_payload(runtime_state),
                "schedule_tracking": build_schedule_tracking_payload(runtime_state),
            },
            state=runtime_state,
        )
    entry = get_schedule_entry_by_key(key, runtime_state)

    if not entry and key:
        selected_sheet_id, selected_sheet_name = parse_sheet_binding_key(key)
        if selected_sheet_id and selected_sheet_name:
            selected_choice = next(
                (
                    choice
                    for choice in get_schedule_sheet_choices(state=runtime_state)
                    if choice["sheet_id"] == selected_sheet_id and choice["sheet_name"].strip().lower() == selected_sheet_name.strip().lower()
                ),
                None,
            )
            if selected_choice:
                entry = upsert_schedule_entry(selected_choice["sheet_id"], selected_choice["sheet_name"], selected_choice["sheet_gid"], runtime_state)

    if not entry:
        return build_ui_json_response("Không tìm thấy sheet lịch bạn vừa chọn.", level="warning", ok=False, state=runtime_state)

    if context == "tracking":
        runtime_state["schedule_tracking_key"] = entry["key"] if entry.get("schedule_mode") != "off" else ""
    else:
        runtime_state["active_schedule_key"] = entry["key"]
        sync_runtime_state_from_schedule_entry(entry, runtime_state)

    return build_ui_json_response(
        "Đã chọn sheet lịch.",
        level="info",
        extra={
            "schedule_config": build_schedule_config_payload(runtime_state),
            "schedule_tracking": build_schedule_tracking_payload(runtime_state),
        },
        state=runtime_state,
    )

_DETECT_TAB_COLUMNS_CACHE = {}  # key: (owner_email, sheet_id, tab_name) -> (timestamp, result)
_DETECT_TAB_COLUMNS_TTL = 900   # 15 minutes

@app.get("/detect-tab-columns")
def detect_tab_columns(request: Request, tab_name: str = ""):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
    runtime_state = get_runtime_state(current_user)
    resolved_tab = (tab_name or "").strip()
    if not resolved_tab or not runtime_state["active_sheet_id"]:
        return JSONResponse({"ok": False, "detected_inputs": {}, "tab_name": resolved_tab})
    owner_email = runtime_state.get("owner_email", "")
    sheet_id = runtime_state["active_sheet_id"]
    cache_key = (owner_email, sheet_id, resolved_tab)
    now = time.time()
    cached_entry = _DETECT_TAB_COLUMNS_CACHE.get(cache_key)
    if cached_entry:
        cached_at, cached_result = cached_entry
        if now - cached_at < _DETECT_TAB_COLUMNS_TTL:
            return JSONResponse(cached_result)
    try:
        ws = get_worksheet(resolved_tab, sheet_id, runtime_state)
        layout = detect_sheet_layout(ws)
        raw_columns = layout.get("columns") or {}
        tab_overrides = runtime_state.get("column_overrides_by_tab", {}).get(resolved_tab)
        col_map = apply_column_overrides(raw_columns, overrides=tab_overrides) if tab_overrides else apply_column_overrides(raw_columns, state=runtime_state)
        fields = ["date", "air_date", "link", "view", "like", "share", "comment", "buzz", "save"]
        detected_inputs = {}
        for field in fields:
            col_idx = col_map.get(field)
            detected_inputs[field] = col_to_a1(col_idx) if col_idx else ""
        saved_overrides = {}
        if tab_overrides:
            for field in fields:
                col_idx = tab_overrides.get(field)
                saved_overrides[field] = col_to_a1(col_idx) if col_idx else ""
        header_row = max(1, int(layout.get("header_row") or 1))
        start_row = resolve_effective_start_row(header_row, runtime_state)
        detected_count = sum(1 for value in detected_inputs.values() if value)
        add_log(f"[{resolved_tab}] AUTO nhận {detected_count} cột ở header dòng {header_row}", runtime_state)
        result = {
            "ok": True,
            "tab_name": resolved_tab,
            "detected_inputs": detected_inputs,
            "saved_overrides": saved_overrides,
            "has_saved_overrides": bool(tab_overrides),
            "start_row": start_row,
            "header_row": header_row,
        }
        _DETECT_TAB_COLUMNS_CACHE[cache_key] = (now, result)
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)[:120], "tab_name": resolved_tab})

@app.get("/clear-tab-columns")
def clear_tab_columns(request: Request, tab_name: str = ""):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
    runtime_state = get_runtime_state(current_user)
    resolved_tab = (tab_name or "").strip()
    if not resolved_tab:
        return JSONResponse({"ok": False, "message": "Thiếu tên tab"})
    overrides_by_tab = runtime_state.get("column_overrides_by_tab", {})
    if resolved_tab in overrides_by_tab:
        del overrides_by_tab[resolved_tab]
        runtime_state["column_overrides_by_tab"] = overrides_by_tab
    # Also clear cache for this tab
    owner_email = runtime_state.get("owner_email", "")
    sheet_id = runtime_state.get("active_sheet_id", "")
    cache_key = (owner_email, sheet_id, resolved_tab)
    _DETECT_TAB_COLUMNS_CACHE.pop(cache_key, None)
    return build_ui_json_response(
        f"Đã reset cấu hình tab '{resolved_tab}' về AUTO detect.",
        level="success",
        ok=True,
        state=runtime_state,
        extra={"column_config": build_column_config_payload(state=runtime_state)},
    )

@app.get("/set-columns")
def set_columns(
    request: Request,
    date: Optional[str] = None,
    air_date: Optional[str] = None,
    link: Optional[str] = None,
    view: Optional[str] = None,
    like: Optional[str] = None,
    share: Optional[str] = None,
    comment: Optional[str] = None,
    buzz: Optional[str] = None,
    save: Optional[str] = None,
    start_row: Optional[str] = None,
    tab_name: Optional[str] = None,
    campaign_label: str = "",
    brand_label: str = "",
    industry_label: Optional[str] = None,
    campaign_description: str = "",
):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return auth_response
    runtime_state = get_runtime_state(current_user)
    if (not runtime_state["is_running"]) and runtime_state["current_task"] == "Đã dừng thủ công":
        if is_fetch_request(request):
            return build_ui_json_response(
                "Đang ở trạng thái Đã dừng. Bấm Bắt đầu để mở lại rồi hãy lưu sheet.",
                level="warning",
                ok=False,
                state=runtime_state,
            )
        return HTMLResponse("<html><script>window.location.href='/?sheet_error=2';</script></html>")
    if runtime_state["is_running"]:
        if is_fetch_request(request):
            return build_ui_json_response("Đang quét dữ liệu nên chưa lưu cấu hình được.", level="warning", ok=False, state=runtime_state)
        return HTMLResponse("<html><script>window.location.href='/?sheet_error=2';</script></html>")

    candidates = {"date": date, "air_date": air_date, "link": link, "view": view, "like": like, "share": share, "comment": comment, "buzz": buzz, "save": save}
    parsed = {}
    for field, val in candidates.items():
        if (val or "").strip() == "":
            parsed[field] = None
            continue
        col_idx = parse_column_input(val)
        if not col_idx:
            if is_fetch_request(request):
                return build_ui_json_response("Cột không hợp lệ. Nhập dạng A/B/C... hoặc số 1/2/3...", level="error", ok=False, state=runtime_state)
            return HTMLResponse("<html><script>window.location.href='/?col_error=1';</script></html>")
        parsed[field] = col_idx

    if start_row is not None:
        parsed_start_row = parse_start_row_input(start_row)
        if parsed_start_row is None:
            if is_fetch_request(request):
                return build_ui_json_response("Dòng bắt đầu không hợp lệ. Nhập số từ 2 trở lên.", level="error", ok=False, state=runtime_state)
            return HTMLResponse("<html><script>window.location.href='/?col_error=2';</script></html>")
        runtime_state["start_row"] = parsed_start_row

    cleaned_campaign_label = str(campaign_label or "").strip()
    if cleaned_campaign_label:
        known_campaign_labels = get_saved_campaign_labels(owner_email=runtime_state["owner_email"])
        if not any(label.lower() == cleaned_campaign_label.lower() for label in known_campaign_labels):
            if is_fetch_request(request):
                return build_ui_json_response(
                    "Chiến dịch này chưa có trong mục Cài đặt. Hãy thêm chiến dịch trước rồi quay lại chọn.",
                    level="warning",
                    ok=False,
                    state=runtime_state,
                )
            return HTMLResponse("<html><script>window.location.href='/?sheet_error=5';</script></html>")

    resolved_tab_name = (tab_name or "").strip()
    if resolved_tab_name:
        runtime_state.setdefault("column_overrides_by_tab", {})[resolved_tab_name] = parsed
    else:
        runtime_state["column_overrides"] = parsed
    if not runtime_state["active_sheet_id"] or not runtime_state["active_sheet_name"]:
        if is_fetch_request(request):
            return build_ui_json_response("Hãy nhập sheet trước khi lưu sheet.", level="warning", ok=False, state=runtime_state)
        return HTMLResponse("<html><script>window.location.href='/?sheet_error=3';</script></html>")
    active_ws = None
    try:
        active_ws = get_worksheet(runtime_state["active_sheet_name"], runtime_state["active_sheet_id"], runtime_state)
    except Exception:
        active_ws = None
    save_sheet_entry(
        runtime_state["active_sheet_id"],
        runtime_state["active_sheet_name"],
        runtime_state["active_sheet_gid"],
        owner_email=runtime_state["owner_email"],
        campaign_label=campaign_label,
        brand_label=brand_label,
        industry_label=industry_label,
        campaign_description=campaign_description,
    )
    if cleaned_campaign_label:
        save_campaign_label(cleaned_campaign_label, owner_email=runtime_state["owner_email"])
    saved_overrides = runtime_state.get("column_overrides_by_tab", {}).get(resolved_tab_name) if resolved_tab_name else runtime_state["column_overrides"]
    tab_label = f"[{resolved_tab_name}] " if resolved_tab_name else ""
    add_log(
        f"Đã lưu {tab_label}sheet vào Bài đăng: {runtime_state['active_sheet_name']} | "
        + ", ".join([f"{k.upper()}={col_to_a1(v) if v else 'AUTO'}" for k, v in (saved_overrides or {}).items()])
        + (
            f" | CAMPAIGN={cleaned_campaign_label}"
            if cleaned_campaign_label
            else ""
        )
        + (
            f" | BRAND={str(brand_label or '').strip()}"
            if str(brand_label or "").strip()
            else ""
        )
        + (
            f" | INDUSTRY={str(industry_label or '').strip()}"
            if str(industry_label or "").strip()
            else ""
        )
        + f", START_ROW={runtime_state['start_row']}",
        runtime_state,
    )
    if is_fetch_request(request):
        return build_ui_json_response(
            "Đã lưu sheet thành công.",
            level="success",
            extra={
                "overview_html": build_overview_panel_for_state(runtime_state, sheet=active_ws),
                "column_config": build_column_config_payload(active_ws, runtime_state),
                "sheet_metadata": build_sheet_metadata_payload(state=runtime_state),
                "posts_html": build_posts_panel_html(active_ws, runtime_state),
                "campaign_html": build_campaign_panel_html(runtime_state),
            },
            state=runtime_state,
        )
    return HTMLResponse("<html><script>window.location.href='/?col_ok=1';</script></html>")


@app.post("/set-sheet-campaign")
async def set_sheet_campaign(request: Request):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return auth_response
    runtime_state = get_runtime_state(current_user)
    try:
        payload = await request.json()
    except Exception:
        return build_ui_json_response("Không đọc được dữ liệu chiến dịch.", level="error", ok=False, state=runtime_state)

    sheet_id = str((payload or {}).get("sheet_id", "") or "").strip()
    sheet_name = str((payload or {}).get("sheet_name", "") or "").strip()
    campaign_label = str((payload or {}).get("campaign_label", "") or "").strip()
    if not sheet_id or not sheet_name:
        return build_ui_json_response("Thiếu sheet để gắn chiến dịch.", level="warning", ok=False, state=runtime_state)

    updated_entries = update_saved_sheet_campaign(sheet_id, sheet_name, campaign_label, owner_email=runtime_state["owner_email"])
    if not updated_entries:
        return build_ui_json_response(f"Không tìm thấy sheet để cập nhật chiến dịch (ID: '{sheet_id}', Tên: '{sheet_name}').", level="warning", ok=False, state=runtime_state)

    add_log(
        f"{'Đã gắn' if campaign_label else 'Đã xóa'} chiến dịch cho sheet '{sheet_name}'"
        + (f": {campaign_label}" if campaign_label else "."),
        runtime_state,
    )

    active_ws = None
    if runtime_state["active_sheet_id"] and runtime_state["active_sheet_name"]:
        try:
            active_ws = get_worksheet(runtime_state["active_sheet_name"], runtime_state["active_sheet_id"], runtime_state)
        except Exception:
            active_ws = None

    return build_ui_json_response(
        "Đã lưu chiến dịch cho sheet." if campaign_label else "Đã xóa chiến dịch khỏi sheet.",
        level="success",
        extra={
            "overview_html": build_overview_panel_for_state(runtime_state, sheet=active_ws),
            "posts_html": build_posts_panel_html(active_ws, runtime_state),
            "campaign_html": build_campaign_panel_html(runtime_state),
        },
        state=runtime_state,
    )


@app.post("/api/update-sheet-metadata")
async def api_update_sheet_metadata(request: Request):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return auth_response
    runtime_state = get_runtime_state(current_user)
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    sheet_id = str((payload or {}).get("sheet_id", "") or "").strip()
    original_name = str((payload or {}).get("original_name", "") or "").strip()
    new_name = str((payload or {}).get("sheet_name", "") or "").strip()
    campaign_label = str((payload or {}).get("campaign_label", "") or "").strip()
    brand_label = str((payload or {}).get("brand_label", "") or "").strip()
    industry_label = str((payload or {}).get("industry_label", "") or "").strip()

    if not sheet_id or not original_name:
        return build_ui_json_response("Thiếu thông tin sheet để cập nhật.", level="error", ok=False, state=runtime_state)
    if not new_name:
        return build_ui_json_response("Tên hiển thị không được để trống.", level="warning", ok=False, state=runtime_state)

    updated_entries = update_saved_sheet_metadata(
        sheet_id=sheet_id,
        sheet_name=original_name,
        new_sheet_name=new_name,
        campaign_label=campaign_label,
        brand_label=brand_label,
        industry_label=industry_label,
        owner_email=runtime_state["owner_email"],
    )
    if not updated_entries:
        return build_ui_json_response("Không tìm thấy sheet để cập nhật.", level="warning", ok=False, state=runtime_state)

    if campaign_label:
        save_campaign_label(campaign_label, owner_email=runtime_state["owner_email"])

    add_log(
        f"Đã cập nhật thông tin sheet '{original_name}' -> '{new_name}'"
        + (f" | BRAND={brand_label}" if brand_label else "")
        + (f" | CAMPAIGN={campaign_label}" if campaign_label else "")
        + (f" | INDUSTRY={industry_label}" if industry_label else ""),
        runtime_state,
    )

    active_ws = None
    if runtime_state.get("active_sheet_id") and runtime_state.get("active_sheet_name"):
        try:
            active_ws = get_worksheet(runtime_state["active_sheet_name"], runtime_state["active_sheet_id"], runtime_state)
        except Exception:
            active_ws = None

    return build_ui_json_response(
        "Đã lưu thay đổi thông tin sheet.",
        level="success",
        extra={
            "overview_html": build_overview_panel_for_state(runtime_state, sheet=active_ws),
            "posts_html": build_posts_panel_html(active_ws, runtime_state),
            "campaign_html": build_campaign_panel_html(runtime_state),
        },
        state=runtime_state,
    )


@app.post("/create-campaign")
async def create_campaign(request: Request):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return auth_response
    runtime_state = get_runtime_state(current_user)
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    campaign_label = str((payload or {}).get("campaign_label", "") or "").strip()
    if not campaign_label:
        return build_ui_json_response("Nhập tên chiến dịch trước khi tạo.", level="warning", ok=False, state=runtime_state)

    save_campaign_label(campaign_label, owner_email=runtime_state["owner_email"])
    add_log(f"Đã tạo chiến dịch mới: {campaign_label}", runtime_state)
    active_ws = None
    if runtime_state.get("active_sheet_id") and runtime_state.get("active_sheet_name"):
        try:
            active_ws = get_worksheet(runtime_state["active_sheet_name"], runtime_state["active_sheet_id"])
        except Exception:
            active_ws = None
    return build_ui_json_response(
        f"Đã tạo chiến dịch: {campaign_label}",
        level="success",
        extra={
            "campaign_html": build_campaign_panel_html(runtime_state),
            "posts_html": build_posts_panel_html(active_ws, runtime_state),
        },
        state=runtime_state,
    )


@app.post("/save-notification-settings")
async def save_notification_settings(request: Request):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return auth_response
    runtime_state = get_runtime_state(current_user)
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    prefs = save_user_notification_preferences(
        current_user,
        {
            "email_notifications": bool((payload or {}).get("email_notifications", True)),
            "system_alerts": bool((payload or {}).get("system_alerts", True)),
        },
    )
    add_log("Đã cập nhật cài đặt thông báo.", runtime_state)
    return build_ui_json_response(
        "Đã lưu cài đặt thông báo.",
        level="success",
        extra={"notification_preferences": prefs},
        state=runtime_state,
    )


@app.get("/stop")
def stop_task(request: Request):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return auth_response
    runtime_state = get_runtime_state(current_user)
    runtime_state["is_running"] = False
    runtime_state["pending_updates"] = []
    runtime_state["current_task"] = "Đã dừng thủ công"
    set_run_progress(phase="stopped", state=runtime_state)
    add_log("Đã gửi lệnh dừng quét.", runtime_state)
    if is_fetch_request(request):
        return build_ui_json_response("Đã dừng quét dữ liệu.", level="info", state=runtime_state)
    return HTMLResponse("<html><script>window.location.href='/';</script></html>")


@app.get("/download")
def download_excel(request: Request):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return auth_response
    runtime_state = get_runtime_state(current_user)
    if not runtime_state["active_sheet_id"] or not runtime_state["active_sheet_name"]:
        return RedirectResponse(url="/?download_error=2", status_code=302)
    try:
        sheet = get_worksheet(runtime_state["active_sheet_name"], runtime_state["active_sheet_id"], runtime_state)
        records, _, _ = get_sheet_records(sheet)
        df = pd.DataFrame(records)
        path = "Social_Export.xlsx"
        df.to_excel(path, index=False, engine="openpyxl")
        safe_sheet_name = re.sub(r"[^A-Za-z0-9_-]+", "_", runtime_state["active_sheet_name"]).strip("_") or "sheet"
        return FileResponse(path, filename=f"Data_{safe_sheet_name}_{datetime.now().strftime('%H%M')}.xlsx")
    except ImportError as e:
        add_log(f"Lỗi export Excel: thiếu thư viện ({str(e)})", runtime_state)
        return RedirectResponse(url="/?download_error=1", status_code=302)
    except Exception as e:
        add_log(f"Lỗi export Excel: {str(e)}", runtime_state)
        return RedirectResponse(url="/?download_error=3", status_code=302)

@app.get("/download-all")
def download_excel_all(request: Request):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return auth_response
    runtime_state = get_runtime_state(current_user)
    if not runtime_state["active_sheet_id"]:
        return RedirectResponse(url="/?download_error=2", status_code=302)
    try:
        gc = get_gspread_client()
        spreadsheet = gc.open_by_key(runtime_state["active_sheet_id"])
        path = "Social_Export_All.xlsx"
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            for ws in spreadsheet.worksheets():
                records, _, _ = get_sheet_records(ws)
                df = pd.DataFrame(records)
                safe_ws_name = (ws.title or "Sheet")[:31]
                df.to_excel(writer, sheet_name=safe_ws_name, index=False)
        return FileResponse(path, filename=f"Data_all_tabs_{datetime.now().strftime('%H%M')}.xlsx")
    except ImportError as e:
        add_log(f"Lỗi export all tabs: thiếu thư viện ({str(e)})", runtime_state)
        return RedirectResponse(url="/?download_error=1", status_code=302)
    except Exception as e:
        add_log(f"Lỗi export all tabs: {str(e)}", runtime_state)
        return RedirectResponse(url="/?download_error=4", status_code=302)


@app.get("/status")
def status(request: Request):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return auth_response
    return build_ui_state(get_runtime_state(current_user))


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.post("/client-log")
async def client_log(request: Request):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return auth_response
    runtime_state = get_runtime_state(current_user)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    message = str((payload or {}).get("message", "") or "").strip()
    if not message:
        return build_ui_json_response("Bỏ qua log rỗng.", level="warning", ok=False, state=runtime_state)
    add_log(message, runtime_state)
    return build_ui_json_response("Đã cập nhật nhật ký hoạt động.", level="info", state=runtime_state)


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
    
    now = datetime.now()
    last_request_time = SHEET_TABS_REQUEST_LIMITER.get(requested_sheet_id)
    time_since_last = (now - last_request_time).total_seconds() if last_request_time else SHEET_TABS_MIN_INTERVAL_SECONDS
    
    if time_since_last < SHEET_TABS_MIN_INTERVAL_SECONDS:
        cached_entry = SHEET_TABS_CACHE.get(requested_sheet_id)
        if cached_entry:
            return {
                "ok": True,
                "sheet_id": requested_sheet_id,
                "tabs": cached_entry.get("tabs", []),
                "message": "Danh sách tab (từ cache gần đây)",
            }
    
    try:
        tabs = list_spreadsheet_tabs(requested_sheet_id)
        SHEET_TABS_REQUEST_LIMITER[requested_sheet_id] = now
        return {
            "ok": True,
            "sheet_id": requested_sheet_id,
            "tabs": tabs,
            "message": f"Tìm thấy {len(tabs)} tab trong spreadsheet.",
        }
    except Exception as exc:
        exc_lower = str(exc).lower()
        is_quota = "quota" in exc_lower or "429" in exc_lower or "readrequests" in exc_lower
        # On quota errors, fall back to stale cache if available
        stale_entry = SHEET_TABS_CACHE.get(requested_sheet_id)
        if is_quota and stale_entry and stale_entry.get("tabs"):
            return {
                "ok": True,
                "sheet_id": requested_sheet_id,
                "tabs": stale_entry["tabs"],
                "message": "Hệ thống đang bận (Google API giới hạn). Đang hiển thị danh sách tab từ lần truy cập trước.",
                "stale": True,
            }
        if is_quota:
            return {
                "ok": False,
                "sheet_id": requested_sheet_id,
                "tabs": [],
                "message": "Google Sheet đang chạm giới hạn đọc dữ liệu. Vui lòng chờ 30–60 giây rồi thử lại.",
                "quota_error": True,
            }
        return {
            "ok": False,
            "sheet_id": requested_sheet_id,
            "tabs": [],
            "message": f"Không tải được danh sách tab: {str(exc)}",
        }

@app.get("/api/dashboard/overview")
def api_dashboard_overview(background_tasks: BackgroundTasks, request: Request):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return {"error": "Unauthorized", "html": ""}
    
    runtime_state = get_runtime_state(current_user)
    active_ws = None
    if runtime_state.get("active_sheet_id") and runtime_state.get("active_sheet_name"):
        try:
            active_ws = get_worksheet(runtime_state["active_sheet_name"], runtime_state["active_sheet_id"], runtime_state)
        except Exception:
            active_ws = None

    html_content = build_overview_panel_for_state(runtime_state, sheet=active_ws)
    now_str = datetime.now().isoformat()
    user_email = current_user.get("email", "")

    global DASHBOARD_CACHE
    if not DASHBOARD_CACHE:
        DASHBOARD_CACHE = load_dashboard_cache()
    DASHBOARD_CACHE[f"{user_email}:overview"] = {"updated_at": now_str, "html": html_content}
    save_dashboard_cache(DASHBOARD_CACHE)

    return {"ok": True, "status": "ready", "html": html_content, "cached": False, "refreshing": False}

@app.get("/api/dashboard/posts")
def api_dashboard_posts(background_tasks: BackgroundTasks, request: Request):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return {"error": "Unauthorized", "html": ""}
    
    user_email = current_user.get("email", "")
    now = datetime.now()
    
    global DASHBOARD_CACHE
    if not DASHBOARD_CACHE:
        DASHBOARD_CACHE = load_dashboard_cache()
        
    cache_entry = DASHBOARD_CACHE.get(f"{user_email}:posts")
    needs_refresh = True
    cached_html = None
    
    if cache_entry:
        updated_at = datetime.fromisoformat(cache_entry["updated_at"])
        cached_html = cache_entry["html"]
        if (now - updated_at).total_seconds() < DASHBOARD_CACHE_TTL_SECONDS:
            needs_refresh = False

    if needs_refresh:
        background_tasks.add_task(background_refresh_dashboard_data, user_email, "posts")

    if cached_html:
        return {"ok": True, "status": "ready", "html": cached_html, "cached": True, "refreshing": needs_refresh}
    
    return {"ok": True, "status": "processing", "html": ""}


@app.get("/api/dashboard/config")
def api_dashboard_config(background_tasks: BackgroundTasks, request: Request):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return {"error": "Unauthorized", "html": ""}
    
    user_email = current_user.get("email", "")
    now = datetime.now()
    
    global DASHBOARD_CACHE
    if not DASHBOARD_CACHE:
        DASHBOARD_CACHE = load_dashboard_cache()
        
    cache_entry = DASHBOARD_CACHE.get(f"{user_email}:config")
    needs_refresh = True
    cached_html = None
    
    if cache_entry:
        updated_at = datetime.fromisoformat(cache_entry["updated_at"])
        cached_html = cache_entry["html"]
        if (now - updated_at).total_seconds() < DASHBOARD_CACHE_TTL_SECONDS:
            needs_refresh = False

    if needs_refresh:
        background_tasks.add_task(background_refresh_dashboard_data, user_email, "config")

    if cached_html:
        return {"ok": True, "status": "ready", "html": cached_html, "cached": True, "refreshing": needs_refresh}
    
    return {"ok": True, "status": "processing", "html": ""}


@app.get("/api/dashboard/schedule")
def api_dashboard_schedule(background_tasks: BackgroundTasks, request: Request):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return {"error": "Unauthorized", "html": ""}
    
    user_email = current_user.get("email", "")
    now = datetime.now()
    
    global DASHBOARD_CACHE
    if not DASHBOARD_CACHE:
        DASHBOARD_CACHE = load_dashboard_cache()
        
    cache_entry = DASHBOARD_CACHE.get(f"{user_email}:schedule")
    needs_refresh = True
    cached_html = None
    
    if cache_entry:
        updated_at = datetime.fromisoformat(cache_entry["updated_at"])
        cached_html = cache_entry["html"]
        if (now - updated_at).total_seconds() < DASHBOARD_CACHE_TTL_SECONDS:
            needs_refresh = False

    if needs_refresh:
        background_tasks.add_task(background_refresh_dashboard_data, user_email, "schedule")

    if cached_html:
        return {"ok": True, "status": "ready", "html": cached_html, "cached": True, "refreshing": needs_refresh}
    
    return {"ok": True, "status": "processing", "html": ""}


@app.get("/", response_class=HTMLResponse)
@app.get("/tong-quan", response_class=HTMLResponse)
@app.get("/cau-hinh", response_class=HTMLResponse)
@app.get("/bai-dang", response_class=HTMLResponse)
@app.get("/chien-dich", response_class=HTMLResponse)
@app.get("/cai-dat", response_class=HTMLResponse)
@app.get("/nhan-vien", response_class=HTMLResponse)
@app.get("/lich-tu-dong", response_class=HTMLResponse)
@app.get("/theo-doi-lan-chay", response_class=HTMLResponse)
def home(request: Request):
    current_user, auth_response = require_authenticated_user(request)
    if auth_response:
        return auth_response
    
    runtime_state = get_runtime_state(current_user)
    current_user_email = html.escape(current_user.get("email", ""))
    current_user_role = html.escape(current_user.get("role_label", "User"))
    initial_dashboard_section = get_dashboard_section_from_path(request.url.path)
    
    global DASHBOARD_CACHE
    if not DASHBOARD_CACHE:
        DASHBOARD_CACHE = load_dashboard_cache()
    
    user_email = current_user.get("email", "")
    
    def get_shell(cached_key, section_id, display_name, active=False):
        entry = DASHBOARD_CACHE.get(f"{user_email}:{cached_key}")
        if entry and entry.get("html"):
            return entry["html"]
        return f"""
        <section id="{section_id}" data-dashboard-section="{section_id}" class="dashboard-section dashboard-panel mb-6 {'is-active' if active else ''}">
            <div class="animate-pulse flex flex-col gap-4 p-6 bg-slate-900/40 rounded-3xl border border-white/5">
                <div class="h-8 bg-slate-800 rounded w-1/4"></div>
                <div class="h-40 bg-slate-800 rounded-2xl"></div>
                <div class="text-center text-slate-500 text-sm">Đang chuẩn bị dữ liệu {display_name}...</div>
            </div>
        </section>
        """

    initial_overview_html = get_shell("overview", "tong-quan", "tổng quan", active=True)
    initial_config_html = get_shell("config", "cau-hinh", "cấu hình")
    initial_posts_html = get_shell("posts", "bai-dang", "bài đăng")
    initial_schedule_html = get_shell("schedule", "lich-tu-dong", "lịch tự động")
    
    settings_panel_html = build_settings_panel_html(current_user, runtime_state)

    # Initialize variables for the asynchronous dashboard content slots
    metric_cols_html = initial_config_html
    schedule_text = "Đang tải lịch..."
    schedule_config = {"sheet_options_html": ""}
    mode_selected = {"off": "selected", "daily": "", "weekly": "", "monthly": ""}
    weekday_options = ""
    schedule_date_value = ""
    schedule_end_value = ""
    schedule_tracking = {
        "entries_html": "<!-- loading -->",
        "active_sheet_name": "Đang tải...",
        "has_active_entry": False,
        "next_run_text": "...",
        "last_started_text": "...",
        "last_finished_text": "...",
        "last_duration_text": "...",
        "is_running_text": "...",
        "last_status_text": "...",
        "last_source_text": "...",
        "last_sheet_text": "...",
        "last_processed_text": "...",
        "last_success_text": "0",
        "last_failed_text": "0",
        "history_html": "",
        "calendar_title": "...",
        "calendar_subtext": "...",
        "calendar_html": ""
    }

    overview_html = f"""
    <section id="tong-quan" data-dashboard-section="tong-quan" class="dashboard-section dashboard-panel is-active mb-6">
        <div class="overview-shell animate-pulse flex flex-col gap-4">
            <div class="h-8 bg-slate-800 rounded w-1/4"></div>
            <div class="grid grid-cols-2 lg:grid-cols-4 gap-4">
                <div class="h-24 bg-slate-800 rounded-2xl"></div>
                <div class="h-24 bg-slate-800 rounded-2xl"></div>
                <div class="h-24 bg-slate-800 rounded-2xl"></div>
                <div class="h-24 bg-slate-800 rounded-2xl"></div>
            </div>
            <div class="h-64 bg-slate-800 rounded-2xl mt-4"></div>
            <div class="text-center mt-2 text-slate-400 text-sm">Đang tải dữ liệu tổng quan, vui lòng chờ...</div>
        </div>
    </section>
    """
    posts_html = f"""
    <section id="bai-dang" data-dashboard-section="bai-dang" class="dashboard-section dashboard-panel posts-board rounded-[2rem] p-6 md:p-8 mb-6 border border-white/5">
        <div class="animate-pulse flex flex-col gap-4">
             <div class="h-8 bg-slate-800 rounded w-1/4 mb-4"></div>
             <div class="h-40 bg-slate-800 rounded-2xl"></div>
             <div class="h-40 bg-slate-800 rounded-2xl"></div>
             <div class="text-center mt-2 text-slate-400 text-sm">Đang tải dữ liệu bài đăng, vui lòng chờ...</div>
        </div>
    </section>
    """
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
            .system-log-shell {{
                background:
                    radial-gradient(circle at top left, rgba(59, 130, 246, 0.08), transparent 28%),
                    linear-gradient(180deg, rgba(8, 13, 26, 0.96), rgba(15, 23, 42, 0.94));
                color: #dbeafe;
                border-color: rgba(148, 163, 184, 0.12);
            }}
            .system-log-line {{
                display: flex;
                gap: 10px;
                padding: 9px 0;
                border-bottom: 1px solid rgba(148, 163, 184, 0.08);
                line-height: 1.45;
            }}
            .system-log-line:last-child {{
                border-bottom: 0;
            }}
            #set-columns-form input,
            #set-columns-form textarea,
            #set-columns-form select {{
                background-color: rgb(15, 23, 42) !important;
                color: #f8fafc !important;
                border-color: rgba(255, 255, 255, 0.1) !important;
            }}
            #set-columns-form input::placeholder,
            #set-columns-form textarea::placeholder {{
                color: rgba(248, 250, 252, 0.6) !important;
            }}
            .system-log-time {{
                color: #60a5fa;
                font-weight: 900;
                white-space: nowrap;
                flex: 0 0 auto;
            }}
            .system-log-message {{
                color: #e2e8f0;
                font-style: italic;
                word-break: break-word;
            }}
            .system-log-tab {{
                color: #22d3ee;
                font-weight: 950;
                font-style: normal;
                white-space: nowrap;
                flex: 0 0 auto;
            }}
            .system-log-empty {{
                margin-top: 5rem;
                text-align: center;
                font-size: 1.15rem;
                font-weight: 900;
                letter-spacing: 0.08em;
                text-transform: uppercase;
                color: #94a3b8;
            }}
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
            .date-shell input[type="date"]::-webkit-calendar-picker-indicator {{
                cursor: pointer;
                opacity: 0.95;
                filter: invert(92%) sepia(12%) saturate(638%) hue-rotate(169deg) brightness(107%) contrast(94%);
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
            .schedule-history-side {{
                display: grid;
                gap: 2px;
                min-width: 0;
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
            .flatpickr-current-month .flatpickr-monthDropdown-months {{
                background: #0f172a;
            }}
            .flatpickr-current-month .flatpickr-monthDropdown-months option {{
                background: #0f172a;
                color: #e2e8f0;
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
            .flatpickr-day.today.schedule-today-muted:not(.selected):not(.startRange):not(.endRange) {{
                border-color: rgba(148, 163, 184, 0.18);
                box-shadow: none;
            }}
            .flatpickr-day.nextMonthDay,
            .flatpickr-day.prevMonthDay {{
                color: #64748b;
            }}
            .overview-shell {{
                display: grid;
                gap: 14px;
            }}
            .overview-header {{
                display: flex;
                align-items: flex-start;
                justify-content: space-between;
                gap: 16px;
                padding: 0;
            }}
            .overview-kicker {{
                font-size: 12px;
                font-weight: 900;
                letter-spacing: 0.12em;
                color: #f8fafc;
            }}
            .overview-title {{
                margin-top: 6px;
                font-size: clamp(1.7rem, 3vw, 2.45rem);
                line-height: 1.08;
                font-weight: 900;
                letter-spacing: -0.04em;
                color: #f8fafc;
            }}
            .overview-subtitle {{
                margin-top: 8px;
                max-width: 680px;
                font-size: 13px;
                line-height: 1.55;
                color: #94a3b8;
            }}
            .overview-actions {{
                display: flex;
                align-items: center;
                justify-content: flex-end;
                flex-wrap: wrap;
                gap: 10px;
            }}
            .overview-action-pill {{
                display: inline-flex;
                align-items: center;
                gap: 10px;
                padding: 10px 14px;
                border-radius: 12px;
                background: rgba(30, 41, 59, 0.7);
                border: 1px solid rgba(148, 163, 184, 0.12);
                color: #dbe2ee;
                font-size: 13px;
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
                grid-template-columns: repeat(4, minmax(0, 1fr));
                gap: 12px;
            }}
            .overview-stat-card {{
                display: flex;
                align-items: center;
                gap: 12px;
                min-height: 86px;
                padding: 14px 16px;
                border-radius: 18px;
                background: linear-gradient(180deg, rgba(23, 31, 48, 0.9), rgba(28, 37, 58, 0.88));
                border: 1px solid rgba(148, 163, 184, 0.12);
                box-shadow: 0 12px 28px rgba(15, 23, 42, 0.18);
            }}
            .overview-stat-icon {{
                width: 46px;
                height: 46px;
                border-radius: 15px;
                display: inline-flex;
                align-items: center;
                justify-content: center;
                font-size: 18px;
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
            .icon-creator {{
                color: #c084fc;
                background: rgba(168, 85, 247, 0.14);
            }}
            .overview-stat-label {{
                font-size: 11px;
                font-weight: 800;
                letter-spacing: 0.1em;
                text-transform: uppercase;
                color: #93a4bf;
            }}
            .overview-stat-value {{
                margin-top: 8px;
                font-size: clamp(1.35rem, 2.4vw, 1.9rem);
                line-height: 1;
                font-weight: 900;
                color: #f8fafc;
            }}
            .overview-section-title {{
                margin-top: 6px;
                font-size: 14px;
                font-weight: 900;
                letter-spacing: 0.04em;
                text-transform: uppercase;
                color: #f8fafc;
            }}
            .overview-campaign-card {{
                padding: 16px;
                border-radius: 20px;
                background: linear-gradient(180deg, rgba(20, 28, 45, 0.92), rgba(26, 35, 55, 0.9));
                border: 1px solid rgba(148, 163, 184, 0.12);
                box-shadow: 0 14px 32px rgba(15, 23, 42, 0.18);
            }}
            .overview-campaign-card-action {{
                width: 100%;
                text-align: left;
                transition: transform 0.18s ease, border-color 0.18s ease, box-shadow 0.18s ease;
                cursor: pointer;
            }}
            .overview-campaign-card-action:hover {{
                transform: translateY(-1px);
                border-color: rgba(96, 165, 250, 0.26);
                box-shadow: 0 18px 34px rgba(15, 23, 42, 0.24);
            }}
            .overview-campaign-title {{
                font-size: clamp(1.15rem, 1.6vw, 1.45rem);
                font-weight: 900;
                color: #f8fafc;
            }}
            .overview-campaign-meta {{
                display: flex;
                align-items: center;
                flex-wrap: wrap;
                gap: 8px;
                margin-top: 10px;
            }}
            .overview-campaign-pill {{
                display: inline-flex;
                align-items: center;
                padding: 7px 10px;
                border-radius: 999px;
                font-size: 12px;
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
                font-size: 12px;
                color: #94a3b8;
                font-weight: 700;
            }}
            .overview-progress-box {{
                min-width: min(100%, 340px);
                padding: 14px 16px;
                border-radius: 16px;
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
                margin-top: 8px;
                font-size: 19px;
                font-weight: 900;
                color: #f8fafc;
            }}
            .overview-campaign-metrics {{
                display: grid;
                grid-template-columns: repeat(3, minmax(0, 1fr));
                gap: 12px;
                margin-top: 14px;
            }}
            .overview-campaign-metric {{
                padding: 13px 15px;
                border-radius: 16px;
                border: 1px solid rgba(148, 163, 184, 0.1);
            }}
            .metric-posts {{
                background: rgba(99, 102, 241, 0.16);
            }}
            .metric-views {{
                background: rgba(71, 85, 105, 0.28);
            }}
            .metric-creators {{
                background: rgba(88, 28, 135, 0.24);
            }}
            .overview-campaign-metric-label {{
                font-size: 11px;
                font-weight: 800;
                letter-spacing: 0.08em;
                text-transform: uppercase;
                color: #bfd2ea;
            }}
            .overview-campaign-metric-value {{
                margin-top: 10px;
                font-size: clamp(1.2rem, 1.8vw, 1.65rem);
                line-height: 1;
                font-weight: 900;
                color: #f8fafc;
            }}
            .overview-control-card {{
                padding: 16px;
                border-radius: 20px;
                background: linear-gradient(180deg, rgba(9, 16, 32, 0.98), rgba(12, 24, 45, 0.94));
                border: 1px solid rgba(148, 163, 184, 0.12);
            }}
            .overview-control-header {{
                display: flex;
                align-items: flex-start;
                justify-content: space-between;
                gap: 14px;
                margin-bottom: 12px;
            }}
            .overview-control-title {{
                font-size: 14px;
                font-weight: 900;
                color: #f8fafc;
            }}
            .overview-control-subtitle {{
                margin-top: 4px;
                font-size: 12px;
                line-height: 1.45;
                color: #94a3b8;
            }}
            .overview-chart-card {{
                position: relative;
                padding: 18px;
                border-radius: 22px;
                background:
                    radial-gradient(circle at top left, rgba(59, 130, 246, 0.08), transparent 26%),
                    radial-gradient(circle at top right, rgba(251, 146, 60, 0.08), transparent 22%),
                    linear-gradient(180deg, rgba(20, 28, 45, 0.97), rgba(15, 23, 42, 0.96));
                border: 1px solid rgba(148, 163, 184, 0.12);
                box-shadow: 0 18px 42px rgba(2, 6, 23, 0.28);
            }}
            .overview-chart-head {{
                display: flex;
                align-items: flex-start;
                justify-content: space-between;
                gap: 14px;
                padding-right: 58px;
            }}
            .overview-head-actions {{
                display: flex;
                align-items: center;
                gap: 8px;
                flex-wrap: wrap;
                justify-content: flex-end;
            }}
            .overview-filter-trigger {{
                display: inline-flex;
                align-items: center;
                gap: 10px;
                height: 36px;
                padding: 0 14px;
                border-radius: 12px;
                border: 1px solid rgba(148, 163, 184, 0.2);
                background: rgba(15, 23, 42, 0.7);
                color: #cbd5e1;
                font-size: 13px;
                font-weight: 800;
                letter-spacing: 0.01em;
                cursor: pointer;
            }}
            .overview-filter-trigger i {{
                font-size: 14px;
            }}
            .overview-filter-trigger-chevron {{
                transition: transform 0.2s ease;
            }}
            .overview-filter-trigger:hover {{
                color: #f8fafc;
                border-color: rgba(56, 189, 248, 0.45);
                box-shadow: 0 0 0 2px rgba(56, 189, 248, 0.12);
            }}
            .overview-filter-trigger.is-active {{
                color: #f8fafc;
                border-color: rgba(56, 189, 248, 0.5);
                background: linear-gradient(135deg, rgba(30, 64, 175, 0.85), rgba(14, 116, 144, 0.85));
            }}
            .overview-filter-trigger.is-active .overview-filter-trigger-chevron {{
                transform: rotate(180deg);
            }}
            .overview-chart-filter-anchor {{
                position: absolute;
                top: 56px;
                right: 18px;
                z-index: 12;
            }}
            .overview-chart-control-wrap {{
                display: flex;
                align-items: flex-start;
                gap: 4px;
                flex-wrap: wrap;
                justify-content: flex-end;
                padding: 4px;
                border-radius: 12px;
                border: 1px solid rgba(148, 163, 184, 0.18);
                background: rgba(2, 6, 23, 0.92);
                box-shadow: 0 18px 32px rgba(2, 6, 23, 0.45);
            }}
            .overview-time-filter-card {{
                width: min(400px, calc(100vw - 56px));
                padding: 7px;
                border-radius: 10px;
                background: rgba(15, 23, 42, 0.68);
                border: 1px solid rgba(148, 163, 184, 0.14);
            }}
            .overview-time-filter-title {{
                font-size: 11px;
                font-weight: 900;
                color: #f1f5f9;
                margin-bottom: 6px;
            }}
            .overview-time-filter-grid {{
                display: grid;
                grid-template-columns: repeat(2, minmax(0, 1fr));
                gap: 5px;
            }}
            .overview-time-filter-grid .overview-chart-toggle {{
                min-height: 34px;
                padding: 6px 9px;
                font-size: 11px;
                letter-spacing: 0.02em;
            }}
            .overview-time-custom-label {{
                margin-top: 8px;
                padding-top: 6px;
                border-top: 1px solid rgba(148, 163, 184, 0.2);
                font-size: 11px;
                font-weight: 900;
                color: #cbd5e1;
            }}
            .overview-chart-segment {{
                display: inline-flex;
                align-items: center;
                gap: 6px;
                padding: 4px;
                border-radius: 12px;
                background: rgba(15, 23, 42, 0.62);
                border: 1px solid rgba(148, 163, 184, 0.12);
                box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.03);
            }}
            .overview-chart-toggle {{
                border: 1px solid rgba(148, 163, 184, 0.2);
                border-radius: 10px;
                padding: 8px 11px;
                background: rgba(15, 23, 42, 0.55);
                color: #cbd5e1;
                font-size: 12px;
                font-weight: 800;
                letter-spacing: 0.04em;
                cursor: pointer;
                transition: background 0.2s ease, color 0.2s ease, box-shadow 0.2s ease;
            }}
            .overview-chart-toggle:hover {{
                background: rgba(255, 255, 255, 0.06);
                color: #fff;
            }}
            .overview-chart-toggle.is-active {{
                background: linear-gradient(135deg, rgba(37, 99, 235, 0.98), rgba(14, 165, 233, 0.95));
                color: #eff6ff;
                box-shadow: 0 10px 22px rgba(37, 99, 235, 0.22);
            }}
            .overview-chart-toggle-full {{
                grid-column: 1 / -1;
            }}
            .overview-chart-meta {{
                display: flex;
                align-items: center;
                gap: 10px;
                flex-wrap: wrap;
                margin-top: 12px;
            }}
            .overview-chart-custom-range {{
                margin-top: 6px;
                display: grid;
                gap: 6px;
            }}
            .overview-chart-custom-row {{
                display: grid;
                grid-template-columns: 42px minmax(120px, 1fr);
                align-items: center;
                gap: 5px;
            }}
            .overview-chart-custom-range label {{
                font-size: 10px;
                font-weight: 800;
                color: #94a3b8;
                letter-spacing: 0.08em;
                text-transform: uppercase;
            }}
            .overview-chart-custom-actions {{
                display: flex;
                justify-content: flex-end;
            }}
            .overview-chart-date-input {{
                height: 28px;
                border-radius: 8px;
                border: 1px solid rgba(148, 163, 184, 0.18);
                background: rgba(15, 23, 42, 0.75);
                color: #e2e8f0;
                padding: 0 8px;
                font-size: 11px;
                font-weight: 700;
            }}
            .overview-chart-date-input::-webkit-calendar-picker-indicator {{
                cursor: pointer;
                opacity: 0.95;
                filter: invert(92%) sepia(12%) saturate(638%) hue-rotate(169deg) brightness(107%) contrast(94%);
            }}
            .overview-chart-date-input:focus {{
                outline: none;
                border-color: rgba(56, 189, 248, 0.45);
                box-shadow: 0 0 0 2px rgba(56, 189, 248, 0.15);
            }}
            .overview-chart-apply {{
                height: 28px;
                border: 1px solid rgba(14, 165, 233, 0.4);
                border-radius: 8px;
                padding: 0 9px;
                background: linear-gradient(135deg, rgba(14, 165, 233, 0.25), rgba(2, 132, 199, 0.2));
                color: #dbeafe;
                font-size: 10px;
                font-weight: 800;
                letter-spacing: 0.04em;
                cursor: pointer;
            }}
            .overview-chart-apply:hover {{
                background: linear-gradient(135deg, rgba(14, 165, 233, 0.35), rgba(2, 132, 199, 0.3));
            }}
            .overview-chart-legend-item {{
                display: inline-flex;
                align-items: center;
                gap: 8px;
                padding: 6px 10px;
                border-radius: 999px;
                background: rgba(15, 23, 42, 0.42);
                border: 1px solid rgba(148, 163, 184, 0.1);
                font-size: 12px;
                font-weight: 700;
                color: #dbe2ee;
            }}
            .overview-chart-dot {{
                width: 10px;
                height: 10px;
                border-radius: 999px;
                display: inline-block;
            }}
            .chart-dot-posts {{
                background: #fb923c;
                box-shadow: 0 0 0 5px rgba(251, 146, 60, 0.12);
            }}
            .chart-dot-creators {{
                background: #c084fc;
                box-shadow: 0 0 0 5px rgba(192, 132, 252, 0.12);
            }}
            .overview-chart-brand-legend {{
                display: flex;
                flex-wrap: wrap;
                gap: 8px;
                margin-top: 10px;
            }}
            .overview-chart-frame {{
                position: relative;
                margin-top: 14px;
                padding: 12px;
                min-height: 410px;
                border-radius: 20px;
                background:
                    radial-gradient(circle at 12% 0%, rgba(59, 130, 246, 0.1), transparent 32%),
                    radial-gradient(circle at 88% 8%, rgba(251, 146, 60, 0.08), transparent 28%),
                    linear-gradient(180deg, rgba(8, 13, 24, 0.95), rgba(11, 18, 32, 0.92));
                border: 1px solid rgba(148, 163, 184, 0.08);
                overflow: hidden;
            }}
            .overview-chart-frame::before {{
                content: "";
                position: absolute;
                inset: 0;
                background:
                    linear-gradient(180deg, rgba(255, 255, 255, 0.018), transparent 24%),
                    linear-gradient(90deg, rgba(255, 255, 255, 0.012), transparent 30%);
                pointer-events: none;
            }}
            .overview-chart-svg {{
                display: block;
                width: 100%;
                height: 370px;
                overflow: visible;
                position: relative;
                z-index: 1;
            }}
            .overview-chart-svg.hidden {{
                display: none;
            }}
            .overview-chart-single {{
                position: absolute;
                inset: 12px;
                display: flex;
                align-items: center;
                justify-content: center;
                z-index: 2;
            }}
            .overview-chart-single-panel {{
                width: min(560px, 100%);
                padding: 22px 24px;
                border-radius: 22px;
                border: 1px solid rgba(148, 163, 184, 0.12);
                background:
                    radial-gradient(circle at top left, rgba(59, 130, 246, 0.12), transparent 34%),
                    radial-gradient(circle at top right, rgba(251, 146, 60, 0.10), transparent 28%),
                    linear-gradient(180deg, rgba(15, 23, 42, 0.78), rgba(15, 23, 42, 0.88));
                box-shadow: 0 20px 42px rgba(2, 6, 23, 0.24);
                text-align: center;
            }}
            .overview-chart-single-kicker {{
                font-size: 11px;
                font-weight: 900;
                letter-spacing: 0.18em;
                text-transform: uppercase;
                color: #7dd3fc;
            }}
            .overview-chart-single-date {{
                margin-top: 10px;
                font-size: clamp(1.4rem, 2vw, 1.9rem);
                line-height: 1;
                font-weight: 900;
                color: #f8fafc;
            }}
            .overview-chart-single-subtitle {{
                margin-top: 10px;
                font-size: 13px;
                line-height: 1.5;
                color: #94a3b8;
            }}
            .overview-chart-single-grid {{
                display: grid;
                grid-template-columns: repeat(2, minmax(0, 1fr));
                gap: 12px;
                margin-top: 18px;
            }}
            .overview-chart-single-stat {{
                padding: 14px 16px;
                border-radius: 18px;
                text-align: left;
                border: 1px solid rgba(148, 163, 184, 0.12);
            }}
            .overview-chart-single-stat.is-posts {{
                background: rgba(251, 146, 60, 0.10);
                border-color: rgba(251, 146, 60, 0.22);
            }}
            .overview-chart-single-stat.is-creators {{
                background: rgba(192, 132, 252, 0.10);
                border-color: rgba(192, 132, 252, 0.22);
            }}
            .overview-chart-single-stat-label {{
                font-size: 11px;
                font-weight: 800;
                letter-spacing: 0.12em;
                text-transform: uppercase;
                color: #cbd5e1;
            }}
            .overview-chart-single-stat-value {{
                margin-top: 8px;
                font-size: clamp(1.2rem, 1.8vw, 1.55rem);
                line-height: 1;
                font-weight: 900;
                color: #f8fafc;
            }}
            .overview-chart-single-note {{
                margin-top: 16px;
                font-size: 12px;
                line-height: 1.55;
                color: #7f93ad;
            }}
            .overview-chart-empty {{
                position: absolute;
                inset: 12px;
                display: flex;
                align-items: center;
                justify-content: center;
                text-align: center;
                border-radius: 16px;
                border: 1px dashed rgba(148, 163, 184, 0.16);
                color: #94a3b8;
                font-size: 13px;
                font-weight: 700;
                background: rgba(15, 23, 42, 0.28);
                z-index: 1;
            }}
            .overview-chart-tooltip {{
                position: absolute;
                width: min(252px, calc(100% - 32px));
                padding: 13px 14px;
                border-radius: 18px;
                border: 1px solid rgba(148, 163, 184, 0.14);
                background: rgba(11, 18, 32, 0.96);
                box-shadow: 0 22px 42px rgba(2, 6, 23, 0.36);
                backdrop-filter: blur(14px);
                pointer-events: none;
                z-index: 6;
            }}
            .overview-chart-tooltip-title {{
                font-size: 13px;
                font-weight: 900;
                color: #f8fafc;
            }}
            .overview-chart-tooltip-row {{
                display: flex;
                align-items: center;
                justify-content: space-between;
                gap: 12px;
                margin-top: 8px;
                font-size: 12px;
                color: #cbd5e1;
            }}
            .overview-chart-tooltip-row strong {{
                color: #f8fafc;
                font-weight: 900;
            }}
            .overview-chart-series-label-text {{
                font-size: 12px;
                font-weight: 900;
                letter-spacing: 0.02em;
            }}
            .overview-chart-area {{
                animation: overviewChartFadeIn 0.45s ease both;
            }}
            .overview-chart-series {{
                animation: overviewChartFadeIn 0.72s ease both;
            }}
            .overview-chart-point {{
                transition: transform 0.16s ease;
            }}
            .overview-chart-point:hover {{
                transform: scale(1.08);
            }}
            .posts-board {{
                background: transparent;
            }}
            .posts-page-head {{
                display: flex;
                align-items: flex-start;
                justify-content: space-between;
                gap: 14px;
            }}
            .posts-page-kicker {{
                font-size: 13px;
                font-weight: 900;
                letter-spacing: 0.08em;
                color: #f8fafc;
            }}
            .posts-page-title {{
                margin-top: 8px;
                font-size: clamp(1.6rem, 2.8vw, 2.2rem);
                line-height: 1.04;
                font-weight: 900;
                letter-spacing: -0.04em;
                color: #f8fafc;
            }}
            .posts-page-subtitle {{
                margin-top: 6px;
                font-size: 13px;
                color: #94a3b8;
            }}
            .posts-counter-pill {{
                min-width: 138px;
                padding: 11px 13px;
                border-radius: 14px;
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
                margin-top: 6px;
                font-size: 18px;
                font-weight: 900;
                color: #f8fafc;
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
                border-radius: 22px;
                overflow-x: auto;
                overflow-y: visible;
                border: 1px solid rgba(148, 163, 184, 0.12);
                background: rgba(20, 28, 45, 0.78);
                position: relative;
                scrollbar-width: thin;
                scrollbar-color: rgba(148, 163, 184, 0.2) transparent;
            }}
            .posts-sheet-summary-grid::-webkit-scrollbar {{
                height: 6px;
            }}
            .posts-sheet-summary-grid::-webkit-scrollbar-track {{
                background: transparent;
            }}
            .posts-sheet-summary-grid::-webkit-scrollbar-thumb {{
                background: rgba(148, 163, 184, 0.2);
                border-radius: 10px;
            }}
            .posts-sheet-summary-grid::-webkit-scrollbar-thumb:hover {{
                background: rgba(148, 163, 184, 0.35);
            }}

            .posts-sheet-list-table {{
                min-width: 1400px;
                position: relative;
                padding-bottom: 120px;
            }}
            .posts-sheet-list-head {{
                display: grid;
                grid-template-columns: minmax(250px, 2.15fr) minmax(170px, 0.95fr) minmax(150px, 0.84fr) minmax(95px, 0.58fr) minmax(110px, 0.72fr) minmax(110px, 0.72fr) minmax(110px, 0.72fr) minmax(100px, 0.7fr) minmax(100px, 0.7fr) minmax(120px, 0.82fr) minmax(138px, 0.9fr);
                gap: 12px;
                align-items: center;
                padding: 12px 16px;
                background: rgba(51, 65, 85, 0.9);
                color: #cbd5e1;
                font-size: 11px;
                font-weight: 800;
                letter-spacing: 0.1em;
                text-transform: uppercase;
            }}
            .posts-sheet-list-row {{
                width: 100%;
                display: grid;
                grid-template-columns: minmax(250px, 2.15fr) minmax(170px, 0.95fr) minmax(150px, 0.84fr) minmax(95px, 0.58fr) minmax(110px, 0.72fr) minmax(110px, 0.72fr) minmax(110px, 0.72fr) minmax(100px, 0.7fr) minmax(100px, 0.7fr) minmax(120px, 0.82fr) minmax(138px, 0.9fr);
                gap: 12px;
                align-items: center;
                padding: 12px 16px;
                text-align: left;
                background: transparent;
                border: none;
                border-top: 1px solid rgba(148, 163, 184, 0.12);
                transition: background 0.18s ease;
                cursor: pointer;
            }}
            .posts-sheet-list-row:hover {{
                background: rgba(148, 163, 184, 0.06);
            }}
            .posts-sheet-list-row.is-active {{
                background: rgba(37, 99, 235, 0.08);
            }}
            .posts-sheet-list-cell {{
                min-width: 0;
            }}
            .posts-sheet-list-head-metric,
            .posts-sheet-list-cell-metric {{
                text-align: right;
            }}
            .posts-sheet-list-head-action,
            .posts-sheet-list-actions {{
                text-align: right;
            }}
            .posts-sheet-list-activity {{
                display: grid;
                gap: 3px;
            }}
            .posts-sheet-list-title {{
                font-size: 14px;
                font-weight: 900;
                color: #f8fafc;
                line-height: 1.25;
            }}
            .posts-sheet-list-sub {{
                font-size: 12px;
                color: #cbd5e1;
            }}
            .posts-sheet-list-campaign {{
                display: grid;
                gap: 5px;
            }}
            .posts-sheet-list-campaign-main {{
                display: inline-flex;
                align-items: center;
                gap: 8px;
                font-size: 13px;
                font-weight: 800;
                color: #f8fafc;
                min-width: 0;
            }}
            .posts-sheet-list-campaign-main span {{
                overflow: hidden;
                text-overflow: ellipsis;
                white-space: nowrap;
            }}
            .posts-sheet-list-campaign-main i {{
                color: #cbd5e1;
                font-size: 12px;
                opacity: 0.9;
                flex-shrink: 0;
            }}
            .posts-sheet-list-campaign-sub {{
                display: inline-flex;
                align-items: center;
                gap: 7px;
                font-size: 12px;
                color: #facc15;
                min-width: 0;
            }}
            .posts-sheet-list-campaign-sub span {{
                overflow: hidden;
                text-overflow: ellipsis;
                white-space: nowrap;
            }}
            .posts-sheet-list-campaign-sub i {{
                font-size: 11px;
                flex-shrink: 0;
            }}
            .posts-sheet-list-brand-empty {{
                font-size: 13px;
                font-weight: 700;
                color: #94a3b8;
            }}
            .posts-sheet-actions-menu {{
                position: relative;
                display: inline-flex;
                justify-content: flex-end;
                width: 100%;
                z-index: 12;
            }}
            .posts-sheet-actions-toggle {{
                display: inline-flex;
                align-items: center;
                justify-content: center;
                gap: 8px;
                padding: 10px 14px;
                border-radius: 14px;
                border: 1px solid rgba(148, 163, 184, 0.16);
                background: rgba(30, 41, 59, 0.72);
                color: #f8fafc;
                font-size: 14px;
                font-weight: 800;
                transition: all 0.18s ease;
            }}
            .posts-sheet-actions-toggle:hover {{
                background: rgba(51, 65, 85, 0.82);
            }}
            .posts-sheet-actions-toggle:focus,
            .posts-sheet-actions-toggle:focus-visible {{
                outline: none;
                border-color: rgba(56, 189, 248, 0.42);
                box-shadow: 0 0 0 2px rgba(56, 189, 248, 0.22);
            }}
            .posts-sheet-actions-toggle i {{
                font-size: 12px;
                color: #cbd5e1;
            }}
            .posts-sheet-actions-dropdown {{
                position: absolute;
                top: -12px;
                right: 100%;
                min-width: 188px;
                display: grid;
                gap: 4px;
                padding: 8px;
                border-radius: 16px;
                border: 1px solid rgba(148, 163, 184, 0.16);
                background: rgba(15, 23, 42, 0.98);
                box-shadow: 0 18px 48px rgba(15, 23, 42, 0.34);
                z-index: 40;
            }}
            .posts-sheet-actions-item {{
                display: inline-flex;
                align-items: center;
                gap: 10px;
                width: 100%;
                padding: 11px 12px;
                border-radius: 12px;
                border: 0;
                background: transparent;
                color: #e2e8f0;
                font-size: 13px;
                font-weight: 700;
                text-align: left;
                transition: background 0.18s ease, color 0.18s ease;
            }}
            .posts-sheet-actions-item:hover {{
                background: rgba(51, 65, 85, 0.78);
                color: #ffffff;
            }}
            .posts-sheet-actions-item i {{
                width: 14px;
                color: #93c5fd;
                flex-shrink: 0;
            }}
            .posts-sheet-list-pill {{
                display: inline-flex;
                align-items: center;
                justify-content: center;
                padding: 6px 10px;
                border-radius: 9px;
                font-size: 11px;
                font-weight: 800;
                white-space: nowrap;
                border: 1px solid rgba(148, 163, 184, 0.12);
                background: rgba(30, 41, 59, 0.72);
                color: #e2e8f0;
                max-width: 100%;
                overflow: hidden;
                text-overflow: ellipsis;
            }}
            .posts-sheet-list-pill-type {{
                background: rgba(20, 83, 45, 0.24);
                border-color: rgba(52, 211, 153, 0.18);
                color: #a7f3d0;
            }}
            .posts-row-status-ready {{
                background: rgba(37, 99, 235, 0.16);
                border-color: rgba(96, 165, 250, 0.2);
                color: #93c5fd;
            }}
            .posts-row-status-empty {{
                background: rgba(71, 85, 105, 0.28);
                border-color: rgba(148, 163, 184, 0.14);
                color: #cbd5e1;
            }}
            .posts-row-status-error {{
                background: rgba(127, 29, 29, 0.2);
                border-color: rgba(248, 113, 113, 0.18);
                color: #fca5a5;
            }}
            .posts-sheet-list-pill-platform {{
                background: rgba(51, 65, 85, 0.82);
                color: #e2e8f0;
            }}
            .posts-sheet-list-cell-metric {{
                font-size: 13px;
                font-weight: 800;
                color: #f8fafc;
                white-space: nowrap;
            }}
            .posts-sheet-list-cell-metric-strong {{
                color: #fde68a;
            }}
            .posts-sheet-list-error {{
                display: flex;
                align-items: flex-start;
                gap: 6px;
                font-size: 11px;
                color: #fca5a5;
                line-height: 1.4;
            }}
            .posts-master-view {{
                display: grid;
                gap: 16px;
            }}
            .posts-master-view.hidden {{
                display: none;
            }}
            .posts-detail-view {{
                display: grid;
                gap: 16px;
            }}
            .posts-detail-view.hidden {{
                display: none;
            }}
            .posts-detail-topbar {{
                display: flex;
                align-items: center;
                justify-content: flex-start;
            }}
            .schedule-track-list-shell {{
                scrollbar-width: thin;
                scrollbar-color: rgba(100, 116, 139, 0.75) transparent;
            }}
            .schedule-track-list-shell::-webkit-scrollbar {{
                height: 8px;
            }}
            .schedule-track-list-shell::-webkit-scrollbar-thumb {{
                background: rgba(100, 116, 139, 0.75);
                border-radius: 999px;
            }}
            .schedule-track-list-shell::-webkit-scrollbar-track {{
                background: transparent;
            }}
            .schedule-track-list-table {{
                display: grid;
                min-width: 1100px;
            }}
            .schedule-track-list-head,
            .schedule-track-list-row {{
                display: grid;
                grid-template-columns: minmax(260px, 1.25fr) minmax(260px, 1.1fr) 160px 180px minmax(210px, 0.95fr);
                gap: 18px;
                align-items: center;
            }}
            .schedule-track-list-head {{
                padding: 0 16px 12px;
                border-bottom: 1px solid rgba(148, 163, 184, 0.14);
            }}
            .schedule-track-list-head-cell {{
                font-size: 12px;
                font-weight: 900;
                letter-spacing: 0.18em;
                text-transform: uppercase;
                color: #cbd5e1;
            }}
            .schedule-track-list-row {{
                width: 100%;
                text-align: left;
                border: 0;
                background: transparent;
                font: inherit;
                padding: 16px;
                border-top: 1px solid rgba(148, 163, 184, 0.08);
                transition: background 0.18s ease, border-color 0.18s ease, box-shadow 0.18s ease;
            }}
            .schedule-track-list-row:first-of-type {{
                border-top: 0;
            }}
            .schedule-track-list-row:hover {{
                background: rgba(30, 41, 59, 0.52);
            }}
            .schedule-track-list-row.is-active {{
                background: rgba(6, 182, 212, 0.12);
                box-shadow: inset 0 0 0 1px rgba(34, 211, 238, 0.32);
            }}
            .schedule-track-list-cell {{
                min-width: 0;
                font-size: 13px;
                color: #e2e8f0;
            }}
            .schedule-track-list-activity {{
                display: grid;
                gap: 5px;
            }}
            .schedule-track-list-title {{
                font-size: 16px;
                font-weight: 900;
                color: #f8fafc;
                line-height: 1.3;
                word-break: break-word;
            }}
            .schedule-track-list-sub {{
                font-size: 12px;
                color: #94a3b8;
                line-height: 1.5;
                word-break: break-all;
            }}
            .schedule-track-list-main {{
                font-size: 13px;
                font-weight: 800;
                color: #e2e8f0;
                line-height: 1.5;
            }}
            .schedule-track-status-pill {{
                display: inline-flex;
                align-items: center;
                justify-content: center;
                min-width: 110px;
                padding: 8px 12px;
                border-radius: 999px;
                font-size: 12px;
                font-weight: 900;
                border: 1px solid rgba(148, 163, 184, 0.14);
                background: rgba(51, 65, 85, 0.72);
                color: #e2e8f0;
            }}
            .schedule-track-status-pill.is-success {{
                background: rgba(16, 185, 129, 0.14);
                border-color: rgba(52, 211, 153, 0.24);
                color: #a7f3d0;
            }}
            .schedule-track-status-pill.is-running {{
                background: rgba(14, 165, 233, 0.16);
                border-color: rgba(56, 189, 248, 0.24);
                color: #bae6fd;
            }}
            .schedule-track-status-pill.is-error {{
                background: rgba(244, 63, 94, 0.16);
                border-color: rgba(251, 113, 133, 0.26);
                color: #fecdd3;
            }}
            .schedule-track-status-pill.is-stopped {{
                background: rgba(245, 158, 11, 0.16);
                border-color: rgba(251, 191, 36, 0.26);
                color: #fde68a;
            }}
            .schedule-track-status-pill.is-idle {{
                background: rgba(51, 65, 85, 0.82);
                border-color: rgba(148, 163, 184, 0.16);
                color: #e2e8f0;
            }}
            @media (max-width: 960px) {{
                .posts-sheet-list-head {{
                    display: grid;
                }}
                .posts-sheet-list-table {{
                    min-width: 1320px;
                }}
                .schedule-track-list-table {{
                    min-width: 980px;
                }}
                .posts-detail-summary-grid {{
                    grid-template-columns: repeat(2, minmax(0, 1fr));
                }}
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
                margin-top: 6px;
                font-size: 18px;
                font-weight: 900;
                color: #f8fafc;
            }}
            .posts-tab-panel-sub {{
                margin-top: 4px;
                font-size: 12px;
                color: #94a3b8;
            }}
            .posts-detail-summary-shell {{
                display: grid;
                gap: 10px;
                padding: 12px 14px;
                border-radius: 16px;
                border: 1px solid rgba(148, 163, 184, 0.12);
                background: linear-gradient(180deg, rgba(23, 31, 48, 0.9), rgba(28, 37, 58, 0.88));
            }}
            .posts-detail-summary-meta {{
                display: flex;
                flex-wrap: wrap;
                gap: 10px;
            }}
            .posts-detail-summary-chip {{
                display: inline-flex;
                align-items: center;
                gap: 6px;
                padding: 6px 9px;
                border-radius: 999px;
                border: 1px solid rgba(148, 163, 184, 0.14);
                background: rgba(15, 23, 42, 0.72);
                color: #e2e8f0;
                font-size: 11px;
                font-weight: 800;
            }}
            .posts-detail-summary-chip i {{
                color: #cbd5e1;
                font-size: 12px;
            }}
            .posts-detail-summary-grid {{
                display: grid;
                grid-template-columns: repeat(6, minmax(0, 1fr));
                gap: 8px;
            }}
            .posts-detail-summary-card {{
                padding: 9px 11px;
                border-radius: 12px;
                border: 1px solid rgba(148, 163, 184, 0.12);
                background: rgba(15, 23, 42, 0.58);
            }}
            .posts-detail-summary-label {{
                font-size: 11px;
                font-weight: 800;
                letter-spacing: 0.16em;
                text-transform: uppercase;
                color: #64748b;
            }}
            .posts-detail-summary-value {{
                margin-top: 6px;
                font-size: 18px;
                font-weight: 900;
                color: #f8fafc;
                line-height: 1;
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
            .posts-columns-wrap {{
                position: relative;
            }}
            .posts-columns-popover {{
                position: absolute;
                top: calc(100% + 10px);
                right: 0;
                width: min(330px, 86vw);
                padding: 14px 14px 12px;
                border-radius: 14px;
                background: #021640;
                border: 1px solid rgba(96, 165, 250, 0.24);
                box-shadow: 0 16px 36px rgba(2, 6, 23, 0.55);
                z-index: 40;
            }}
            .posts-columns-head {{
                display: flex;
                align-items: center;
                justify-content: space-between;
                gap: 8px;
                padding-bottom: 12px;
                border-bottom: 1px solid rgba(148, 163, 184, 0.2);
            }}
            .posts-columns-title {{
                font-size: 18px;
                font-weight: 900;
                color: #f8fafc;
            }}
            .posts-columns-all {{
                border: 0;
                background: transparent;
                color: #f8fafc;
                font-size: 16px;
                font-weight: 800;
                border-radius: 8px;
                padding: 2px 6px;
                cursor: pointer;
            }}
            .posts-columns-all:hover {{
                color: #bae6fd;
            }}
            .posts-columns-list {{
                max-height: 320px;
                overflow: auto;
                display: flex;
                flex-direction: column;
                gap: 4px;
                margin-top: 12px;
                padding-right: 2px;
            }}
            .posts-columns-item {{
                width: 100%;
                border: 0;
                background: transparent;
                display: flex;
                align-items: center;
                justify-content: space-between;
                gap: 8px;
                color: #f8fafc;
                font-size: 15px;
                font-weight: 700;
                padding: 10px 4px;
                border-radius: 8px;
                cursor: pointer;
            }}
            .posts-columns-item span {{
                font-size: 15px;
                line-height: 1.1;
            }}
            .posts-columns-item i {{
                font-size: 18px;
                color: #f8fafc;
            }}
            .posts-columns-item:hover {{
                background: rgba(148, 163, 184, 0.1);
            }}
            .posts-columns-item.is-hidden {{
                color: #9fb0cc;
            }}
            .posts-columns-item.is-hidden i {{
                color: #9fb0cc;
            }}
            .posts-mini-campaign-feedback {{
                font-size: 12px;
                font-weight: 700;
                line-height: 1.4;
                padding-left: 2px;
            }}
            .posts-mini-campaign-feedback.is-success {{
                color: #6ee7b7;
            }}
            .posts-mini-campaign-feedback.is-warning {{
                color: #fcd34d;
            }}
            .posts-mini-campaign-feedback.is-error {{
                color: #fca5a5;
            }}
            .posts-mini-campaign-feedback.is-info {{
                color: #93c5fd;
            }}
            .posts-toolbar-btn {{
                display: inline-flex;
                align-items: center;
                gap: 8px;
                padding: 9px 12px;
                border-radius: 12px;
                background: rgba(30, 41, 59, 0.72);
                border: 1px solid rgba(148, 163, 184, 0.16);
                color: #e2e8f0;
                font-size: 13px;
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
                gap: 10px;
                width: 100%;
                max-width: 500px;
                background: rgba(30, 41, 59, 0.72);
                border: 1px solid rgba(148, 163, 184, 0.16);
                border-radius: 12px;
                padding: 10px 13px;
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
            .campaign-sheet-list {{
                display: flex;
                flex-direction: column;
                gap: 14px;
            }}
            .campaign-sheet-row {{
                position: relative;
                overflow: visible;
                transition: border-color 0.18s ease, box-shadow 0.18s ease, transform 0.18s ease;
            }}
            .campaign-sheet-row:focus-within {{
                z-index: 16;
                border-color: rgba(34, 211, 238, 0.42);
                box-shadow: 0 14px 28px rgba(15, 23, 42, 0.22);
                transform: translateY(-1px);
            }}
            .campaign-sheet-row-inner {{
                display: grid;
                grid-template-columns: minmax(0, 1fr) minmax(320px, 430px);
                gap: 16px;
                align-items: center;
            }}
            .campaign-sheet-meta {{
                min-width: 0;
            }}
            .campaign-inline-form {{
                width: 100%;
                display: grid;
                gap: 8px;
            }}
            .campaign-inline-grid {{
                display: grid;
                grid-template-columns: minmax(0, 1fr) 112px;
                gap: 10px;
                align-items: center;
            }}
            .campaign-inline-note {{
                min-height: 16px;
                font-size: 11px;
                color: #94a3b8;
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
            .settings-layout {{
                display: grid;
                grid-template-columns: minmax(220px, 260px) minmax(0, 1fr);
                gap: 20px;
                align-items: start;
            }}
            .settings-nav-shell,
            .settings-pane-shell {{
                border-radius: 24px;
                border: 1px solid rgba(148, 163, 184, 0.12);
                background: linear-gradient(180deg, rgba(15, 23, 42, 0.78), rgba(20, 28, 45, 0.82));
            }}
            .settings-nav-shell {{
                padding: 16px;
            }}
            .settings-nav-title {{
                margin-bottom: 12px;
                font-size: 12px;
                font-weight: 900;
                letter-spacing: 0.22em;
                text-transform: uppercase;
                color: #64748b;
            }}
            .settings-nav-list {{
                display: grid;
                gap: 10px;
            }}
            .settings-nav-item {{
                width: 100%;
                display: flex;
                align-items: flex-start;
                gap: 12px;
                padding: 14px 12px;
                border-radius: 18px;
                border: 1px solid transparent;
                background: transparent;
                color: #e2e8f0;
                text-align: left;
                transition: all 0.18s ease;
            }}
            .settings-nav-item:hover {{
                background: rgba(51, 65, 85, 0.4);
                border-color: rgba(148, 163, 184, 0.12);
            }}
            .settings-nav-item.is-active {{
                background: rgba(71, 85, 105, 0.38);
                border-color: rgba(148, 163, 184, 0.16);
            }}
            .settings-nav-item-icon {{
                width: 34px;
                height: 34px;
                border-radius: 12px;
                display: inline-flex;
                align-items: center;
                justify-content: center;
                background: rgba(15, 23, 42, 0.86);
                border: 1px solid rgba(148, 163, 184, 0.1);
                color: #cbd5e1;
                flex: 0 0 auto;
            }}
            .settings-nav-item-copy {{
                display: grid;
                gap: 3px;
                min-width: 0;
            }}
            .settings-nav-item-copy strong {{
                font-size: 15px;
                font-weight: 900;
                color: #f8fafc;
            }}
            .settings-nav-item-copy span {{
                font-size: 13px;
                color: #94a3b8;
                line-height: 1.45;
            }}
            .settings-content-shell {{
                min-width: 0;
            }}
            .settings-tab-pane.hidden {{
                display: none !important;
            }}
            .settings-pane-shell {{
                padding: 24px;
            }}
            .settings-pane-title {{
                font-size: 18px;
                font-weight: 900;
                color: #f8fafc;
            }}
            .settings-pane-sub {{
                margin-top: 8px;
                font-size: 14px;
                color: #94a3b8;
                line-height: 1.6;
            }}
            .settings-notify-list {{
                display: grid;
                gap: 18px;
                margin-top: 24px;
            }}
            .settings-toggle-row {{
                display: flex;
                align-items: center;
                justify-content: space-between;
                gap: 18px;
                padding-bottom: 18px;
                border-bottom: 1px solid rgba(148, 163, 184, 0.12);
            }}
            .settings-toggle-row:last-child {{
                border-bottom: 0;
                padding-bottom: 0;
            }}
            .settings-toggle-copy {{
                display: grid;
                gap: 4px;
                min-width: 0;
            }}
            .settings-toggle-copy strong {{
                font-size: 15px;
                font-weight: 900;
                color: #f8fafc;
            }}
            .settings-toggle-copy span {{
                font-size: 14px;
                color: #94a3b8;
            }}
            .settings-toggle-switch {{
                position: relative;
                display: inline-flex;
                flex: 0 0 auto;
            }}
            .settings-toggle-switch input {{
                position: absolute;
                opacity: 0;
                pointer-events: none;
            }}
            .settings-toggle-slider {{
                width: 46px;
                height: 26px;
                border-radius: 999px;
                background: rgba(51, 65, 85, 0.9);
                border: 1px solid rgba(148, 163, 184, 0.18);
                position: relative;
                transition: background 0.18s ease, border-color 0.18s ease;
            }}
            .settings-toggle-slider::after {{
                content: "";
                position: absolute;
                top: 2px;
                left: 2px;
                width: 20px;
                height: 20px;
                border-radius: 999px;
                background: #ffffff;
                box-shadow: 0 2px 8px rgba(15, 23, 42, 0.28);
                transition: transform 0.18s ease;
            }}
            .settings-toggle-switch input:checked + .settings-toggle-slider {{
                background: rgba(37, 99, 235, 0.9);
                border-color: rgba(96, 165, 250, 0.42);
            }}
            .settings-toggle-switch input:checked + .settings-toggle-slider::after {{
                transform: translateX(20px);
            }}
            .settings-pane-actions {{
                display: flex;
                align-items: center;
                justify-content: space-between;
                gap: 14px;
                margin-top: 28px;
                padding-top: 18px;
                border-top: 1px solid rgba(148, 163, 184, 0.12);
            }}
            .settings-save-btn {{
                display: inline-flex;
                align-items: center;
                gap: 10px;
                padding: 12px 16px;
                border-radius: 14px;
                background: rgba(248, 250, 252, 0.96);
                color: #0f172a;
                font-size: 14px;
                font-weight: 900;
            }}
            .settings-inline-feedback {{
                font-size: 13px;
                font-weight: 700;
                color: #93c5fd;
            }}
            .settings-inline-feedback.is-success {{
                color: #6ee7b7;
            }}
            .settings-inline-feedback.is-error {{
                color: #fca5a5;
            }}
            .settings-inline-feedback.is-warning {{
                color: #fcd34d;
            }}
            .settings-empty-note {{
                padding: 18px;
                border-radius: 18px;
                border: 1px dashed rgba(148, 163, 184, 0.16);
                color: #94a3b8;
                font-size: 14px;
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
            .post-content-wrap {{
                min-width: 0;
                max-width: 100%;
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
                font-variant-numeric: tabular-nums;
                font-feature-settings: "tnum" 1, "lnum" 1;
                font-kerning: none;
            }}
            .posts-cell-metric-strong {{
                color: #dbeafe;
            }}
            .posts-table thead th[data-post-col="view"],
            .posts-table thead th[data-post-col="reaction"],
            .posts-table thead th[data-post-col="share"],
            .posts-table thead th[data-post-col="comment"],
            .posts-table thead th[data-post-col="buzz"] {{
                text-align: right;
                padding-right: 16px;
                font-variant-numeric: tabular-nums;
                font-feature-settings: "tnum" 1, "lnum" 1;
                font-kerning: none;
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
                display: block;
                color: #f8fafc;
                font-weight: 800;
                line-height: 1.45;
                text-decoration: none;
                white-space: normal;
                overflow-wrap: anywhere;
                word-break: break-word;
            }}
            .post-title-link:hover {{
                color: #93c5fd;
            }}
            .post-content-meta {{
                display: block;
                margin-top: 8px;
                font-size: 12px;
                color: #64748b;
                white-space: normal;
                overflow-wrap: anywhere;
                word-break: break-word;
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
                display: inline-flex;
                align-items: center;
                gap: 8px;
                font-size: 15px;
                font-weight: 800;
                color: #dbeafe;
            }}
            .posts-campaign-sub {{
                display: inline-flex;
                align-items: center;
                gap: 7px;
                margin-top: 5px;
                font-size: 12px;
                color: #94a3b8;
            }}
            .posts-campaign-main i {{
                color: #cbd5e1;
                font-size: 12px;
                opacity: 0.9;
            }}
            .posts-campaign-sub i {{
                color: #facc15;
                font-size: 11px;
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
                overflow: hidden;
                transition: padding 0.22s ease, border-radius 0.22s ease;
            }}
            .sidebar-brand {{
                display: flex;
                align-items: center;
                justify-content: space-between;
                gap: 12px;
                margin-bottom: 22px;
            }}
            .sidebar-brand-copy {{
                min-width: 0;
            }}
            .sidebar-brand-title {{
                font-size: 28px;
                font-weight: 900;
                line-height: 1;
                color: #f8fafc;
            }}
            .sidebar-brand-actions {{
                display: inline-flex;
                align-items: center;
                gap: 10px;
                flex-shrink: 0;
            }}
            .sidebar-collapse-btn {{
                width: 42px;
                height: 42px;
                border-radius: 14px;
                display: inline-flex;
                align-items: center;
                justify-content: center;
                background: rgba(15, 23, 42, 0.82);
                border: 1px solid rgba(148, 163, 184, 0.16);
                color: #e2e8f0;
                transition: all 0.2s ease;
            }}
            .sidebar-collapse-btn:hover {{
                transform: translateY(-1px);
                border-color: rgba(148, 163, 184, 0.22);
                background: rgba(30, 41, 59, 0.9);
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
            .sidebar-link-label {{
                min-width: 0;
                overflow: hidden;
                text-overflow: ellipsis;
                white-space: nowrap;
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
            .dashboard-shell.is-sidebar-collapsed {{
                grid-template-columns: 92px minmax(0, 1fr);
            }}
            .dashboard-shell.is-sidebar-collapsed .dashboard-sidebar {{
                padding: 18px 10px;
                border-radius: 24px;
            }}
            .dashboard-shell.is-sidebar-collapsed .sidebar-brand {{
                flex-direction: column;
                align-items: center;
                justify-content: flex-start;
                gap: 10px;
            }}
            .dashboard-shell.is-sidebar-collapsed .sidebar-brand-copy,
            .dashboard-shell.is-sidebar-collapsed .sidebar-link-label {{
                display: none;
            }}
            .dashboard-shell.is-sidebar-collapsed .sidebar-brand-actions {{
                flex-direction: column;
                gap: 10px;
            }}
            .dashboard-shell.is-sidebar-collapsed .sidebar-nav {{
                align-items: center;
            }}
            .dashboard-shell.is-sidebar-collapsed .sidebar-link {{
                justify-content: center;
                gap: 0;
                padding: 12px 10px;
                width: 100%;
            }}
            .dashboard-shell.is-sidebar-collapsed .sidebar-link-icon {{
                margin: 0;
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
            html[data-theme="light"] body {{
                background: linear-gradient(180deg, #f5f7fb, #fcfdff) !important;
                color: #0f172a !important;
            }}
            html[data-theme="light"] .dashboard-sidebar {{
                background:
                    radial-gradient(circle at top, rgba(148, 163, 184, 0.08), transparent 34%),
                    linear-gradient(180deg, rgba(255, 255, 255, 0.98), rgba(247, 249, 252, 0.96));
                border-color: rgba(148, 163, 184, 0.18);
                box-shadow: 0 18px 40px rgba(148, 163, 184, 0.14);
            }}
            html[data-theme="light"] .dashboard-main {{
                background:
                    radial-gradient(circle at top left, rgba(148, 163, 184, 0.06), transparent 24%),
                    linear-gradient(180deg, rgba(255, 255, 255, 0.98), rgba(249, 250, 252, 0.98));
                border-color: rgba(148, 163, 184, 0.18);
                box-shadow: 0 22px 50px rgba(148, 163, 184, 0.14);
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
            html[data-theme="light"] .posts-sheet-list-title,
            html[data-theme="light"] .posts-sheet-list-campaign-main,
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
            html[data-theme="light"] .posts-sheet-list-cell-metric,
            html[data-theme="light"] .posts-campaign-main,
            html[data-theme="light"] .sidebar-schedule-name,
            html[data-theme="light"] .sidebar-schedule-link,
            html[data-theme="light"] .theme-toggle-label,
            html[data-theme="light"] .theme-toggle-icon,
            html[data-theme="light"] .utility-user-email,
            html[data-theme="light"] .utility-logout,
            html[data-theme="light"] .schedule-track-list-title,
            html[data-theme="light"] .schedule-track-list-main {{
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
            html[data-theme="light"] .posts-sheet-list-sub,
            html[data-theme="light"] .posts-sheet-list-campaign-sub,
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
            html[data-theme="light"] .text-slate-500,
            html[data-theme="light"] .schedule-track-list-sub {{
                color: #64748b !important;
            }}
            html[data-theme="light"] .dashboard-utilitybar,
            html[data-theme="light"] .sidebar-collapse-btn,
            html[data-theme="light"] .theme-toggle-btn,
            html[data-theme="light"] .utility-user-pill,
            html[data-theme="light"] .utility-logout,
            html[data-theme="light"] .overview-stat-card,
            html[data-theme="light"] .overview-campaign-card,
            html[data-theme="light"] .overview-chart-card,
            html[data-theme="light"] .posts-counter-pill,
            html[data-theme="light"] .employee-summary-pill,
            html[data-theme="light"] .posts-empty-card,
            html[data-theme="light"] .posts-toolbar,
            html[data-theme="light"] .posts-table-shell,
            html[data-theme="light"] .employee-form-card,
            html[data-theme="light"] .link-history-shell,
            html[data-theme="light"] .link-history-table-shell,
            html[data-theme="light"] .link-history-stat,
            html[data-theme="light"] .schedule-history-item,
            html[data-theme="light"] .schedule-history-empty,
            html[data-theme="light"] .bg-black\\/20,
            html[data-theme="light"] .bg-slate-950\\/40,
            html[data-theme="light"] .bg-slate-900\\/60,
            html[data-theme="light"] .bg-slate-900\\/70,
            html[data-theme="light"] .bg-slate-900\\/55 {{
                background: rgba(255, 255, 255, 0.92) !important;
                border-color: rgba(148, 163, 184, 0.18) !important;
                box-shadow: 0 10px 24px rgba(148, 163, 184, 0.1);
            }}
            html[data-theme="light"] .metric-posts {{
                background: rgba(99, 102, 241, 0.1);
            }}
            html[data-theme="light"] .metric-views {{
                background: rgba(148, 163, 184, 0.16);
            }}
            html[data-theme="light"] .metric-creators {{
                background: rgba(168, 85, 247, 0.1);
            }}
            html[data-theme="light"] .overview-action-pill,
            html[data-theme="light"] .overview-chart-segment,
            html[data-theme="light"] .overview-time-filter-card,
            html[data-theme="light"] .overview-chart-control-wrap,
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
            html[data-theme="light"] .posts-sheet-list-pill {{
                background: rgba(248, 250, 252, 0.92) !important;
                border-color: rgba(148, 163, 184, 0.18) !important;
                color: #0f172a;
            }}
            html[data-theme="light"] .overview-filter-trigger {{
                background: rgba(255, 255, 255, 0.95);
                color: #334155;
                border-color: rgba(148, 163, 184, 0.26);
            }}
            html[data-theme="light"] .overview-time-filter-title,
            html[data-theme="light"] .overview-time-custom-label,
            html[data-theme="light"] .overview-chart-custom-range label {{
                color: #334155;
            }}
            html[data-theme="light"] .overview-chart-date-input {{
                background: #ffffff;
                color: #0f172a;
                border-color: rgba(148, 163, 184, 0.24);
            }}
            html[data-theme="light"] .date-shell input[type="date"]::-webkit-calendar-picker-indicator,
            html[data-theme="light"] .overview-chart-date-input::-webkit-calendar-picker-indicator {{
                opacity: 0.9;
                filter: none;
            }}
            html[data-theme="light"] .posts-sheet-summary-grid {{
                background: rgba(255, 255, 255, 0.92) !important;
                border-color: rgba(148, 163, 184, 0.18) !important;
                box-shadow: 0 10px 24px rgba(148, 163, 184, 0.1);
            }}
            html[data-theme="light"] .posts-columns-popover {{
                background: linear-gradient(180deg, rgba(255, 255, 255, 0.99), rgba(248, 250, 252, 0.99));
                border-color: rgba(148, 163, 184, 0.22);
            }}
            html[data-theme="light"] .posts-columns-title {{
                color: #0f172a;
            }}
            html[data-theme="light"] .posts-columns-item {{
                color: #0f172a;
            }}
            html[data-theme="light"] .posts-sheet-list-head {{
                background: rgba(241, 245, 249, 0.96);
                color: #475569;
            }}
            html[data-theme="light"] .schedule-track-list-head {{
                background: rgba(241, 245, 249, 0.96);
            }}
            html[data-theme="light"] .schedule-track-list-head-cell {{
                color: #475569;
            }}
            html[data-theme="light"] .posts-sheet-list-row:hover {{
                background: rgba(226, 232, 240, 0.56);
            }}
            html[data-theme="light"] .schedule-track-list-row:hover {{
                background: rgba(226, 232, 240, 0.56);
            }}
            html[data-theme="light"] .posts-sheet-list-row.is-active {{
                background: rgba(59, 130, 246, 0.08);
            }}
            html[data-theme="light"] .posts-sheet-actions-toggle {{
                background: rgba(248, 250, 252, 0.92);
                border-color: rgba(148, 163, 184, 0.18);
                color: #0f172a;
            }}
            html[data-theme="light"] .posts-sheet-actions-toggle:focus,
            html[data-theme="light"] .posts-sheet-actions-toggle:focus-visible {{
                outline: none;
                border-color: rgba(59, 130, 246, 0.34);
                box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.16);
            }}
            html[data-theme="light"] .posts-sheet-actions-toggle i {{
                color: #475569;
            }}
            html[data-theme="light"] .posts-sheet-actions-dropdown {{
                background: rgba(255, 255, 255, 0.98);
                border-color: rgba(148, 163, 184, 0.22);
                box-shadow: 0 18px 48px rgba(148, 163, 184, 0.24);
            }}
            html[data-theme="light"] .posts-sheet-actions-item {{
                color: #0f172a;
            }}
            html[data-theme="light"] .posts-sheet-actions-item:hover {{
                background: rgba(226, 232, 240, 0.82);
                color: #0f172a;
            }}
            html[data-theme="light"] .schedule-track-list-row.is-active {{
                background: rgba(59, 130, 246, 0.08);
                box-shadow: inset 0 0 0 1px rgba(59, 130, 246, 0.2);
            }}
            html[data-theme="light"] #theo-doi-lan-chay .bg-black\\/20 {{
                background:
                    radial-gradient(circle at top left, rgba(59, 130, 246, 0.05), transparent 26%),
                    linear-gradient(180deg, rgba(255, 255, 255, 0.98), rgba(248, 250, 252, 0.98)) !important;
                border-color: rgba(148, 163, 184, 0.16) !important;
                box-shadow: 0 14px 34px rgba(148, 163, 184, 0.12);
            }}
            html[data-theme="light"] #theo-doi-lan-chay .bg-slate-950\\/35,
            html[data-theme="light"] #theo-doi-lan-chay .bg-slate-900\\/35,
            html[data-theme="light"] #theo-doi-lan-chay .bg-slate-900\\/40,
            html[data-theme="light"] #theo-doi-lan-chay .bg-slate-900\\/60 {{
                background:
                    linear-gradient(180deg, rgba(255, 255, 255, 0.98), rgba(244, 248, 252, 0.98)) !important;
                border-color: rgba(148, 163, 184, 0.18) !important;
                box-shadow: 0 10px 22px rgba(148, 163, 184, 0.08);
            }}
            html[data-theme="light"] #theo-doi-lan-chay .text-cyan-200 {{
                color: #0f766e !important;
            }}
            html[data-theme="light"] #theo-doi-lan-chay .schedule-track-status-pill {{
                background: rgba(226, 232, 240, 0.9);
                border-color: rgba(148, 163, 184, 0.18);
                color: #334155;
            }}
            html[data-theme="light"] #theo-doi-lan-chay .schedule-track-status-pill.is-success {{
                background: rgba(16, 185, 129, 0.12);
                border-color: rgba(16, 185, 129, 0.22);
                color: #047857;
            }}
            html[data-theme="light"] #theo-doi-lan-chay .schedule-track-status-pill.is-running {{
                background: rgba(14, 165, 233, 0.12);
                border-color: rgba(14, 165, 233, 0.2);
                color: #0369a1;
            }}
            html[data-theme="light"] #theo-doi-lan-chay .schedule-track-status-pill.is-error {{
                background: rgba(244, 63, 94, 0.1);
                border-color: rgba(244, 63, 94, 0.16);
                color: #be123c;
            }}
            html[data-theme="light"] #theo-doi-lan-chay .schedule-track-status-pill.is-stopped {{
                background: rgba(245, 158, 11, 0.12);
                border-color: rgba(245, 158, 11, 0.18);
                color: #b45309;
            }}
            html[data-theme="light"] #theo-doi-lan-chay .schedule-track-status-pill.is-idle {{
                background: rgba(226, 232, 240, 0.88);
                border-color: rgba(148, 163, 184, 0.18);
                color: #475569;
            }}
            html[data-theme="light"] #theo-doi-lan-chay .schedule-history-item {{
                background:
                    linear-gradient(180deg, rgba(255, 255, 255, 0.98), rgba(247, 250, 252, 0.98));
                border-color: rgba(148, 163, 184, 0.18);
                box-shadow: 0 8px 18px rgba(148, 163, 184, 0.08);
            }}
            html[data-theme="light"] #theo-doi-lan-chay .schedule-history-empty {{
                background: rgba(248, 250, 252, 0.96);
                border-color: rgba(148, 163, 184, 0.18);
            }}
            html[data-theme="light"] #chien-dich .posts-toolbar {{
                background:
                    radial-gradient(circle at top left, rgba(59, 130, 246, 0.05), transparent 24%),
                    linear-gradient(180deg, rgba(255, 255, 255, 0.98), rgba(248, 250, 252, 0.98)) !important;
                border-color: rgba(148, 163, 184, 0.18) !important;
                box-shadow: 0 12px 28px rgba(148, 163, 184, 0.1);
            }}
            html[data-theme="light"] #chien-dich .bg-slate-950\\/35,
            html[data-theme="light"] #chien-dich .campaign-sheet-row {{
                background:
                    linear-gradient(180deg, rgba(255, 255, 255, 0.98), rgba(245, 248, 252, 0.98)) !important;
                border-color: rgba(148, 163, 184, 0.18) !important;
                box-shadow: 0 10px 22px rgba(148, 163, 184, 0.08);
            }}
            html[data-theme="light"] #chien-dich .text-slate-100,
            html[data-theme="light"] #chien-dich .text-slate-200,
            html[data-theme="light"] #chien-dich .text-slate-300,
            html[data-theme="light"] #chien-dich .text-slate-400,
            html[data-theme="light"] #chien-dich .text-slate-500 {{
                color: #475569 !important;
            }}
            html[data-theme="light"] #chien-dich .posts-page-title,
            html[data-theme="light"] #chien-dich .text-base.font-black,
            html[data-theme="light"] #chien-dich .posts-counter-value {{
                color: #0f172a !important;
            }}
            html[data-theme="light"] #chien-dich [data-create-campaign-form] input,
            html[data-theme="light"] #chien-dich .campaign-inline-form select {{
                background: rgba(255, 255, 255, 0.98) !important;
                border-color: rgba(148, 163, 184, 0.2) !important;
                color: #0f172a !important;
                box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.75);
            }}
            html[data-theme="light"] #chien-dich [data-create-campaign-form] input::placeholder {{
                color: #94a3b8 !important;
            }}
            html[data-theme="light"] #chien-dich [data-create-campaign-form] button,
            html[data-theme="light"] #chien-dich .campaign-inline-form button {{
                background: linear-gradient(180deg, #334155, #475569) !important;
                border: 1px solid rgba(51, 65, 85, 0.22);
                color: #ffffff !important;
                box-shadow: 0 10px 20px rgba(148, 163, 184, 0.14);
            }}
            html[data-theme="light"] #chien-dich [data-create-campaign-form] button:hover,
            html[data-theme="light"] #chien-dich .campaign-inline-form button:hover {{
                background: linear-gradient(180deg, #1e293b, #334155) !important;
            }}
            html[data-theme="light"] #chien-dich .posts-chip {{
                background: rgba(248, 250, 252, 0.96) !important;
                border-color: rgba(148, 163, 184, 0.18) !important;
                color: #0f172a !important;
                box-shadow: 0 8px 18px rgba(148, 163, 184, 0.08);
            }}
            html[data-theme="light"] #chien-dich .campaign-sheet-row:focus-within {{
                border-color: rgba(59, 130, 246, 0.24) !important;
                box-shadow: 0 14px 30px rgba(59, 130, 246, 0.08);
            }}
            html[data-theme="light"] #chien-dich .campaign-inline-note {{
                color: #64748b;
            }}
            html[data-theme="light"] #chien-dich .posts-chip.is-active {{
                background: rgba(59, 130, 246, 0.08);
                border-color: rgba(59, 130, 246, 0.16);
                color: #1d4ed8;
                box-shadow: 0 10px 20px rgba(148, 163, 184, 0.08);
            }}
            html[data-theme="light"] #chien-dich .posts-chip.is-active span {{
                color: #475569;
            }}
            html[data-theme="light"] .settings-nav-shell,
            html[data-theme="light"] .settings-pane-shell {{
                background: linear-gradient(180deg, rgba(255, 255, 255, 0.98), rgba(249, 250, 252, 0.98));
                border-color: rgba(148, 163, 184, 0.18);
                box-shadow: 0 10px 24px rgba(148, 163, 184, 0.1);
            }}
            html[data-theme="light"] .settings-nav-item:hover,
            html[data-theme="light"] .settings-nav-item.is-active {{
                background: rgba(226, 232, 240, 0.72);
                border-color: rgba(148, 163, 184, 0.16);
            }}
            html[data-theme="light"] .settings-nav-item-icon,
            html[data-theme="light"] .settings-toggle-slider {{
                background: rgba(241, 245, 249, 0.96);
                border-color: rgba(148, 163, 184, 0.18);
                color: #0f172a;
            }}
            html[data-theme="light"] .settings-nav-item-copy strong,
            html[data-theme="light"] .settings-pane-title,
            html[data-theme="light"] .settings-toggle-copy strong {{
                color: #0f172a;
            }}
            html[data-theme="light"] .settings-nav-item-copy span,
            html[data-theme="light"] .settings-pane-sub,
            html[data-theme="light"] .settings-toggle-copy span,
            html[data-theme="light"] .settings-empty-note {{
                color: #64748b;
            }}
            html[data-theme="light"] .settings-save-btn {{
                background: #0f172a;
                color: #f8fafc;
            }}
            html[data-theme="light"] .sidebar-link {{
                color: #334155;
            }}
            html[data-theme="light"] .sidebar-link:hover {{
                background: rgba(226, 232, 240, 0.8);
                color: #0f172a;
            }}
            html[data-theme="light"] .sidebar-link.is-active {{
                background: linear-gradient(135deg, rgba(226, 232, 240, 0.82), rgba(241, 245, 249, 0.88));
                color: #0f172a;
                box-shadow: inset 0 0 0 1px rgba(148, 163, 184, 0.18);
            }}
            html[data-theme="light"] .overview-chart-frame {{
                background:
                    radial-gradient(circle at 10% 0%, rgba(59, 130, 246, 0.1), transparent 34%),
                    radial-gradient(circle at 86% 5%, rgba(251, 146, 60, 0.08), transparent 28%),
                    linear-gradient(180deg, rgba(255, 255, 255, 0.96), rgba(241, 245, 249, 0.92));
                border-color: rgba(148, 163, 184, 0.18);
            }}
            html[data-theme="light"] .overview-chart-empty {{
                background: rgba(248, 250, 252, 0.78);
                border-color: rgba(148, 163, 184, 0.22);
                color: #64748b;
            }}
            html[data-theme="light"] .overview-chart-legend-item,
            html[data-theme="light"] .overview-chart-tooltip-title,
            html[data-theme="light"] .overview-chart-tooltip-row strong {{
                color: #0f172a;
            }}
            html[data-theme="light"] .overview-chart-tooltip {{
                background: rgba(255, 255, 255, 0.96);
                border-color: rgba(148, 163, 184, 0.22);
                box-shadow: 0 20px 34px rgba(148, 163, 184, 0.18);
            }}
            html[data-theme="light"] .overview-chart-tooltip-row {{
                color: #475569;
            }}
            html[data-theme="light"] .overview-chart-legend-item {{
                background: rgba(248, 250, 252, 0.94);
                border-color: rgba(148, 163, 184, 0.18);
                color: #0f172a;
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
            html[data-theme="light"] .text-slate-300 {{
                color: #0f172a !important;
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
                background: #f8fbff !important;
                color: #0f172a !important;
                border-color: rgba(148, 163, 184, 0.25) !important;
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
            html[data-theme="light"] .flatpickr-current-month .flatpickr-monthDropdown-months {{
                background: #ffffff;
            }}
            html[data-theme="light"] .flatpickr-current-month .flatpickr-monthDropdown-months option {{
                background: #ffffff;
                color: #0f172a;
            }}
            html[data-theme="light"] .system-log-shell {{
                background:
                    radial-gradient(circle at top left, rgba(59, 130, 246, 0.08), transparent 28%),
                    linear-gradient(180deg, rgba(239, 246, 255, 0.98), rgba(226, 232, 240, 0.96));
                border-color: rgba(148, 163, 184, 0.2) !important;
                box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.62);
            }}
            html[data-theme="light"] .system-log-line {{
                border-bottom-color: rgba(148, 163, 184, 0.16);
            }}
            html[data-theme="light"] .system-log-time {{
                color: #2563eb;
            }}
            html[data-theme="light"] .system-log-message {{
                color: #334155;
            }}
            html[data-theme="light"] .system-log-empty {{
                color: #64748b;
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
            @keyframes overviewChartFadeIn {{
                from {{
                    opacity: 0;
                }}
                to {{
                    opacity: 1;
                }}
            }}
            @media (max-width: 1480px) {{
                .overview-stat-grid {{
                    grid-template-columns: repeat(2, minmax(0, 1fr));
                }}
            }}
            @media (max-width: 1180px) {{
                .dashboard-shell {{
                    grid-template-columns: 220px minmax(0, 1fr);
                    min-height: calc(100vh - 16px);
                }}
                .dashboard-sidebar {{
                    position: sticky;
                    top: 8px;
                    min-height: calc(100vh - 16px);
                    padding: 18px 14px;
                }}
                .employee-layout {{
                    grid-template-columns: 1fr;
                }}
                .settings-layout {{
                    grid-template-columns: 1fr;
                }}
                .sidebar-nav {{
                    flex-direction: column;
                    flex-wrap: nowrap;
                }}
                .sidebar-link {{
                    flex: 0 0 auto;
                }}
                .overview-header {{
                    flex-direction: column;
                }}
                .overview-chart-head {{
                    flex-direction: column;
                    padding-right: 0;
                }}
                .overview-head-actions {{
                    width: 100%;
                    justify-content: flex-start;
                }}
                .overview-chart-filter-anchor {{
                    position: absolute;
                    top: 56px;
                    right: 18px;
                    margin-top: 0;
                }}
                .overview-actions {{
                    justify-content: flex-start;
                }}
                .overview-chart-control-wrap {{
                    justify-content: flex-start;
                }}
            }}
            @media (max-width: 768px) {{
                .dashboard-shell {{
                    grid-template-columns: 84px minmax(0, 1fr);
                    gap: 10px;
                }}
                .dashboard-shell:not(.is-sidebar-collapsed) {{
                    grid-template-columns: 188px minmax(0, 1fr);
                }}
                .dashboard-sidebar {{
                    padding: 14px 10px;
                    border-radius: 24px;
                }}
                .sidebar-brand {{
                    margin-bottom: 18px;
                }}
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
                .settings-pane-actions {{
                    flex-direction: column;
                    align-items: stretch;
                }}
                .settings-toggle-row {{
                    align-items: flex-start;
                }}
                .utility-user-email {{
                    max-width: 160px;
                }}
                .posts-page-head,
                .posts-toolbar-row {{
                    flex-direction: column;
                    align-items: stretch;
                }}
                .campaign-sheet-row-inner,
                .campaign-inline-grid {{
                    grid-template-columns: 1fr;
                }}
                .overview-stat-grid,
                .overview-campaign-metrics {{
                    grid-template-columns: 1fr;
                }}
                .overview-chart-single-grid {{
                    grid-template-columns: 1fr;
                }}
                .overview-chart-meta {{
                    align-items: flex-start;
                }}
                .overview-chart-filter-anchor {{
                    position: static;
                    margin-top: 8px;
                }}
                .overview-chart-control-wrap {{
                    width: 100%;
                }}
                .overview-time-filter-card {{
                    width: 100%;
                }}
                .overview-chart-segment {{
                    justify-content: space-between;
                    flex-wrap: wrap;
                }}
                .overview-chart-toggle {{
                    flex: 1 1 88px;
                }}
                .overview-chart-custom-range {{
                    gap: 6px;
                }}
                .overview-chart-custom-row {{
                    grid-template-columns: 1fr;
                    gap: 6px;
                }}
                .overview-chart-custom-range label {{
                    margin-top: 4px;
                }}
                .overview-control-header {{
                    flex-direction: column;
                }}
            }}
        </style>
        <script>
            document.addEventListener("DOMContentLoaded", () => {{
                const initialDashboardSection = {json.dumps(initial_dashboard_section)};
                let sheetUrlInput = document.getElementById("sheet-url-input");
                let sheetNameInput = document.getElementById("sheet-name-input");
                const setSheetForm = document.querySelector("form[action='/set-sheet']");
                const setColumnsForm = document.getElementById("set-columns-form");
                const setSheetSubmitBtn = setSheetForm?.querySelector("button[type='submit']");
                const setColumnsSubmitBtn = setColumnsForm?.querySelector("button[type='submit']");
                let sheetTabsState = document.getElementById("sheet-tabs-state");
                let sheetTabsList = document.getElementById("sheet-tabs-list");
                let sheetNameOptions = document.getElementById("sheet-name-options");
                let colConfigTabBar = document.getElementById("col-config-tab-bar");
                let colConfigApplyNote = document.getElementById("col-config-apply-note");
                let colConfigActiveTabInput = document.getElementById("col-config-active-tab-input");
                let autoFillColumnsBtn = document.getElementById("auto-fill-columns-btn");
                const scheduleForm = document.querySelector("form[action='/set-schedule']");
                const scheduleSheetSearch = document.getElementById("schedule-sheet-search");
                const scheduleSheetDatalist = document.getElementById("schedule-sheet-datalist");
                const scheduleSheetSelect = document.getElementById("schedule-sheet-select");
                const scheduleModeSelect = document.getElementById("schedule-mode-select");
                const scheduleWeekdayShell = document.getElementById("schedule-weekday-shell");
                const scheduleMonthdateShell = document.getElementById("schedule-monthdate-shell");
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
                const scheduleTrackList = document.getElementById("schedule-track-list");
                const scheduleTrackActiveName = document.getElementById("schedule-track-active-name");
                const scheduleTrackCalendarTitle = document.getElementById("schedule-track-calendar-title");
                const scheduleTrackCalendarSubtext = document.getElementById("schedule-track-calendar-subtext");
                const scheduleTrackCalendar = document.getElementById("schedule-track-calendar");
                const scheduleTrackDetailBody = document.getElementById("schedule-track-detail-body");
                const scheduleTrackEmptyState = document.getElementById("schedule-track-empty-state");
                const scheduleTargetSummary = document.getElementById("schedule-target-summary");
                const dashboardShell = document.getElementById("dashboard-shell");
                const sidebarCollapseToggle = document.getElementById("sidebar-collapse-toggle");
                const sidebarCollapseIcon = document.getElementById("sidebar-collapse-icon");
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
                const employeeCancelBtn = document.getElementById("employee-cancel-btn");
                const employeeSaveBtn = document.getElementById("employee-save-btn");
                const employeeFormTitle = document.getElementById("employee-form-title");
                const employeeFormSub = document.getElementById("employee-form-sub");
                const employeeTotalCount = document.getElementById("employee-total-count");
                const employeeVerifiedCount = document.getElementById("employee-verified-count");
                const employeeAdminCount = document.getElementById("employee-admin-count");
                const employeeChipAll = document.getElementById("employee-chip-all");
                const employeeChipPending = document.getElementById("employee-chip-pending");
                const employeeChipVerified = document.getElementById("employee-chip-verified");
                const settingsTabTriggers = Array.from(document.querySelectorAll("[data-settings-tab-trigger]"));
                const settingsTabPanes = Array.from(document.querySelectorAll("[data-settings-tab-pane]"));
                const settingsNotificationFeedback = document.getElementById("settings-notification-feedback");
                const settingsEmailNotifications = document.getElementById("settings-email-notifications");
                const settingsSystemAlerts = document.getElementById("settings-system-alerts");
                const saveNotificationSettingsBtn = document.getElementById("save-notification-settings-btn");
                const activeSheetNameEls = Array.from(document.querySelectorAll("[data-active-sheet-name]"));
                const activeSheetIdEls = Array.from(document.querySelectorAll("[data-active-sheet-id]"));
                const columnDetectedTextEls = Array.from(document.querySelectorAll("[data-column-detected-text]"));
                const columnInputEls = Object.fromEntries(
                    ["date", "air_date", "link", "view", "like", "share", "comment", "buzz", "save"].map((field) => [
                        field,
                        document.querySelector(`[data-column-input="${{field}}"]`),
                    ])
                );
                const columnSourceEls = Object.fromEntries(
                    ["date", "air_date", "link", "view", "like", "share", "comment", "buzz", "save"].map((field) => [
                        field,
                        document.querySelector(`[data-column-source="${{field}}"]`),
                    ])
                );
                const sheetCampaignNameInput = document.getElementById("sheet-campaign-name-input");
                const sheetBrandInput = document.getElementById("sheet-brand-input");
                const sheetCampaignDescriptionInput = document.getElementById("sheet-campaign-description-input");
                const sheetMetadataGate = document.getElementById("sheet-metadata-gate");
                const sheetMetadataPanel = document.getElementById("sheet-metadata-panel");
                let monthPicker = null;
                let endPicker = null;
                let sheetTabsRequestId = 0;
                let sheetTabsDebounce = null;
                const sheetTabsCache = {{}};
                const normalizeSheetUrl = (value) => String(value || "").trim();
                let selectedSheetTabs = new Set();
                let latestSheetTabs = [];
                let scheduleSheetOptionItems = [];
                let employeeUsersState = [];
                let employeeStatusFilter = "all";
                let employeeEditingEmail = "";
                let configLocked = false;
                let pendingSheetMetadataReveal = false;
                let pendingInlineAction = "";

                const refreshSheetTabDomRefs = () => {{
                    sheetUrlInput = document.getElementById("sheet-url-input");
                    sheetNameInput = document.getElementById("sheet-name-input");
                    sheetTabsState = document.getElementById("sheet-tabs-state");
                    sheetTabsList = document.getElementById("sheet-tabs-list");
                    sheetNameOptions = document.getElementById("sheet-name-options");
                    colConfigTabBar = document.getElementById("col-config-tab-bar");
                    colConfigApplyNote = document.getElementById("col-config-apply-note");
                    colConfigActiveTabInput = document.getElementById("col-config-active-tab-input");
                    autoFillColumnsBtn = document.getElementById("auto-fill-columns-btn");
                }};

                const applySidebarCollapsed = (collapsed) => {{
                    const normalizedCollapsed = Boolean(collapsed);
                    if (dashboardShell) {{
                        dashboardShell.classList.toggle("is-sidebar-collapsed", normalizedCollapsed);
                    }}
                    if (sidebarCollapseIcon) {{
                        sidebarCollapseIcon.innerHTML = normalizedCollapsed
                            ? '<i class="fa-solid fa-angles-right"></i>'
                            : '<i class="fa-solid fa-angles-left"></i>';
                    }}
                    if (sidebarCollapseToggle) {{
                        const toggleText = normalizedCollapsed ? "Mở rộng menu" : "Thu gọn menu";
                        sidebarCollapseToggle.setAttribute("title", toggleText);
                        sidebarCollapseToggle.setAttribute("aria-label", toggleText);
                    }}
                }};

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
                try {{
                    applySidebarCollapsed(localStorage.getItem("dashboard_sidebar_collapsed") === "1");
                }} catch (_) {{
                    applySidebarCollapsed(false);
                }}
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
                if (sidebarCollapseToggle) {{
                    sidebarCollapseToggle.addEventListener("click", () => {{
                        const nextCollapsed = !dashboardShell?.classList.contains("is-sidebar-collapsed");
                        applySidebarCollapsed(nextCollapsed);
                        try {{
                            localStorage.setItem("dashboard_sidebar_collapsed", nextCollapsed ? "1" : "0");
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

                const setSettingsNotificationFeedback = (message = "", level = "info") => {{
                    if (!settingsNotificationFeedback) return;
                    if (!message) {{
                        settingsNotificationFeedback.textContent = "";
                        settingsNotificationFeedback.className = "settings-inline-feedback hidden";
                        return;
                    }}
                    const normalized = ["success", "warning", "error", "info"].includes(level) ? level : "info";
                    settingsNotificationFeedback.textContent = message;
                    settingsNotificationFeedback.className = `settings-inline-feedback is-${{normalized}}`;
                }};

                const setActiveSettingsTab = (tabId, options = {{}}) => {{
                    const availableIds = settingsTabPanes
                        .map((pane) => pane.dataset.settingsTabPane || "")
                        .filter(Boolean);
                    const targetId = availableIds.includes(tabId) ? tabId : (availableIds[0] || "notifications");
                    settingsTabTriggers.forEach((trigger) => {{
                        trigger.classList.toggle("is-active", trigger.dataset.settingsTabTrigger === targetId);
                    }});
                    settingsTabPanes.forEach((pane) => {{
                        pane.classList.toggle("hidden", pane.dataset.settingsTabPane !== targetId);
                    }});
                    if (options.persist !== false) {{
                        try {{
                            localStorage.setItem("settings_active_tab", targetId);
                        }} catch (_) {{
                        }}
                    }}
                }};

                settingsTabTriggers.forEach((trigger) => {{
                    trigger.addEventListener("click", () => {{
                        setActiveSettingsTab(trigger.dataset.settingsTabTrigger || "notifications");
                    }});
                }});
                try {{
                    const settingsAliasTab =
                        window.location.pathname === "/nhan-vien"
                            ? "employees"
                            : window.location.pathname === "/chien-dich"
                                ? "campaigns"
                                : "";
                    const savedSettingsTab = localStorage.getItem("settings_active_tab");
                    setActiveSettingsTab(settingsAliasTab || savedSettingsTab || "notifications", {{ persist: false }});
                }} catch (_) {{
                    setActiveSettingsTab(
                        window.location.pathname === "/nhan-vien"
                            ? "employees"
                            : window.location.pathname === "/chien-dich"
                                ? "campaigns"
                                : "notifications",
                        {{ persist: false }}
                    );
                }}
                if (saveNotificationSettingsBtn) {{
                    saveNotificationSettingsBtn.addEventListener("click", async () => {{
                        saveNotificationSettingsBtn.disabled = true;
                        setSettingsNotificationFeedback("Đang lưu cài đặt thông báo...", "info");
                        try {{
                            const response = await fetch("/save-notification-settings", {{
                                method: "POST",
                                headers: {{ "Content-Type": "application/json", "X-Requested-With": "fetch" }},
                                body: JSON.stringify({{
                                    email_notifications: Boolean(settingsEmailNotifications?.checked),
                                    system_alerts: Boolean(settingsSystemAlerts?.checked),
                                }}),
                            }});
                            const data = await response.json();
                            applyStatusState(data);
                            setSettingsNotificationFeedback(
                                data.message || (data.ok ? "Đã lưu cài đặt thông báo." : "Không lưu được cài đặt thông báo."),
                                data.level || (data.ok ? "success" : "error")
                            );
                        }} catch (_) {{
                            setSettingsNotificationFeedback("Không lưu được cài đặt thông báo. Vui lòng thử lại.", "error");
                        }} finally {{
                            saveNotificationSettingsBtn.disabled = false;
                        }}
                    }});
                }}

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

                let colConfigActiveTab = "";
                // Cache of per-tab input values keyed by tab name: {{ tabName: {{ link:"C", like:"F", ... }} }}
                let tabColConfigCache = {{}};
                // Authoritative server-saved per-tab overrides received via polling
                let serverColConfigByTab = {{}};

                // Gather current values of all column config inputs into a plain object
                const readColConfigInputs = () => {{
                    const result = {{}};
                    document.querySelectorAll("[data-column-input]").forEach((el) => {{
                        result[el.dataset.columnInput] = el.value || "";
                    }});
                    // also grab start_row and hidden fields
                    const srEl = document.querySelector("[form='set-columns-form'][name='start_row']");
                    if (srEl) result["start_row"] = srEl.value || "";
                    return result;
                }};

                // Write a cached/server config object back into the form inputs
                const writeColConfigInputs = (cfg) => {{
                    if (!cfg || typeof cfg !== "object") return;
                    document.querySelectorAll("[data-column-input]").forEach((el) => {{
                        const key = el.dataset.columnInput;
                        if (key in cfg) el.value = cfg[key];
                    }});
                    const srEl = document.querySelector("[form='set-columns-form'][name='start_row']");
                    if (srEl && "start_row" in cfg) srEl.value = cfg["start_row"];
                }};

                const AUTO_COLUMN_FIELDS = ["date", "air_date", "link", "view", "like", "share", "comment", "buzz", "save"];
                const fetchAutoColumnConfigForTab = async (tabName) => {{
                    const resolvedTabName = String(tabName || "").trim();
                    if (!resolvedTabName) {{
                        return null;
                    }}
                    try {{
                        const response = await fetch(`/detect-tab-columns?tab_name=${{encodeURIComponent(resolvedTabName)}}`, {{
                            headers: {{"X-Requested-With": "fetch"}},
                            cache: "no-store",
                        }});
                        const data = await response.json();
                        if (!data.ok || !data.detected_inputs) {{
                            return null;
                        }}
                        const normalizedConfig = {{}};
                        AUTO_COLUMN_FIELDS.forEach((field) => {{
                            normalizedConfig[field] = String((data.detected_inputs || {{}})[field] || "").trim();
                        }});
                        if (data.start_row) {{
                            normalizedConfig.start_row = String(data.start_row);
                        }}
                        tabColConfigCache[resolvedTabName] = normalizedConfig;
                        serverColConfigByTab[resolvedTabName] = normalizedConfig;
                        return {{
                            tabName: resolvedTabName,
                            detectedInputs: normalizedConfig,
                            startRow: normalizedConfig.start_row || "",
                        }};
                    }} catch (_) {{
                        return null;
                    }}
                }};

                const refreshStatusNow = async () => {{
                    try {{
                        const response = await fetch("/status", {{
                            headers: {{ "X-Requested-With": "fetch" }},
                            cache: "no-store",
                        }});
                        const data = await response.json();
                        applyStatusState(data);
                        applyScheduleConfigState(data);
                        applyScheduleTrackingState(data);
                        return data;
                    }} catch (_) {{
                        return null;
                    }}
                }};

                document.addEventListener("click", async (event) => {{
                    const autoButton = event.target.closest("#auto-fill-columns-btn");
                    if (!autoButton) return;
                    event.preventDefault();
                    const tabsForAuto = Array.from(selectedSheetTabs);
                    if (!tabsForAuto.length) {{
                        const fallbackTab = String(colConfigActiveTab || sheetNameInput?.value || "").trim();
                        if (fallbackTab) tabsForAuto.push(fallbackTab);
                    }}
                    if (!tabsForAuto.length) {{
                        showNotice("Chưa có tab nào để AUTO. Hãy chọn ít nhất 1 tab.", "warning");
                        return;
                    }}
                    autoButton.disabled = true;
                    autoButton.textContent = "...";
                    void pushRealtimeLog(`Bắt đầu AUTO quét ${{tabsForAuto.length}} tab...`);
                    let scannedTabs = 0;
                    let totalFilledAcrossTabs = 0;
                    let activeDetected = null;
                    let activeStartRow = "";
                    const activeTabForForm = String(colConfigActiveTab || tabsForAuto[0] || "").trim();
                    for (const tabName of tabsForAuto) {{
                        const detected = await fetchAutoColumnConfigForTab(tabName);
                        if (!detected || !detected.detectedInputs) {{
                            void pushRealtimeLog(`[${{tabName}}] AUTO không nhận được cột.`);
                            continue;
                        }}
                        scannedTabs += 1;
                        const filledForTab = AUTO_COLUMN_FIELDS.reduce((sum, field) => sum + (detected.detectedInputs[field] ? 1 : 0), 0);
                        totalFilledAcrossTabs += filledForTab;
                        if (tabName === activeTabForForm) {{
                            activeDetected = detected.detectedInputs;
                            activeStartRow = detected.startRow || "";
                        }}
                        void pushRealtimeLog(`[${{tabName}}] AUTO nhận ${{filledForTab}} cột.`);
                    }}
                    autoButton.disabled = false;
                    autoButton.textContent = "AUTO";
                    if (!scannedTabs) {{
                        void pushRealtimeLog("AUTO chưa nhận được cột nào từ các tab đã chọn.");
                        showNotice("AUTO chưa nhận được cột nào từ các tab đã chọn.", "warning");
                        return;
                    }}
                    if (activeDetected) {{
                        document.querySelectorAll("[data-column-input]").forEach((input) => {{
                            const field = input.dataset.columnInput || "";
                            const autoValue = String(activeDetected?.[field] || "").trim();
                            input.dataset.detectedValue = autoValue;
                            if (!autoValue) return;
                            input.value = autoValue;
                            input.dataset.manualValue = "";
                            input.dispatchEvent(new Event("input", {{ bubbles: true }}));
                        }});
                        const srEl = document.querySelector("[form='set-columns-form'][name='start_row']");
                        if (srEl && activeStartRow && document.activeElement !== srEl) {{
                            srEl.value = activeStartRow;
                        }}
                    }}
                    if (activeTabForForm) {{
                        tabColConfigCache[activeTabForForm] = readColConfigInputs();
                    }}
                    renderColConfigTabBar(activeTabForForm);
                    void pushRealtimeLog(`AUTO đã quét ${{scannedTabs}}/${{tabsForAuto.length}} tab, nhận tổng ${{totalFilledAcrossTabs}} cột.`);
                    showNotice(`AUTO đã quét ${{scannedTabs}}/${{tabsForAuto.length}} tab, nhận tổng ${{totalFilledAcrossTabs}} cột.`, "success");
                    await refreshStatusNow();
                }});

                const updateResetTabBtnVisibility = () => {{
                    refreshSheetTabDomRefs();
                }};

                const renderColConfigTabBar = (switchToTab) => {{
                    refreshSheetTabDomRefs();
                    if (!colConfigTabBar) return;
                    const tabs = Array.from(selectedSheetTabs);
                    if (tabs.length <= 1) {{
                        colConfigTabBar.classList.add("hidden");
                        colConfigTabBar.classList.remove("flex");
                        const singleTab = tabs.length === 1 ? tabs[0] : "";
                        if (colConfigApplyNote) colConfigApplyNote.textContent = singleTab
                            ? `Cấu hình riêng cho tab: ${{singleTab}}`
                            : "Cấu hình đã được áp dụng cho sheet đã chọn.";
                        if (colConfigActiveTabInput) colConfigActiveTabInput.value = singleTab;
                        // Load this tab's saved config if we just switched to it
                        if (singleTab && singleTab !== colConfigActiveTab) {{
                            if (colConfigActiveTab) tabColConfigCache[colConfigActiveTab] = readColConfigInputs();
                            const cached = tabColConfigCache[singleTab];
                            const serverSaved = serverColConfigByTab[singleTab];
                            if (cached) writeColConfigInputs(cached);
                            else if (serverSaved) writeColConfigInputs(serverSaved);
                            else {{
                                fetch(`/detect-tab-columns?tab_name=${{encodeURIComponent(singleTab)}}`, {{
                                    headers: {{"X-Requested-With": "fetch"}},
                                    cache: "no-store",
                                }}).then(r => r.json()).then(data => {{
                                    if (data.ok && data.detected_inputs && colConfigActiveTab === singleTab) {{
                                        writeColConfigInputs(data.detected_inputs);
                                        if (data.start_row) {{
                                            const srEl = document.querySelector("[form='set-columns-form'][name='start_row']");
                                            if (srEl && document.activeElement !== srEl) srEl.value = data.start_row;
                                        }}
                                    }}
                                }}).catch(() => {{}});
                            }}
                        }}
                        colConfigActiveTab = singleTab;
                        updateResetTabBtnVisibility();
                        return;
                    }}
                    colConfigTabBar.classList.remove("hidden");
                    colConfigTabBar.classList.add("flex");

                    const prevTab = colConfigActiveTab;
                    // Determine which tab to show
                    let nextTab = switchToTab || colConfigActiveTab;
                    if (!nextTab || !tabs.includes(nextTab)) nextTab = tabs[0];

                    // Save current inputs to cache for the previous tab (before switching)
                    if (prevTab && prevTab !== nextTab) {{
                        tabColConfigCache[prevTab] = readColConfigInputs();
                    }}

                    // Switch to new tab
                    colConfigActiveTab = nextTab;
                    if (colConfigActiveTabInput) colConfigActiveTabInput.value = nextTab;
                    updateResetTabBtnVisibility();

                    // Load inputs for the new active tab from cache, then fall back to server data
                    if (prevTab !== nextTab) {{
                        const cached = tabColConfigCache[nextTab];
                        const serverSaved = serverColConfigByTab[nextTab];
                        if (cached) {{
                            writeColConfigInputs(cached);
                        }} else if (serverSaved) {{
                            writeColConfigInputs(serverSaved);
                        }} else {{
                            // No saved config for this tab — fetch auto-detected columns from server
                            fetch(`/detect-tab-columns?tab_name=${{encodeURIComponent(nextTab)}}`, {{
                                headers: {{"X-Requested-With": "fetch"}},
                                cache: "no-store",
                            }}).then(r => r.json()).then(data => {{
                                if (data.ok && data.detected_inputs && colConfigActiveTab === nextTab) {{
                                    writeColConfigInputs(data.detected_inputs);
                                    if (data.start_row) {{
                                        const srEl = document.querySelector("[form='set-columns-form'][name='start_row']");
                                        if (srEl && document.activeElement !== srEl) srEl.value = data.start_row;
                                    }}
                                }}
                            }}).catch(() => {{}});
                        }}
                    }}

                    colConfigTabBar.innerHTML = tabs.map((t) => {{
                        const isAct = t === colConfigActiveTab;
                        const baseStyle = isAct
                            ? "background:rgba(14,165,233,0.14);color:#7dd3fc;border-bottom:2px solid #38bdf8;"
                            : "background:transparent;color:#64748b;border-bottom:2px solid transparent;";
                        return `<div style="display:inline-flex;align-items:center;gap:6px;padding:0 2px;${{baseStyle}}">
                            <button type="button" data-col-tab="${{t}}" style="padding:6px 2px 8px 10px;font-size:12px;font-weight:800;border:none;cursor:pointer;transition:.15s;background:transparent;color:inherit;">${{t}}</button>
                            <button type="button" data-col-tab-remove="${{t}}" title="Bỏ chọn tab này" aria-label="Bỏ chọn tab ${{t}}" style="padding:0 8px 2px 0;border:none;background:transparent;color:${{isAct ? "#38bdf8" : "#94a3b8"}};font-size:14px;font-weight:900;line-height:1;cursor:pointer;">×</button>
                        </div>`;
                    }}).join("");
                    if (colConfigApplyNote) colConfigApplyNote.textContent = `Cấu hình riêng cho tab: ${{colConfigActiveTab}}`;
                    colConfigTabBar.querySelectorAll("[data-col-tab]").forEach((btn) => {{
                        btn.addEventListener("click", () => {{
                            renderColConfigTabBar(btn.dataset.colTab || "");
                        }});
                    }});
                    colConfigTabBar.querySelectorAll("[data-col-tab-remove]").forEach((btn) => {{
                        btn.addEventListener("click", (event) => {{
                            event.preventDefault();
                            event.stopPropagation();
                            const tabToRemove = btn.dataset.colTabRemove || "";
                            if (!tabToRemove) return;
                            selectedSheetTabs.delete(tabToRemove);
                            if (sheetNameInput && (sheetNameInput.value || "").trim() === tabToRemove) {{
                                const nextTab = Array.from(selectedSheetTabs)[0] || "";
                                sheetNameInput.value = nextTab;
                                if (nextTab) {{
                                    sessionStorage.setItem("draft_sheet_name", nextTab);
                                }} else {{
                                    sessionStorage.removeItem("draft_sheet_name");
                                }}
                            }}
                            renderSheetTabs(latestSheetTabs);
                        }});
                    }});
                }};

                const renderSheetTabs = (tabs) => {{
                    refreshSheetTabDomRefs();
                    latestSheetTabs = Array.isArray(tabs) ? tabs : [];
                    if (sheetNameOptions) {{
                        sheetNameOptions.innerHTML = latestSheetTabs
                            .map((tab) => `<option value="${{tab.title}}"></option>`)
                            .join("");
                    }}
                    if (!sheetTabsList) return;
                    if (!latestSheetTabs.length) {{
                        sheetTabsList.innerHTML = "";
                        sheetTabsList.classList.add("hidden");
                        return;
                    }}
                    const count = selectedSheetTabs.size;
                    const countBadge = count > 0
                        ? `<span style="margin-left:6px;background:rgba(16,185,129,0.18);color:#6ee7b7;border:1px solid rgba(52,211,153,0.22);border-radius:999px;padding:1px 10px;font-size:12px;font-weight:800;">${{count}} tab</span>`
                        : "";
                    const selectAllBtn = `<button type="button" id="sheet-tabs-select-all" style="font-size:12px;color:#94a3b8;background:none;border:none;cursor:pointer;padding:0;font-weight:700;">Chọn tất cả</button>`;
                    const clearBtn = count > 0 ? `<button type="button" id="sheet-tabs-clear" style="font-size:12px;color:#f87171;background:none;border:none;cursor:pointer;padding:0 0 0 10px;font-weight:700;">Bỏ chọn</button>` : "";
                    const header = `<div style="display:flex;align-items:center;gap:4px;margin-bottom:8px;flex-wrap:wrap;">
                        <span style="font-size:12px;color:#64748b;font-weight:700;text-transform:uppercase;letter-spacing:.1em;">Tab được quét${{countBadge}}</span>
                        <span style="flex:1"></span>${{selectAllBtn}}${{clearBtn}}
                    </div>`;
                    sheetTabsList.innerHTML = header + latestSheetTabs
                        .map((tab) => {{
                            const isActive = selectedSheetTabs.has(tab.title);
                            const activeClass = isActive ? " is-active" : "";
                            const checkIcon = isActive ? `<i class="fa-solid fa-check" style="font-size:10px;"></i>` : "";
                            return `<button type="button" class="sheet-tab-chip${{activeClass}}" data-sheet-tab="${{tab.title}}" data-sheet-gid="${{tab.gid || "0"}}">${{checkIcon}}${{tab.title}}</button>`;
                        }})
                        .join("");
                    sheetTabsList.classList.remove("hidden");
                    sheetTabsList.querySelectorAll("[data-sheet-tab]").forEach((button) => {{
                        button.addEventListener("click", () => {{
                            const tabTitle = button.dataset.sheetTab || "";
                            if (selectedSheetTabs.has(tabTitle)) {{
                                selectedSheetTabs.delete(tabTitle);
                            }} else {{
                                selectedSheetTabs.add(tabTitle);
                            }}
                            // Keep sheetNameInput synced to last clicked tab (for Nhập Sheet form)
                            if (sheetNameInput) {{
                                sheetNameInput.value = tabTitle;
                                sessionStorage.setItem("draft_sheet_name", tabTitle);
                            }}
                            renderSheetTabs(latestSheetTabs);
                        }});
                    }});
                    const selectAllBtnEl = document.getElementById("sheet-tabs-select-all");
                    if (selectAllBtnEl) {{
                        selectAllBtnEl.addEventListener("click", () => {{
                            latestSheetTabs.forEach((tab) => selectedSheetTabs.add(tab.title));
                            if (sheetNameInput && latestSheetTabs.length) {{
                                sheetNameInput.value = latestSheetTabs[0].title;
                                sessionStorage.setItem("draft_sheet_name", latestSheetTabs[0].title);
                            }}
                            renderSheetTabs(latestSheetTabs);
                        }});
                    }}
                    const clearBtnEl = document.getElementById("sheet-tabs-clear");
                    if (clearBtnEl) {{
                        clearBtnEl.addEventListener("click", () => {{
                            selectedSheetTabs.clear();
                            renderSheetTabs(latestSheetTabs);
                        }});
                    }}
                    renderColConfigTabBar();
                }};

                const snapshotScheduleSheetOptions = () => {{
                    if (!scheduleSheetSelect) return;
                    scheduleSheetOptionItems = Array.from(scheduleSheetSelect.options).map((option) => ({{
                        value: option.value || "",
                        label: option.textContent || "",
                        sheetId: option.dataset.sheetId || "",
                        sheetName: option.dataset.sheetName || "",
                        gid: option.dataset.sheetGid || "",
                    }})).filter((item) => item.value && item.label && item.label !== "Chưa có sheet nào để chọn");
                }};

                const getScheduleSheetMatches = (rawValue = "") => {{
                    const normalizedFilter = String(rawValue || "").trim().toLowerCase();
                    if (!normalizedFilter) {{
                        return [...scheduleSheetOptionItems];
                    }}
                    return scheduleSheetOptionItems.filter((item) => String(item.label || "").toLowerCase().includes(normalizedFilter));
                }};

                const resolveScheduleSheetItem = (rawValue = "") => {{
                    const normalizedValue = String(rawValue || "").trim().toLowerCase();
                    if (!normalizedValue) return null;
                    const exactMatch = scheduleSheetOptionItems.find((item) => {{
                        const label = String(item.label || "").trim().toLowerCase();
                        const value = String(item.value || "").trim().toLowerCase();
                        return label === normalizedValue || value === normalizedValue;
                    }});
                    if (exactMatch) return exactMatch;
                    const matches = getScheduleSheetMatches(rawValue);
                    return matches.length === 1 ? matches[0] : null;
                }};

                const parseScheduleBindingValue = (rawValue = "") => {{
                    const [sheetId, ...nameParts] = String(rawValue || "").split("::");
                    return {{
                        sheetId: String(sheetId || "").trim(),
                        sheetName: String(nameParts.join("::") || "").trim(),
                    }};
                }};

                const extractSheetIdFromInput = (rawValue = "") => {{
                    const value = String(rawValue || "").trim();
                    if (!value) return "";
                    const directMatch = value.match(/^[a-zA-Z0-9-_]{{20,}}$/);
                    if (directMatch) return directMatch[0];
                    const urlMatch = value.match(/\/spreadsheets\/(?:u\/\d+\/)?d\/([a-zA-Z0-9-_]+)/i);
                    if (urlMatch && urlMatch[1]) return urlMatch[1];
                    try {{
                        const parsed = new URL(value);
                        const queryId = parsed.searchParams.get("id") || parsed.searchParams.get("key") || "";
                        return /^[a-zA-Z0-9-_]{{20,}}$/.test(queryId) ? queryId : "";
                    }} catch (_) {{
                        return "";
                    }}
                }};

                const extractSheetGidFromInput = (rawValue = "") => {{
                    const value = String(rawValue || "").trim();
                    if (!value) return "";
                    const hashMatch = value.match(/[#&?]gid=(\d+)/i);
                    if (hashMatch && hashMatch[1]) return hashMatch[1];
                    try {{
                        const parsed = new URL(value);
                        const gid = parsed.searchParams.get("gid") || "";
                        return /^\d+$/.test(gid) ? gid : "";
                    }} catch (_) {{
                        return "";
                    }}
                }};

                const resolveScheduleSheetFromSheetInput = async (rawValue = "") => {{
                    const normalizedInput = String(rawValue || "").trim();
                    if (!normalizedInput) return null;
                    const sheetId = extractSheetIdFromInput(normalizedInput);
                    if (!sheetId) return null;
                    const gid = extractSheetGidFromInput(normalizedInput);
                    const sameSheetItems = scheduleSheetOptionItems.filter((item) => {{
                        const parsed = parseScheduleBindingValue(item.value);
                        return parsed.sheetId === sheetId;
                    }});
                    if (!sameSheetItems.length) return null;
                    if (sameSheetItems.length === 1 && !gid) return sameSheetItems[0];
                    if (gid) {{
                        const itemByGid = sameSheetItems.find((item) => String(item.gid || "") === gid);
                        if (itemByGid) return itemByGid;
                        try {{
                            const response = await fetch(`/sheet-tabs?sheet_url=${{encodeURIComponent(normalizedInput)}}`, {{
                                headers: {{ "X-Requested-With": "fetch" }},
                                cache: "no-store",
                            }});
                            if (response.ok) {{
                                const data = await response.json();
                                if (data?.ok && Array.isArray(data.tabs)) {{
                                    const tab = data.tabs.find((entry) => String(entry?.gid || "") === gid);
                                    if (tab?.title) {{
                                        const tabName = String(tab.title).trim().toLowerCase();
                                        const itemByName = sameSheetItems.find((item) => String(item.sheetName || "").trim().toLowerCase() === tabName);
                                        if (itemByName) return itemByName;
                                    }}
                                }}
                            }}
                        }} catch (_) {{}}
                    }}
                    return sameSheetItems[0];
                }};

                const renderScheduleSheetOptions = (filterValue = "", preferredValue = "") => {{
                    if (scheduleSheetDatalist) {{
                        const visibleItems = getScheduleSheetMatches(filterValue);
                        scheduleSheetDatalist.innerHTML = "";
                        visibleItems.forEach((item) => {{
                            const option = document.createElement("option");
                            option.value = item.label;
                            scheduleSheetDatalist.appendChild(option);
                        }});
                    }}
                    if (!scheduleSheetSelect) return;
                    if (!scheduleSheetOptionItems.length) {{
                        scheduleSheetSelect.value = "";
                        scheduleSheetSelect.disabled = true;
                        if (scheduleSheetSearch && document.activeElement !== scheduleSheetSearch) {{
                            scheduleSheetSearch.value = "";
                        }}
                        return;
                    }}
                    scheduleSheetSelect.disabled = false;
                    const preferredItem = scheduleSheetOptionItems.find((item) => item.value === String(preferredValue || "").trim());
                    const resolvedItem = preferredItem || resolveScheduleSheetItem(filterValue);
                    if (resolvedItem) {{
                        scheduleSheetSelect.value = resolvedItem.value;
                        if (scheduleSheetSearch && document.activeElement !== scheduleSheetSearch) {{
                            scheduleSheetSearch.value = resolvedItem.label;
                        }}
                        return;
                    }}
                    if (!String(filterValue || "").trim()) {{
                        const fallbackItem = scheduleSheetOptionItems.find((item) => item.value === String(scheduleSheetSelect.value || "").trim())
                            || scheduleSheetOptionItems[0];
                        if (fallbackItem) {{
                            scheduleSheetSelect.value = fallbackItem.value;
                            if (scheduleSheetSearch && document.activeElement !== scheduleSheetSearch) {{
                                scheduleSheetSearch.value = fallbackItem.label;
                            }}
                        }}
                    }} else {{
                        scheduleSheetSelect.value = "";
                    }}
                }};

                const commitScheduleSheetSearch = async (showToast = false) => {{
                    if (!scheduleSheetSearch || !scheduleSheetSelect) return null;
                    const rawInput = scheduleSheetSearch.value || "";
                    let resolvedItem = resolveScheduleSheetItem(rawInput);
                    if (!resolvedItem) {{
                        resolvedItem = await resolveScheduleSheetFromSheetInput(rawInput);
                    }}
                    if (!resolvedItem) {{
                        scheduleSheetSelect.value = "";
                        return null;
                    }}
                    scheduleSheetSearch.value = resolvedItem.label;
                    scheduleSheetSelect.value = resolvedItem.value;
                    return await selectScheduleEntry(resolvedItem.value, "form", showToast);
                }};

                const rebuildScheduleSheetSelectOptions = (preferredValue = "") => {{
                    if (!scheduleSheetSelect) return;
                    scheduleSheetSelect.innerHTML = "";
                    if (!scheduleSheetOptionItems.length) {{
                        const option = document.createElement("option");
                        option.value = "";
                        option.textContent = "Chưa có sheet nào để chọn";
                        scheduleSheetSelect.appendChild(option);
                        scheduleSheetSelect.disabled = true;
                        return;
                    }}
                    scheduleSheetOptionItems.forEach((item) => {{
                        const parsed = parseScheduleBindingValue(item.value);
                        const normalizedSheetId = String(item.sheetId || parsed.sheetId || "").trim();
                        const normalizedSheetName = String(item.sheetName || parsed.sheetName || "").trim();
                        const normalizedGid = String(item.gid || "").trim();
                        item.sheetId = normalizedSheetId;
                        item.sheetName = normalizedSheetName;
                        item.gid = normalizedGid;
                        const option = document.createElement("option");
                        option.value = item.value;
                        option.textContent = item.label;
                        option.dataset.sheetId = normalizedSheetId;
                        option.dataset.sheetName = normalizedSheetName;
                        option.dataset.sheetGid = normalizedGid;
                        scheduleSheetSelect.appendChild(option);
                    }});
                    scheduleSheetSelect.disabled = false;
                    if (preferredValue) {{
                        scheduleSheetSelect.value = preferredValue;
                    }}
                }};

                const selectScheduleEntry = async (key, context = "form", showToast = false) => {{
                    const normalizedKey = String(key || "").trim();
                    if (!normalizedKey && context !== "tracking") return null;
                    try {{
                        const response = await fetch("/set-active-schedule", {{
                            method: "POST",
                            headers: {{ "Content-Type": "application/json", "X-Requested-With": "fetch" }},
                            body: JSON.stringify({{ key: normalizedKey, context }}),
                        }});
                        const data = await response.json();
                        applyStatusState(data);
                        applyScheduleConfigState(data);
                        applyScheduleTrackingState(data);
                        if (showToast) {{
                            showNotice(
                                data.message || (data.ok ? "Đã chọn sheet lịch." : "Không chọn được sheet lịch."),
                                data.level || (data.ok ? "success" : "error")
                            );
                        }}
                        return data;
                    }} catch (_) {{
                        if (showToast) {{
                            showNotice("Không chọn được sheet lịch. Vui lòng thử lại.", "error");
                        }}
                        return null;
                    }}
                }};

                snapshotScheduleSheetOptions();
                rebuildScheduleSheetSelectOptions(scheduleSheetSelect?.value || "");
                renderScheduleSheetOptions("", scheduleSheetSelect?.value || "");
                if (scheduleSheetSearch) {{
                    scheduleSheetSearch.addEventListener("input", () => {{
                        renderScheduleSheetOptions(scheduleSheetSearch.value, scheduleSheetSelect?.value || "");
                    }});
                    scheduleSheetSearch.addEventListener("focus", () => {{
                        renderScheduleSheetOptions("", scheduleSheetSelect?.value || "");
                    }});
                    scheduleSheetSearch.addEventListener("click", () => {{
                        renderScheduleSheetOptions("", scheduleSheetSelect?.value || "");
                    }});
                    scheduleSheetSearch.addEventListener("change", async () => {{
                        await commitScheduleSheetSearch(false);
                    }});
                    scheduleSheetSearch.addEventListener("keydown", async (event) => {{
                        if (event.key !== "Enter") return;
                        event.preventDefault();
                        await commitScheduleSheetSearch(false);
                    }});
                }}
                if (scheduleSheetSelect) {{
                    scheduleSheetSelect.addEventListener("change", async () => {{
                        await selectScheduleEntry(scheduleSheetSelect.value, "form", false);
                    }});
                }}

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

                    const cachedTabs = sheetTabsCache[rawValue];
                    if (cachedTabs) {{
                        renderSheetTabs(cachedTabs.tabs);
                        setSheetTabsMessage(cachedTabs.message, "success");
                        return;
                    }}

                    const requestId = ++sheetTabsRequestId;
                    if (!silent) {{
                        setSheetTabsMessage("Đang tải danh sách tab...", "loading");
                        const nowMs = Date.now();
                        if (lastSheetTabsLogKey !== rawValue || nowMs - lastSheetTabsLogAt > 6000) {{
                            lastSheetTabsLogKey = rawValue;
                            lastSheetTabsLogAt = nowMs;
                            void pushRealtimeLog("Đang kiểm tra link sheet để tải danh sách tab...");
                        }}
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
                            const msgStyle = data.quota_error ? "warning" : "error";
                            setSheetTabsMessage(data.message || "Không tải được danh sách tab.", msgStyle);
                            return;
                        }}
                        const tabs = Array.isArray(data.tabs) ? data.tabs : [];
                        sheetTabsCache[rawValue] = {{
                            tabs,
                            message: data.message || `Tìm thấy ${{tabs.length}} tab trong spreadsheet.`,
                        }};
                        renderSheetTabs(tabs);
                        const tabMsgStyle = data.stale ? "warning" : "success";
                        setSheetTabsMessage(data.message || "Đã tải danh sách tab.", tabMsgStyle);
                        if (!silent) {{
                            void pushRealtimeLog(data.message || `Tìm thấy ${{tabs.length}} tab trong spreadsheet.`);
                        }}
                    }} catch (_) {{
                        if (requestId !== sheetTabsRequestId) return;
                        clearSheetTabs();
                        setSheetTabsMessage("Không tải được danh sách tab. Kiểm tra link sheet và quyền truy cập.", "error");
                        if (!silent) {{
                            void pushRealtimeLog("Không tải được danh sách tab. Kiểm tra link sheet và quyền truy cập.");
                        }}
                    }}
                }};

                const scheduleSheetTabsFetch = () => {{
                    if (sheetTabsDebounce) {{
                        clearTimeout(sheetTabsDebounce);
                    }}
                    sheetTabsDebounce = setTimeout(() => {{
                        fetchSheetTabs(sheetUrlInput?.value || "");
                    }}, 800);
                }};

                const bindSheetTabLookupControls = () => {{
                    refreshSheetTabDomRefs();
                    if (sheetUrlInput && sheetUrlInput.dataset.sheetTabsBound !== "1") {{
                        sheetUrlInput.dataset.sheetTabsBound = "1";
                        restoreDraft(sheetUrlInput, "draft_sheet_url");
                        sheetUrlInput.addEventListener("input", scheduleSheetTabsFetch);
                        sheetUrlInput.addEventListener("paste", () => setTimeout(scheduleSheetTabsFetch, 0));
                        sheetUrlInput.addEventListener("change", () => fetchSheetTabs(sheetUrlInput.value, true));
                        sheetUrlInput.addEventListener("blur", () => fetchSheetTabs(sheetUrlInput.value, true));
                    }}
                    if (sheetNameInput && sheetNameInput.dataset.sheetTabsBound !== "1") {{
                        sheetNameInput.dataset.sheetTabsBound = "1";
                        restoreDraft(sheetNameInput, "draft_sheet_name");
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
                    const activeSetSheetForm = document.querySelector("form[action='/set-sheet']");
                    if (activeSetSheetForm && activeSetSheetForm.dataset.inlineBound !== "1") {{
                        activeSetSheetForm.dataset.inlineBound = "1";
                        activeSetSheetForm.addEventListener("submit", async (event) => {{
                            event.preventDefault();
                            await submitSheetFormInline();
                        }});
                    }}
                }};
                window.bindSheetTabLookupControls = bindSheetTabLookupControls;
                bindSheetTabLookupControls();

                const applyConfigLockState = (locked, message = "") => {{
                    configLocked = Boolean(locked);
                    [setSheetSubmitBtn, setColumnsSubmitBtn].forEach((btn) => {{
                        if (!btn) return;
                        btn.disabled = configLocked;
                        btn.classList.toggle("opacity-50", configLocked);
                        btn.classList.toggle("cursor-not-allowed", configLocked);
                        if (configLocked && message) {{
                            btn.setAttribute("title", message);
                        }} else {{
                            btn.removeAttribute("title");
                        }}
                    }});
                }};

                const submitSheetFormInline = async () => {{
                    refreshSheetTabDomRefs();
                    const activeSetSheetForm = document.querySelector("form[action='/set-sheet']");
                    if (!activeSetSheetForm) return null;
                    void pushRealtimeLog("Bắt đầu xử lý nhập/lưu sheet...");
                    if (configLocked) {{
                        const message = "Đang ở trạng thái Đã dừng. Bấm Bắt đầu để mở lại rồi hãy nhập sheet.";
                        showNotice(message, "warning");
                        return {{ ok: false, message }};
                    }}
                    const selectedTabsArr = Array.from(selectedSheetTabs);
                    if (selectedTabsArr.length > 0) {{
                        const tabsByTitle = Object.fromEntries(latestSheetTabs.map((tab) => [tab.title, tab]));
                        const selectedTabsPayload = selectedTabsArr.map((title) => ({{
                            title,
                            gid: tabsByTitle[title]?.gid || "0",
                        }}));
                        try {{
                            const response = await fetch("/save-selected-sheets", {{
                                method: "POST",
                                headers: {{ "Content-Type": "application/json", "X-Requested-With": "fetch" }},
                                cache: "no-store",
                                body: JSON.stringify({{
                                    sheet_url: sheetUrlInput?.value || "",
                                    tabs: selectedTabsPayload,
                                }}),
                            }});
                            const data = await response.json();
                            if (data.ok) {{
                                sessionStorage.removeItem("draft_sheet_url");
                                sessionStorage.removeItem("draft_sheet_name");
                                applyActiveSheetMeta(data, true);
                                applyColumnConfigState(data);
                                applySheetMetadataState(data);
                                applyScheduleConfigState(data);
                                applyScheduleTrackingState(data);
                                if (typeof data.overview_html === "string") {{
                                    replaceOverviewPanelHtml(data.overview_html);
                                }}
                                if (typeof data.posts_html === "string") {{
                                    replacePostsPanelHtml(data.posts_html);
                                }}
                                if (typeof data.campaign_html === "string") {{
                                    replaceCampaignPanelHtml(data.campaign_html);
                                }}
                                if (sheetUrlInput?.value) {{
                                    fetchSheetTabs(sheetUrlInput.value, true);
                                }}
                            }}
                            applyStatusState(data);
	                            showNotice(
	                                data.message || (data.ok ? "Đã lưu các tab sheet." : "Không lưu được các tab sheet."),
	                                data.level || (data.ok ? "success" : "error")
	                            );
	                            await refreshStatusNow();
	                            return data;
                        }} catch (_) {{
                            showNotice("Không lưu được các tab sheet. Vui lòng thử lại.", "error");
                            return null;
                        }}
                    }}
                    const params = new URLSearchParams(new FormData(activeSetSheetForm));
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
                            applySheetMetadataState(data);
                            applyScheduleConfigState(data);
                            applyScheduleTrackingState(data);
                            if (typeof data.overview_html === "string") {{
                                replaceOverviewPanelHtml(data.overview_html);
                            }}
                            if (typeof data.posts_html === "string") {{
                                replacePostsPanelHtml(data.posts_html);
                            }}
                            if (typeof data.campaign_html === "string") {{
                                replaceCampaignPanelHtml(data.campaign_html);
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
	                        await refreshStatusNow();
	                        return data;
                    }} catch (_) {{
                        showNotice("Không nhập được sheet. Vui lòng thử lại.", "error");
                        return null;
                    }}
                }};

                if (setSheetForm) {{
                    setSheetForm.addEventListener("submit", async (event) => {{
                        event.preventDefault();
                        await submitSheetFormInline();
                    }});
                }}

                if (setColumnsForm) {{
                    setColumnsForm.addEventListener("submit", async (event) => {{
                        event.preventDefault();
                        void pushRealtimeLog("Bắt đầu lưu cấu hình cột nhập liệu...");
                        if (configLocked) {{
                            showNotice("Đang ở trạng thái Đã dừng. Bấm Bắt đầu để mở lại rồi hãy lưu sheet.", "warning");
                            return;
                        }}
                        const setColumnsDraftValues = {{}};
                        document.querySelectorAll("[form='set-columns-form'][name]").forEach((field) => {{
                            setColumnsDraftValues[field.name] = field.value || "";
                        }});
                        const sheetData = await submitSheetFormInline();
                        if (!sheetData || !sheetData.ok) {{
                            return;
                        }}
                        document.querySelectorAll("[form='set-columns-form'][name]").forEach((field) => {{
                            if (Object.prototype.hasOwnProperty.call(setColumnsDraftValues, field.name)) {{
                                field.value = setColumnsDraftValues[field.name];
                            }}
                        }});
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
                                pendingSheetMetadataReveal = true;
                                applySheetMetadataState(data);
                                if (typeof data.overview_html === "string") {{
                                    replaceOverviewPanelHtml(data.overview_html);
                                }}
                                if (typeof data.posts_html === "string") {{
                                    replacePostsPanelHtml(data.posts_html);
                                }}
                                if (typeof data.campaign_html === "string") {{
                                    replaceCampaignPanelHtml(data.campaign_html);
                                }}
                            }}
	                            showNotice(
	                                data.message || (data.ok ? "Đã lưu sheet thành công." : "Không lưu được sheet."),
	                                data.level || (data.ok ? "success" : "error")
	                            );
	                            await refreshStatusNow();
	                        }} catch (_) {{
                            showNotice("Không lưu được sheet. Vui lòng thử lại.", "error");
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
                    const scheduleTrackButton = event.target.closest("[data-schedule-track-entry-key]");
                    if (scheduleTrackButton) {{
                        event.preventDefault();
                        const isActiveScheduleTrack = scheduleTrackButton.classList.contains("is-active");
                        await selectScheduleEntry(isActiveScheduleTrack ? "" : (scheduleTrackButton.dataset.scheduleTrackEntryKey || ""), "tracking", false);
                        return;
                    }}

                    const actionLink = event.target.closest("[data-inline-action]");
                    if (!actionLink) return;
                    event.preventDefault();

                    const action = actionLink.dataset.inlineAction || "";
                    if (pendingInlineAction) {{
                        showNotice("Đang gửi lệnh, vui lòng chờ một chút.", "info");
                        return;
                    }}
                    pendingInlineAction = action || "action";
                    actionLink.classList.add("opacity-60", "pointer-events-none");
                    const baseUrl = actionLink.getAttribute("href") || (action === "stop" ? "/stop" : "/start");
                    let requestUrl = baseUrl;
                    if (action === "start") {{
                        const params = new URLSearchParams();
                        const draftSheetUrl = (sheetUrlInput?.value || "").trim();
                        const draftSheetName = (sheetNameInput?.value || "").trim();
                        if (draftSheetUrl) {{
                            params.set("sheet_url", draftSheetUrl);
                        }}
                        const selectedTabsArr = Array.from(selectedSheetTabs);
                        if (selectedTabsArr.length > 1) {{
                            params.set("sheet_names", selectedTabsArr.join(","));
                        }} else if (selectedTabsArr.length === 1) {{
                            params.set("sheet_name", selectedTabsArr[0]);
                        }} else if (draftSheetName) {{
                            params.set("sheet_name", draftSheetName);
                        }}
                        ["date", "air_date", "link", "view", "like", "share", "comment", "buzz", "save", "start_row"].forEach((name) => {{
                            const field = document.querySelector(`[form='set-columns-form'][name='${{name}}']`);
                            if (!field) return;
                            params.set(name, (field.value || "").trim());
                        }});
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
                            applySheetMetadataState(data);
                            if (typeof data.campaign_html === "string") {{
                                replaceCampaignPanelHtml(data.campaign_html);
                            }}
                            if (typeof data.overview_html === "string") {{
                                replaceOverviewPanelHtml(data.overview_html);
                            }}
                            if (sheetUrlInput?.value) {{
                                fetchSheetTabs(sheetUrlInput.value, true);
                            }}
                        }}
                        showNotice(
                            data.message || (data.ok ? "Đã cập nhật tác vụ." : "Không thực hiện được tác vụ."),
                            data.level || (data.ok ? "success" : "error")
                        );
                        if (action === "start" && data.ok) {{
                            setTimeout(async () => {{
                                try {{
                                    const statusResponse = await fetch("/status", {{
                                        headers: {{ "X-Requested-With": "fetch" }},
                                        cache: "no-store",
                                    }});
                                    applyStatusState(await statusResponse.json());
                                }} catch (_) {{}}
                            }}, 800);
                        }}
                    }} catch (_) {{
                        showNotice(
                            action === "stop"
                                ? "Không dừng được tác vụ. Vui lòng thử lại."
                                : "Không bắt đầu được tác vụ. Vui lòng thử lại.",
                            "error"
                        );
                    }} finally {{
                        pendingInlineAction = "";
                        actionLink.classList.remove("opacity-60", "pointer-events-none");
                    }}
                }});

                const getWeeklyJsDay = () => {{
                    const normalizedWeekday = parseInt(weekdaySelect?.value || "0", 10);
                    return normalizedWeekday === 6 ? 0 : normalizedWeekday + 1;
                }};
                const isScheduleWeekdayMatch = (dateObj, activeYear, activeMonth) => {{
                    if (!dateObj) return false;
                    if (dateObj.getFullYear() !== activeYear || dateObj.getMonth() !== activeMonth) return false;
                    return dateObj.getDay() === getWeeklyJsDay();
                }};
                const updateSchedulePreview = () => {{
                    const mode = scheduleModeSelect?.value || "off";
                    if (scheduleMonthdateHelp) {{
                        scheduleMonthdateHelp.textContent = mode === "weekly"
                            ? "Mở lịch để xem toàn bộ ngày đúng thứ đã chọn được khoanh sẵn."
                            : "Hằng tháng: chọn ngày chạy. Hằng tuần: mở lịch để xem các ngày của thứ đã chọn được khoanh sẵn.";
                    }}
                }};
                const syncScheduleModeFields = () => {{
                    const mode = scheduleModeSelect?.value || "off";
                    const showWeekday = mode === "weekly";
                    const showMonthDate = mode === "weekly" || mode === "monthly";
                    if (scheduleWeekdayShell) {{
                        scheduleWeekdayShell.classList.toggle("hidden", !showWeekday);
                    }}
                    if (scheduleMonthdateShell) {{
                        scheduleMonthdateShell.classList.toggle("hidden", !showMonthDate);
                    }}
                    if (weekdaySelect) {{
                        weekdaySelect.disabled = !showWeekday;
                    }}
                    if (monthDateInput) {{
                        monthDateInput.disabled = !showMonthDate;
                    }}
                    if (monthDateBtn) {{
                        monthDateBtn.disabled = !showMonthDate;
                        monthDateBtn.classList.toggle("opacity-40", !showMonthDate);
                        monthDateBtn.classList.toggle("pointer-events-none", !showMonthDate);
                    }}
                }};
                const syncScheduleWeekdayHighlights = () => {{
                    if (!monthPicker?.calendarContainer) return;
                    const mode = scheduleModeSelect?.value || "off";
                    const activeMonth = monthPicker.currentMonth;
                    const activeYear = monthPicker.currentYear;
                    monthPicker.calendarContainer.querySelectorAll(".flatpickr-day").forEach((dayElem) => {{
                        dayElem.classList.remove("schedule-weekday-match");
                        dayElem.classList.remove("schedule-today-muted");
                        if (mode !== "weekly" || !dayElem.dateObj) return;
                        if (isScheduleWeekdayMatch(dayElem.dateObj, activeYear, activeMonth)) {{
                            dayElem.classList.add("schedule-weekday-match");
                        }} else if (dayElem.classList.contains("today")) {{
                            dayElem.classList.add("schedule-today-muted");
                        }}
                    }});
                }};
                const redrawScheduleCalendar = () => {{
                    if (monthPicker && typeof monthPicker.redraw === "function") {{
                        monthPicker.redraw();
                    }}
                    requestAnimationFrame(syncScheduleWeekdayHighlights);
                    setTimeout(syncScheduleWeekdayHighlights, 0);
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
                            dayElem.classList.remove("schedule-today-muted");
                            const mode = scheduleModeSelect?.value || "off";
                            if (mode !== "weekly" || !dayElem.dateObj) return;
                            if (isScheduleWeekdayMatch(dayElem.dateObj, fp.currentYear, fp.currentMonth)) {{
                                dayElem.classList.add("schedule-weekday-match");
                            }} else if (dayElem.classList.contains("today")) {{
                                dayElem.classList.add("schedule-today-muted");
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
                        if (scheduleSheetSearch && scheduleSheetOptionItems.length) {{
                            await commitScheduleSheetSearch(false);
                            if (!String(scheduleSheetSelect?.value || "").trim()) {{
                                showNotice("Hãy chọn đúng một sheet từ gợi ý trước khi lưu lịch.", "warning");
                                scheduleSheetSearch.focus();
                                return;
                            }}
                        }}
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
                    scheduleModeSelect.addEventListener("change", () => {{
                        syncScheduleModeFields();
                        redrawScheduleCalendar();
                    }});
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
                syncScheduleModeFields();
                redrawScheduleCalendar();

                const statusBadge = document.getElementById("status-badge");
                const currentTaskLabel = document.getElementById("current-task");
                const progressBar = document.getElementById("progress-bar");
                const progressText = document.getElementById("progress-text");
                const logSection = document.getElementById("log-section");
                const primaryAction = document.getElementById("primary-action");
                const scheduleLabelEls = Array.from(document.querySelectorAll("[data-schedule-label]"));
                const sidebarStatusText = document.getElementById("sidebar-status-text");
                const sidebarStatusTask = document.getElementById("sidebar-status-task");
                let postsVisibleCount = document.getElementById("posts-visible-count");
                let postsActiveTabLabel = document.getElementById("posts-active-tab-label");
                let postsTabCards = Array.from(document.querySelectorAll("[data-posts-tab-trigger]"));
                let postsTabPanels = Array.from(document.querySelectorAll("[data-posts-tab-panel]"));
                let postsSelectionPlaceholder = document.getElementById("posts-selection-placeholder");
                let postsMasterView = document.getElementById("posts-master-view");
                let postsDetailView = document.getElementById("posts-detail-view");
                let postsBackButton = document.getElementById("posts-back-button");
                let postsMasterSearchField = document.querySelector(".posts-master-search-field");
                let postsMasterCampaignChips = Array.from(document.querySelectorAll("[data-master-campaign]"));
                let postsMasterEmptyPanel = document.querySelector(".posts-master-empty-panel");
                let campaignSheetList = document.getElementById("campaign-sheet-list");
                const sidebarLinks = Array.from(document.querySelectorAll("[data-nav-link]"));
                const dashboardSections = Array.from(document.querySelectorAll("[data-dashboard-section]"));
                let refreshInFlight = false;
                let postsMasterCampaignFilter = "all";
                let postsSheetActionsCloseBound = false;
                let postsColumnsMenusCloseBound = false;
                let overviewCampaignOpenBound = false;
                let lastSheetTabsLogKey = "";
                let lastSheetTabsLogAt = 0;

                const showNotice = (_message = "", _level = "info") => {{}};
                const pushRealtimeLog = async (message = "") => {{
                    const text = String(message || "").trim();
                    if (!text) return;
                    const activeLogSection = document.getElementById("log-section");
                    if (activeLogSection) {{
                        const now = new Date();
                        const hh = String(now.getHours()).padStart(2, "0");
                        const mm = String(now.getMinutes()).padStart(2, "0");
                        const ss = String(now.getSeconds()).padStart(2, "0");
                        const safeText = text.replace(/[&<>]/g, (ch) => ({{ "&": "&amp;", "<": "&lt;", ">": "&gt;" }}[ch] || ch));
                        const row = document.createElement("div");
                        row.className = "system-log-line";
                        row.innerHTML = `<span class="system-log-time">[${{hh}}:${{mm}}:${{ss}}]</span><span class="system-log-message">${{safeText}}</span>`;
                        activeLogSection.insertBefore(row, activeLogSection.firstChild);
                    }}
                    try {{
                        const response = await fetch("/client-log", {{
                            method: "POST",
                            headers: {{ "Content-Type": "application/json", "X-Requested-With": "fetch" }},
                            cache: "no-store",
                            body: JSON.stringify({{ message: text }}),
                        }});
                        if (!response.ok) return;
                        const data = await response.json();
                        applyStatusState(data);
                    }} catch (_) {{}}
                }};
                const syncPostsDomRefs = () => {{
                    postsVisibleCount = document.getElementById("posts-visible-count");
                    postsActiveTabLabel = document.getElementById("posts-active-tab-label");
                    postsTabCards = Array.from(document.querySelectorAll("[data-posts-tab-trigger]"));
                    postsTabPanels = Array.from(document.querySelectorAll("[data-posts-tab-panel]"));
                    postsSelectionPlaceholder = document.getElementById("posts-selection-placeholder");
                    postsMasterView = document.getElementById("posts-master-view");
                    postsDetailView = document.getElementById("posts-detail-view");
                    postsBackButton = document.getElementById("posts-back-button");
                    postsMasterSearchField = document.querySelector(".posts-master-search-field");
                    postsMasterCampaignChips = Array.from(document.querySelectorAll("[data-master-campaign]"));
                    postsMasterEmptyPanel = document.querySelector(".posts-master-empty-panel");
                    campaignSheetList = document.getElementById("campaign-sheet-list");
                }};
                const closePostsSheetActionMenus = (exceptMenu = null) => {{
                    document.querySelectorAll("[data-posts-sheet-action-menu]").forEach((menu) => {{
                        const shouldKeepOpen = exceptMenu && menu === exceptMenu;
                        menu.classList.toggle("hidden", !shouldKeepOpen);
                        const toggle = menu.parentElement?.querySelector("[data-posts-sheet-action-toggle]");
                        if (toggle) {{
                            toggle.setAttribute("aria-expanded", shouldKeepOpen ? "true" : "false");
                        }}
                    }});
                }};
                const closePostsColumnMenus = (exceptMenu = null) => {{
                    document.querySelectorAll("[data-post-columns-menu]").forEach((menu) => {{
                        const shouldKeepOpen = exceptMenu && menu === exceptMenu;
                        menu.classList.toggle("hidden", !shouldKeepOpen);
                        const wrap = menu.closest(".posts-columns-wrap");
                        const toggle = wrap?.querySelector("[data-post-columns-toggle]");
                        if (toggle) {{
                            toggle.setAttribute("aria-expanded", shouldKeepOpen ? "true" : "false");
                        }}
                    }});
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

                const resetEmployeeEditor = () => {{
                    employeeEditingEmail = "";
                    if (employeeEmailInput) employeeEmailInput.value = "";
                    if (employeeRoleInput) employeeRoleInput.value = "user";
                    if (employeeFormTitle) employeeFormTitle.textContent = "Thêm nhanh";
                    if (employeeFormSub) employeeFormSub.textContent = "Nhập email để thêm vào whitelist đăng nhập và chỉnh role ngay tại đây.";
                    if (employeeAddBtn) {{
                        employeeAddBtn.innerHTML = '<i class="fa-solid fa-user-plus"></i> Thêm nhân viên';
                    }}
                    if (employeeCancelBtn) {{
                        employeeCancelBtn.classList.add("hidden");
                    }}
                }};

                const startEmployeeEdit = (email) => {{
                    const found = employeeUsersState.find((item) => item.email === email);
                    if (!found || found.is_forced_admin) return;
                    employeeEditingEmail = found.email;
                    if (employeeEmailInput) employeeEmailInput.value = found.email;
                    if (employeeRoleInput) employeeRoleInput.value = found.role === "admin" ? "admin" : "user";
                    if (employeeFormTitle) employeeFormTitle.textContent = "Chỉnh sửa nhân viên";
                    if (employeeFormSub) employeeFormSub.textContent = "Sửa email hoặc role trong form này rồi bấm cập nhật.";
                    if (employeeAddBtn) {{
                        employeeAddBtn.innerHTML = '<i class="fa-regular fa-pen-to-square"></i> Cập nhật nhân viên';
                    }}
                    if (employeeCancelBtn) {{
                        employeeCancelBtn.classList.remove("hidden");
                    }}
                    employeeEmailInput?.focus();
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
                                        <button type="button" class="employee-icon-btn" data-employee-edit="${{item.email}}" title="Sửa nhân viên" ${{item.is_forced_admin ? "disabled" : ""}}>
                                            <i class="fa-regular fa-pen-to-square"></i>
                                        </button>
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
                        select.addEventListener("change", async () => {{
                            const email = select.getAttribute("data-employee-role") || "";
                            employeeUsersState = employeeUsersState.map((item) => (
                                item.email === email ? {{ ...item, role: select.value === "admin" ? "admin" : "user", role_label: select.value === "admin" ? "Admin" : "User" }} : item
                            ));
                            updateEmployeeSummary(employeeUsersState);
                            renderEmployeeRows();
                            await saveEmployeeUsers({{
                                success: "Đã cập nhật role nhân viên.",
                                error: "Không cập nhật được role nhân viên.",
                            }});
                        }});
                    }});

                    employeeTableBody.querySelectorAll("[data-employee-remove]").forEach((button) => {{
                        button.addEventListener("click", async () => {{
                            const email = button.getAttribute("data-employee-remove") || "";
                            employeeUsersState = employeeUsersState.filter((item) => item.email !== email);
                            if (employeeEditingEmail === email) {{
                                resetEmployeeEditor();
                            }}
                            updateEmployeeSummary(employeeUsersState);
                            renderEmployeeRows();
                            await saveEmployeeUsers({{
                                success: "Đã xóa nhân viên.",
                                error: "Không xóa được nhân viên.",
                            }});
                        }});
                    }});

                    employeeTableBody.querySelectorAll("[data-employee-edit]").forEach((button) => {{
                        button.addEventListener("click", () => {{
                            const email = button.getAttribute("data-employee-edit") || "";
                            startEmployeeEdit(email);
                        }});
                    }});
                }};

                const saveEmployeeUsers = async (messages = {{}}) => {{
                    const successMessage = messages.success || "Đã lưu danh sách nhân viên.";
                    const errorMessage = messages.error || "Không lưu được danh sách nhân viên.";
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
                            data.message || (data.ok ? successMessage : errorMessage),
                            data.level || (data.ok ? "success" : "error")
                        );
                        return Boolean(data.ok);
                    }} catch (_) {{
                        showNotice(`${{errorMessage}} Vui lòng thử lại.`, "error");
                        return false;
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
                    employeeAddBtn.addEventListener("click", async () => {{
                        const email = String(employeeEmailInput?.value || "").trim().toLowerCase();
                        const role = String(employeeRoleInput?.value || "user").trim().toLowerCase() === "admin" ? "admin" : "user";
                        const isEditingEmployee = Boolean(employeeEditingEmail);
                        if (!email || !email.includes("@")) {{
                            showNotice("Email nhân viên không hợp lệ.", "error");
                            return;
                        }}
                        const duplicate = employeeUsersState.find((item) => item.email === email && item.email !== employeeEditingEmail);
                        if (duplicate) {{
                            showNotice("Email này đã có trong danh sách.", "warning");
                            return;
                        }}
                        if (employeeEditingEmail) {{
                            employeeUsersState = employeeUsersState.map((item) => {{
                                if (item.email !== employeeEditingEmail) return item;
                                return {{
                                    ...item,
                                    email,
                                    role,
                                    role_label: role === "admin" ? "Admin" : "User",
                                }};
                            }});
                        }} else {{
                            const existing = employeeUsersState.find((item) => item.email === email);
                            if (existing) {{
                                employeeUsersState = employeeUsersState.map((item) => (
                                    item.email === email ? {{ ...item, role, role_label: role === "admin" ? "Admin" : "User" }} : item
                                ));
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
                            }}
                        }}
                        resetEmployeeEditor();
                        updateEmployeeSummary(employeeUsersState);
                        renderEmployeeRows();
                        await saveEmployeeUsers({{
                            success: isEditingEmployee ? "Đã cập nhật nhân viên." : "Đã thêm nhân viên.",
                            error: isEditingEmployee ? "Không cập nhật được nhân viên." : "Không thêm được nhân viên.",
                        }});
                    }});
                }}

                if (employeeCancelBtn) {{
                    employeeCancelBtn.addEventListener("click", resetEmployeeEditor);
                }}

                const applyActiveSheetMeta = (data, syncInputs = false) => {{
                    const sheetName = (data?.active_sheet_name || "").trim() || "Chưa cài đặt";
                    const sheetId = (data?.active_sheet_id || "").trim() || "Chưa cài đặt";
                    document.querySelectorAll("[data-active-sheet-name]").forEach((el) => {{
                        el.textContent = sheetName;
                    }});
                    document.querySelectorAll("[data-active-sheet-id]").forEach((el) => {{
                        el.textContent = sheetId;
                    }});
                    refreshSheetTabDomRefs();
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
                    const inputValues = columnConfig.input_values || {{}};
                    const detectedInputs = columnConfig.detected_inputs || {{}};
                    const manualInputs = columnConfig.manual_inputs || {{}};
                    const inputSources = columnConfig.input_sources || {{}};
                    ["date", "air_date", "link", "view", "like", "share", "comment", "buzz", "save"].forEach((field) => {{
                        const el = document.querySelector(`[data-column-input="${{field}}"]`);
                        if (!el) return;
                        const nextValue = inputValues[field] || "";
                        if (document.activeElement !== el) {{
                            el.value = nextValue;
                        }}
                        el.dataset.detectedValue = detectedInputs[field] || "";
                        el.dataset.manualValue = manualInputs[field] || "";
                    }});
                    ["date", "air_date", "link", "view", "like", "share", "comment", "buzz", "save"].forEach((field) => {{
                        const el = document.querySelector(`[data-column-source="${{field}}"]`);
                        if (!el) return;
                        el.textContent = inputSources[field] || "CHƯA THẤY";
                    }});
                    const startRowField = setColumnsForm?.querySelector("[name='start_row']");
                    if (startRowField && document.activeElement !== startRowField) {{
                        startRowField.value = `${{columnConfig.start_row || 2}}`;
                    }}
                    const detectedText = columnConfig.detected_text || "";
                    document.querySelectorAll("[data-column-detected-text]").forEach((el) => {{
                        el.textContent = detectedText;
                    }});
                    // Re-apply active tab's per-tab overrides on top of the global defaults
                    if (colConfigActiveTab) {{
                        const tabCfg = serverColConfigByTab[colConfigActiveTab] || tabColConfigCache[colConfigActiveTab];
                        if (tabCfg) writeColConfigInputs(tabCfg);
                    }}
                }};

                const applySheetMetadataState = (data) => {{
                    const sheetMetadata = data?.sheet_metadata;
                    if (!sheetMetadata) return;
                    const canEditMetadata = Boolean(sheetMetadata.can_edit_metadata);
                    const shouldRevealMetadata = pendingSheetMetadataReveal && canEditMetadata;
                    if (sheetMetadataPanel) {{
                        sheetMetadataPanel.classList.toggle("hidden", !canEditMetadata);
                    }}
                    if (sheetMetadataGate) {{
                        sheetMetadataGate.classList.toggle("hidden", canEditMetadata);
                    }}
                    if (sheetCampaignNameInput) {{
                        if (typeof sheetMetadata.campaign_options_html === "string" && sheetMetadata.campaign_options_html.trim()) {{
                            sheetCampaignNameInput.innerHTML = sheetMetadata.campaign_options_html;
                        }}
                        sheetCampaignNameInput.disabled = !Boolean(sheetMetadata.campaign_has_options);
                        if (document.activeElement !== sheetCampaignNameInput) {{
                            sheetCampaignNameInput.value = sheetMetadata.campaign_label || "";
                        }}
                    }}
                    if (sheetBrandInput && document.activeElement !== sheetBrandInput) {{
                        sheetBrandInput.value = sheetMetadata.brand_label || "";
                    }}
                    if (sheetCampaignDescriptionInput && document.activeElement !== sheetCampaignDescriptionInput) {{
                        sheetCampaignDescriptionInput.value = sheetMetadata.campaign_description || "";
                    }}
                    if (shouldRevealMetadata && sheetMetadataPanel) {{
                        setTimeout(() => {{
                            sheetMetadataPanel.scrollIntoView({{ behavior: "smooth", block: "center" }});
                            if (sheetCampaignNameInput && !(sheetCampaignNameInput.value || "").trim()) {{
                                sheetCampaignNameInput.focus();
                            }}
                        }}, 120);
                    }}
                    pendingSheetMetadataReveal = false;
                }};

                const applyScheduleConfigState = (data) => {{
                    const scheduleConfig = data?.schedule_config;
                    if (!scheduleConfig) return;
                    const label = scheduleConfig.label || "Chưa bật";
                    scheduleLabelEls.forEach((el) => {{
                        el.textContent = label;
                    }});
                    if (scheduleSheetSelect && typeof scheduleConfig.sheet_options_html === "string") {{
                        scheduleSheetSelect.innerHTML = scheduleConfig.sheet_options_html;
                        snapshotScheduleSheetOptions();
                        rebuildScheduleSheetSelectOptions(scheduleConfig.sheet_binding_key || scheduleSheetSelect.value || "");
                        const currentFilter = scheduleSheetSearch && document.activeElement === scheduleSheetSearch
                            ? scheduleSheetSearch.value
                            : "";
                        renderScheduleSheetOptions(currentFilter, scheduleConfig.sheet_binding_key || scheduleSheetSelect.value || "");
                    }}
                    if (scheduleSheetSelect && typeof scheduleConfig.sheet_binding_key === "string") {{
                        if (!scheduleSheetSelect.disabled) {{
                            scheduleSheetSelect.value = scheduleConfig.sheet_binding_key;
                        }}
                    }}
                    if (scheduleSheetSearch && document.activeElement !== scheduleSheetSearch) {{
                        const selectedItem = scheduleSheetOptionItems.find((item) => item.value === String(scheduleConfig.sheet_binding_key || scheduleSheetSelect?.value || "").trim());
                        scheduleSheetSearch.value = selectedItem ? selectedItem.label : "";
                    }}
                    if (scheduleBoundSheetName) {{
                        scheduleBoundSheetName.textContent = scheduleConfig.sheet_name_text || "Chưa chốt tab nào";
                    }}
                    if (scheduleBoundSheetId) {{
                        scheduleBoundSheetId.textContent = scheduleConfig.sheet_id_text || "Chưa có Spreadsheet ID";
                    }}
                    if (scheduleBoundScope) {{
                        scheduleBoundScope.textContent = scheduleConfig.scope_text || "";
                    }}
                    if (scheduleTargetSummary) {{
                        scheduleTargetSummary.textContent = scheduleConfig.target_summary_text || "Lịch hiện đang chạy toàn bộ link trong tab đã chốt.";
                    }}
                    if (scheduleBoundLink) {{
                        const hasLink = Boolean(scheduleConfig.snapshot_url);
                        scheduleBoundLink.classList.toggle("hidden", !hasLink);
                        scheduleBoundLink.href = hasLink ? scheduleConfig.snapshot_url : "#";
                    }}
                }};

                const applyScheduleTrackingState = (data) => {{
                    const tracking = data?.schedule_tracking;
                    if (!tracking) return;
                    if (scheduleTrackList && typeof tracking.entries_html === "string") {{
                        scheduleTrackList.innerHTML = tracking.entries_html;
                    }}
                    if (scheduleTrackDetailBody) {{
                        scheduleTrackDetailBody.classList.toggle("hidden", !tracking.has_active_entry);
                    }}
                    if (scheduleTrackEmptyState) {{
                        scheduleTrackEmptyState.classList.toggle("hidden", !!tracking.has_active_entry);
                    }}
                    if (scheduleTrackActiveName) {{
                        scheduleTrackActiveName.textContent = tracking.active_sheet_name || "Chưa chọn sheet";
                    }}
                    if (scheduleTrackCalendarTitle) {{
                        scheduleTrackCalendarTitle.textContent = tracking.calendar_title || "Chưa có lịch";
                    }}
                    if (scheduleTrackCalendarSubtext) {{
                        scheduleTrackCalendarSubtext.textContent = tracking.calendar_subtext || "";
                    }}
                    if (scheduleTrackCalendar && typeof tracking.calendar_html === "string") {{
                        scheduleTrackCalendar.innerHTML = tracking.calendar_html;
                    }}
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

                const createOverviewDate = (value) => {{
                    const raw = String(value || "").trim();
                    if (!raw) return null;
                    const parsed = new Date(`${{raw}}T00:00:00`);
                    return Number.isNaN(parsed.getTime()) ? null : parsed;
                }};

                const startOfOverviewWeek = (sourceDate) => {{
                    const nextDate = new Date(sourceDate.getTime());
                    const weekday = (nextDate.getDay() + 6) % 7;
                    nextDate.setDate(nextDate.getDate() - weekday);
                    nextDate.setHours(0, 0, 0, 0);
                    return nextDate;
                }};

                const formatOverviewShortDate = (sourceDate) => {{
                    const dd = `${{sourceDate.getDate()}}`.padStart(2, "0");
                    const mm = `${{sourceDate.getMonth() + 1}}`.padStart(2, "0");
                    return `${{dd}}/${{mm}}`;
                }};

                const formatOverviewLongDate = (sourceDate) => {{
                    const dd = `${{sourceDate.getDate()}}`.padStart(2, "0");
                    const mm = `${{sourceDate.getMonth() + 1}}`.padStart(2, "0");
                    const yyyy = sourceDate.getFullYear();
                    return `${{dd}}/${{mm}}/${{yyyy}}`;
                }};

                const formatOverviewMonth = (sourceDate) => `T${{sourceDate.getMonth() + 1}}/${{sourceDate.getFullYear()}}`;

                const formatOverviewAxisLabel = (sourceDate, granularity) => {{
                    if (granularity === "month") return formatOverviewMonth(sourceDate);
                    if (granularity === "week") return `Từ ${{formatOverviewShortDate(sourceDate)}}`;
                    return formatOverviewShortDate(sourceDate);
                }};

                const formatOverviewTitleLabel = (sourceDate, granularity) => {{
                    if (granularity === "month") {{
                        return `Tháng ${{sourceDate.getMonth() + 1}}/${{sourceDate.getFullYear()}}`;
                    }}
                    if (granularity === "week") {{
                        return `Tuần bắt đầu ${{formatOverviewLongDate(sourceDate)}}`;
                    }}
                    return formatOverviewLongDate(sourceDate);
                }};

                const aggregateOverviewChartEntries = (rawEntries, rangeKey, granularity) => {{
                    const normalized = (Array.isArray(rawEntries) ? rawEntries : [])
                        .map((item) => {{
                            const parsedDate = createOverviewDate(item?.date);
                            if (!parsedDate) return null;
                            return {{
                                date: parsedDate,
                                creator: String(item?.creator || "").trim().toLowerCase(),
                                view: Number(item?.view || 0) || 0,
                            }};
                        }})
                        .filter(Boolean)
                        .sort((a, b) => a.date - b.date);
                    if (!normalized.length) return [];

                    let filtered = normalized;
                    const latestDate = normalized[normalized.length - 1].date;
                    if (rangeKey === "7d" || rangeKey === "30d") {{
                        const daysBack = rangeKey === "7d" ? 6 : 29;
                        const cutoff = new Date(latestDate.getTime());
                        cutoff.setDate(cutoff.getDate() - daysBack);
                        cutoff.setHours(0, 0, 0, 0);
                        filtered = normalized.filter((item) => item.date >= cutoff);
                    }}

                    const bucketMap = new Map();
                    filtered.forEach((item) => {{
                        let bucketDate;
                        if (granularity === "month") {{
                            bucketDate = new Date(item.date.getFullYear(), item.date.getMonth(), 1);
                        }} else if (granularity === "week") {{
                            bucketDate = startOfOverviewWeek(item.date);
                        }} else {{
                            bucketDate = new Date(item.date.getFullYear(), item.date.getMonth(), item.date.getDate());
                        }}
                        const key = bucketDate.toISOString().slice(0, 10);
                        if (!bucketMap.has(key)) {{
                            bucketMap.set(key, {{
                                key,
                                date: bucketDate,
                                posts: 0,
                                creators: new Set(),
                                views: 0,
                            }});
                        }}
                        const bucket = bucketMap.get(key);
                        bucket.posts += 1;
                        if (item.creator) bucket.creators.add(item.creator);
                        bucket.views += item.view;
                    }});

                    return Array.from(bucketMap.values())
                        .sort((a, b) => a.date - b.date)
                        .map((bucket) => ({{
                            key: bucket.key,
                            date: bucket.date,
                            label: formatOverviewAxisLabel(bucket.date, granularity),
                            title: formatOverviewTitleLabel(bucket.date, granularity),
                            posts: bucket.posts,
                            creators: bucket.creators.size,
                            views: bucket.views,
                        }}));
                }};

                const aggregateAirDateForChart = (rawEntries, options = {{}}) => {{
                    const normalizeDay = (sourceDate) => new Date(
                        sourceDate.getFullYear(),
                        sourceDate.getMonth(),
                        sourceDate.getDate()
                    );
                    const parseIsoDateInput = (value) => {{
                        const raw = String(value || "").trim();
                        if (!raw) return null;
                        const parsed = new Date(`${{raw}}T00:00:00`);
                        return Number.isNaN(parsed.getTime()) ? null : normalizeDay(parsed);
                    }};
                    const addDays = (sourceDate, amount) => {{
                        const next = new Date(sourceDate.getTime());
                        next.setDate(next.getDate() + amount);
                        return normalizeDay(next);
                    }};

                    const normalized = (Array.isArray(rawEntries) ? rawEntries : [])
                        .map((item) => {{
                            const parsedDate = createOverviewDate(item?.date);
                            if (!parsedDate) return null;
                            return {{
                                date: normalizeDay(parsedDate),
                                views: Number(item?.view || 0) || 0,
                                buzz: Number(item?.buzz || 0) || 0,
                                brand: String(item?.brand || "").trim(),
                                sheetName: String(item?.sheet_name || item?.sheet || "").trim(),
                            }};
                        }})
                        .filter(Boolean)
                        .sort((a, b) => a.date - b.date);
                    if (!normalized.length) return [];

                    const latestDate = normalized[normalized.length - 1].date;
                    const nowDate = normalizeDay(new Date());
                    const rangeKey = String(options.range || "30d");
                    const granularity = String(options.granularity || "day");
                    let fromDate = null;
                    let toDate = null;

                    if (rangeKey === "today") {{
                        fromDate = nowDate;
                        toDate = nowDate;
                    }} else if (rangeKey === "24h") {{
                        toDate = nowDate;
                        fromDate = addDays(nowDate, -1);
                    }} else if (rangeKey === "7d") {{
                        toDate = nowDate;
                        fromDate = addDays(nowDate, -6);
                    }} else if (rangeKey === "30d") {{
                        toDate = nowDate;
                        fromDate = addDays(nowDate, -29);
                    }} else if (rangeKey === "this_month") {{
                        fromDate = new Date(nowDate.getFullYear(), nowDate.getMonth(), 1);
                        toDate = new Date(nowDate.getFullYear(), nowDate.getMonth() + 1, 0);
                    }} else if (rangeKey === "last_month") {{
                        fromDate = new Date(nowDate.getFullYear(), nowDate.getMonth() - 1, 1);
                        toDate = new Date(nowDate.getFullYear(), nowDate.getMonth(), 0);
                    }} else if (rangeKey === "all_time") {{
                        fromDate = normalized[0].date;
                        toDate = latestDate;
                    }} else if (rangeKey === "custom") {{
                        const requestedFrom = parseIsoDateInput(options.customFrom);
                        const requestedTo = parseIsoDateInput(options.customTo);
                        if (requestedFrom && requestedTo) {{
                            fromDate = requestedFrom <= requestedTo ? requestedFrom : requestedTo;
                            toDate = requestedTo >= requestedFrom ? requestedTo : requestedFrom;
                        }} else if (requestedFrom) {{
                            fromDate = requestedFrom;
                            toDate = addDays(requestedFrom, 30);
                        }} else if (requestedTo) {{
                            toDate = requestedTo;
                            fromDate = addDays(requestedTo, -30);
                        }}
                        if (fromDate && toDate && (toDate.getTime() - fromDate.getTime()) > 30 * 24 * 60 * 60 * 1000) {{
                            toDate = addDays(fromDate, 30);
                        }}
                    }}

                    if (!fromDate || !toDate) {{
                        toDate = nowDate;
                        fromDate = addDays(nowDate, -29);
                    }}

                    const effectiveToDate = toDate > latestDate ? latestDate : toDate;
                    const bucketBySevenDaysInMonth =
                        (rangeKey === "this_month" || rangeKey === "last_month") && granularity === "day";
                    const filtered = normalized.filter((item) => item.date >= fromDate && item.date <= effectiveToDate);
                    if (!filtered.length) return [];

                    const bucketMap = new Map();
                    filtered.forEach((item) => {{
                        let bucketDate = item.date;
                        let bucketEndDate = item.date;
                        if (granularity === "month") {{
                            bucketDate = new Date(item.date.getFullYear(), item.date.getMonth(), 1);
                        }} else if (granularity === "week") {{
                            bucketDate = startOfOverviewWeek(item.date);
                        }} else if (bucketBySevenDaysInMonth) {{
                            const dayIndex = item.date.getDate() - 1;
                            const chunkStartDay = Math.floor(dayIndex / 7) * 7 + 1;
                            bucketDate = new Date(item.date.getFullYear(), item.date.getMonth(), chunkStartDay);
                            const monthEnd = new Date(item.date.getFullYear(), item.date.getMonth() + 1, 0);
                            const chunkEnd = addDays(bucketDate, 6);
                            bucketEndDate = chunkEnd < monthEnd ? chunkEnd : monthEnd;
                            if (bucketEndDate > effectiveToDate) bucketEndDate = effectiveToDate;
                        }}
                        const key = bucketBySevenDaysInMonth
                            ? `${{bucketDate.toISOString().slice(0, 10)}}__${{bucketEndDate.toISOString().slice(0, 10)}}`
                            : bucketDate.toISOString().slice(0, 10);
                        if (!bucketMap.has(key)) {{
                            bucketMap.set(key, {{
                                key,
                                date: bucketDate,
                                endDate: bucketEndDate,
                                views: 0,
                                buzz: 0,
                                brandStats: new Map(),
                                sheetStats: new Map(),
                            }});
                        }}
                        const bucket = bucketMap.get(key);
                        bucket.views += item.views;
                        bucket.buzz += item.buzz;
                        if (item.brand) {{
                            const currentBrand = bucket.brandStats.get(item.brand) || {{ views: 0, posts: 0 }};
                            currentBrand.views += item.views;
                            currentBrand.posts += 1;
                            bucket.brandStats.set(item.brand, currentBrand);
                        }}
                        if (item.sheetName) {{
                            const currentSheet = bucket.sheetStats.get(item.sheetName) || {{ views: 0, posts: 0 }};
                            currentSheet.views += item.views;
                            currentSheet.posts += 1;
                            bucket.sheetStats.set(item.sheetName, currentSheet);
                        }}
                    }});

                    const filledBuckets = [];
                    if (bucketBySevenDaysInMonth) {{
                        let cursor = new Date(fromDate.getFullYear(), fromDate.getMonth(), fromDate.getDate());
                        const monthEnd = new Date(cursor.getFullYear(), cursor.getMonth() + 1, 0);
                        while (cursor <= effectiveToDate) {{
                            let rangeEnd = addDays(cursor, 6);
                            if (rangeEnd > monthEnd) rangeEnd = monthEnd;
                            if (rangeEnd > effectiveToDate) rangeEnd = effectiveToDate;
                            const key = `${{cursor.toISOString().slice(0, 10)}}__${{rangeEnd.toISOString().slice(0, 10)}}`;
                            const existing = bucketMap.get(key);
                            filledBuckets.push(existing || {{
                                key,
                                date: new Date(cursor),
                                endDate: new Date(rangeEnd),
                                views: 0,
                                buzz: 0,
                                brandStats: new Map(),
                                sheetStats: new Map(),
                            }});
                            cursor = addDays(cursor, 7);
                        }}
                    }} else if (granularity === "month") {{
                        let cursor = new Date(fromDate.getFullYear(), fromDate.getMonth(), 1);
                        const limit = new Date(effectiveToDate.getFullYear(), effectiveToDate.getMonth(), 1);
                        while (cursor <= limit) {{
                            const key = cursor.toISOString().slice(0, 10);
                            const existing = bucketMap.get(key);
                            filledBuckets.push(existing || {{
                                key,
                                date: new Date(cursor),
                                endDate: new Date(cursor),
                                views: 0,
                                buzz: 0,
                                brandStats: new Map(),
                                sheetStats: new Map(),
                            }});
                            cursor = new Date(cursor.getFullYear(), cursor.getMonth() + 1, 1);
                        }}
                    }} else if (granularity === "week") {{
                        let cursor = startOfOverviewWeek(fromDate);
                        const limit = startOfOverviewWeek(effectiveToDate);
                        while (cursor <= limit) {{
                            const key = cursor.toISOString().slice(0, 10);
                            const existing = bucketMap.get(key);
                            filledBuckets.push(existing || {{
                                key,
                                date: new Date(cursor),
                                endDate: new Date(cursor),
                                views: 0,
                                buzz: 0,
                                brandStats: new Map(),
                                sheetStats: new Map(),
                            }});
                            cursor = addDays(cursor, 7);
                        }}
                    }} else {{
                        let cursor = new Date(fromDate.getFullYear(), fromDate.getMonth(), fromDate.getDate());
                        while (cursor <= effectiveToDate) {{
                            const key = cursor.toISOString().slice(0, 10);
                            const existing = bucketMap.get(key);
                            filledBuckets.push(existing || {{
                                key,
                                date: new Date(cursor),
                                endDate: new Date(cursor),
                                views: 0,
                                buzz: 0,
                                brandStats: new Map(),
                                sheetStats: new Map(),
                            }});
                            cursor = addDays(cursor, 1);
                        }}
                    }}

                    return filledBuckets
                        .sort((a, b) => a.date - b.date)
                        .map((bucket) => {{
                            const allBrands = Array.from(bucket.brandStats.entries())
                                .sort((a, b) => (b[1].views - a[1].views) || (b[1].posts - a[1].posts))
                                .map(([name, stat]) => ({{
                                    name,
                                    views: Number(stat?.views || 0) || 0,
                                    posts: Number(stat?.posts || 0) || 0,
                                }}));
                            const allSheets = Array.from(bucket.sheetStats.entries())
                                .sort((a, b) => (b[1].views - a[1].views) || (b[1].posts - a[1].posts))
                                .map(([name, stat]) => ({{
                                    name,
                                    views: Number(stat?.views || 0) || 0,
                                    posts: Number(stat?.posts || 0) || 0,
                                }}));
                            let label = formatOverviewShortDate(bucket.date);
                            if (bucketBySevenDaysInMonth) {{
                                const rangeEnd = bucket.endDate || bucket.date;
                                label = `${{formatOverviewShortDate(bucket.date)}}-${{formatOverviewShortDate(rangeEnd)}}`;
                            }} else if (granularity === "week") {{
                                label = `W ${{formatOverviewShortDate(bucket.date)}}`;
                            }} else if (granularity === "month") {{
                                label = formatOverviewMonth(bucket.date);
                            }}
                            return {{
                                ...bucket,
                                label,
                                topBrands: allBrands,
                                topSheets: allSheets,
                            }};
                        }});
                }};

                const createSvgNode = (name, attrs = {{}}) => {{
                    const node = document.createElementNS("http://www.w3.org/2000/svg", name);
                    Object.entries(attrs).forEach(([key, value]) => {{
                        node.setAttribute(key, String(value));
                    }});
                    return node;
                }};

                const buildSmoothLinePath = (points) => {{
                    if (!Array.isArray(points) || !points.length) return "";
                    if (points.length === 1) return `M ${{points[0].x}} ${{points[0].y}}`;
                    let path = `M ${{points[0].x}} ${{points[0].y}}`;
                    for (let index = 0; index < points.length - 1; index += 1) {{
                        const p0 = points[Math.max(0, index - 1)];
                        const p1 = points[index];
                        const p2 = points[index + 1];
                        const p3 = points[Math.min(points.length - 1, index + 2)];
                        const cp1x = p1.x + (p2.x - p0.x) / 6;
                        const cp1y = p1.y + (p2.y - p0.y) / 6;
                        const cp2x = p2.x - (p3.x - p1.x) / 6;
                        const cp2y = p2.y - (p3.y - p1.y) / 6;
                        path += ` C ${{cp1x}} ${{cp1y}}, ${{cp2x}} ${{cp2y}}, ${{p2.x}} ${{p2.y}}`;
                    }}
                    return path;
                }};

                const buildSmoothAreaPath = (points, baseY) => {{
                    if (!Array.isArray(points) || !points.length) return "";
                    const linePath = buildSmoothLinePath(points);
                    return `${{linePath}} L ${{points[points.length - 1].x}} ${{baseY}} L ${{points[0].x}} ${{baseY}} Z`;
                }};

                const initializeOverviewCharts = () => {{
                    document.querySelectorAll("[data-overview-chart-card]").forEach((card) => {{
                        const dataScript = card.querySelector("[data-overview-chart-data]");
                        const svg = card.querySelector("[data-overview-chart-svg]");
                        const singleState = card.querySelector("[data-overview-chart-single]");
                        const emptyState = card.querySelector("[data-overview-chart-empty]");
                        const tooltip = card.querySelector("[data-overview-chart-tooltip]");
                        const periodLabel = card.querySelector("[data-overview-chart-period]");
                        const brandLegend = card.querySelector("[data-overview-brand-legend]");
                        const filterTrigger = card.querySelector("[data-overview-filter-trigger]");
                        const filterPanel = card.querySelector("[data-overview-filter-panel]");
                        if (!dataScript || !svg || !singleState || !emptyState || !tooltip) return;

                        let payload = {{ entries: [] }};
                        try {{
                            payload = JSON.parse(dataScript.textContent || "{{}}");
                        }} catch (_) {{
                            payload = {{ entries: [] }};
                        }}
                        const rawEntries = Array.isArray(payload.entries) ? payload.entries : [];
                        const customFromInput = card.querySelector("[data-overview-custom-from]");
                        const customToInput = card.querySelector("[data-overview-custom-to]");
                        const customApplyButton = card.querySelector("[data-overview-apply-custom]");
                        if (!card._overviewChartState) {{
                            card._overviewChartState = {{ range: "7d", granularity: "day", customFrom: "", customTo: "" }};
                        }}
                        const setChartControlActiveState = () => {{
                            card.querySelectorAll("[data-overview-range]").forEach((button) => {{
                                const key = button.dataset.overviewRange || "";
                                button.classList.toggle("is-active", key === card._overviewChartState.range);
                            }});
                            card.querySelectorAll("[data-overview-granularity]").forEach((button) => {{
                                const key = button.dataset.overviewGranularity || "";
                                button.classList.toggle("is-active", key === card._overviewChartState.granularity);
                            }});
                        }};
                        const applyDefaultCustomRangeInputs = () => {{
                            if (!customFromInput || !customToInput) return;
                            const today = new Date();
                            today.setHours(0, 0, 0, 0);
                            const fromDate = new Date(today.getTime());
                            fromDate.setDate(fromDate.getDate() - 29);
                            const toIso = today.toISOString().slice(0, 10);
                            const fromIso = fromDate.toISOString().slice(0, 10);
                            if (!customFromInput.value) customFromInput.value = fromIso;
                            if (!customToInput.value) customToInput.value = toIso;
                            if (!card._overviewChartState.customFrom) card._overviewChartState.customFrom = fromIso;
                            if (!card._overviewChartState.customTo) card._overviewChartState.customTo = toIso;
                        }};
                        applyDefaultCustomRangeInputs();
                        setChartControlActiveState();
                        const setFilterPanelOpen = (isOpen) => {{
                            if (!filterPanel) return;
                            filterPanel.classList.toggle("hidden", !isOpen);
                            if (filterTrigger) {{
                                filterTrigger.classList.toggle("is-active", isOpen);
                                filterTrigger.setAttribute("aria-expanded", isOpen ? "true" : "false");
                            }}
                        }};
                        setFilterPanelOpen(false);

                        const renderChart = () => {{
                            try {{
                                const points = aggregateAirDateForChart(rawEntries, card._overviewChartState);

                                tooltip.classList.add("hidden");
                                svg.replaceChildren();
                                singleState.classList.add("hidden");
                                singleState.innerHTML = "";
                                svg.classList.remove("hidden");

                                if (!points.length) {{
                                    emptyState.classList.remove("hidden");
                                    if (periodLabel) periodLabel.textContent = "Chưa có dữ liệu ngày air bài";
                                    if (brandLegend) brandLegend.innerHTML = "";
                                    return;
                                }}

                                emptyState.classList.add("hidden");
                                
                                const formatMetric = (v) => {{
                                    if (v >= 1000000) return (v / 1000000).toFixed(1).replace(/\\.0$/, "") + "M";
                                    if (v >= 1000) return (v / 1000).toFixed(1).replace(/\\.0$/, "") + "K";
                                    return String(v);
                                }};

                                const totalBuzz = points.reduce((sum, b) => sum + (b.buzz || 0), 0);
                                const totalViews = points.reduce((sum, b) => sum + (b.views || 0), 0);
                                const fBuzz = new Intl.NumberFormat("vi-VN").format(totalBuzz);
                                const firstPoint = points[0];
                                const lastPoint = points[points.length - 1];
                                const spanText = firstPoint && lastPoint
                                    ? `${{formatOverviewLongDate(firstPoint.date)}} - ${{formatOverviewLongDate(lastPoint.date)}}`
                                    : "";
                                if (periodLabel) periodLabel.textContent = "Tổng cộng " + fBuzz + " buzz, " + formatMetric(totalViews) + " views" + (spanText ? (" | " + spanText) : "");
                                const showTooltip = (event, pointData) => {{
                                    if (!tooltip || !pointData) return;
                                    const topBrands = Array.isArray(pointData.topBrands) ? pointData.topBrands : [];
                                    const topSheets = Array.isArray(pointData.topSheets) ? pointData.topSheets : [];
                                    const topBrandHtml = topBrands.length
                                        ? topBrands.map((item, idx) => `<div style="color:#93c5fd;">${{idx + 1}}. ${{item.name}} · ${{new Intl.NumberFormat("vi-VN").format(item.views || 0)}} views</div>`).join("")
                                        : '<div style="color:#64748b;">Chưa có dữ liệu thương hiệu</div>';
                                    const topSheetHtml = topSheets.length
                                        ? topSheets.map((item, idx) => `<div style="color:#a5b4fc;">${{idx + 1}}. ${{item.name}} · ${{new Intl.NumberFormat("vi-VN").format(item.views || 0)}} views</div>`).join("")
                                        : '<div style="color:#64748b;">Chưa có dữ liệu sheet</div>';
                                    tooltip.innerHTML = `
                                        <div style="font-weight:800;color:#f8fafc;">${{pointData.label || pointData.key || "N/A"}}</div>
                                        <div style="margin-top:4px;color:#cbd5e1;">Buzz: <strong>${{new Intl.NumberFormat("vi-VN").format(pointData.buzz || 0)}}</strong></div>
                                        <div style="color:#cbd5e1;">Lượt xem: <strong>${{new Intl.NumberFormat("vi-VN").format(pointData.views || 0)}}</strong></div>
                                        <div style="margin-top:8px;color:#94a3b8;font-weight:700;">Thương hiệu (${{topBrands.length}}):</div>
                                        <div style="margin-top:2px;">${{topBrandHtml}}</div>
                                        <div style="margin-top:8px;color:#94a3b8;font-weight:700;">Sheet (${{topSheets.length}}):</div>
                                        <div style="margin-top:2px;">${{topSheetHtml}}</div>
                                    `;
                                    tooltip.classList.remove("hidden");
                                    const rect = svg.getBoundingClientRect();
                                    const x = (event?.clientX || rect.left) - rect.left + 14;
                                    const y = (event?.clientY || rect.top) - rect.top - 14;
                                    tooltip.style.left = `${{Math.max(12, x)}}px`;
                                    tooltip.style.top = `${{Math.max(12, y)}}px`;
                                }};
                                const hideTooltip = () => {{
                                    if (!tooltip) return;
                                    tooltip.classList.add("hidden");
                                }};

                                const width = Math.max(960, Math.round(svg.clientWidth || 960));
                                const height = Math.max(380, Math.round(svg.clientHeight || 380));
                                svg.setAttribute("viewBox", `0 0 ${{width}} ${{height}}`);
                                svg.setAttribute("preserveAspectRatio", "xMidYMid meet");

                                const padding = {{ top: 34, right: 50, bottom: 48, left: 50 }};
                                const innerWidth = width - padding.left - padding.right;
                                const innerHeight = height - padding.top - padding.bottom;
                                
                                const rawMaxBuzz = Math.max(1, ...points.map(b => b.buzz || 0));
                                const rawMaxViews = Math.max(1, ...points.map(b => b.views || 0));
                                const maxBuzz = Math.max(1, Math.ceil(rawMaxBuzz * 1.15));
                                const maxViews = Math.max(1, Math.ceil(rawMaxViews * 1.15));
                                const tickCount = 4;
                                
                                const tentativeStep = points.length <= 1
                                    ? innerWidth
                                    : innerWidth / Math.max(1, points.length - 1);
                                const barW = Math.min(34, Math.max(18, (tentativeStep || 96) * 0.22));
                                const edgeGap = Math.max(12, Math.ceil(barW / 2) + 4);
                                const effectiveInnerWidth = Math.max(1, innerWidth - edgeGap * 2);
                                const groupStep = points.length <= 1
                                    ? 0
                                    : effectiveInnerWidth / Math.max(1, points.length - 1);
                                const firstGroupX = padding.left + edgeGap;
                                const buzzColor = "#f59e0b";
                                if (brandLegend) brandLegend.innerHTML = "";
                                const xAt = (index) => {{
                                    if (points.length === 1) return padding.left + innerWidth / 2;
                                    return firstGroupX + (groupStep * index);
                                }};
                                const yBuzz = (val) => padding.top + innerHeight - ((val || 0) / maxBuzz * innerHeight);
                                const yViews = (val) => padding.top + innerHeight - ((val || 0) / maxViews * innerHeight);

                                const chartUid = card.dataset.overviewChartUid || ("overview-" + Math.random().toString(36).slice(2, 10));
                                card.dataset.overviewChartUid = chartUid;

                                const defs = createSvgNode("defs");
                                const barGrad = createSvgNode("linearGradient", {{ id: "bar-grad-" + chartUid, x1:0, y1:0, x2:0, y2:1 }});
                                barGrad.appendChild(createSvgNode("stop", {{ offset: "0%", "stop-color": "#10b981", "stop-opacity": "1" }}));
                                barGrad.appendChild(createSvgNode("stop", {{ offset: "100%", "stop-color": "#059669", "stop-opacity": "0.85" }}));
                                defs.appendChild(barGrad);
                                
                                const greyGrad = createSvgNode("linearGradient", {{ id: "grey-grad-" + chartUid, x1:0, y1:0, x2:0, y2:1 }});
                                greyGrad.appendChild(createSvgNode("stop", {{ offset: "0%", "stop-color": "rgba(148, 163, 184, 0.35)", "stop-opacity": "1" }}));
                                greyGrad.appendChild(createSvgNode("stop", {{ offset: "100%", "stop-color": "rgba(148, 163, 184, 0.15)", "stop-opacity": "1" }}));
                                defs.appendChild(greyGrad);
                                svg.appendChild(defs);

                                const bgRect = createSvgNode("rect", {{
                                    x: padding.left, y: padding.top, width: innerWidth, height: innerHeight,
                                    rx: 24, fill: "rgba(15, 23, 42, 0.4)", stroke: "rgba(148, 163, 184, 0.1)", "stroke-width": 1
                                }});
                                svg.appendChild(bgRect);

                                for (let i = 0; i <= tickCount; i++) {{
                                    const y = padding.top + (innerHeight * i) / tickCount;
                                    const buzzVal = Math.round(maxBuzz * (tickCount - i) / tickCount);
                                    const viewVal = Math.round(maxViews * (tickCount - i) / tickCount);
                                    
                                    svg.appendChild(createSvgNode("line", {{
                                        x1: padding.left, y1: y, x2: width - padding.right, y2: y,
                                        stroke: "rgba(148, 163, 184, 0.1)", "stroke-width": 1, "stroke-dasharray": "4 8"
                                    }}));
                                    
                                    const lblLeft = createSvgNode("text", {{
                                        x: padding.left - 12, y: y + 4, fill: "rgba(16, 185, 129, 0.6)",
                                        "font-size": 11, "font-weight": 700, "text-anchor": "end"
                                    }});
                                    lblLeft.textContent = formatMetric(buzzVal);
                                    svg.appendChild(lblLeft);

                                    const lblRight = createSvgNode("text", {{
                                        x: width - padding.right + 12, y: y + 4, fill: "rgba(56, 189, 248, 0.6)",
                                        "font-size": 11, "font-weight": 700, "text-anchor": "start"
                                    }});
                                    lblRight.textContent = formatMetric(viewVal);
                                    svg.appendChild(lblRight);
                                }}

                                const viewLinePoints = [];
                                const xLabelStep = Math.max(1, Math.ceil(points.length / 10));
                                points.forEach((b, i) => {{
                                    const x = xAt(i);
                                    const buzzY = yBuzz(b.buzz);
                                    const buzzH = Math.max(4, innerHeight - (buzzY - padding.top));
                                    const viewY = yViews(b.views);
                                    const buzzX = x - barW / 2;

                                    const pointBuzz = Number(b?.buzz || 0) || 0;
                                    if (pointBuzz > 0) {{
                                        const bar = createSvgNode("rect", {{
                                            x: buzzX, y: buzzY, width: barW, height: buzzH + 20, 
                                            rx: 6, fill: buzzColor, stroke: "rgba(15, 23, 42, 0.5)", "stroke-width": 1,
                                            style: "transition: all 0.3s ease; clip-path: inset(0 0 20px 0);"
                                        }});
                                        bar.addEventListener("mouseenter", (e) => showTooltip(e, b));
                                        bar.addEventListener("mouseleave", hideTooltip);
                                        svg.appendChild(bar);
                                    }}
                                    viewLinePoints.push({{ x, y: viewY, data: b }});

                                    if (i % xLabelStep === 0 || i === points.length - 1) {{
                                        const dateTxt = createSvgNode("text", {{
                                            x,
                                            y: height - 14,
                                            fill: "rgba(148, 163, 184, 0.9)",
                                            "font-size": 10,
                                            "font-weight": 700,
                                            "text-anchor": "middle"
                                        }});
                                        dateTxt.textContent = b.label || "";
                                        svg.appendChild(dateTxt);
                                    }}
                                }});

                                if (viewLinePoints.length > 0) {{
                                    const linePathData = viewLinePoints
                                        .map((pt, idx) => `${{idx === 0 ? "M" : "L"}}${{pt.x}} ${{pt.y}}`)
                                        .join(" ");
                                    const linePath = createSvgNode("path", {{
                                        d: linePathData,
                                        fill: "none",
                                        stroke: "#38bdf8",
                                        "stroke-width": 3,
                                        "stroke-linecap": "round",
                                        "stroke-linejoin": "round",
                                        opacity: "0.95"
                                    }});
                                    svg.appendChild(linePath);

                                    viewLinePoints.forEach((pt) => {{
                                        const pointViews = Number(pt.data?.views || 0) || 0;
                                        if (pointViews > 0) {{
                                            const point = createSvgNode("circle", {{
                                                cx: pt.x,
                                                cy: pt.y,
                                                r: 5,
                                                fill: "#0f172a",
                                                stroke: "#38bdf8",
                                                "stroke-width": 3
                                            }});
                                            point.addEventListener("mouseenter", (e) => showTooltip(e, pt.data));
                                            point.addEventListener("mouseleave", hideTooltip);
                                            svg.appendChild(point);
                                        }}
                                        if (pointViews > 0) {{
                                            const viewTxt = createSvgNode("text", {{
                                                x: pt.x,
                                                y: pt.y - 12,
                                                fill: "#38bdf8",
                                                "font-size": 11,
                                                "font-weight": 800,
                                                "text-anchor": "middle"
                                            }});
                                            viewTxt.textContent = formatMetric(pointViews);
                                            svg.appendChild(viewTxt);
                                        }}
                                    }});
                                }}
                            }} catch (error) {{
                                emptyState.classList.add("hidden");
                                svg.classList.add("hidden");
                                singleState.innerHTML = '<div style="color:#ef4444; padding:1rem; font-family:monospace; background:rgba(0,0,0,0.5); border-radius:8px; width:100%;">' + String(error.message) + "</div>";
                                singleState.classList.remove("hidden");
                            }}
                        }};

                                                if (!card._overviewChartBound) {{
                            card.querySelectorAll("[data-overview-range]").forEach((button) => {{
                                button.addEventListener("click", () => {{
                                    card._overviewChartState.range = button.dataset.overviewRange || "7d";
                                    setChartControlActiveState();
                                    renderChart();
                                }});
                            }});
                            card.querySelectorAll("[data-overview-granularity]").forEach((button) => {{
                                button.addEventListener("click", () => {{
                                    card._overviewChartState.granularity = button.dataset.overviewGranularity || "day";
                                    setChartControlActiveState();
                                    renderChart();
                                }});
                            }});
                            if (customApplyButton) {{
                                customApplyButton.addEventListener("click", () => {{
                                    card._overviewChartState.range = "custom";
                                    card._overviewChartState.customFrom = customFromInput ? customFromInput.value : "";
                                    card._overviewChartState.customTo = customToInput ? customToInput.value : "";
                                    setChartControlActiveState();
                                    renderChart();
                                }});
                            }}
                            if (filterTrigger && filterPanel) {{
                                filterTrigger.addEventListener("click", (event) => {{
                                    event.stopPropagation();
                                    const isHidden = filterPanel.classList.contains("hidden");
                                    setFilterPanelOpen(isHidden);
                                }});
                                document.addEventListener("click", (event) => {{
                                    const target = event.target;
                                    if (filterPanel.contains(target) || target === filterTrigger) return;
                                    setFilterPanelOpen(false);
                                }});
                                document.addEventListener("keydown", (event) => {{
                                    if (event.key === "Escape") setFilterPanelOpen(false);
                                }});
                            }}
                            [customFromInput, customToInput].forEach((input) => {{
                                if (!input) return;
                                input.addEventListener("keydown", (event) => {{
                                    if (event.key !== "Enter") return;
                                    card._overviewChartState.range = "custom";
                                    card._overviewChartState.customFrom = customFromInput ? customFromInput.value : "";
                                    card._overviewChartState.customTo = customToInput ? customToInput.value : "";
                                    setChartControlActiveState();
                                    renderChart();
                                }});
                            }});

                            const chartFrame = card.querySelector(".overview-chart-frame") || card;
                            if (window.ResizeObserver && !card._overviewChartResizeObserver) {{
                                card._overviewChartResizeObserver = new ResizeObserver(() => {{
                                    if (card._overviewChartResizeTimeout) clearTimeout(card._overviewChartResizeTimeout);
                                    card._overviewChartResizeTimeout = setTimeout(() => {{
                                        if (svg.clientWidth) renderChart();
                                    }}, 50);
                                }});
                                card._overviewChartResizeObserver.observe(chartFrame);
                            }}

                            card._overviewChartBound = true;
                        }}

                        renderChart();
                    }});
                }};

                const applyStatusState = (data) => {{
                    if (!data) return;
                    const currentStatusBadge = document.getElementById("status-badge");
                    const currentTaskEl = document.getElementById("current-task");
                    const currentProgressBar = document.getElementById("progress-bar");
                    const currentProgressText = document.getElementById("progress-text");
                    const currentLogSection = document.getElementById("log-section");
                    const currentPrimaryAction = document.getElementById("primary-action");
                    if (currentStatusBadge) {{
                        currentStatusBadge.className = data.status_badge_class;
                        currentStatusBadge.textContent = data.status_badge_text;
                    }}
                    if (sidebarStatusText) {{
                        sidebarStatusText.textContent = data.status_badge_text;
                    }}
                    if (sidebarStatusTask) {{
                        sidebarStatusTask.textContent = data.current_task;
                    }}
                    if (currentTaskEl) {{
                        currentTaskEl.textContent = data.current_task;
                    }}
                    if (currentProgressBar) {{
                        currentProgressBar.style.width = data.progress_width;
                    }}
                    if (currentProgressText) {{
                        currentProgressText.textContent = data.progress_text || "";
                    }}
                    if (currentLogSection && typeof data.log_html === "string") {{
                        currentLogSection.innerHTML = data.log_html;
                    }}
                    if (currentPrimaryAction && typeof data.primary_action_html === "string") {{
                        currentPrimaryAction.innerHTML = data.primary_action_html;
                    }}
                    applyConfigLockState(Boolean(data.config_locked), data.config_lock_message || "");
                    // Merge server-saved per-tab column overrides into our local reference dict
                    if (data.column_overrides_by_tab && typeof data.column_overrides_by_tab === "object") {{
                        const hadConfig = colConfigActiveTab && !!serverColConfigByTab[colConfigActiveTab];
                        Object.assign(serverColConfigByTab, data.column_overrides_by_tab);
                        // If there's an active tab and we just received its config for the first time, load it
                        if (colConfigActiveTab && !hadConfig && serverColConfigByTab[colConfigActiveTab]) {{
                            writeColConfigInputs(serverColConfigByTab[colConfigActiveTab]);
                        }}
                    }}
                    applyTabProgressState(data);
                }};

                const applyTabProgressState = (data) => {{
                    const tabProgressSection = document.getElementById("tab-progress-section");
                    if (!tabProgressSection) return;
                    const tp = data?.tab_progress;
                    if (!tp || typeof tp !== "object" || !Object.keys(tp).length) {{
                        tabProgressSection.classList.add("hidden");
                        tabProgressSection.innerHTML = "";
                        return;
                    }}
                    tabProgressSection.classList.remove("hidden");
                    tabProgressSection.innerHTML = Object.entries(tp).map(([tabName, info]) => {{
                        const cur = info.current || 0;
                        const tot = info.total || 0;
                        const status = info.status || "scanning";
                        const pct = tot > 0 ? Math.round((cur / tot) * 100) : (status === "completed" ? 100 : 0);
                        const isCompleted = status === "completed";
                        const isStopped = status === "stopped";
                        const isError = status === "error";
                        const barColor = isCompleted ? "#22c55e" : isStopped ? "#f59e0b" : isError ? "#ef4444" : "#0ea5e9";
                        const badgeColor = isCompleted
                            ? "background:rgba(16,185,129,0.15);color:#6ee7b7;border-color:rgba(52,211,153,0.25);"
                            : isStopped
                            ? "background:rgba(251,191,36,0.13);color:#fcd34d;border-color:rgba(253,230,138,0.25);"
                            : isError
                            ? "background:rgba(239,68,68,0.13);color:#fca5a5;border-color:rgba(252,165,165,0.25);"
                            : "background:rgba(14,165,233,0.13);color:#7dd3fc;border-color:rgba(125,211,252,0.22);";
                        const badgeLabel = isCompleted ? "✓ HOÀN TẤT" : isStopped ? "ĐÃ DỪNG" : isError ? "LỖI" : "ĐANG CHẠY";
                        return `<div style="display:grid;grid-template-columns:minmax(0,1.1fr) minmax(120px,1.8fr) 44px minmax(92px,auto);align-items:center;gap:12px;padding:8px 0;">
                            <div style="display:flex;align-items:center;gap:9px;min-width:0;">
                                <i class="fa-regular fa-file-lines" style="color:#22d3ee;font-size:15px;flex:0 0 auto;"></i>
                                <span style="font-size:12px;font-weight:850;color:#f8fafc;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${{tabName}}</span>
                            </div>
                            <div style="background:rgba(30,41,59,0.82);border:1px solid rgba(56,189,248,0.22);border-radius:999px;height:9px;overflow:hidden;">
                                <div style="height:100%;border-radius:999px;width:${{pct}}%;background:${{barColor}};box-shadow:0 0 16px ${{barColor}};transition:width .8s ease;"></div>
                            </div>
                            <span style="font-size:12px;font-weight:900;color:#e2e8f0;text-align:right;">${{pct}}%</span>
                            <span style="font-size:10px;font-weight:950;letter-spacing:.06em;text-align:center;padding:6px 10px;border-radius:999px;border:1px solid;white-space:nowrap;${{badgeColor}}">${{badgeLabel}}</span>
                        </div>`;
                    }}).join("");
                }};

                applyConfigLockState(
                    (statusBadge?.textContent || "").trim() === "Đã dừng" && (currentTaskLabel?.textContent || "").trim() === "Đã dừng thủ công",
                    "Đang ở trạng thái Đã dừng. Bấm Bắt đầu để mở lại rồi hãy nhập hoặc lưu sheet."
                );

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
                        applyTabProgressState(data);
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
                    const selectionCountEl = panel.querySelector("[data-posts-selection-count]");
                    const allRowChecks = Array.from(panel.querySelectorAll("[data-post-select]"));
                    const checkedCount = allRowChecks.filter((item) => item.checked).length;
                    if (selectionCountEl) {{
                        selectionCountEl.textContent = `${{checkedCount}} bài đã chọn`;
                    }}
                    if (!selectAll) return;
                    const rowChecks = getVisibleRowChecks(panel);
                    const visibleCheckedCount = rowChecks.filter((item) => item.checked).length;
                    selectAll.checked = rowChecks.length > 0 && visibleCheckedCount === rowChecks.length;
                    selectAll.indeterminate = visibleCheckedCount > 0 && visibleCheckedCount < rowChecks.length;
                }};

                const syncPostsSelectionState = () => {{
                    postsTabPanels.forEach((panel) => updatePanelSelectAllState(panel));
                }};

                const getActivePostsPanel = () => postsTabPanels.find((panel) => panel.classList.contains("is-active")) || null;

                const collectScheduleTargetsFromPanel = (panel) => {{
                    if (!panel) return [];
                    return Array.from(panel.querySelectorAll("[data-post-select]:checked"))
                        .map((checkbox) => ({{
                            sheet_id: checkbox.dataset.sheetId || "",
                            sheet_gid: checkbox.dataset.sheetGid || "0",
                            sheet_name: checkbox.dataset.sheetName || "",
                            row_idx: checkbox.dataset.rowIdx || "",
                            link: checkbox.dataset.link || "",
                            title: checkbox.dataset.title || "",
                            platform: checkbox.dataset.platformName || "",
                        }}))
                        .filter((item) => item.sheet_id && item.sheet_name && item.row_idx);
                }};

                const submitScheduleTargets = async (targets = [], panel = null) => {{
                    const fallbackCheckbox = (panel || getActivePostsPanel())?.querySelector("[data-post-select]");
                    const fallbackSheetId = fallbackCheckbox?.dataset.sheetId || "";
                    const fallbackSheetGid = fallbackCheckbox?.dataset.sheetGid || "0";
                    try {{
                        const response = await fetch("/set-schedule-targets", {{
                            method: "POST",
                            headers: {{
                                "Content-Type": "application/json",
                                "X-Requested-With": "fetch",
                            }},
                            body: JSON.stringify({{
                                targets,
                                sheet_id: fallbackSheetId,
                                sheet_gid: fallbackSheetGid,
                            }}),
                        }});
                        const data = await response.json();
                        applyScheduleConfigState(data);
                        showNotice(
                            data.message || (data.ok ? "Đã cập nhật danh sách bài cho lịch." : "Không cập nhật được danh sách bài cho lịch."),
                            data.level || (data.ok ? "success" : "error")
                        );
                        return data;
                    }} catch (_) {{
                        showNotice("Không cập nhật được danh sách bài cho lịch. Vui lòng thử lại.", "error");
                        return null;
                    }}
                }};

                const rerunSelectedPosts = async (targets = [], panel = null) => {{
                    const fallbackCheckbox = (panel || getActivePostsPanel())?.querySelector("[data-post-select]");
                    const fallbackSheetId = fallbackCheckbox?.dataset.sheetId || "";
                    if (!targets.length) {{
                        setPostsRerunFeedback("Bạn chưa chọn bài nào để chạy lại.", "warning", panel);
                        showNotice("Bạn chưa chọn bài nào để chạy lại.", "warning");
                        return null;
                    }}
                    setPostsRerunFeedback("Đang gửi lệnh chạy lại các bài đã chọn...", "info", panel);
                    try {{
                        const response = await fetch("/start-selected", {{
                            method: "POST",
                            headers: {{
                                "Content-Type": "application/json",
                                "X-Requested-With": "fetch",
                            }},
                            body: JSON.stringify({{
                                targets,
                                sheet_id: fallbackSheetId,
                            }}),
                        }});
                        const data = await response.json();
                        applyStatusState(data);
                        applyActiveSheetMeta(data);
                        applyColumnConfigState(data);
                        setPostsRerunFeedback(
                            data.message || (data.ok ? "Đã bắt đầu quét lại các bài đã chọn." : "Không chạy lại được các bài đã chọn."),
                            data.level || (data.ok ? "success" : "error"),
                            panel
                        );
                        showNotice(
                            data.message || (data.ok ? "Đã bắt đầu quét lại các bài đã chọn." : "Không chạy lại được các bài đã chọn."),
                            data.level || (data.ok ? "success" : "error")
                        );
                        return data;
                    }} catch (_) {{
                        setPostsRerunFeedback("Không chạy lại được các bài đã chọn. Vui lòng thử lại.", "error", panel);
                        showNotice("Không chạy lại được các bài đã chọn. Vui lòng thử lại.", "error");
                        return null;
                    }}
                }};

                const applyPostFilters = (panel = getActivePostsPanel()) => {{
                    if (!panel) {{
                        if (postsVisibleCount) {{
                            postsVisibleCount.textContent = "0 bài";
                        }}
                        if (postsActiveTabLabel) {{
                            postsActiveTabLabel.textContent = "Chưa chọn";
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

                const applyPostsMasterFilters = () => {{
                    const term = (postsMasterSearchField?.value || "").trim().toLowerCase();
                    const activeCampaign = postsMasterCampaignFilter || "all";
                    let visibleCards = 0;
                    postsTabCards.forEach((card) => {{
                        const cardCampaign = card.dataset.postsMasterCampaign || "khong-gan";
                        const haystack = card.dataset.postsMasterSearch || "";
                        const matchesCampaign = activeCampaign === "all" || cardCampaign === activeCampaign;
                        const matchesTerm = !term || haystack.includes(term);
                        const shouldShow = matchesCampaign && matchesTerm;
                        card.classList.toggle("hidden", !shouldShow);
                        if (shouldShow) {{
                            visibleCards += 1;
                        }}
                    }});
                    if (postsMasterEmptyPanel) {{
                        postsMasterEmptyPanel.classList.toggle("hidden", visibleCards !== 0);
                    }}
                }};

                const setPostsMasterCampaignFilter = (campaignSlug = "all") => {{
                    const normalizedSlug = String(campaignSlug || "all").trim() || "all";
                    const hasMatch = postsMasterCampaignChips.some((chip) => (chip.dataset.masterCampaign || "all") === normalizedSlug);
                    postsMasterCampaignFilter = hasMatch ? normalizedSlug : "all";
                    postsMasterCampaignChips.forEach((chip) => {{
                        chip.classList.toggle("is-active", (chip.dataset.masterCampaign || "all") === postsMasterCampaignFilter);
                    }});
                    applyPostsMasterFilters();
                }};

                const openCampaignSheetsInPosts = (campaignSlug = "all") => {{
                    closePostsSheetActionMenus();
                    closePostsColumnMenus();
                    setActivePanel("bai-dang", {{ historyMode: "push" }});
                    setActivePostsTab("");
                    if (postsMasterSearchField) {{
                        postsMasterSearchField.value = "";
                    }}
                    setPostsMasterCampaignFilter(campaignSlug || "all");
                    const postsPanel = document.getElementById("bai-dang");
                    if (postsPanel && typeof postsPanel.scrollIntoView === "function") {{
                        postsPanel.scrollIntoView({{ behavior: "smooth", block: "start" }});
                    }}
                }};

                const editMetaForm = document.getElementById("edit-metadata-form");
                if (editMetaForm) {{
                    editMetaForm.addEventListener("submit", async (e) => {{
                        e.preventDefault();
                        const formData = new FormData(editMetaForm);
                        const p = Object.fromEntries(formData.entries());
                        const modal = document.getElementById("edit-metadata-modal");
                        
                        try {{
                            const res = await fetch("/api/update-sheet-metadata", {{
                                method: "POST",
                                headers: {{
                                    "Content-Type": "application/json",
                                    "X-Requested-With": "fetch",
                                }},
                                body: JSON.stringify(p)
                            }});
                            const data = await res.json();
                            if (data?.ok) {{
                                if (modal) {{
                                    modal.classList.add("hidden");
                                    modal.classList.remove("flex");
                                }}
                                showNotice(data.message || "Cập nhật thành công", "success");
                                if (typeof data.overview_html === "string") {{
                                    replaceOverviewPanelHtml(data.overview_html);
                                }}
                                if (typeof data.posts_html === "string") {{
                                    replacePostsPanelHtml(data.posts_html);
                                }}
                                if (typeof data.campaign_html === "string") {{
                                    replaceCampaignPanelHtml(data.campaign_html);
                                }}
                            }} else {{
                                showNotice(data?.message || "Lỗi cập nhật", "error");
                            }}
                        }} catch (err) {{
                            showNotice("Lỗi kết nối server", "error");
                        }}
                    }});
                }}

                const setActivePostsTab = (tabSlug) => {{
                    const safeSlug = postsTabCards.some((card) => card.dataset.postsTabTrigger === tabSlug)
                        ? tabSlug
                        : "";
                    if (postsMasterView) {{
                        postsMasterView.classList.toggle("hidden", !!safeSlug);
                    }}
                    if (postsDetailView) {{
                        postsDetailView.classList.toggle("hidden", !safeSlug);
                    }}
                    postsTabCards.forEach((card) => {{
                        card.classList.toggle("is-active", !!safeSlug && card.dataset.postsTabTrigger === safeSlug);
                    }});
                    postsTabPanels.forEach((panel) => {{
                        panel.classList.toggle("is-active", !!safeSlug && panel.dataset.postsTabPanel === safeSlug);
                    }});
                    if (postsSelectionPlaceholder) {{
                        postsSelectionPlaceholder.classList.toggle("hidden", !!safeSlug);
                    }}
                    applyPostFilters(safeSlug ? getActivePostsPanel() : null);
                }};

                const initializePostsPanel = () => {{
                    if (!postsSheetActionsCloseBound) {{
                        document.addEventListener("click", () => closePostsSheetActionMenus());
                        postsSheetActionsCloseBound = true;
                    }}
                    if (!postsColumnsMenusCloseBound) {{
                        document.addEventListener("click", () => closePostsColumnMenus());
                        postsColumnsMenusCloseBound = true;
                    }}
                    if (!overviewCampaignOpenBound) {{
                        document.addEventListener("click", (event) => {{
                            const trigger = event.target.closest("[data-overview-open-campaign]");
                            if (!trigger) return;
                            event.preventDefault();
                            openCampaignSheetsInPosts(trigger.dataset.overviewOpenCampaign || "all");
                        }});
                        overviewCampaignOpenBound = true;
                    }}

                    postsTabCards.forEach((card) => {{
                        card.addEventListener("click", (event) => {{
                            if (event.target.closest("[data-posts-sheet-action-toggle], [data-posts-sheet-action-menu]")) {{
                                return;
                            }}
                            closePostsSheetActionMenus();
                            closePostsColumnMenus();
                            setActivePostsTab(card.dataset.postsTabTrigger || "");
                        }});
                    }});

                    document.querySelectorAll("[data-posts-sheet-action-toggle]").forEach((toggle) => {{
                        toggle.addEventListener("click", (event) => {{
                            event.preventDefault();
                            event.stopPropagation();
                            closePostsColumnMenus();
                            const menu = toggle.parentElement?.querySelector("[data-posts-sheet-action-menu]");
                            if (!menu) return;
                            const shouldOpen = menu.classList.contains("hidden");
                            closePostsSheetActionMenus(shouldOpen ? menu : null);
                        }});
                    }});

                    document.querySelectorAll("[data-posts-sheet-action='open-detail']").forEach((button) => {{
                        button.addEventListener("click", (event) => {{
                            event.preventDefault();
                            event.stopPropagation();
                            closePostsSheetActionMenus();
                            closePostsColumnMenus();
                            setActivePostsTab(button.dataset.postsSheetTarget || "");
                        }});
                    }});

                    document.querySelectorAll("[data-posts-sheet-action='edit-metadata']").forEach((button) => {{
                        button.addEventListener("click", (event) => {{
                            event.preventDefault();
                            event.stopPropagation();
                            closePostsSheetActionMenus();
                            closePostsColumnMenus();
                            
                            const modal = document.getElementById("edit-metadata-modal");
                            if (!modal) return;
                            
                            modal.querySelector("[data-edit-meta-id]").value = button.dataset.postsSheetId || "";
                            modal.querySelector("[data-edit-meta-orig-name]").value = button.dataset.postsSheetName || "";
                            modal.querySelector("[data-edit-meta-name]").value = button.dataset.postsSheetName || "";
                            modal.querySelector("[data-edit-meta-brand]").value = button.dataset.postsSheetBrand || "";
                            modal.querySelector("[data-edit-meta-industry]").value = button.dataset.postsSheetIndustry || "";
                            
                            modal.classList.remove("hidden");
                            modal.classList.add("flex");
                        }});
                    }});

                    document.querySelectorAll("[data-posts-sheet-action-menu]").forEach((menu) => {{
                        menu.addEventListener("click", (event) => {{
                            event.stopPropagation();
                        }});
                    }});

                    if (postsBackButton) {{
                        postsBackButton.addEventListener("click", () => {{
                            closePostsSheetActionMenus();
                            closePostsColumnMenus();
                            setActivePostsTab("");
                        }});
                    }}

                    postsMasterCampaignChips.forEach((chip) => {{
                        chip.addEventListener("click", () => {{
                            closePostsSheetActionMenus();
                            setPostsMasterCampaignFilter(chip.dataset.masterCampaign || "all");
                        }});
                    }});

                    if (postsMasterSearchField) {{
                        postsMasterSearchField.addEventListener("input", () => {{
                            closePostsSheetActionMenus();
                            applyPostsMasterFilters();
                        }});
                    }}

                    postsTabPanels.forEach((panel) => {{
                        const searchField = panel.querySelector(".posts-search-field");
                        const rerunButton = panel.querySelector(".posts-rerun-btn");
                        const chips = Array.from(panel.querySelectorAll(".posts-chip"));
                        const selectAll = panel.querySelector("[data-select-all-posts]");
                        const rowChecks = Array.from(panel.querySelectorAll("[data-post-select]"));
                        const columnsToggle = panel.querySelector("[data-post-columns-toggle]");
                        const columnsMenu = panel.querySelector("[data-post-columns-menu]");
                        const columnsList = panel.querySelector("[data-post-columns-list]");
                        const columnsCount = panel.querySelector("[data-post-columns-count]");
                        const columnsShowAll = panel.querySelector("[data-post-columns-show-all]");
                        const postsTable = panel.querySelector(".posts-table");

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

                        if (rerunButton) {{
                            rerunButton.addEventListener("click", async () => {{
                                const targets = collectScheduleTargetsFromPanel(panel);
                                rerunButton.disabled = true;
                                await rerunSelectedPosts(targets, panel);
                                rerunButton.disabled = false;
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
                                syncPostsSelectionState();
                            }});
                        }});

                        if (columnsToggle && columnsMenu && columnsList && postsTable) {{
                            const columnDefs = Array.from(postsTable.querySelectorAll("thead th[data-post-col]"))
                                .map((th) => {{
                                    const key = th.dataset.postCol || "";
                                    const rawLabel = (th.textContent || "").trim();
                                    const displayLabelMap = {{
                                        content: "Nội dung",
                                        creator: "Creator",
                                        status: "Trạng thái",
                                        plan: "Tham gia",
                                        line: "Line",
                                        tier: "Tier",
                                        date: "Ngày",
                                        view: "View",
                                        reaction: "Reaction",
                                        share: "Share",
                                        comment: "Comment",
                                        buzz: "Buzz",
                                    }};
                                    const label = displayLabelMap[key] || rawLabel;
                                    return key && label ? {{ key, label }} : null;
                                }})
                                .filter(Boolean);
                            const hiddenColumns = new Set();

                            const renderColumnItems = () => {{
                                const visibleCount = columnDefs.filter((col) => !hiddenColumns.has(col.key)).length;
                                if (columnsCount) columnsCount.textContent = `${{visibleCount}}/${{columnDefs.length}}`;
                                if (!columnsList) return;
                                columnsList.innerHTML = columnDefs.map((col) => {{
                                    const visible = !hiddenColumns.has(col.key);
                                    return `
                                        <button type="button" class="posts-columns-item${{visible ? "" : " is-hidden"}}" data-post-columns-item="${{col.key}}">
                                            <span>${{col.label}}</span>
                                            <i class="fa-regular ${{visible ? "fa-eye" : "fa-eye-slash"}}"></i>
                                        </button>
                                    `;
                                }}).join("");
                                columnsList.querySelectorAll("[data-post-columns-item]").forEach((button) => {{
                                    button.addEventListener("click", () => {{
                                        const key = button.dataset.postColumnsItem || "";
                                        if (!key) return;
                                        if (hiddenColumns.has(key)) {{
                                            hiddenColumns.delete(key);
                                        }} else {{
                                            hiddenColumns.add(key);
                                        }}
                                        postsTable.querySelectorAll(`[data-post-col="${{key}}"]`).forEach((cell) => {{
                                            cell.classList.toggle("hidden", hiddenColumns.has(key));
                                        }});
                                        renderColumnItems();
                                    }});
                                }});
                            }};

                            renderColumnItems();

                            columnsToggle.addEventListener("click", (event) => {{
                                event.preventDefault();
                                event.stopPropagation();
                                const shouldOpen = columnsMenu.classList.contains("hidden");
                                closePostsColumnMenus(shouldOpen ? columnsMenu : null);
                            }});
                            columnsMenu.addEventListener("click", (event) => event.stopPropagation());
                            if (columnsShowAll) {{
                                columnsShowAll.addEventListener("click", () => {{
                                    hiddenColumns.clear();
                                    columnDefs.forEach((col) => {{
                                        postsTable.querySelectorAll(`[data-post-col="${{col.key}}"]`).forEach((cell) => {{
                                            cell.classList.remove("hidden");
                                        }});
                                    }});
                                    renderColumnItems();
                                }});
                            }}
                        }}
                    }});

                    if (postsTabCards.length) {{
                        const initialPostsTab = postsTabCards.find((card) => card.classList.contains("is-active"))?.dataset.postsTabTrigger || "";
                        setActivePostsTab(initialPostsTab);
                    }} else {{
                        if (postsVisibleCount) postsVisibleCount.textContent = "0 bài";
                        if (postsActiveTabLabel) postsActiveTabLabel.textContent = "Chưa chọn";
                    }}
                    applyPostsMasterFilters();
                    syncPostsSelectionState();
                }};

                const replacePostsPanelHtml = (postsHtml) => {{
                    console.log("[DASH] Updating Posts panel...");
                    if (typeof postsHtml !== "string" || !postsHtml.trim()) {{
                        console.warn("[DASH] Invalid Posts HTML received.");
                        return;
                    }}
                    const currentPostsSection = document.getElementById("bai-dang");
                    if (!currentPostsSection) {{
                        console.error("[DASH] #bai-dang section not found in DOM.");
                        return;
                    }}
                    const template = document.createElement("template");
                    template.innerHTML = postsHtml.trim();
                    const nextPostsSection = template.content.firstElementChild;
                    if (!nextPostsSection) return;
                    
                    currentPostsSection.innerHTML = nextPostsSection.innerHTML;
                    console.log("[DASH] Posts panel updated. Initializing events...");
                    syncPostsDomRefs();
                    initializePostsPanel();
                }};

                const replaceOverviewPanelHtml = (overviewHtml) => {{
                    console.log("[DASH] Updating Overview panel...");
                    if (typeof overviewHtml !== "string" || !overviewHtml.trim()) {{
                        console.warn("[DASH] Invalid Overview HTML received.");
                        return;
                    }}
                    const currentOverviewSection = document.getElementById("tong-quan");
                    if (!currentOverviewSection) {{
                        console.error("[DASH] #tong-quan section not found in DOM.");
                        return;
                    }}
                    const template = document.createElement("template");
                    template.innerHTML = overviewHtml.trim();
                    const nextOverviewSection = template.content.firstElementChild;
                    if (!nextOverviewSection) return;
                    
                    // Replace INNER content only if it starts with the shell
                    currentOverviewSection.innerHTML = nextOverviewSection.innerHTML;
                    console.log("[DASH] Overview panel updated. Initializing charts...");
                    initializeOverviewCharts();
                }};

                const replaceCampaignPanelHtml = (campaignHtml) => {{
                    if (typeof campaignHtml !== "string" || !campaignHtml.trim()) return;
                    const currentCampaignSection = document.getElementById("chien-dich");
                    if (!currentCampaignSection) return;
                    const template = document.createElement("template");
                    template.innerHTML = campaignHtml.trim();
                    const nextCampaignSection = template.content.firstElementChild;
                    if (!nextCampaignSection) return;
                    currentCampaignSection.innerHTML = nextCampaignSection.innerHTML;
                    syncPostsDomRefs();
                }};

                const setPostsRerunFeedback = (message = "", level = "info", panel = null) => {{
                    const normalized = ["success", "warning", "error", "info"].includes(level) ? level : "info";
                    const targetPanel = panel || getActivePostsPanel();
                    if (!targetPanel) return;
                    const el = targetPanel.querySelector("[data-posts-rerun-feedback]");
                    if (!el) return;
                    if (el._hideTimer) {{
                        clearTimeout(el._hideTimer);
                        el._hideTimer = null;
                    }}
                    if (!message) {{
                        el.textContent = "";
                        el.className = "posts-mini-campaign-feedback hidden";
                        return;
                    }}
                    el.textContent = message;
                    el.className = `posts-mini-campaign-feedback is-${{normalized}}`;
                    const autoHideDelay = normalized === "error" ? 7000 : 4000;
                    el._hideTimer = setTimeout(() => {{
                        el.textContent = "";
                        el.className = "posts-mini-campaign-feedback hidden";
                        el._hideTimer = null;
                    }}, autoHideDelay);
                }};

                document.addEventListener("submit", async (event) => {{
                    const createCampaignForm = event.target.closest("[data-create-campaign-form]");
                    if (createCampaignForm) {{
                        event.preventDefault();
                        const submitButton = createCampaignForm.querySelector('button[type="submit"]');
                        if (submitButton) {{
                            submitButton.disabled = true;
                        }}
                        try {{
                            const payload = Object.fromEntries(new FormData(createCampaignForm).entries());
                            const campaignLabel = String(payload.campaign_label || "").trim();
                            const response = await fetch("/create-campaign", {{
                                method: "POST",
                                headers: {{ "Content-Type": "application/json", "X-Requested-With": "fetch" }},
                                body: JSON.stringify(payload),
                            }});
                            const data = await response.json();
                            applyStatusState(data);
                            if (typeof data.posts_html === "string") {{
                                replacePostsPanelHtml(data.posts_html);
                            }}
                            if (typeof data.campaign_html === "string") {{
                                replaceCampaignPanelHtml(data.campaign_html);
                            }}
                            showNotice(
                                data.message || (data.ok ? `Đã tạo chiến dịch: ${{campaignLabel}}` : "Không tạo được chiến dịch."),
                                data.level || (data.ok ? "success" : "error")
                            );
                        }} catch (_) {{
                            showNotice("Không tạo được chiến dịch. Vui lòng thử lại.", "error");
                        }} finally {{
                            if (submitButton) {{
                                submitButton.disabled = false;
                            }}
                        }}
                        return;
                    }}
                    const campaignForm = event.target.closest("[data-campaign-form]");
                    if (!campaignForm) return;
                    event.preventDefault();
                    const submitButton = campaignForm.querySelector('button[type="submit"]');
                    if (submitButton) {{
                        submitButton.disabled = true;
                    }}
                    try {{
                        const payload = Object.fromEntries(new FormData(campaignForm).entries());
                        const response = await fetch("/set-sheet-campaign", {{
                            method: "POST",
                            headers: {{ "Content-Type": "application/json", "X-Requested-With": "fetch" }},
                            body: JSON.stringify(payload),
                        }});
                        const data = await response.json();
                        applyStatusState(data);
                        if (typeof data.overview_html === "string") {{
                            replaceOverviewPanelHtml(data.overview_html);
                        }}
                        if (typeof data.posts_html === "string") {{
                            replacePostsPanelHtml(data.posts_html);
                        }}
                        if (typeof data.campaign_html === "string") {{
                            replaceCampaignPanelHtml(data.campaign_html);
                        }}
                        showNotice(
                            data.message || (data.ok ? "Đã lưu chiến dịch cho sheet." : "Không lưu được chiến dịch."),
                            data.level || (data.ok ? "success" : "error")
                        );
                    }} catch (_) {{
                        showNotice("Không lưu được chiến dịch. Vui lòng thử lại.", "error");
                    }} finally {{
                        if (submitButton) {{
                            submitButton.disabled = false;
                        }}
                    }}
                }});

                const getPanelPath = (sectionId) => sectionId === "tong-quan" ? "/" : `/${{sectionId}}`;
                const getPanelIdFromPath = (pathname) => {{
                    const cleaned = String(pathname || "/").replace(/^\\/+|\\/+$/g, "");
                    if (!cleaned) {{
                        return "tong-quan";
                    }}
                    if (cleaned === "chien-dich" || cleaned === "nhan-vien") {{
                        return "cai-dat";
                    }}
                    return cleaned;
                }};

                const setActivePanel = (sectionId, options = {{}}) => {{
                    const updateHistory = options.updateHistory !== false;
                    const historyMode = options.historyMode || "replace";
                    const availableIds = dashboardSections.map((section) => section.dataset.dashboardSection);
                    const targetId = availableIds.includes(sectionId) ? sectionId : "tong-quan";

                    sidebarLinks.forEach((link) => {{
                        link.classList.toggle("is-active", link.dataset.navLink === targetId);
                    }});
                    dashboardSections.forEach((section) => {{
                        section.classList.toggle("is-active", section.dataset.dashboardSection === targetId);
                    }});

                    if (updateHistory) {{
                        const targetPath = getPanelPath(targetId);
                        if ((window.location.pathname || "/") !== targetPath) {{
                            const historyFn = historyMode === "push" ? history.pushState.bind(history) : history.replaceState.bind(history);
                            historyFn(null, "", targetPath);
                        }}
                    }}
                }};

                sidebarLinks.forEach((link) => {{
                    link.addEventListener("click", (event) => {{
                        event.preventDefault();
                        setActivePanel(link.dataset.navLink || "tong-quan", {{ historyMode: "push" }});
                    }});
                }});

                window.addEventListener("popstate", () => {{
                    setActivePanel(getPanelIdFromPath(window.location.pathname), {{ updateHistory: false }});
                }});

                const loadAsyncDashboardData = async () => {{
                    console.log("[DASH] Starting background population...");
                    const panels = [
                        {{ id: "overview", url: "/api/dashboard/overview", replacer: replaceOverviewPanelHtml }},
                        {{ id: "posts", url: "/api/dashboard/posts", replacer: replacePostsPanelHtml }},
                        {{ id: "config", url: "/api/dashboard/config", replacer: (html) => {{
                            const el = document.getElementById("cau-hinh");
                            if (el) el.innerHTML = html;
                            window.bindSheetTabLookupControls?.();
                        }} }},
                        {{ id: "schedule", url: "/api/dashboard/schedule", replacer: (html) => {{
                            const el = document.getElementById("lich-tu-dong");
                            if (el) el.innerHTML = html;
                        }} }}
                    ];

                    panels.forEach(async (panel) => {{
                        let attempts = 0;
                        const fetchData = async () => {{
                            try {{
                                console.log(`[DASH] Fetching ${{panel.id}} (attempt ${{attempts + 1}})...`);
                                const res = await fetch(panel.url, {{ headers: {{ "X-Requested-With": "fetch" }} }});
                                if (!res.ok) throw new Error(`API status ${{res.status}}`);
                                const data = await res.json();
                                
                                if (data.status === "processing") {{
                                    attempts++;
                                    if (attempts < 15) {{ // Retry for up to ~1 minute
                                        console.log(`[DASH] ${{panel.id}} is processing, retrying in 5s...`);
                                        setTimeout(fetchData, 5000);
                                    }} else {{
                                        console.warn(`[DASH] ${{panel.id}} timed out after 15 attempts.`);
                                    }}
                                    return;
                                }}

                                if (data.ok && data.html) {{
                                    console.log(`[DASH] ${{panel.id}} ready. Updating UI...`);
                                    panel.replacer(data.html);
                                }}
                            }} catch (e) {{
                                console.error(`[DASH] ${{panel.id}} load error:`, e);
                            }}
                        }};
                        fetchData();
                    }});
                }};

                // 1. Start data loading immediately
                loadAsyncDashboardData();

                // 2. Initialize UI components with protection
                try {{
                    initializeOverviewCharts();
                }} catch (e) {{
                    console.warn("[DASH] initializeOverviewCharts failed on skeleton:", e);
                }}

                try {{
                    initializePostsPanel();
                }} catch (e) {{
                    console.warn("[DASH] initializePostsPanel failed on skeleton:", e);
                }}

                try {{
                    setActivePanel(initialDashboardSection || getPanelIdFromPath(window.location.pathname), {{ updateHistory: false }});
                }} catch (e) {{
                    console.error("[DASH] setActivePanel failed:", e);
                }}

                // 3. Start polling
                refreshDashboard();
                setInterval(refreshDashboard, 1200);
            }});
        </script>
    </head>
    <body class="bg-[#0b0f1a] text-slate-200 min-h-screen p-2 md:p-3">
        <div class="dashboard-shell" id="dashboard-shell">
            <aside class="dashboard-sidebar">
                <div class="sidebar-brand">
                    <div class="sidebar-brand-copy">
                        <div class="sidebar-brand-title">Social Monitor</div>
                    </div>
                    <div class="sidebar-brand-actions">
                        <div class="sidebar-pulse"><i class="fa-solid fa-compass-drafting"></i></div>
                        <button type="button" id="sidebar-collapse-toggle" class="sidebar-collapse-btn" title="Thu gọn menu" aria-label="Thu gọn menu">
                            <span id="sidebar-collapse-icon"><i class="fa-solid fa-angles-left"></i></span>
                        </button>
                    </div>
                </div>
                <nav class="sidebar-nav">
                    <a href="/tong-quan" class="sidebar-link is-active" data-nav-link="tong-quan" title="Tổng quan"><span class="sidebar-link-icon"><i class="fa-solid fa-gauge-high"></i></span><span class="sidebar-link-label">Tổng quan</span></a>
                    <a href="/cau-hinh" class="sidebar-link" data-nav-link="cau-hinh" title="Cấu hình"><span class="sidebar-link-icon"><i class="fa-solid fa-sliders"></i></span><span class="sidebar-link-label">Cấu hình</span></a>
                    <a href="/bai-dang" class="sidebar-link" data-nav-link="bai-dang" title="Bài đăng"><span class="sidebar-link-icon"><i class="fa-regular fa-newspaper"></i></span><span class="sidebar-link-label">Bài đăng</span></a>
                    <a href="/lich-tu-dong" class="sidebar-link" data-nav-link="lich-tu-dong" title="Lịch tự động"><span class="sidebar-link-icon"><i class="fa-regular fa-calendar-days"></i></span><span class="sidebar-link-label">Lịch tự động</span></a>
                    <a href="/theo-doi-lan-chay" class="sidebar-link" data-nav-link="theo-doi-lan-chay" title="Theo dõi lần chạy"><span class="sidebar-link-icon"><i class="fa-solid fa-wave-square"></i></span><span class="sidebar-link-label">Theo dõi lần chạy</span></a>
                    <a href="/cai-dat" class="sidebar-link" data-nav-link="cai-dat" title="Cài đặt"><span class="sidebar-link-icon"><i class="fa-solid fa-gear"></i></span><span class="sidebar-link-label">Cài đặt</span></a>
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
                        {metric_cols_html}
                    </section>

                    {posts_html}
                    {settings_panel_html}

                    <section id="lich-tu-dong" data-dashboard-section="lich-tu-dong" class="dashboard-section dashboard-panel mb-6">
                        <div class="dashboard-section-title">Lịch tự động</div>
                        <div class="bg-black/20 rounded-2xl p-4 md:p-5 border border-white/5">
                            <div class="flex justify-between items-center mb-3 text-sm font-bold text-slate-500 uppercase">
                                <span>Lịch tự động</span><span class="hidden" data-schedule-label>{schedule_text}</span>
                            </div>
                            <form action="/set-schedule" method="get" class="flex flex-col gap-2">
                                <div>
                                    <label class="block text-[11px] text-slate-400 mb-1 uppercase tracking-wider">Sheet áp dụng cho lịch</label>
                                    <input id="schedule-sheet-search" type="text" list="schedule-sheet-datalist" autocomplete="off" placeholder="Gõ để tìm hoặc chọn sheet..." value="{html.escape(runtime_state['schedule_sheet_name'] or '', quote=True)}" class="w-full bg-slate-900 text-slate-100 rounded-xl px-3 py-2 border border-white/10 outline-none focus:border-cyan-400" />
                                    <datalist id="schedule-sheet-datalist"></datalist>
                                    <select id="schedule-sheet-select" name="sheet_binding" class="hidden">
                                        {schedule_config["sheet_options_html"]}
                                    </select>
                                    <p class="mt-1 text-[10px] text-slate-500">Gõ tên sheet ở ngay ô này để hiện gợi ý, rồi chọn luôn trong cùng một dòng.</p>
                                </div>
                                <div class="grid grid-cols-1 md:grid-cols-2 gap-2">
                                    <div>
                                        <label class="block text-[11px] text-slate-400 mb-1 uppercase tracking-wider">Chế độ chạy</label>
                                        <select id="schedule-mode-select" name="mode" class="w-full bg-slate-900 text-slate-100 rounded-xl px-3 py-2 border border-white/10 outline-none focus:border-cyan-400">
                                            <option value="off" {mode_selected["off"]}>Chưa bật</option>
                                            <option value="daily" {mode_selected["daily"]}>Hằng ngày</option>
                                            <option value="weekly" {mode_selected["weekly"]}>Hằng tuần</option>
                                            <option value="monthly" {mode_selected["monthly"]}>Hằng tháng</option>
                                        </select>
                                    </div>
                                    <div>
                                        <label class="block text-[11px] text-slate-400 mb-1 uppercase tracking-wider">Giờ chạy (HH:MM)</label>
                                        <input name="at" value="{runtime_state['schedule_time']}" placeholder="VD: 09:00" class="w-full bg-slate-900 text-slate-100 rounded-xl px-3 py-2 border border-white/10 outline-none focus:border-cyan-400" />
                                    </div>
                                </div>
                                <div class="grid grid-cols-1 md:grid-cols-2 gap-2">
                                    <div id="schedule-weekday-shell">
                                        <label class="block text-[11px] text-slate-400 mb-1 uppercase tracking-wider">Thứ trong tuần</label>
                                        <select id="schedule-weekday-select" name="weekday" class="w-full bg-slate-900 text-slate-100 rounded-xl px-3 py-2 border border-white/10 outline-none focus:border-cyan-400">
                                            {weekday_options}
                                        </select>
                                        <p class="mt-1 text-[10px] text-slate-500">Dùng cho chế độ hằng tuần.</p>
                                    </div>
                                    <div id="schedule-monthdate-shell">
                                        <label class="block text-[11px] text-slate-400 mb-1 uppercase tracking-wider">Ngày trong tháng / lịch xem trước</label>
                                        <div class="date-shell">
                                            <input id="schedule-monthdate-input" name="monthdate" type="text" value="{schedule_date_value}" placeholder="YYYY-MM-DD" class="flex-1 bg-slate-900 text-slate-100 rounded-xl px-3 py-2 border border-white/10 outline-none focus:border-cyan-400" />
                                            <button id="monthdate-picker-btn" type="button" class="date-picker-btn" title="Mở lịch">
                                                <i class="fa-solid fa-calendar-days"></i>
                                            </button>
                                        </div>
                                        <input name="monthday" type="hidden" value="{runtime_state['schedule_monthday']}" />
                                        <p id="schedule-monthdate-help" class="mt-1 text-[10px] text-slate-500">Hằng tháng: chọn ngày chạy. Hằng tuần: mở lịch để xem các ngày của thứ đã chọn được khoanh sẵn.</p>
                                    </div>
                                </div>
                                <div class="grid grid-cols-1 gap-2">
                                    <div>
                                        <label class="block text-[11px] text-slate-400 mb-1 uppercase tracking-wider">Ngày kết thúc vòng lặp</label>
                                        <div class="date-shell">
                                            <input id="schedule-enddate-input" name="enddate" type="text" value="{schedule_end_value}" placeholder="YYYY-MM-DD" class="flex-1 bg-slate-900 text-slate-100 rounded-xl px-3 py-2 border border-white/10 outline-none focus:border-cyan-400" />
                                            <button id="enddate-picker-btn" type="button" class="date-picker-btn" title="Mở lịch kết thúc">
                                                <i class="fa-solid fa-calendar-check"></i>
                                            </button>
                                        </div>
                                        <p class="mt-1 text-[10px] text-slate-500">Để trống nếu muốn lặp vô thời hạn. Nếu có ngày này thì lịch sẽ tự dừng sau ngày đã chọn.</p>
                                    </div>
                                </div>
                                <button type="submit" class="w-full py-2 bg-slate-700 hover:bg-slate-600 text-white rounded-xl font-black uppercase text-xs shadow-sm shadow-slate-900/10">Lưu lịch</button>
                            </form>
                        </div>
                    </section>

                    <section id="theo-doi-lan-chay" data-dashboard-section="theo-doi-lan-chay" class="dashboard-section dashboard-panel mb-6">
                        <div class="dashboard-section-title">Theo dõi lần chạy</div>
                        <div class="bg-black/20 rounded-2xl p-4 md:p-5 border border-white/5">
                            <div class="grid grid-cols-1 gap-4">
                                <div class="rounded-2xl border border-white/10 bg-slate-950/35 px-3 py-3">
                                    <div class="flex items-center justify-between gap-3 mb-3">
                                        <div>
                                            <div class="text-[11px] uppercase tracking-[0.22em] text-slate-400 font-black">Sheet đã đặt lịch</div>
                                            <div class="mt-1 text-xs text-slate-500">Bấm vào sheet nào thì bảng tracking bên dưới hiện theo sheet đó.</div>
                                        </div>
                                    </div>
                                    <div id="schedule-track-list" class="schedule-track-list-shell overflow-x-auto">
                                        {schedule_tracking["entries_html"]}
                                    </div>
                                </div>
                                <div id="schedule-track-detail-shell" class="rounded-2xl border border-white/10 bg-slate-950/35 px-3 py-3">
                                    <div class="flex items-center justify-between gap-3 mb-3">
                                        <div>
                                            <div class="text-[11px] uppercase tracking-[0.22em] text-slate-400 font-black">Theo dõi lần chạy</div>
                                            <div id="schedule-track-active-name" class="mt-1 text-sm font-black text-slate-100">{html.escape(schedule_tracking["active_sheet_name"] or "Chưa chọn sheet")}</div>
                                        </div>
                                        <div class="text-xs text-slate-500">Tự cập nhật theo lịch và khi bấm chạy tay</div>
                                    </div>
                                    <div id="schedule-track-empty-state" class="rounded-2xl border border-dashed border-white/10 bg-slate-900/35 px-3 py-8 text-center {'hidden' if schedule_tracking['has_active_entry'] else ''}">
                                        <div class="text-sm font-black text-slate-200">Chọn một sheet ở danh sách bên trên</div>
                                        <div class="mt-2 text-xs text-slate-500">Khi bạn bấm đúng sheet muốn xem, bảng tracking bên dưới mới hiện chi tiết của sheet đó.</div>
                                    </div>
                                    <div id="schedule-track-detail-body" class="grid grid-cols-1 2xl:grid-cols-[minmax(0,1fr),340px] gap-4 items-start {'hidden' if not schedule_tracking['has_active_entry'] else ''}">
                                        <div>
                                            <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-5 gap-2">
                                                <div class="bg-slate-900/60 rounded-xl px-3 py-2 border border-white/8">
                                                    <div class="text-[11px] uppercase tracking-[0.18em] text-slate-500 font-bold">Lần kế tiếp</div>
                                                    <div id="schedule-track-next" class="mt-2 text-sm font-black text-slate-100">{html.escape(schedule_tracking["next_run_text"])}</div>
                                                </div>
                                                <div class="bg-slate-900/60 rounded-xl px-3 py-2 border border-white/8">
                                                    <div class="text-[11px] uppercase tracking-[0.18em] text-slate-500 font-bold">Bắt đầu gần nhất</div>
                                                    <div id="schedule-track-started" class="mt-2 text-sm font-black text-slate-100">{html.escape(schedule_tracking["last_started_text"])}</div>
                                                </div>
                                                <div class="bg-slate-900/60 rounded-xl px-3 py-2 border border-white/8">
                                                    <div class="text-[11px] uppercase tracking-[0.18em] text-slate-500 font-bold">Kết thúc gần nhất</div>
                                                    <div id="schedule-track-finished" class="mt-2 text-sm font-black text-slate-100">{html.escape(schedule_tracking["last_finished_text"])}</div>
                                                </div>
                                                <div class="bg-slate-900/60 rounded-xl px-3 py-2 border border-white/8">
                                                    <div class="text-[11px] uppercase tracking-[0.18em] text-slate-500 font-bold">Thời lượng</div>
                                                    <div id="schedule-track-duration" class="mt-2 text-sm font-black text-cyan-200">{html.escape(schedule_tracking["last_duration_text"])}</div>
                                                </div>
                                                <div class="bg-slate-900/60 rounded-xl px-3 py-2 border border-white/8">
                                                    <div class="text-[11px] uppercase tracking-[0.18em] text-slate-500 font-bold">Đang chạy từ</div>
                                                    <div id="schedule-track-running" class="mt-2 text-sm font-black text-slate-100">{html.escape(schedule_tracking["is_running_text"])}</div>
                                                </div>
                                            </div>
                                            <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-5 gap-2 mt-2">
                                                <div class="bg-slate-900/60 rounded-xl px-3 py-2 border border-white/8">
                                                    <div class="text-[11px] uppercase tracking-[0.18em] text-slate-500 font-bold">Trạng thái</div>
                                                    <div id="schedule-track-status" class="mt-2 text-sm font-black text-slate-100">{html.escape(schedule_tracking["last_status_text"])}</div>
                                                </div>
                                                <div class="bg-slate-900/60 rounded-xl px-3 py-2 border border-white/8">
                                                    <div class="text-[11px] uppercase tracking-[0.18em] text-slate-500 font-bold">Nguồn chạy</div>
                                                    <div id="schedule-track-source" class="mt-2 text-sm font-black text-slate-100">{html.escape(schedule_tracking["last_source_text"])}</div>
                                                </div>
                                                <div class="bg-slate-900/60 rounded-xl px-3 py-2 border border-white/8">
                                                    <div class="text-[11px] uppercase tracking-[0.18em] text-slate-500 font-bold">Tab đã chạy</div>
                                                    <div id="schedule-track-sheet" class="mt-2 text-sm font-black text-slate-100">{html.escape(schedule_tracking["last_sheet_text"])}</div>
                                                </div>
                                                <div class="bg-slate-900/60 rounded-xl px-3 py-2 border border-white/8">
                                                    <div class="text-[11px] uppercase tracking-[0.18em] text-slate-500 font-bold">Link đã quét</div>
                                                    <div id="schedule-track-processed" class="mt-2 text-sm font-black text-slate-100">{html.escape(schedule_tracking["last_processed_text"])}</div>
                                                </div>
                                                <div class="bg-slate-900/60 rounded-xl px-3 py-2 border border-white/8">
                                                    <div class="text-[11px] uppercase tracking-[0.18em] text-slate-500 font-bold">Thành công / trượt</div>
                                                    <div class="mt-2 text-sm font-black text-slate-100"><span id="schedule-track-success">{html.escape(schedule_tracking["last_success_text"])}</span> / <span id="schedule-track-failed">{html.escape(schedule_tracking["last_failed_text"])}</span></div>
                                                </div>
                                            </div>
                                            <div class="mt-3">
                                                <div class="text-[11px] uppercase tracking-[0.22em] text-slate-500 font-black mb-2">Lịch sử gần nhất</div>
                                                <div id="schedule-track-history" class="grid gap-2">
                                                    {schedule_tracking["history_html"]}
                                                </div>
                                            </div>
                                        </div>
                                        <div class="rounded-2xl border border-white/10 bg-slate-900/40 p-3">
                                            <div class="text-[11px] uppercase tracking-[0.22em] text-slate-400 font-black">Lịch xem trước</div>
                                            <div id="schedule-track-calendar-title" class="mt-2 text-base font-black text-slate-100">{html.escape(schedule_tracking["calendar_title"])}</div>
                                            <div id="schedule-track-calendar-subtext" class="mt-1 text-xs text-slate-400 leading-5">{html.escape(schedule_tracking["calendar_subtext"])}</div>
                                            <div id="schedule-track-calendar" class="mt-4">
                                                {schedule_tracking["calendar_html"]}
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </section>
                </div>

                <div id="edit-metadata-modal" class="fixed inset-0 z-[100] hidden items-center justify-center p-4 bg-slate-950/80 backdrop-blur-md">
                    <div class="bg-slate-900 border border-white/10 rounded-[2.5rem] w-full max-w-lg p-8 shadow-2xl shadow-black/50">
                        <div class="mb-6">
                            <h3 class="text-2xl font-black text-white">Chỉnh sửa thông tin sheet</h3>
                            <p class="text-slate-400 text-sm mt-1">Cập nhật các nhãn hiển thị cho sheet này trong danh sách.</p>
                        </div>
                        <form id="edit-metadata-form" class="space-y-5">
                            <input type="hidden" data-edit-meta-id name="sheet_id" />
                            <input type="hidden" data-edit-meta-orig-name name="original_name" />
                            
                            <div class="space-y-1.5">
                                <label class="text-[11px] uppercase tracking-widest text-slate-500 font-black ml-1">Tên hiển thị</label>
                                <input type="text" data-edit-meta-name name="sheet_name" class="w-full bg-slate-800 border-0 rounded-2xl p-4 text-white placeholder-slate-500 focus:ring-2 focus:ring-emerald-500 transition-all" placeholder="Nhập tên mới cho sheet..." />
                            </div>

                            <div class="space-y-1.5">
                                <label class="text-[11px] uppercase tracking-widest text-slate-500 font-black ml-1">Thương hiệu</label>
                                <input type="text" data-edit-meta-brand name="brand_label" class="w-full bg-slate-800 border-0 rounded-2xl p-4 text-white placeholder-slate-500 focus:ring-2 focus:ring-emerald-500 transition-all" placeholder="Tên thương hiệu..." />
                            </div>

                            <div class="space-y-1.5">
                                <label class="text-[11px] uppercase tracking-widest text-slate-500 font-black ml-1">Ngành hàng / Mục lục</label>
                                <input type="text" data-edit-meta-industry name="industry_label" class="w-full bg-slate-800 border-0 rounded-2xl p-4 text-white placeholder-slate-500 focus:ring-2 focus:ring-emerald-500 transition-all" placeholder="Ngành hàng..." />
                            </div>

                            <div class="flex gap-3 pt-4">
                                <button type="button" onclick="document.getElementById('edit-metadata-modal').classList.add('hidden'); document.getElementById('edit-metadata-modal').classList.remove('flex');" class="flex-1 py-4 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-2xl font-bold transition-all">Hủy</button>
                                <button type="submit" class="flex-[2] py-4 bg-emerald-600 hover:bg-emerald-500 text-white rounded-2xl font-black shadow-lg shadow-emerald-900/20 transition-all">Lưu thay đổi</button>
                            </div>
                        </form>
                    </div>
                </div>
            </main>
        </div>
    </body>
    </html>
    """


if __name__ == "__main__":
    uvicorn.run("scraper:app", host="127.0.0.1", port=8000, reload=True)

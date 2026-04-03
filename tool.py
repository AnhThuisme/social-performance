import json
import os
import re
import time
from datetime import datetime
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound
from social_selenium import create_selenium_driver, close_selenium_driver, fetch_social_stats

# ==========================================
# CAU HINH THONG SO
# ==========================================
BASE_DIR = Path(__file__).resolve().parent
SERVICE_ACCOUNT_FILE = str(BASE_DIR / "credential.json")

config_path = BASE_DIR / "config.json"
config = {}
if config_path.exists():
    with config_path.open("r", encoding="utf-8") as f:
        config = json.load(f)

def extract_sheet_id(value: str) -> str:
    if not value:
        return ""
    value = value.strip()
    m = re.search(r"/spreadsheets/(?:u/\\d+/)?d/([a-zA-Z0-9-_]+)", value)
    if m:
        return m.group(1)
    return value

SHEET_ID = extract_sheet_id(config.get("sheet_id", "1UrjPxF-YdRuk9j5LzKRA17_QzneGdNchz3DuWbhv1gs"))
SHEET_NAME = config.get("sheet_name", "Tracking")
ROW_SCAN_DELAY_SECONDS = float(os.getenv("ROW_SCAN_DELAY_SECONDS", "0.12"))

YOUTUBE_API_KEY = "AIzaSyAbMDEzmIVpsVTASYhTaXI6oC7BudQWzlU"
creds = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ],
)
gc = gspread.authorize(creds)
print(f"Config path: {config_path}")
print(f"Using SHEET_ID: {SHEET_ID}")
print(f"Using SHEET_NAME: {SHEET_NAME}")
try:
    sheet = gc.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
except SpreadsheetNotFound:
    raise SystemExit(
        f"Không tìm thấy spreadsheet '{SHEET_ID}'. "
        "Kiểm tra sheet_id trong config.json và share file cho service account."
    )
except WorksheetNotFound:
    raise SystemExit(
        f"Không tìm thấy tab '{SHEET_NAME}'. "
        "Kiểm tra sheet_name trong config.json."
    )


def get_youtube_stats(url):
    try:
        video_id_match = re.search(r"(?:v=|\/)([0-9A-Za-z_-]{11}).*", url)
        if not video_id_match:
            return None
        video_id = video_id_match.group(1)
        youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
        item = youtube.videos().list(part="statistics,snippet", id=video_id).execute()["items"][0]
        return {
            "v": int(item["statistics"].get("viewCount", 0)),
            "l": int(item["statistics"].get("likeCount", 0)),
            "s": 0,
            "c": int(item["statistics"].get("commentCount", 0)),
            "cap": item["snippet"].get("title", ""),
        }
    except Exception:
        return None


def get_social_stats(url, platform):
    return fetch_social_stats(url, platform, logger=print)


def main():
    urls = sheet.col_values(4)
    now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    social_driver = None
    social_driver_failed = False
    print("--- BẮT ĐẦU QUÉT ---")

    try:
        for i in range(2, len(urls) + 1):
            url = urls[i - 1].strip()
            if not url or "http" not in url:
                continue
            print(f"Đang quét dòng {i}: {url[:40]}...")

            stats = None
            if "youtube" in url or "youtu.be" in url:
                stats = get_youtube_stats(url)
            else:
                platform = ""
                if "facebook.com" in url or "fb.watch" in url:
                    platform = "facebook"
                elif "tiktok.com" in url:
                    platform = "tiktok"
                elif "instagram.com" in url:
                    platform = "instagram"

                if platform:
                    if social_driver is None and not social_driver_failed:
                        try:
                            social_driver = create_selenium_driver(logger=print)
                        except Exception as exc:
                            social_driver_failed = True
                            print(f"[!] Không mở được Selenium: {exc}")
                    if not social_driver_failed:
                        stats = fetch_social_stats(url, platform, driver=social_driver, logger=print)

            if stats:
                sheet.update_acell(f"A{i}", now)
                sheet.update(
                    range_name=f"E{i}:I{i}",
                    values=[[stats["v"], stats["l"], stats["s"], stats["c"], stats["cap"]]],
                )
                print("Cập nhật thành công")
            time.sleep(max(0.0, ROW_SCAN_DELAY_SECONDS))
    finally:
        close_selenium_driver(social_driver)


if __name__ == "__main__":
    main()

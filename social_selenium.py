import json
import os
import re
import time
import urllib.parse
from datetime import datetime
from typing import Callable, Optional

import requests
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.edge.options import Options as EdgeOptions

def _env_float(name: str, default: float, min_value: float) -> float:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
        return value if value >= min_value else default
    except Exception:
        return default


DEFAULT_PAGE_LOAD_TIMEOUT_SECONDS = _env_float("SELENIUM_PAGE_LOAD_TIMEOUT_SECONDS", 20.0, 5.0)
TIKTOK_PAGE_LOAD_TIMEOUT_SECONDS = _env_float("SELENIUM_TIKTOK_PAGE_LOAD_TIMEOUT_SECONDS", 12.0, 3.0)
FACEBOOK_PAGE_LOAD_TIMEOUT_SECONDS = _env_float("SELENIUM_FACEBOOK_PAGE_LOAD_TIMEOUT_SECONDS", 10.0, 3.0)
INSTAGRAM_PAGE_LOAD_TIMEOUT_SECONDS = _env_float("SELENIUM_INSTAGRAM_PAGE_LOAD_TIMEOUT_SECONDS", 10.0, 3.0)
TIKTOK_SOFT_RETRY_ATTEMPTS = int(_env_float("SELENIUM_TIKTOK_SOFT_RETRY_ATTEMPTS", 1.0, 0.0))
TIKTOK_SOFT_RETRY_DELAY_SECONDS = _env_float("SELENIUM_TIKTOK_SOFT_RETRY_DELAY_SECONDS", 1.2, 0.0)
DEFAULT_SETTLE_SECONDS = _env_float("SELENIUM_SETTLE_SECONDS", 1.7, 0.1)
READY_POLL_SECONDS = _env_float("SELENIUM_READY_POLL_SECONDS", 0.25, 0.05)
READY_TIMEOUT_SECONDS = _env_float("SELENIUM_READY_TIMEOUT_SECONDS", 8.0, 1.0)
TIKTOK_MANUAL_CHALLENGE_TIMEOUT_SECONDS = _env_float("TIKTOK_MANUAL_CHALLENGE_TIMEOUT_SECONDS", 40.0, 5.0)
TIKTOK_MANUAL_CHALLENGE_POLL_SECONDS = _env_float("TIKTOK_MANUAL_CHALLENGE_POLL_SECONDS", 1.2, 0.2)
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/136.0.0.0 Safari/537.36"
)
_FB_COOKIES_CACHE = None
_FB_COOKIES_CACHE_KEY = None
_TT_COOKIES_CACHE = None
_TT_COOKIES_CACHE_KEY = None


def _emit(logger: Optional[Callable[[str], None]], message: str):
    if not logger:
        return
    try:
        logger(message)
    except Exception:
        pass


def _load_fb_cookies_from_env(logger: Optional[Callable[[str], None]] = None):
    global _FB_COOKIES_CACHE, _FB_COOKIES_CACHE_KEY
    raw = (os.getenv("FB_COOKIES_JSON") or "").strip()
    if not raw:
        _FB_COOKIES_CACHE = []
        _FB_COOKIES_CACHE_KEY = ""
        return []
    if raw == _FB_COOKIES_CACHE_KEY and _FB_COOKIES_CACHE is not None:
        return _FB_COOKIES_CACHE
    try:
        payload = json.loads(raw)
        cookies = payload if isinstance(payload, list) else payload.get("cookies", [])
        normalized = []
        for item in cookies if isinstance(cookies, list) else []:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            value = str(item.get("value") or "")
            if not name:
                continue
            cookie = {"name": name, "value": value}
            domain = str(item.get("domain") or "").strip()
            path = str(item.get("path") or "").strip() or "/"
            if domain:
                cookie["domain"] = domain
            cookie["path"] = path
            if "secure" in item:
                cookie["secure"] = bool(item.get("secure"))
            if "httpOnly" in item:
                cookie["httpOnly"] = bool(item.get("httpOnly"))
            expiry = item.get("expiry")
            try:
                if expiry is not None:
                    cookie["expiry"] = int(expiry)
            except Exception:
                pass
            normalized.append(cookie)
        _FB_COOKIES_CACHE_KEY = raw
        _FB_COOKIES_CACHE = normalized
        return normalized
    except Exception as exc:
        _emit(logger, f"FB_COOKIES_JSON không hợp lệ: {str(exc)[:120]}")
        _FB_COOKIES_CACHE_KEY = raw
        _FB_COOKIES_CACHE = []
        return []


def _ensure_facebook_cookies(driver, logger: Optional[Callable[[str], None]] = None):
    if getattr(driver, "_fb_cookies_applied", False):
        return
    cookies = _load_fb_cookies_from_env(logger=logger)
    if not cookies:
        driver._fb_cookies_applied = True
        return
    try:
        driver.get("https://www.facebook.com/")
    except Exception:
        pass
    applied = 0
    for cookie in cookies:
        try:
            driver.add_cookie(cookie)
            applied += 1
        except Exception:
            continue
    try:
        driver.get("https://www.facebook.com/")
    except Exception:
        pass
    driver._fb_cookies_applied = True
    _emit(logger, f"Đã nạp {applied}/{len(cookies)} cookie Facebook trước khi quét.")


def _load_tt_cookies_from_env(logger: Optional[Callable[[str], None]] = None):
    global _TT_COOKIES_CACHE, _TT_COOKIES_CACHE_KEY
    raw = (os.getenv("TT_COOKIES_JSON") or "").strip()
    if not raw:
        _TT_COOKIES_CACHE = []
        _TT_COOKIES_CACHE_KEY = ""
        return []
    if raw == _TT_COOKIES_CACHE_KEY and _TT_COOKIES_CACHE is not None:
        return _TT_COOKIES_CACHE
    try:
        payload = json.loads(raw)
        cookies = payload if isinstance(payload, list) else payload.get("cookies", [])
        normalized = []
        for item in cookies if isinstance(cookies, list) else []:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            value = str(item.get("value") or "")
            if not name:
                continue
            cookie = {"name": name, "value": value}
            domain = str(item.get("domain") or "").strip() or ".tiktok.com"
            path = str(item.get("path") or "").strip() or "/"
            cookie["domain"] = domain
            cookie["path"] = path
            if "secure" in item:
                cookie["secure"] = bool(item.get("secure"))
            if "httpOnly" in item:
                cookie["httpOnly"] = bool(item.get("httpOnly"))
            expiry = item.get("expiry")
            try:
                if expiry is not None:
                    cookie["expiry"] = int(expiry)
            except Exception:
                pass
            normalized.append(cookie)
        _TT_COOKIES_CACHE_KEY = raw
        _TT_COOKIES_CACHE = normalized
        return normalized
    except Exception as exc:
        _emit(logger, f"TT_COOKIES_JSON không hợp lệ: {str(exc)[:120]}")
        _TT_COOKIES_CACHE_KEY = raw
        _TT_COOKIES_CACHE = []
        return []


def _ensure_tiktok_cookies(driver, logger: Optional[Callable[[str], None]] = None):
    if getattr(driver, "_tt_cookies_applied", False):
        return
    cookies = _load_tt_cookies_from_env(logger=logger)
    if not cookies:
        driver._tt_cookies_applied = True
        return
    try:
        driver.get("https://www.tiktok.com/")
    except Exception:
        pass
    applied = 0
    for cookie in cookies:
        try:
            driver.add_cookie(cookie)
            applied += 1
        except Exception:
            continue
    try:
        driver.get("https://www.tiktok.com/")
    except Exception:
        pass
    driver._tt_cookies_applied = True
    _emit(logger, f"Đã nạp {applied}/{len(cookies)} cookie TikTok trước khi quét.")


def _add_common_browser_args(options, headless: bool = True):
    args = [
        "--disable-gpu",
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-notifications",
        "--disable-popup-blocking",
        "--disable-blink-features=AutomationControlled",
        "--window-size=1440,2200",
        "--lang=en-US",
        f"--user-agent={DEFAULT_USER_AGENT}",
    ]
    if headless:
        args.insert(0, "--headless=new")
    for arg in args:
        options.add_argument(arg)


def _apply_stealth(driver):
    script = """
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4] });
        window.chrome = window.chrome || { runtime: {} };
    """
    try:
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": script})
    except Exception:
        pass


def _build_chrome_driver(headless: bool = True):
    options = ChromeOptions()
    _add_common_browser_args(options, headless=headless)
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(DEFAULT_PAGE_LOAD_TIMEOUT_SECONDS)
    _apply_stealth(driver)
    return driver


def _build_edge_driver(headless: bool = True):
    options = EdgeOptions()
    _add_common_browser_args(options, headless=headless)
    driver = webdriver.Edge(options=options)
    driver.set_page_load_timeout(DEFAULT_PAGE_LOAD_TIMEOUT_SECONDS)
    _apply_stealth(driver)
    return driver


def create_selenium_driver(
    logger: Optional[Callable[[str], None]] = None,
    headless: bool = True,
    preferred_browser: str = "",
):
    errors = []
    builders = [("Chrome", _build_chrome_driver), ("Edge", _build_edge_driver)]
    preferred = (preferred_browser or "").strip().lower()
    if preferred:
        builders.sort(key=lambda item: 0 if item[0].lower() == preferred else 1)
    for browser_name, builder in builders:
        try:
            driver = builder(headless=headless)
            mode = "headless" if headless else "thường"
            _emit(logger, f"Selenium đang dùng {browser_name} {mode}")
            return driver
        except Exception as exc:
            errors.append(f"{browser_name}: {str(exc)[:180]}")
    raise RuntimeError("Không mở được Selenium browser. " + " | ".join(errors))


def close_selenium_driver(driver):
    if not driver:
        return
    try:
        driver.quit()
    except Exception:
        pass


def _detect_platform_from_url(url: str) -> str:
    url_lower = str(url or "").strip().lower()
    if "facebook.com" in url_lower or "fb.watch" in url_lower:
        return "facebook"
    if "tiktok.com" in url_lower or "vt.tiktok.com" in url_lower or "vm.tiktok.com" in url_lower:
        return "tiktok"
    if "instagram.com" in url_lower:
        return "instagram"
    if "youtube.com" in url_lower or "youtu.be" in url_lower:
        return "youtube"
    return ""


def _is_tiktok_url(url: str) -> bool:
    return _detect_platform_from_url(url) == "tiktok"


def _is_facebook_login_gate(url: str) -> bool:
    raw = str(url or "").strip().lower()
    return "facebook.com/login" in raw and "next=" in raw


def _resolve_page_load_timeout(url: str) -> float:
    platform = _detect_platform_from_url(url)
    if platform == "tiktok":
        return TIKTOK_PAGE_LOAD_TIMEOUT_SECONDS
    if platform == "facebook":
        return FACEBOOK_PAGE_LOAD_TIMEOUT_SECONDS
    if platform == "instagram":
        return INSTAGRAM_PAGE_LOAD_TIMEOUT_SECONDS
    return DEFAULT_PAGE_LOAD_TIMEOUT_SECONDS


def resolve_fb_url(url: str, logger: Optional[Callable[[str], None]] = None) -> str:
    try:
        if not url:
            return url
        url = url.split("#")[0]
        parsed = urllib.parse.urlparse(url)
        netloc = parsed.netloc.lower()

        if netloc.startswith("m.") or netloc.startswith("mobile.") or netloc.startswith("mbasic."):
            parsed = parsed._replace(netloc="www.facebook.com")
            url = urllib.parse.urlunparse(parsed)

        if "l.facebook.com" in url:
            try:
                qs = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
                if "u" in qs and qs["u"]:
                    return urllib.parse.unquote(qs["u"][0]).split("#")[0]
            except Exception:
                pass

        try:
            response = requests.head(url, allow_redirects=True, timeout=10)
            if response.url:
                return response.url.split("#")[0]
        except Exception:
            try:
                response = requests.get(url, allow_redirects=True, timeout=10, stream=True)
                final_url = response.url
                response.close()
                if final_url:
                    return final_url.split("#")[0]
            except Exception as exc:
                _emit(logger, f"resolve_fb_url request error: {exc}")
                return url
    except Exception as exc:
        _emit(logger, f"resolve_fb_url error: {exc}")
    return url


def _wait_until_ready(driver):
    deadline = time.time() + READY_TIMEOUT_SECONDS
    while time.time() < deadline:
        try:
            if driver.execute_script("return document.readyState") == "complete":
                break
        except Exception:
            pass
        time.sleep(READY_POLL_SECONDS)


def _focus_visible_browser_window(driver):
    if not driver:
        return
    try:
        driver.set_window_position(30, 30)
    except Exception:
        pass
    try:
        driver.set_window_size(1280, 960)
    except Exception:
        pass
    try:
        driver.maximize_window()
    except Exception:
        pass
    try:
        driver.execute_script("window.focus();")
    except Exception:
        pass


def _read_current_page_bundle(driver):
    page_source = driver.page_source or ""
    try:
        body_text = driver.execute_script("return document.body ? document.body.innerText : ''") or ""
    except Exception:
        body_text = ""
    try:
        metas = driver.execute_script(
            """
            const output = {};
            document.querySelectorAll('meta[property], meta[name]').forEach((el) => {
                const key = el.getAttribute('property') || el.getAttribute('name');
                const value = el.getAttribute('content');
                if (key && value && !output[key]) {
                    output[key] = value;
                }
            });
            return output;
            """
        ) or {}
    except Exception:
        metas = {}
    try:
        title = driver.title or ""
    except Exception:
        title = ""
    return {
        "source": page_source,
        "text": body_text,
        "metas": metas,
        "title": title,
        "url": driver.current_url,
    }


def _collect_page_bundle(driver, url: str, logger: Optional[Callable[[str], None]] = None):
    target_timeout = _resolve_page_load_timeout(url)
    try:
        driver.set_page_load_timeout(target_timeout)
        driver.get(url)
    except TimeoutException:
        _emit(logger, f"Timeout khi tải trang: {url[:90]}")
    finally:
        try:
            driver.set_page_load_timeout(DEFAULT_PAGE_LOAD_TIMEOUT_SECONDS)
        except Exception:
            pass
    _wait_until_ready(driver)
    time.sleep(DEFAULT_SETTLE_SECONDS)
    try:
        driver.execute_script("window.scrollTo(0, Math.min(900, document.body.scrollHeight * 0.25));")
        time.sleep(0.8)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(0.4)
    except Exception:
        pass
    return _read_current_page_bundle(driver)


def _parse_compact_number(value) -> Optional[int]:
    if value is None:
        return None
    raw = str(value).strip().strip("\"'").replace("\xa0", "").replace(" ", "")
    if not raw:
        return None
    suffix_match = re.match(r"^([\d.,]+)([KMB])$", raw.upper())
    if suffix_match:
        number_part = suffix_match.group(1).replace(",", ".")
        try:
            base = float(number_part)
        except Exception:
            return None
        multiplier = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}[suffix_match.group(2)]
        return int(base * multiplier)
    digits = re.sub(r"[^\d]", "", raw)
    return int(digits) if digits else None


def _extract_number(text: str, patterns) -> Optional[int]:
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if match:
            parsed = _parse_compact_number(match.group(1))
            if parsed is not None:
                return parsed
    return None


def _extract_url_group(url: str, pattern: str) -> str:
    match = re.search(pattern, url or "", re.IGNORECASE)
    return match.group(1) if match else ""


def _iter_json_nodes(node):
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from _iter_json_nodes(value)
    elif isinstance(node, list):
        for item in node:
            yield from _iter_json_nodes(item)


def _iter_json_script_payloads(source: str, required_substring: str = "", application_only: bool = False):
    pattern = r'<script type="application/json"[^>]*>(.*?)</script>' if application_only else r'<script[^>]*>(.*?)</script>'
    for raw in re.findall(pattern, source or "", re.IGNORECASE | re.DOTALL):
        if required_substring and required_substring not in raw:
            continue
        candidate = (raw or "").strip()
        if not candidate or candidate[0] not in "{[":
            continue
        try:
            yield json.loads(candidate)
        except Exception:
            continue


def _pick_dict_value(primary, secondary, key: str):
    if isinstance(primary, dict) and key in primary:
        return primary.get(key)
    if isinstance(secondary, dict):
        return secondary.get(key)
    return None


def _decode_jsonish_string(value: str) -> str:
    if not value:
        return ""
    try:
        return json.loads(f'"{value}"')
    except Exception:
        try:
            return bytes(value, "utf-8").decode("unicode_escape")
        except Exception:
            return value


def _extract_string(text: str, patterns) -> str:
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if match:
            value = _decode_jsonish_string(match.group(1)).strip()
            if value:
                return value
    return ""


def _extract_text_metric(text: str, labels) -> Optional[int]:
    if not text:
        return None
    label_pattern = "|".join(re.escape(label) for label in labels)
    patterns = [
        rf"([\d.,]+(?:[KMB])?)\s*(?:{label_pattern})\b",
        rf"\b(?:{label_pattern})\s*([\d.,]+(?:[KMB])?)",
    ]
    return _extract_number(text, patterns)


def _format_air_date(day, month) -> str:
    try:
        day_num = int(day)
        month_num = int(month)
    except Exception:
        return ""
    if not (1 <= day_num <= 31 and 1 <= month_num <= 12):
        return ""
    return f"{day_num}-{month_num}"


def _format_air_date_from_datetime(value) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return _format_air_date(value.day, value.month)
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 1_000_000_000_000:
            timestamp /= 1000.0
        try:
            return _format_air_date_from_datetime(datetime.fromtimestamp(timestamp))
        except Exception:
            return ""
    raw = str(value).strip()
    if not raw:
        return ""
    if re.fullmatch(r"\d{10,13}", raw):
        try:
            return _format_air_date_from_datetime(float(raw))
        except Exception:
            return ""
    normalized = raw.replace("T", " ").replace("Z", "").strip()
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y",
        "%m-%d",
        "%m/%d",
        "%m.%d",
    ):
        try:
            parsed = datetime.strptime(normalized, fmt)
            return _format_air_date(parsed.day, parsed.month)
        except Exception:
            continue
    try:
        return _format_air_date_from_datetime(datetime.fromisoformat(normalized))
    except Exception:
        return ""


def _extract_air_date_from_text(text: str) -> str:
    if not text:
        return ""
    lines = [line.strip() for line in str(text).splitlines() if line.strip()]
    for line in lines[:20]:
        cleaned = line.replace("·", " ").replace("•", " ").strip()
        match = re.fullmatch(r"([01]?\d)\s*[-/.]\s*([0-3]?\d)", cleaned)
        if match:
            month_token, day_token = match.groups()
            return _format_air_date(day_token, month_token)
    return ""


def _has_tiktok_challenge(bundle) -> bool:
    text = (bundle.get("text") or "").strip().lower()
    source = (bundle.get("source") or "").lower()
    visible_markers = (
        "drag the slider to fit the puzzle",
        "verify to continue",
        "complete the puzzle",
    )
    if any(marker in text for marker in visible_markers):
        return True
    source_markers = (
        "secsdk-captcha",
        "captcha-verify-container",
        "captcha_container",
        "drag the slider to fit the puzzle",
    )
    return any(marker in source for marker in source_markers)


def _payload_has_tiktok_signal(payload) -> bool:
    if not isinstance(payload, dict):
        return False
    signal_keys = ("v", "l", "s", "c", "save")
    signal_count = 0
    for key in signal_keys:
        try:
            if int(payload.get(key) or 0) > 0:
                signal_count += 1
        except Exception:
            continue
    return signal_count >= 2


def _extract_tiktok_caption(bundle) -> str:
    metas = bundle.get("metas", {}) or {}
    title = str(bundle.get("title") or "").strip()
    if title and title.lower() != "tiktok - make your day":
        title = re.sub(r"\s+\.\.\.\s*\|\s+.*$", "", title).strip()
        title = re.sub(r"\s*\|\s*tiktok.*$", "", title, flags=re.IGNORECASE).strip()
        if title and title.lower() != "tiktok":
            return title
    return metas.get("og:description", "") or metas.get("og:title", "")


def _extract_tiktok_photo_from_text(bundle):
    text = bundle.get("text") or ""
    if not text:
        return None
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return None

    comment_index = -1
    for idx, line in enumerate(lines):
        if line.lower() == "comments":
            comment_index = idx
            break
    if comment_index < 0:
        for idx, line in enumerate(lines):
            if "comments" in line.lower():
                comment_index = idx
                break
    if comment_index < 0:
        return None

    metric_window = lines[max(0, comment_index - 8) : comment_index]
    metric_values = []
    for line in metric_window:
        parsed = _parse_compact_number(line)
        if parsed is not None:
            metric_values.append(parsed)
    if len(metric_values) < 3:
        return None

    payload = {
        "v": 0,
        "l": metric_values[-4] if len(metric_values) >= 4 else metric_values[-3],
        "c": metric_values[-3] if len(metric_values) >= 4 else metric_values[-2],
        "s": metric_values[-1],
        "cap": _extract_tiktok_caption(bundle),
    }
    air_date = _extract_air_date_from_text(text)
    if air_date:
        payload["air_date"] = air_date
    if len(metric_values) >= 4:
        payload["save"] = metric_values[-2]
    return payload


def _extract_tiktok(bundle):
    source = bundle["source"]
    metas = bundle["metas"]
    target_post_id = _extract_url_group(bundle.get("url", ""), r"/(?:video|photo)/(\d+)")
    is_photo_post = "/photo/" in (bundle.get("url", "") or "").lower()

    for data in _iter_json_script_payloads(source, required_substring="webapp.video-detail"):
        scope = data.get("__DEFAULT_SCOPE__", {}) if isinstance(data, dict) else {}
        detail = scope.get("webapp.video-detail", {}) if isinstance(scope, dict) else {}
        item = detail.get("itemInfo", {}).get("itemStruct", {}) if isinstance(detail, dict) else {}
        item_target_id = str(item.get("id") or item.get("awemeId") or "")
        if target_post_id and item_target_id and item_target_id != target_post_id:
            continue
        stats_v2 = item.get("statsV2", {}) if isinstance(item, dict) else {}
        stats = item.get("stats", {}) if isinstance(item, dict) else {}
        anchors = item.get("anchors") if isinstance(item, dict) else []
        anchor_infos = item.get("anchorInfos") if isinstance(item, dict) else []
        has_shop_anchor = bool(anchors or anchor_infos)
        is_ad_video = bool(item.get("isAd")) if isinstance(item, dict) else False
        air_date = _format_air_date_from_datetime(
            item.get("createTime") if isinstance(item, dict) else None
        ) or _extract_air_date_from_text(bundle.get("text") or "")
        payload = {
            "v": _parse_compact_number(_pick_dict_value(stats_v2, stats, "playCount")) or 0,
            "l": _parse_compact_number(_pick_dict_value(stats_v2, stats, "diggCount")) or 0,
            "s": _parse_compact_number(_pick_dict_value(stats_v2, stats, "shareCount")) or 0,
            "c": _parse_compact_number(_pick_dict_value(stats_v2, stats, "commentCount")) or 0,
            "cap": (item.get("desc") if isinstance(item, dict) else "")
            or (detail.get("shareMeta", {}).get("desc", "") if isinstance(detail, dict) else "")
            or metas.get("og:description", "")
            or metas.get("og:title", ""),
        }
        if air_date:
            payload["air_date"] = air_date
        save_count = _parse_compact_number(_pick_dict_value(stats_v2, stats, "collectCount"))
        if save_count is not None:
            payload["save"] = save_count
        if has_shop_anchor or is_ad_video:
            warning_parts = []
            if has_shop_anchor:
                warning_parts.append("gắn giỏ hàng")
            if is_ad_video:
                warning_parts.append("được đánh dấu quảng cáo")
            warning_text = " và ".join(warning_parts) if warning_parts else "đặc biệt"
            payload["_warning"] = (
                f"TikTok video {warning_text}. TikTok web public có thể chỉ trả số liệu giới hạn, nên các chỉ số có thể lệch."
            )
        if any(payload.get(key) for key in ("v", "l", "s", "c", "save")) or payload.get("cap") or payload.get("air_date"):
            return payload

    if is_photo_post:
        text_payload = _extract_tiktok_photo_from_text(bundle)
        if text_payload and any(text_payload.get(key) for key in ("l", "s", "c", "save")):
            return text_payload

    payload = {
        "v": _extract_number(source, [r'"playCount"\s*:\s*("?[\d.,KMB]+"?)', r'"videoViewCount"\s*:\s*("?[\d.,KMB]+"?)']) or 0,
        "l": _extract_number(source, [r'"diggCount"\s*:\s*("?[\d.,KMB]+"?)', r'"likeCount"\s*:\s*("?[\d.,KMB]+"?)']) or 0,
        "s": _extract_number(source, [r'"shareCount"\s*:\s*("?[\d.,KMB]+"?)', r'"sharesCount"\s*:\s*("?[\d.,KMB]+"?)']) or 0,
        "c": _extract_number(source, [r'"commentCount"\s*:\s*("?[\d.,KMB]+"?)', r'"commentsCount"\s*:\s*("?[\d.,KMB]+"?)']) or 0,
        "cap": _extract_string(source, [r'"desc"\s*:\s*"((?:\\.|[^"\\])*)"', r'"text"\s*:\s*"((?:\\.|[^"\\])*)"'])
        or metas.get("og:description", "")
        or metas.get("og:title", ""),
    }
    air_date = _format_air_date_from_datetime(
        _extract_string(source, [r'"createTime"\s*:\s*"?(\d{10,13})"?'])
    ) or _extract_air_date_from_text(bundle.get("text") or "")
    if air_date:
        payload["air_date"] = air_date
    save_count = _extract_number(
        source,
        [
            r'"collectCount"\s*:\s*("?[\d.,KMB]+"?)',
            r'"bookmarksCount"\s*:\s*("?[\d.,KMB]+"?)',
            r'"bookmarkCount"\s*:\s*("?[\d.,KMB]+"?)',
            r'"savedCount"\s*:\s*("?[\d.,KMB]+"?)',
            r'"savesCount"\s*:\s*("?[\d.,KMB]+"?)',
        ],
    )
    if save_count is not None:
        payload["save"] = save_count
    if is_photo_post and not any(payload.get(key) for key in ("l", "s", "c", "save")):
        text_payload = _extract_tiktok_photo_from_text(bundle)
        if text_payload:
            return text_payload
    return payload


def _extract_instagram(bundle):
    source = bundle["source"]
    text = bundle["text"]
    metas = bundle["metas"]
    shortcode = _extract_url_group(bundle.get("url", ""), r"/(?:reel|p|tv)/([A-Za-z0-9_-]+)")

    for data in _iter_json_script_payloads(source, required_substring="xdt_api__v1__media__shortcode__web_info", application_only=True):
        for node in _iter_json_nodes(data):
            media_info = node.get("xdt_api__v1__media__shortcode__web_info") if isinstance(node, dict) else None
            if not isinstance(media_info, dict):
                continue
            items = media_info.get("items") or []
            for item in items:
                code = str(item.get("code") or item.get("shortcode") or "")
                if shortcode and code and code != shortcode:
                    continue
                caption_obj = item.get("caption")
                caption_text = caption_obj.get("text", "") if isinstance(caption_obj, dict) else str(caption_obj or "")
                air_date = (
                    _format_air_date_from_datetime(item.get("taken_at"))
                    or _format_air_date_from_datetime(item.get("taken_at_timestamp"))
                    or _format_air_date_from_datetime(item.get("published_at"))
                    or _extract_air_date_from_text(text)
                )
                payload = {
                    "v": _parse_compact_number(item.get("view_count"))
                    or _parse_compact_number(item.get("play_count"))
                    or _parse_compact_number(item.get("video_view_count"))
                    or 0,
                    "l": _parse_compact_number(item.get("like_count")) or 0,
                    "s": _parse_compact_number(item.get("share_count"))
                    or _parse_compact_number(item.get("media_repost_count"))
                    or 0,
                    "c": _parse_compact_number(item.get("comment_count")) or 0,
                    "cap": caption_text or metas.get("og:description", "") or metas.get("og:title", ""),
                }
                if air_date:
                    payload["air_date"] = air_date
                if any(payload.get(key) for key in ("v", "l", "s", "c")) or payload.get("cap") or payload.get("air_date"):
                    return payload

    payload = {
        "v": _extract_number(
            source,
            [
                r'"video_view_count"\s*:\s*("?[\d.,KMB]+"?)',
                r'"videoPlayCount"\s*:\s*("?[\d.,KMB]+"?)',
                r'"play_count"\s*:\s*("?[\d.,KMB]+"?)',
            ],
        ) or 0,
        "l": _extract_number(
            source,
            [
                r'"edge_media_preview_like"\s*:\s*\{[^}]{0,220}"count"\s*:\s*(\d+)',
                r'"like_count"\s*:\s*("?[\d.,KMB]+"?)',
                r'"likes_count"\s*:\s*("?[\d.,KMB]+"?)',
            ],
        ) or _extract_text_metric(text, ["likes", "like"]) or 0,
        "s": _extract_number(source, [r'"share_count"\s*:\s*("?[\d.,KMB]+"?)']) or 0,
        "c": _extract_number(
            source,
            [
                r'"edge_media_to_comment"\s*:\s*\{[^}]{0,220}"count"\s*:\s*(\d+)',
                r'"comment_count"\s*:\s*("?[\d.,KMB]+"?)',
            ],
        ) or _extract_text_metric(text, ["comments", "comment"]) or 0,
        "cap": _extract_string(
            source,
            [
                r'"edge_media_to_caption"\s*:\s*\{.*?"text"\s*:\s*"((?:\\.|[^"\\])*)"',
                r'"caption"\s*:\s*"((?:\\.|[^"\\])*)"',
            ],
        )
        or metas.get("og:description", "")
        or metas.get("og:title", ""),
    }
    air_date = (
        _format_air_date_from_datetime(
            _extract_string(
                source,
                [
                    r'"taken_at"\s*:\s*"?(\d{10,13})"?',
                    r'"taken_at_timestamp"\s*:\s*"?(\d{10,13})"?',
                    r'"published_at"\s*:\s*"?(\d{10,13})"?',
                ],
            )
        )
        or _extract_air_date_from_text(text)
    )
    if air_date:
        payload["air_date"] = air_date
    return payload


def _extract_facebook_target_ids(url: str):
    patterns = [
        r"/reel/(\d+)",
        r"/videos/(?:[^/]+/)?(\d+)",
        r"[?&]v=(\d+)",
        r"[?&]multi_permalinks=(\d+)",
        r"/groups/[^/?#]+/(?:posts|permalink)/(\d+)",
        r"/posts/(\d+)",
        r"/permalink/(\d+)",
    ]
    seen = set()
    target_ids = []
    for pattern in patterns:
        for match in re.findall(pattern, url or "", re.IGNORECASE):
            value = str(match or "").strip()
            if value and value not in seen:
                seen.add(value)
                target_ids.append(value)
    return target_ids


def _collect_target_context(text: str, identifiers, before: int = 1800, after: int = 9000, max_chunks: int = 8):
    if not text:
        return ""
    if not identifiers:
        return text
    chunks = []
    seen_spans = set()
    for identifier in identifiers:
        start_at = 0
        while len(chunks) < max_chunks:
            idx = text.find(identifier, start_at)
            if idx < 0:
                break
            span_start = max(0, idx - before)
            span_end = min(len(text), idx + after)
            span_key = (span_start, span_end)
            if span_key not in seen_spans:
                seen_spans.add(span_key)
                chunks.append(text[span_start:span_end])
            start_at = idx + len(identifier)
    return "\n".join(chunks) if chunks else text


def _extract_facebook_metric_from_meta(metas, patterns) -> Optional[int]:
    meta_candidates = [
        str((metas or {}).get("og:title") or ""),
        str((metas or {}).get("og:description") or ""),
        str((metas or {}).get("og:image:alt") or ""),
    ]
    for candidate in meta_candidates:
        parsed = _extract_number(candidate, patterns)
        if parsed is not None:
            return parsed
    return None


def _extract_scoped_number(text: str, target_ids, scoped_patterns) -> Optional[int]:
    if not text:
        return None
    for target_id in target_ids:
        escaped_id = re.escape(str(target_id or "").strip())
        if not escaped_id:
            continue
        patterns = [pattern.format(id=escaped_id) for pattern in scoped_patterns]
        parsed = _extract_number(text, patterns)
        if parsed is not None:
            return parsed
    return None


def _extract_facebook_reel_text_counts(text: str):
    if not text:
        return {}
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return {}
    reel_idx = -1
    for idx, line in enumerate(lines):
        line_lower = line.lower()
        if line_lower == "reels" or line_lower.startswith("reels "):
            reel_idx = idx
            break
    if reel_idx < 0:
        return {}
    numeric_lines = []
    for line in lines[max(0, reel_idx - 12):reel_idx]:
        parsed = _parse_compact_number(line)
        if parsed is not None:
            numeric_lines.append(parsed)
    if len(numeric_lines) < 2:
        return {}
    return {
        "c": numeric_lines[-2],
        "s": numeric_lines[-1],
    }


def _extract_facebook(bundle):
    source = bundle["source"]
    text = bundle["text"]
    metas = bundle["metas"]
    target_ids = _extract_facebook_target_ids(bundle.get("url", ""))
    scoped_source = _collect_target_context(source, target_ids)

    payload = {
        "cap": metas.get("og:description", "")
        or _extract_string(
            scoped_source,
            [
                r'"message"\s*:\s*\{[^}]{0,400}"text"\s*:\s*"((?:\\.|[^"\\])*)"',
                r'"seo_title"\s*:\s*"((?:\\.|[^"\\])*)"',
            ],
        )
        or metas.get("og:title", ""),
    }
    air_date = (
        _format_air_date_from_datetime(
            _extract_string(
                scoped_source,
                [
                    r'"creation_time"\s*:\s*"?(\d{10,13})"?',
                    r'"publish_time"\s*:\s*"?(\d{10,13})"?',
                    r'"story_creation_time"\s*:\s*"?(\d{10,13})"?',
                ],
            )
        )
        or _extract_air_date_from_text(text)
    )
    if air_date:
        payload["air_date"] = air_date

    view_value = (
        _extract_number(
            scoped_source,
            [
                r'"view_count"\s*:\s*("?[\d.,KMB]+"?)',
                r'"play_count"\s*:\s*("?[\d.,KMB]+"?)',
                r'"video_view_count"\s*:\s*("?[\d.,KMB]+"?)',
            ],
        )
        or _extract_facebook_metric_from_meta(
            metas,
            [
                r'([\d.,]+(?:[KMB])?)\s*views?\b',
            ],
        )
    )
    if view_value is not None:
        payload["v"] = view_value

    reaction_value = (
        _extract_scoped_number(
            source,
            target_ids,
            [
                r'"subscription_target_id":"{id}".{{0,8000}}?"reaction_count"\s*:\s*\{{[^}}]{{0,220}}"count"\s*:\s*(\d+)',
                r'"share_fbid":"{id}".{{0,8000}}?"reaction_count"\s*:\s*\{{[^}}]{{0,220}}"count"\s*:\s*(\d+)',
                r'"subscription_target_id":"{id}".{{0,8000}}?"i18n_reaction_count"\s*:\s*"([\d.,KMB]+)"',
                r'"share_fbid":"{id}".{{0,8000}}?"i18n_reaction_count"\s*:\s*"([\d.,KMB]+)"',
            ],
        )
        or _extract_scoped_number(
            source,
            target_ids,
            [
                r'"top_level_post_id":"{id}".{{0,2200}}?"reaction_count"\s*:\s*\{{[^}}]{{0,220}}"count"\s*:\s*(\d+)',
            ],
        )
        or
        _extract_number(
            scoped_source,
            [
                r'"reaction_count"\s*:\s*\{[^}]{0,220}"count"\s*:\s*(\d+)',
                r'"reaction_count"\s*:\s*("?[\d.,KMB]+"?)',
                r'"i18n_reaction_count"\s*:\s*"([\d.,KMB]+)"',
            ],
        )
        or _extract_facebook_metric_from_meta(
            metas,
            [
                r'([\d.,]+(?:[KMB])?)\s*reactions?\b',
                r'([\d.,]+(?:[KMB])?)\s*likes?\b',
            ],
        )
        or _extract_text_metric(text, ["reactions", "reaction", "likes", "like"])
    )
    if reaction_value is not None:
        payload["l"] = reaction_value

    share_value = (
        _extract_scoped_number(
            source,
            target_ids,
            [
                r'"top_level_post_id":"{id}".{{0,2200}}?"share_count_reduced"\s*:\s*"([\d.,KMB]+)"',
                r'"subscription_target_id":"{id}".{{0,8000}}?"share_count"\s*:\s*\{{[^}}]{{0,220}}"count"\s*:\s*(\d+)',
                r'"share_fbid":"{id}".{{0,8000}}?"share_count"\s*:\s*\{{[^}}]{{0,220}}"count"\s*:\s*(\d+)',
                r'"subscription_target_id":"{id}".{{0,8000}}?"i18n_share_count"\s*:\s*"([\d.,KMB]+)"',
                r'"share_fbid":"{id}".{{0,8000}}?"i18n_share_count"\s*:\s*"([\d.,KMB]+)"',
            ],
        )
        or
        _extract_number(
            scoped_source,
            [
                r'"share_count_reduced"\s*:\s*"?(?:\\)?([\d.,KMB]+)"?',
                r'"share_count"\s*:\s*\{[^}]{0,220}"count"\s*:\s*(\d+)',
                r'"share_count"\s*:\s*("?[\d.,KMB]+"?)',
                r'"i18n_share_count"\s*:\s*"([\d.,KMB]+)"',
            ],
        )
        or _extract_text_metric(text, ["shares", "share"])
    )
    if share_value is not None:
        payload["s"] = share_value

    comment_value = (
        _extract_scoped_number(
            source,
            target_ids,
            [
                r'"top_level_post_id":"{id}".{{0,2200}}?"total_comment_count"\s*:\s*(\d+)',
                r'"subscription_target_id":"{id}".{{0,9000}}?"comments"\s*:\s*\{{[^}}]{{0,220}}"total_count"\s*:\s*(\d+)',
                r'"share_fbid":"{id}".{{0,9000}}?"comments"\s*:\s*\{{[^}}]{{0,220}}"total_count"\s*:\s*(\d+)',
                r'"subscription_target_id":"{id}".{{0,9000}}?"comments_count_summary_renderer".{{0,1600}}?"total_count"\s*:\s*(\d+)',
                r'"share_fbid":"{id}".{{0,9000}}?"comments_count_summary_renderer".{{0,1600}}?"total_count"\s*:\s*(\d+)',
            ],
        )
        or
        _extract_number(
            scoped_source,
            [
                r'"total_comment_count"\s*:\s*"?(?:\\)?([\d.,KMB]+)"?',
                r'"comment_count"\s*:\s*\{[^}]{0,220}"total_count"\s*:\s*(\d+)',
                r'"comment_count"\s*:\s*("?[\d.,KMB]+"?)',
                r'"comments"\s*:\s*\{[^}]{0,220}"total_count"\s*:\s*(\d+)',
                r'"comment_rendering_instance"\s*:\s*\{.{0,1600}?"comments"\s*:\s*\{[^}]{0,220}"total_count"\s*:\s*(\d+)',
                r'"i18n_comment_count"\s*:\s*"([\d.,KMB]+)"',
            ],
        )
        or _extract_text_metric(text, ["comments", "comment"])
    )
    if comment_value is not None:
        payload["c"] = comment_value

    if "/reel/" in str(bundle.get("url") or "").lower():
        reel_text_counts = _extract_facebook_reel_text_counts(text)
        if "c" not in payload and reel_text_counts.get("c") is not None:
            payload["c"] = reel_text_counts["c"]
        if "s" not in payload and reel_text_counts.get("s") is not None:
            payload["s"] = reel_text_counts["s"]

    return payload


def _should_retry_tiktok_visually(bundle, payload) -> bool:
    if _has_tiktok_challenge(bundle):
        # Neu van doc duoc so lieu kha day du thi khong can day Chrome thuong len nua.
        if _payload_has_tiktok_signal(payload):
            return False
        return True

    # Chi mo Chrome thuong khi that su gap slider captcha.
    # Cac case TikTok Shop/App banner van co so lieu thi khong can day cua so len nua.
    return False


def _wait_for_tiktok_manual_challenge(
    driver,
    bundle,
    logger: Optional[Callable[[str], None]] = None,
):
    if not _has_tiktok_challenge(bundle):
        return bundle

    _focus_visible_browser_window(driver)
    _emit(
        logger,
        "TikTok đang hiện captcha slider. Chrome thường đã mở ra, bạn kéo captcha xong thì tool sẽ tự đọc tiếp số liệu.",
    )

    best_bundle = bundle
    deadline = time.time() + TIKTOK_MANUAL_CHALLENGE_TIMEOUT_SECONDS
    while time.time() < deadline:
        time.sleep(TIKTOK_MANUAL_CHALLENGE_POLL_SECONDS)
        try:
            current_bundle = _read_current_page_bundle(driver)
        except Exception:
            continue
        if len(current_bundle.get("text", "")) >= len(best_bundle.get("text", "")):
            best_bundle = current_bundle
        if not _has_tiktok_challenge(current_bundle):
            _emit(logger, "Đã qua captcha TikTok, đang lấy lại số liệu...")
            time.sleep(0.6)
            try:
                refreshed_bundle = _read_current_page_bundle(driver)
                if len(refreshed_bundle.get("text", "")) >= len(best_bundle.get("text", "")):
                    best_bundle = refreshed_bundle
            except Exception:
                pass
            return best_bundle

    _emit(logger, "TikTok captcha chưa được giải xong trong thời gian chờ, bỏ qua lần đọc này.")
    return best_bundle


def _collect_tiktok_visible_bundle(driver, url: str, logger: Optional[Callable[[str], None]] = None):
    try:
        driver.set_page_load_timeout(TIKTOK_PAGE_LOAD_TIMEOUT_SECONDS)
        driver.get(url)
    except TimeoutException:
        _emit(logger, f"Timeout khi tải TikTok visual retry: {url[:90]}")
    finally:
        try:
            driver.set_page_load_timeout(DEFAULT_PAGE_LOAD_TIMEOUT_SECONDS)
        except Exception:
            pass
    _wait_until_ready(driver)

    best_bundle = _read_current_page_bundle(driver)
    best_bundle = _wait_for_tiktok_manual_challenge(driver, best_bundle, logger=logger)
    deadline = time.time() + 12
    while time.time() < deadline:
        time.sleep(1)
        current_bundle = _read_current_page_bundle(driver)
        if len(current_bundle.get("text", "")) > len(best_bundle.get("text", "")):
            best_bundle = current_bundle
        if _extract_tiktok_photo_from_text(current_bundle):
            return current_bundle
    return best_bundle


def _retry_tiktok_with_visible_browser(url: str, logger: Optional[Callable[[str], None]] = None):
    retry_driver = None
    best_payload = None
    try:
        _emit(logger, "TikTok/headless bị chặn, thử lại bằng Chrome thường 1 lần")
        retry_driver = create_selenium_driver(logger=logger, headless=False, preferred_browser="chrome")
        retry_bundle = _collect_tiktok_visible_bundle(retry_driver, url, logger=logger)
        retry_payload = _extract_tiktok(retry_bundle)
        if retry_payload and any(retry_payload.get(key) for key in ("v", "l", "s", "c", "save")):
            return retry_payload
        if retry_payload:
            best_payload = retry_payload
    except Exception as exc:
        _emit(logger, f"Retry TikTok visual thất bại: {str(exc)[:160]}")
    finally:
        close_selenium_driver(retry_driver)
    return best_payload


def fetch_social_stats(url: str, platform_name: str, driver=None, logger: Optional[Callable[[str], None]] = None):
    requested_platform = (platform_name or "").strip().lower()
    detected_platform = _detect_platform_from_url(url)
    platform = detected_platform or requested_platform
    if detected_platform and requested_platform and detected_platform != requested_platform:
        _emit(
            logger,
            f"Phát hiện lệch platform: sheet ghi {requested_platform}, nhưng link thực là {detected_platform}. Dùng theo link thực.",
        )
    if platform == "facebook":
        if driver is not None:
            _ensure_facebook_cookies(driver, logger=logger)
        url = resolve_fb_url(url, logger=logger)
        if _is_facebook_login_gate(url):
            _emit(logger, "Facebook trả về trang login/chặn truy cập, bỏ qua link này để chạy tiếp nhanh.")
            return None
    elif platform == "tiktok":
        if driver is not None:
            _ensure_tiktok_cookies(driver, logger=logger)

    own_driver = driver is None
    if own_driver:
        driver = create_selenium_driver(logger=logger)

    try:
        bundle = _collect_page_bundle(driver, url, logger=logger)
        extractor_map = {
            "tiktok": _extract_tiktok,
            "instagram": _extract_instagram,
            "facebook": _extract_facebook,
        }
        extractor = extractor_map.get(platform)
        if not extractor:
            return None
        payload = extractor(bundle)
        if platform == "tiktok" and not payload and TIKTOK_SOFT_RETRY_ATTEMPTS > 0:
            for attempt in range(1, TIKTOK_SOFT_RETRY_ATTEMPTS + 1):
                if TIKTOK_SOFT_RETRY_DELAY_SECONDS > 0:
                    time.sleep(TIKTOK_SOFT_RETRY_DELAY_SECONDS)
                _emit(logger, f"TikTok retry ngắn lần {attempt}/{TIKTOK_SOFT_RETRY_ATTEMPTS}...")
                retry_bundle = _collect_page_bundle(driver, url, logger=logger)
                retry_payload = extractor(retry_bundle)
                if retry_payload:
                    payload = retry_payload
                    break
        if platform == "tiktok" and _is_tiktok_url(url) and _should_retry_tiktok_visually(bundle, payload):
            retry_payload = _retry_tiktok_with_visible_browser(url, logger=logger)
            if retry_payload:
                payload = retry_payload
        if not payload:
            return None
        warning_message = str(payload.get("_warning", "") or "").strip()
        if warning_message:
            _emit(logger, warning_message)
        has_signal = any(payload.get(key) for key in ("v", "l", "s", "c", "save"))
        if has_signal or payload.get("cap") or payload.get("air_date"):
            return payload
        return None
    except WebDriverException as exc:
        _emit(logger, f"Lỗi Selenium {platform}: {str(exc)[:160]}")
        return None
    except Exception as exc:
        _emit(logger, f"Lỗi đọc dữ liệu {platform}: {str(exc)[:160]}")
        return None
    finally:
        if own_driver:
            close_selenium_driver(driver)

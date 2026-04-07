import os
import json
import re
import time
import urllib.parse
from typing import Callable, Optional

import requests
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.edge.service import Service as EdgeService

DEFAULT_PAGE_LOAD_TIMEOUT_SECONDS = 25
DEFAULT_SETTLE_SECONDS = 2.2
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/136.0.0.0 Safari/537.36"
)
IS_VERCEL_RUNTIME = bool(str(os.getenv("VERCEL", "") or "").strip())


def _emit(logger: Optional[Callable[[str], None]], message: str):
    if not logger:
        return
    try:
        logger(message)
    except Exception:
        pass


def _first_env(*names: str) -> str:
    for name in names:
        value = str(os.getenv(name, "") or "").strip()
        if value:
            return value
    return ""


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


def _remote_url() -> str:
    raw_value = _first_env("SELENIUM_REMOTE_URL", "REMOTE_WEBDRIVER_URL")
    if not raw_value:
        return ""

    normalized = raw_value.strip().strip("\"'").rstrip("/")
    if normalized.lower() in {"value", "your-value", "your_url_here"}:
        return ""
    if normalized.endswith("/wd/hub/status"):
        normalized = normalized[: -len("/wd/hub/status")]
    elif normalized.endswith("/status"):
        normalized = normalized[: -len("/status")]
    return normalized


def _build_remote_driver(headless: bool = True, browser_name: str = "chrome"):
    remote_url = _remote_url()
    if not remote_url:
        raise RuntimeError(
            "Missing or invalid SELENIUM_REMOTE_URL. "
            "Use a Selenium server URL like https://your-service.onrender.com/wd/hub"
        )

    browser_key = (browser_name or "chrome").strip().lower()
    if browser_key == "edge":
        options = EdgeOptions()
    else:
        browser_key = "chrome"
        options = ChromeOptions()
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)

    _add_common_browser_args(options, headless=headless)
    try:
        driver = webdriver.Remote(command_executor=remote_url, options=options)
    except KeyError as exc:
        raise RuntimeError(
            f"Remote Selenium URL khong dung dinh dang: {remote_url}. "
            "Hay dung URL server, khong dung trang /status."
        ) from exc
    driver.set_page_load_timeout(DEFAULT_PAGE_LOAD_TIMEOUT_SECONDS)
    _apply_stealth(driver)
    return driver


def _remote_driver_builders(preferred_browser: str = ""):
    remote_url = _remote_url()
    if not remote_url:
        return []

    requested = (
        _first_env("SELENIUM_REMOTE_BROWSER")
        or (preferred_browser or "").strip().lower()
        or "chrome"
    )
    browser_order = ["chrome", "edge"]
    if requested in browser_order:
        browser_order.remove(requested)
        browser_order.insert(0, requested)

    return [
        (
            f"Remote {browser_name.capitalize()}",
            lambda headless, browser_name=browser_name: _build_remote_driver(
                headless=headless,
                browser_name=browser_name,
            ),
        )
        for browser_name in browser_order
    ]


def _build_chrome_driver(headless: bool = True):
    options = ChromeOptions()
    _add_common_browser_args(options, headless=headless)
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    chrome_binary = _first_env("CHROME_BIN", "GOOGLE_CHROME_BIN", "CHROMIUM_BIN")
    chromedriver_path = _first_env("CHROMEDRIVER_PATH")
    if chrome_binary:
        options.binary_location = chrome_binary
    service = ChromeService(executable_path=chromedriver_path) if chromedriver_path else None
    driver = webdriver.Chrome(service=service, options=options) if service else webdriver.Chrome(options=options)
    driver.set_page_load_timeout(DEFAULT_PAGE_LOAD_TIMEOUT_SECONDS)
    _apply_stealth(driver)
    return driver


def _build_edge_driver(headless: bool = True):
    options = EdgeOptions()
    _add_common_browser_args(options, headless=headless)
    edge_binary = _first_env("EDGE_BIN")
    edgedriver_path = _first_env("MSEDGEDRIVER_PATH", "EDGEDRIVER_PATH")
    if edge_binary:
        options.binary_location = edge_binary
    service = EdgeService(executable_path=edgedriver_path) if edgedriver_path else None
    driver = webdriver.Edge(service=service, options=options) if service else webdriver.Edge(options=options)
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


def create_selenium_driver(
    logger: Optional[Callable[[str], None]] = None,
    headless: bool = True,
    preferred_browser: str = "",
):
    errors = []
    preferred = (preferred_browser or "").strip().lower()
    builders = _remote_driver_builders(preferred_browser=preferred_browser)
    local_builders = [("Chrome", _build_chrome_driver), ("Edge", _build_edge_driver)]
    if preferred:
        local_builders.sort(key=lambda item: 0 if item[0].lower() == preferred else 1)
    builders.extend(local_builders)

    for browser_name, builder in builders:
        try:
            driver = builder(headless=headless)
            mode = "headless" if headless else "normal"
            _emit(logger, f"Selenium dang dung {browser_name} {mode}")
            return driver
        except Exception as exc:
            errors.append(f"{browser_name}: {str(exc)[:180]}")

    message = "Khong mo duoc Selenium browser. " + " | ".join(errors)
    if IS_VERCEL_RUNTIME and not _remote_url():
        message += (
            " Vercel runtime khong co Chrome/chromedriver local. "
            "Hay deploy phan quet Selenium bang Docker (Render/Railway/Fly) "
            "hoac cung cap SELENIUM_REMOTE_URL."
        )
    raise RuntimeError(message)


def close_selenium_driver(driver):
    if not driver:
        return
    try:
        driver.quit()
    except Exception:
        pass


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
    for _ in range(24):
        try:
            if driver.execute_script("return document.readyState") == "complete":
                break
        except Exception:
            pass
        time.sleep(0.25)


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
    try:
        driver.get(url)
    except TimeoutException:
        _emit(logger, f"Timeout khi tải trang: {url[:90]}")
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


def _has_tiktok_challenge(bundle) -> bool:
    text = (bundle.get("text") or "").strip().lower()
    source = (bundle.get("source") or "").lower()
    return any(
        marker in text or marker in source
        for marker in (
            "drag the slider to fit the puzzle",
            "verifying...",
            "captcha",
        )
    )


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
        if any(payload.get(key) for key in ("v", "l", "s", "c", "save")) or payload.get("cap"):
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
                if any(payload.get(key) for key in ("v", "l", "s", "c")) or payload.get("cap"):
                    return payload

    return {
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


def _extract_facebook(bundle):
    source = bundle["source"]
    text = bundle["text"]
    metas = bundle["metas"]
    return {
        "v": _extract_number(source, [r'"view_count"\s*:\s*("?[\d.,KMB]+"?)', r'"play_count"\s*:\s*("?[\d.,KMB]+"?)']) or 0,
        "l": _extract_number(
            source,
            [
                r'"reaction_count"\s*:\s*\{[^}]{0,220}"count"\s*:\s*(\d+)',
                r'"reaction_count"\s*:\s*("?[\d.,KMB]+"?)',
            ],
        ) or _extract_text_metric(text, ["reactions", "reaction", "likes", "like"]) or 0,
        "s": _extract_number(
            source,
            [
                r'"share_count"\s*:\s*\{[^}]{0,220}"count"\s*:\s*(\d+)',
                r'"share_count"\s*:\s*("?[\d.,KMB]+"?)',
            ],
        ) or _extract_text_metric(text, ["shares", "share"]) or 0,
        "c": _extract_number(
            source,
            [
                r'"comment_count"\s*:\s*\{[^}]{0,220}"total_count"\s*:\s*(\d+)',
                r'"comment_count"\s*:\s*("?[\d.,KMB]+"?)',
            ],
        ) or _extract_text_metric(text, ["comments", "comment"]) or 0,
        "cap": metas.get("og:description", "")
        or _extract_string(source, [r'"message"\s*:\s*\{[^}]{0,400}"text"\s*:\s*"((?:\\.|[^"\\])*)"'])
        or metas.get("og:title", ""),
    }


def _should_retry_tiktok_visually(bundle, payload) -> bool:
    url = (bundle.get("url") or "").lower()
    text = (bundle.get("text") or "").strip().lower()
    has_signal = any((payload or {}).get(key) for key in ("v", "l", "s", "c", "save"))
    photo_post = "/photo/" in url
    login_only = text in {"", "log in"} or text.startswith("log in\n")
    return (photo_post or _has_tiktok_challenge(bundle)) and (not has_signal or login_only or _has_tiktok_challenge(bundle))


def _collect_tiktok_visible_bundle(driver, url: str, logger: Optional[Callable[[str], None]] = None):
    try:
        driver.get(url)
    except TimeoutException:
        _emit(logger, f"Timeout khi tải TikTok visual retry: {url[:90]}")
    _wait_until_ready(driver)

    best_bundle = _read_current_page_bundle(driver)
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
    platform = (platform_name or "").strip().lower()
    if platform == "facebook":
        url = resolve_fb_url(url, logger=logger)

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
        if platform == "tiktok" and _should_retry_tiktok_visually(bundle, payload):
            retry_payload = _retry_tiktok_with_visible_browser(url, logger=logger)
            if retry_payload:
                payload = retry_payload
        if not payload:
            return None
        warning_message = str(payload.get("_warning", "") or "").strip()
        if warning_message:
            _emit(logger, warning_message)
        has_signal = any(payload.get(key) for key in ("v", "l", "s", "c", "save"))
        if has_signal or payload.get("cap"):
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

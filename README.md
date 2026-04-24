# social-performance

FastAPI dashboard de quet social metrics tu Google Sheets, co Selenium, OTP mail va lich chay tu dong.

## Chay local

1. Tao virtualenv va cai dependencies:
   `pip install -r requirements.txt`
2. Tao file `.env` tu `.env.example`.
3. Cung cap Google service account bang 1 trong 3 cach:
   `SERVICE_ACCOUNT_FILE=credential.json`
   `GOOGLE_SERVICE_ACCOUNT_JSON=...`
   `GOOGLE_SERVICE_ACCOUNT_JSON_BASE64=...`
4. Chay app:
   `python scraper.py`

App mac dinh lang nghe tren `0.0.0.0:${PORT:-8000}`.

## Timezone va giam read quota

App da ho tro 2 bien moi truong de dashboard hien dung gio va giam viec doc Google Sheets qua day:

- `APP_TIMEZONE=Asia/Bangkok`
- `SHEET_CACHE_TTL_SECONDS=45`
- `ROW_SCAN_DELAY_SECONDS=0.04`
- `BATCH_SHEET_WRITES=true`

Neu thay loi `Quota exceeded for quota metric 'Read requests'`, hay redeploy ban moi, doi khoang 60 giay cho quota nguoi dung reset, roi thu lai. Cache ngan nay giup giam viec goi lap lai khi nhap sheet, tai danh sach tab va render dashboard.

## Tang toc do quet

Ban moi hien tai da duoc toi uu nhanh hon o 2 diem:

- Ghi du lieu theo lo cho tung dong thay vi goi `update_cell` tung o.
- Selenium dung che do tai trang nhe hon va rut ngan cac khoang cho mac dinh.

Neu muon tinh chinh tiep, sua cac bien sau:

- `ROW_SCAN_DELAY_SECONDS=0.04`
- `SELENIUM_PAGE_LOAD_TIMEOUT_SECONDS=16`
- `SELENIUM_SETTLE_SECONDS=0.9`
- `SELENIUM_SCROLL_SETTLE_SECONDS=0.18`
- `SELENIUM_READY_TIMEOUT_SECONDS=2.4`

## Luu user ben vung tren Vercel

Neu deploy tren Vercel thi danh sach nhan vien, access policy va mail config khong nen luu vao file local vi serverless filesystem la tam thoi. App nay da ho tro luu auth settings vao Redis REST.

Dung nhanh nhat voi Vercel Redis:

- `REDIS_URL`

Dung 1 trong 2 cap bien moi truong sau:

- `KV_REST_API_URL` + `KV_REST_API_TOKEN`
- `UPSTASH_REDIS_REST_URL` + `UPSTASH_REDIS_REST_TOKEN`

Key mac dinh:

- `AUTH_SETTINGS_KV_KEY=social-monitor:auth-settings`
- `APP_RUNTIME_STATE_KEY=social-monitor:runtime-state`

Chi can ket noi Redis tren Vercel Marketplace / Upstash, them cap env phu hop, roi redeploy. Sau do cac thay doi o tab Nhan vien, access policy va mail config se khong bi mat sau reload.

App cung luu ben vung mot phan runtime state tren Redis de tranh bi mat sau moi lan push/deploy, bao gom:

- sheet dang chon
- cau hinh cot va dong bat dau
- lich tu dong
- danh sach bai da gan cho lich
- lich su lan chay gan day va log moi nhat

## Selenium tren Vercel

Neu thay log kieu:

`Khong mo duoc Selenium browser` hoac `Unable to obtain driver for chrome`

thi do runtime Vercel khong co `Chrome` va `chromedriver` local de mo Selenium browser.

App nay ho tro 2 huong:

- Khuyen dung: deploy phan quet Selenium bang Docker tren Render, Railway, Fly.io, VPS...
- Hoac cung cap browser tu xa qua:
  - `SELENIUM_REMOTE_URL`
  - `SELENIUM_REMOTE_BROWSER=chrome`

Neu da co remote WebDriver/Grid thi app se uu tien dung browser tu xa. Neu khong co remote URL, Selenium tren Vercel se khong mo duoc browser local.

## Deploy bang Docker

Repo nay phu hop hon voi host ho tro Docker nhu Render, Railway, Fly.io, VPS, hoac bat ky noi nao chay container duoc.

Build local:

```bash
docker build -t social-performance .
docker run --env-file .env -p 8000:8000 social-performance
```

Container da cai `chromium` va `chromedriver` de Selenium chay headless.

## Deploy len Render

Khuyen dung tao `Web Service` voi `Environment = Docker` de Render build tu `Dockerfile`.

Neu ban dang dung Python service va thay log:

`Running 'gunicorn your_application.wsgi'`

thi service dang bi tao sai template. App nay khong dung Django WSGI.

Lua chon dung:

1. Tao lai service tren Render voi `Docker`.
2. Hoac dung Blueprint/`render.yaml` trong repo nay.

Health check path:

`/healthz`

## Luu y voi Netlify

Netlify dang goi `hugo` vi Build command cua site dang set sai. Tuy nhien repo nay khong phai Hugo/static site; day la FastAPI app can Python runtime, Selenium browser, background task va API routes dong.

Vi vay:

- Xoa Build command `hugo` trong Netlify UI de het loi nham framework.
- Khong deploy app day du tren Netlify theo dang static site.
- Neu can chay day du, chuyen sang host ho tro Docker.

## Bien moi truong chinh

Xem mau trong `.env.example`.

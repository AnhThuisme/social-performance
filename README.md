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

## Deploy bang Docker

Repo nay phu hop hon voi host ho tro Docker nhu Render, Railway, Fly.io, VPS, hoac bat ky noi nao chay container duoc.

Build local:

```bash
docker build -t social-performance .
docker run --env-file .env -p 8000:8000 social-performance
```

Container da cai `chromium` va `chromedriver` de Selenium chay headless.

## Luu y voi Netlify

Netlify dang goi `hugo` vi Build command cua site dang set sai. Tuy nhien repo nay khong phai Hugo/static site; day la FastAPI app can Python runtime, Selenium browser, background task va API routes dong.

Vi vay:

- Xoa Build command `hugo` trong Netlify UI de het loi nham framework.
- Khong deploy app day du tren Netlify theo dang static site.
- Neu can chay day du, chuyen sang host ho tro Docker.

## Bien moi truong chinh

Xem mau trong `.env.example`.

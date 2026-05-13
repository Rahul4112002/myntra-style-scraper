"""
Myntra Style ID Scraper — FastAPI + httpx + BeautifulSoup
=========================================================
Upload an Excel sheet with Myntra Style IDs, scrape each product page,
stream real-time logs via SSE, and export results to Excel.

⚠  Educational / personal-use only. Scraping may violate Myntra's ToS.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
from bs4 import BeautifulSoup
from fastapi import FastAPI, File, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
app = FastAPI(title="Myntra Style ID Scraper")
templates = Jinja2Templates(directory="templates")

DOWNLOADS_DIR = Path("downloads")
DOWNLOADS_DIR.mkdir(exist_ok=True)

UPLOADS_DIR = Path("uploads")
UPLOADS_DIR.mkdir(exist_ok=True)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("style_id_scraper")

# ---------------------------------------------------------------------------
# Global scraper state
# ---------------------------------------------------------------------------
scraper_state: dict[str, Any] = {
    "running": False,
    "stop_event": asyncio.Event(),
    "logs": [],          # list[str]
    "products": [],      # list[dict]
    "total_items": 0,
    "scraped_count": 0,
    "failed_count": 0,
    "output_file": None,
    "task": None,
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}

MAX_CONCURRENCY = 1  # keep product-detail fetches gentle on Render/cloud hosts
REQUEST_DELAY_RANGE = (2.0, 4.0)

MYNTRA_ERROR_MARKERS = (
    "Oops! Something went wrong",
    "Please contact your administrator",
    "Site Maintenance",
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _log(msg: str) -> None:
    line = f"{_ts()} | {msg}"
    scraper_state["logs"].append(line)
    logger.info(msg)


def _clean_url(raw: str) -> str:
    try:
        cleaned = raw.encode("utf-8").decode("unicode_escape")
    except (UnicodeDecodeError, UnicodeError):
        cleaned = raw
    cleaned = cleaned.replace("\\u002F", "/")
    cleaned = cleaned.replace("\\u002f", "/")
    cleaned = re.sub(r'(?<!:)/{2,}', '/', cleaned)
    return cleaned


def _style_id_to_url(style_id_or_url: str) -> str:
    value = style_id_or_url.strip()
    if value.startswith(("http://", "https://")):
        return value
    return f"https://www.myntra.com/{value}"


def _is_myntra_error_page(html: str) -> bool:
    return any(marker.lower() in html.lower() for marker in MYNTRA_ERROR_MARKERS)


def _has_product_data(data: dict[str, Any]) -> bool:
    required = (data.get("title"), data.get("brand"), data.get("current_price"))
    return any(bool(value) for value in required) and data.get("title") != "Oops! Something went wrong"


def _parse_product_page(html: str, url: str, style_id: str) -> dict[str, Any]:
    """Extract product data from a Myntra product detail page."""
    soup = BeautifulSoup(html, "lxml")
    data: dict[str, Any] = {
        "style_id": style_id,
        "url": url,
        "title": "",
        "brand": "",
        "current_price": "",
        "original_price": "",
        "discount": "",
        "rating": "",
        "rating_count": "",
        "review_count": "",
        "description": "",
        "category": "",
        "images": "",
        "video_url": "",
        "colors": "",
        "sizes": "",
        "material": "",
        "seller": "",
        "customer_reviews": "",
        "customer_reviews_count": "",
        "customer_ratings_count": "",
    }

    # ================================================================
    # 1. LD+JSON — Product schema + BreadcrumbList
    # ================================================================
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script.string or "")
            if isinstance(ld, list):
                ld = ld[0]

            if ld.get("@type") == "Product":
                data["title"] = ld.get("name", "")
                data["brand"] = (
                    (ld.get("brand") or {}).get("name", "")
                    if isinstance(ld.get("brand"), dict)
                    else str(ld.get("brand", ""))
                )
                data["description"] = ld.get("description", "")
                offers = ld.get("offers", {})
                if isinstance(offers, dict):
                    data["current_price"] = offers.get("price", "")
                elif isinstance(offers, list) and offers:
                    data["current_price"] = offers[0].get("price", "")
                agg = ld.get("aggregateRating", {})
                if agg:
                    data["rating"] = agg.get("ratingValue", "")
                    data["rating_count"] = agg.get("ratingCount", "")
                    data["review_count"] = agg.get("reviewCount", "")

            elif ld.get("@type") == "BreadcrumbList":
                items = ld.get("itemListElement", [])
                if items:
                    data["category"] = " > ".join(
                        item.get("item", {}).get("name", "")
                        for item in sorted(items, key=lambda x: x.get("position", 0))
                    )
        except (json.JSONDecodeError, AttributeError, TypeError):
            pass

    # ================================================================
    # 2. Embedded script JSON
    # ================================================================
    for script in soup.find_all("script"):
        txt = script.string or ""
        if len(txt) < 100:
            continue

        # prices
        if not data["current_price"]:
            m = re.search(r'"discountedPrice"\s*:\s*(\d+)', txt)
            if m:
                data["current_price"] = m.group(1)
        if not data["original_price"]:
            m = re.search(r'"mrp"\s*:\s*(\d+)', txt)
            if m:
                data["original_price"] = m.group(1)
        if not data["discount"]:
            m = re.search(r'"discountDisplayLabel"\s*:\s*"([^"]*)"', txt)
            if m:
                data["discount"] = m.group(1)

        # brand / title
        if not data["brand"]:
            m = re.search(r'"brand"\s*:\s*\{[^}]*"name"\s*:\s*"([^"]*)"', txt)
            if m:
                data["brand"] = m.group(1)
        if not data["title"]:
            m = re.search(r'"productName"\s*:\s*"([^"]*)"', txt)
            if m:
                data["title"] = m.group(1)

        # ratings
        if not data["rating"]:
            m = re.search(r'"averageRating"\s*:\s*([\d.]+)', txt)
            if m:
                data["rating"] = m.group(1)
        if not data["rating_count"]:
            m = re.search(r'"totalRatingsCount"\s*:\s*(\d+)', txt)
            if m:
                data["rating_count"] = m.group(1)
        if not data["review_count"]:
            m = re.search(r'"totalReviewsCount"\s*:\s*(\d+)', txt)
            if m:
                data["review_count"] = m.group(1)

        # sizes
        if not data["sizes"]:
            sizes = re.findall(
                r'"label"\s*:\s*"([^"]+)"[^}]*"available"\s*:\s*true', txt
            )
            if sizes:
                data["sizes"] = ", ".join(sizes)

        # colors
        if not data["colors"]:
            colors = re.findall(r'"colorValue"\s*:\s*"([^"]+)"', txt)
            if not colors:
                colors = re.findall(r'"color"\s*:\s*"([^"]+)"', txt)
            if colors:
                data["colors"] = ", ".join(dict.fromkeys(colors))

        # seller
        if not data["seller"]:
            m = re.search(r'"sellerName"\s*:\s*"([^"]*)"', txt)
            if m:
                data["seller"] = m.group(1)

        # material / fabric
        if not data["material"]:
            m = re.search(r'"Fabric"\s*:\s*"([^"]*)"', txt)
            if not m:
                m = re.search(r'"Material"\s*:\s*"([^"]*)"', txt)
            if m:
                data["material"] = m.group(1)

    # ================================================================
    # 3. Product images
    # ================================================================
    raw_img_urls = re.findall(r'"imageURL"\s*:\s*"([^"]+)"', html)
    product_imgs: list[str] = []
    seen_filenames: set[str] = set()
    for raw_url in raw_img_urls:
        try:
            img_url = raw_url.encode("utf-8").decode("unicode_escape")
        except (UnicodeDecodeError, UnicodeError):
            img_url = raw_url.replace("\\u002F", "/").replace("\\u002f", "/")
        if img_url.startswith("http://"):
            img_url = img_url.replace("http://", "https://", 1)
        elif not img_url.startswith("http"):
            img_url = "https://" + img_url
        if "myntassets.com" in img_url and "/h_" not in img_url:
            img_url = re.sub(
                r'(myntassets\.com/)(.*)$',
                r'\1h_720,q_90,w_540/\2',
                img_url,
            )
        fname_m = re.search(r'/([^/]+\.(?:jpg|jpeg|png|webp))', img_url)
        if fname_m:
            fname = fname_m.group(1)
            if fname in seen_filenames:
                continue
            seen_filenames.add(fname)
        product_imgs.append(img_url)

    if product_imgs:
        data["images"] = " | ".join(product_imgs[:10])

    # ================================================================
    # 3b. Customer reviews
    # ================================================================
    review_texts = re.findall(r'"reviewText"\s*:\s*"([^"]+)"', html)
    review_users = re.findall(r'"userName"\s*:\s*"([^"]+)"', html)
    review_ratings = re.findall(r'"userRating"\s*:\s*(\d+)', html)

    if review_texts:
        reviews_list = []
        for idx, text in enumerate(review_texts):
            user = review_users[idx] if idx < len(review_users) else "Anonymous"
            star = review_ratings[idx] if idx < len(review_ratings) else "?"
            reviews_list.append(f"★{star} {user}: {text}")
        data["customer_reviews"] = " | ".join(reviews_list)

    rc = re.search(r'"reviewsCount"\s*:\s*"?(\d+)"?', html)
    if rc:
        data["customer_reviews_count"] = rc.group(1)

    for rating_pat in [
        r'"totalRatingsCount"\s*:\s*"?(\d+)"?',
        r'"ratingCount"\s*:\s*"?(\d+)"?',
    ]:
        rtc = re.search(rating_pat, html)
        if rtc:
            data["customer_ratings_count"] = rtc.group(1)
            break

    # ================================================================
    # 3c. Video URL
    # ================================================================
    if not data["video_url"]:
        # Extract video from embedded "videos" JSON array (Myntra's format)
        videos_match = re.search(r'"videos"\s*:\s*\[([^\]]+)\]', html)
        if videos_match:
            videos_json = videos_match.group(1)
            # Check for Brightcove video
            if "Brightcove" in videos_json:
                # Extract video reference ID
                vid_id_match = re.search(r'"id"\s*:\s*"([^"]+)"', videos_json)
                if vid_id_match:
                    ref_id = vid_id_match.group(1)
                    # Myntra uses Brightcove account 5745608584001
                    bc_account = "5745608584001"
                    data["video_url"] = f"https://players.brightcove.net/{bc_account}/default_default/index.html?videoId=ref:{ref_id}"
        
        # Check for Brightcove video player (dynamically loaded)
        if not data["video_url"]:
            bc_video_id = re.search(r'data-video-id="(\d+)"', html)
            bc_account_id = re.search(r'players\.brightcove\.net/(\d+)/', html)
            
            if bc_video_id and bc_account_id:
                # Construct Brightcove player embed URL
                vid_id = bc_video_id.group(1)
                acc_id = bc_account_id.group(1)
                data["video_url"] = f"https://players.brightcove.net/{acc_id}/default_default/index.html?videoId={vid_id}"
            else:
                # Also try reference ID pattern (ref:rw-STYLEID)
                bc_ref_id = re.search(r'data-video-id="(ref:[^"]+)"', html)
                if bc_ref_id and bc_account_id:
                    ref_id = bc_ref_id.group(1)
                    acc_id = bc_account_id.group(1)
                    data["video_url"] = f"https://players.brightcove.net/{acc_id}/default_default/index.html?videoId={ref_id}"
        
        # Fallback to direct video URL patterns
        if not data["video_url"]:
            for vid_pat in [
                r'"videoUrl"\s*:\s*"([^"]+)"',
                r'"videoURL"\s*:\s*"([^"]+)"',
                r'"src"\s*:\s*"([^"]*\.mp4[^"]*)"',
            ]:
                vm = re.search(vid_pat, html)
                if vm:
                    vid_url = vm.group(1)
                    try:
                        vid_url = vid_url.encode("utf-8").decode("unicode_escape")
                    except (UnicodeDecodeError, UnicodeError):
                        vid_url = vid_url.replace("\\u002F", "/")
                    data["video_url"] = vid_url
                    break

    # ================================================================
    # 4. HTML selectors (server-rendered content)
    # ================================================================
    def _text(selector: str) -> str:
        el = soup.select_one(selector)
        return el.get_text(strip=True) if el else ""

    if not data["title"]:
        data["title"] = _text("h1.pdp-title") or _text(".pdp-name") or _text("h1")
    if not data["brand"]:
        data["brand"] = (
            _text("h1.pdp-title .pdp-title")
            or _text(".pdp-name .brand-name")
            or _text("h1 a")
        )
    if not data["current_price"]:
        data["current_price"] = _text(".pdp-price strong") or _text(".pdp-price")
    if not data["original_price"]:
        data["original_price"] = _text(".pdp-mrp s") or _text(".pdp-mrp")
    if not data["discount"]:
        data["discount"] = _text(".pdp-discount")
    if not data["rating"]:
        data["rating"] = _text(".index-overallRating") or _text(".rating-star span")

    # Sizes from HTML
    if not data["sizes"]:
        size_els = soup.select(
            ".size-buttons-buttonContainer button, .size-buttons-tipAnd498 p"
        )
        if size_els:
            data["sizes"] = ", ".join(s.get_text(strip=True) for s in size_els)

    # Colors from HTML
    if not data["colors"]:
        color_els = soup.select(".colors-list li, .desktop-swatch")
        if color_els:
            data["colors"] = ", ".join(
                c.get("title", c.get_text(strip=True)) for c in color_els
            )

    # Images from HTML (last resort)
    if not data["images"]:
        grid_imgs = []
        for div in soup.select(".image-grid-imageContainer .image-grid-image"):
            style = div.get("style", "")
            m_bg = re.search(r'url\(["\']?([^"\')+]+)["\']?\)', style)
            if m_bg:
                grid_imgs.append(m_bg.group(1))
        if not grid_imgs:
            grid_imgs = [
                img.get("src") or img.get("data-src", "")
                for img in soup.select(".pdp-image img")
            ]
        data["images"] = " | ".join(i for i in grid_imgs[:10] if i)

    # Video from HTML (last resort)
    if not data["video_url"]:
        video = soup.select_one("video source")
        if video:
            data["video_url"] = video.get("src", "")

    # Material / seller from spec table
    if not data["material"] or not data["seller"]:
        for row in soup.select(
            ".index-tableContainer .index-row, .pdp-sizeFitDesc tr"
        ):
            cells = row.select("td, div")
            if len(cells) >= 2:
                key = cells[0].get_text(strip=True).lower()
                val = cells[1].get_text(strip=True)
                if ("material" in key or "fabric" in key) and not data["material"]:
                    data["material"] = val
                elif "seller" in key and not data["seller"]:
                    data["seller"] = val

    # ================================================================
    # 5. Meta tag fallbacks
    # ================================================================
    if not data["description"]:
        meta = soup.find("meta", attrs={"name": "description"})
        if meta:
            data["description"] = meta.get("content", "")

    if not data["category"]:
        canonical = soup.find("link", rel="canonical")
        if canonical:
            parts = canonical.get("href", "").replace("https://www.myntra.com/", "").split("/")
            if len(parts) >= 2:
                data["category"] = parts[0].replace("-", " ").title()

    # Calculate discount %
    if not data["discount"] and data["current_price"] and data["original_price"]:
        try:
            cp = float(str(data["current_price"]).replace(",", ""))
            op = float(str(data["original_price"]).replace(",", ""))
            if op > cp:
                pct = round((1 - cp / op) * 100)
                data["discount"] = f"({pct}% OFF)"
        except (ValueError, ZeroDivisionError):
            pass

    return data


# ---------------------------------------------------------------------------
# Core async scraping
# ---------------------------------------------------------------------------

async def _fetch(client: httpx.AsyncClient, url: str) -> str | None:
    """GET a URL with retries."""
    for attempt in range(3):
        try:
            resp = await client.get(url, headers=HEADERS, follow_redirects=True, timeout=20)
            if resp.status_code == 200:
                return resp.text
            _log(f"⚠ HTTP {resp.status_code} for {url}")
        except httpx.HTTPError as exc:
            _log(f"⚠ Attempt {attempt+1} error: {exc}")
        await asyncio.sleep(random.uniform(1.0, 2.0))
    return None


async def _scrape_by_style_id(
    sem: asyncio.Semaphore,
    client: httpx.AsyncClient,
    style_id: str,
) -> dict[str, Any] | None:
    """Scrape a single product by its Myntra style ID."""
    async with sem:
        if scraper_state["stop_event"].is_set():
            return None
        await asyncio.sleep(random.uniform(0.5, 1.5))

        url = f"https://www.myntra.com/{style_id}"
        html = await _fetch(client, url)

        if not html:
            _log(f"✗ Failed to fetch style ID: {style_id}")
            scraper_state["failed_count"] += 1
            scraper_state["scraped_count"] += 1
            count = scraper_state["scraped_count"]
            total = scraper_state["total_items"]
            _log(f"→ [{count}/{total}] FAILED — {style_id}")
            return None

        data = _parse_product_page(html, url, style_id)
        scraper_state["scraped_count"] += 1
        count = scraper_state["scraped_count"]
        total = scraper_state["total_items"]
        label = data.get("title") or data.get("brand") or style_id
        _log(f"→ [{count}/{total}] {label}")
        return data


async def _fetch(client: httpx.AsyncClient, url: str) -> str | None:
    """GET a URL with retries and reject Myntra error pages."""
    for attempt in range(3):
        try:
            resp = await client.get(url, headers=HEADERS, follow_redirects=True, timeout=20)
            if resp.status_code == 200:
                if _is_myntra_error_page(resp.text):
                    _log(f"Myntra returned an error page for {url}; retrying...")
                    await asyncio.sleep(random.uniform(8.0, 14.0))
                    continue
                return resp.text
            _log(f"HTTP {resp.status_code} for {url}")
        except httpx.HTTPError as exc:
            _log(f"Attempt {attempt + 1} error: {exc}")
        await asyncio.sleep(random.uniform(4.0, 8.0))
    return None


def _mark_failed(style_id: str, reason: str) -> None:
    scraper_state["failed_count"] += 1
    scraper_state["scraped_count"] += 1
    count = scraper_state["scraped_count"]
    total = scraper_state["total_items"]
    _log(f"FAILED {style_id}: {reason}")
    _log(f"-> [{count}/{total}] FAILED - {style_id}")


async def _scrape_by_style_id(
    sem: asyncio.Semaphore,
    client: httpx.AsyncClient,
    style_id: str,
) -> dict[str, Any] | None:
    """Scrape a single product by style ID or full Myntra URL."""
    async with sem:
        if scraper_state["stop_event"].is_set():
            return None
        await asyncio.sleep(random.uniform(*REQUEST_DELAY_RANGE))

        url = _style_id_to_url(style_id)
        html = await _fetch(client, url)

        if not html:
            _mark_failed(style_id, "no usable product page returned")
            return None

        data = _parse_product_page(html, url, style_id)
        if not _has_product_data(data):
            _mark_failed(style_id, "Myntra returned non-product HTML")
            return None

        scraper_state["scraped_count"] += 1
        count = scraper_state["scraped_count"]
        total = scraper_state["total_items"]
        label = data.get("title") or data.get("brand") or style_id
        _log(f"-> [{count}/{total}] {label}")
        return data


async def scrape_task(style_ids: list[str]) -> None:
    """Main background scraping coroutine."""
    state = scraper_state
    state["running"] = True
    state["stop_event"].clear()
    state["logs"].clear()
    state["products"].clear()
    state["scraped_count"] = 0
    state["failed_count"] = 0
    state["total_items"] = len(style_ids)
    state["output_file"] = None

    _log(f"🔍 Starting scrape for {len(style_ids)} style IDs…")

    async with httpx.AsyncClient(http2=False) as client:
        sem = asyncio.Semaphore(MAX_CONCURRENCY)

        tasks = []
        for sid in style_ids:
            if state["stop_event"].is_set():
                break
            tasks.append(_scrape_by_style_id(sem, client, str(sid)))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, dict):
                state["products"].append(r)

    # Save Excel
    if state["products"]:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = DOWNLOADS_DIR / f"myntra_styleid_{ts}.xlsx"
        df = pd.DataFrame(state["products"])
        df.to_excel(filename, index=False, engine="openpyxl")
        state["output_file"] = str(filename)
        _log(f"✅ Saved {len(state['products'])} products → {filename.name}")
        if state["failed_count"] > 0:
            _log(f"⚠ {state['failed_count']} style IDs failed to scrape.")
    else:
        _log("⚠ No products scraped — nothing to save.")

    if state["stop_event"].is_set():
        _log("🛑 STOPPED by user.")
    else:
        _log("🏁 DONE")

    state["running"] = False


# ---------------------------------------------------------------------------
# FastAPI routes
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.head("/")
async def index_head():
    return Response(status_code=200)


@app.post("/upload")
async def upload_and_start(file: UploadFile = File(...)):
    """Upload an Excel file with Style IDs and start scraping."""
    if scraper_state["running"]:
        return {"status": "error", "message": "A scraping task is already running."}

    # Validate file type
    fname = file.filename or ""
    if not fname.lower().endswith((".xlsx", ".xls")):
        return {"status": "error", "message": "Please upload an .xlsx or .xls file."}

    # Save uploaded file
    save_path = UPLOADS_DIR / fname
    contents = await file.read()
    with open(save_path, "wb") as f:
        f.write(contents)

    # Read style IDs
    try:
        df = pd.read_excel(save_path)
    except Exception as e:
        return {"status": "error", "message": f"Could not read Excel file: {e}"}

    # Find the style ID column (case-insensitive)
    style_col = None
    for col in df.columns:
        if "style" in str(col).lower() and "id" in str(col).lower():
            style_col = col
            break
    if style_col is None:
        # Also try just "id" or first column
        for col in df.columns:
            if str(col).strip().lower() in ("id", "styleid", "style_id"):
                style_col = col
                break
    if style_col is None:
        # Use first column as fallback
        style_col = df.columns[0]

    style_ids = df[style_col].dropna().astype(str).str.strip().tolist()
    # Remove .0 from float conversion
    style_ids = [sid.replace(".0", "") for sid in style_ids if sid and sid != "nan"]

    if not style_ids:
        return {"status": "error", "message": "No style IDs found in the uploaded file."}

    # Launch background task
    loop = asyncio.get_event_loop()
    scraper_state["task"] = loop.create_task(scrape_task(style_ids))

    return {
        "status": "started",
        "message": f"Found {len(style_ids)} style IDs. Scraping started!",
        "total": len(style_ids),
    }


@app.post("/stop")
async def stop_scraping():
    if not scraper_state["running"]:
        return {"status": "info", "message": "No active task."}
    scraper_state["stop_event"].set()
    return {"status": "stopped", "message": "Stop signal sent."}


@app.get("/progress")
async def progress():
    """SSE endpoint streaming log lines."""
    async def event_generator():
        last_index = 0
        while True:
            logs = scraper_state["logs"]
            while last_index < len(logs):
                yield f"data: {logs[last_index]}\n\n"
                last_index += 1

            if not scraper_state["running"] and last_index >= len(logs):
                if scraper_state.get("output_file"):
                    yield f"data: __FILE_READY__\n\n"
                yield f"data: __END__\n\n"
                break

            await asyncio.sleep(0.4)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.get("/download")
async def download():
    filepath = scraper_state.get("output_file")
    if not filepath or not os.path.isfile(filepath):
        return {"status": "error", "message": "No file available for download."}
    return FileResponse(
        filepath,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=os.path.basename(filepath),
    )


@app.get("/status")
async def status():
    return {
        "running": scraper_state["running"],
        "scraped": scraper_state["scraped_count"],
        "total": scraper_state["total_items"],
        "failed": scraper_state["failed_count"],
        "has_file": scraper_state.get("output_file") is not None,
    }


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8001))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=True)

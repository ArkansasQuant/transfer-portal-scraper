import asyncio
import random
import pandas as pd
import re
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from datetime import datetime
from fake_useragent import UserAgent

# --- CONFIGURATION ---
BASE_URL = "https://247sports.com/season/2026-football/transferportalpositionranking/"
CONCURRENCY_LIMIT = 4
MAX_RETRIES = 5
OUTPUT_FILE = f"transfer_portal_2026_FINAL_{datetime.now().strftime('%Y%m%d')}.csv"

# Valid Football Positions (Whitelisted to ignore State Ranks like KS, FL, TX)
VALID_POSITIONS = {
    'QB', 'RB', 'WR', 'TE', 'OT', 'IOL', 'OC', 'DL', 'EDGE',
    'LB', 'CB', 'S', 'ATH', 'K', 'P', 'LS', 'RET'
}

# --- UTILS ---
def clean_text(text):
    if not text:
        return None
    # normalize N/A, -, empty strings to "NA"
    t = text.strip()
    if t in ["N/A", "", "-"]:
        return "NA"
    return t

def extract_id_from_url(url):
    match = re.search(r'-(\d+)(?:/|$)', url)
    return match.group(1) if match else "NA"

async def random_delay():
    await asyncio.sleep(random.uniform(1.0, 2.0))

# ---------- Helper small functions for robust parsing ----------
def _find_section_by_heading(soup, heading_snippet):
    """Find a container nearest the heading text. Returns a sensible section node or None."""
    # find any tag whose text contains the snippet (common headings are h2/h3/h4/div/span)
    heading = soup.find(lambda t: t.name in ['h1','h2','h3','h4','div','span'] and heading_snippet.lower() in (t.get_text(" ", strip=True) or "").lower())
    if not heading:
        return None
    # prefer a parent with explicit 'rank'/'rating' class or a section element
    for anc in heading.parents:
        classes = " ".join(anc.get("class") or [])
        if anc.name in ['section','div'] and ('rank' in classes.lower() or 'rating' in classes.lower() or 'player' in classes.lower() or 'profile' in classes.lower()):
            return anc
        if anc.name == 'section':
            return anc
    # fallback to next sibling container or heading.parent
    sib = heading.find_next_sibling()
    if sib and sib.name in ['div','section']:
        return sib
    return heading.parent

def _count_stars(container):
    """Robust star counting inside a container: count elements whose class contains 'star' and are solid."""
    if not container:
        return 0
    stars = 0
    for el in container.select('*'):
        cls = " ".join(el.get('class') or [])
        # common solid-star class patterns: 'icon-starsolid', 'starsolid', 'star solid'
        # count only when 'star' in class and 'solid' or 'starsolid' exists or icon pattern exists
        if 'star' in cls.lower() and ('solid' in cls.lower() or 'starsolid' in cls.lower() or 'icon' in cls.lower()):
            stars += 1
    return stars

def _normalize_label(label_text):
    """Normalize label text to canonical short tokens (OVR, NATL, POS code)"""
    if not label_text:
        return ""
    t = re.sub(r'[^A-Za-z0-9 ]', ' ', label_text).upper().strip()
    # if contains OVERALL or OVR -> OVR
    if 'OVERALL' in t or 'OVR' in t:
        return 'OVR'
    if 'NATL' in t or 'NATIONAL' in t:
        return 'NATL'
    # produce tokens like QB, RB, etc (match any VALID_POSITIONS member appearing anywhere)
    for pos in VALID_POSITIONS:
        # match exact token in split to avoid accidental substrings
        if pos in t.split():
            return pos
    return t

# --- PARSING LOGIC ---
def parse_profile(html, url, player_id):
    soup = BeautifulSoup(html, 'lxml')
    data = {}

    # 1. HEADER INFO
    data['247 ID'] = player_id
    name_tag = soup.select_one('.name') or soup.select_one('h1.name')
    data['Player Name'] = clean_text(name_tag.text) if name_tag else "NA"

    # Defaults
    data['Position'] = "NA"
    data['Height'] = "NA"
    data['Weight'] = "NA"
    data['High School'] = "NA"
    data['City, ST'] = "NA"
    data['EXP'] = "NA"

    # Header Parsing
    all_header_items = soup.select('.metrics-list li') + soup.select('.details li')
    for item in all_header_items:
        text = item.get_text(strip=True)
        # Use regex to find Label:Value patterns
        if 'Pos' in text or 'Position' in text:
            match = re.search(r'(?:Pos|Position)[:\s]*(.*)', text, re.IGNORECASE)
            if match:
                data['Position'] = match.group(1).strip()
        elif 'Height' in text:
            match = re.search(r'Height[:\s]*(.*)', text, re.IGNORECASE)
            if match:
                data['Height'] = f"'{match.group(1).strip()}"
        elif 'Weight' in text:
            match = re.search(r'Weight[:\s]*(.*)', text, re.IGNORECASE)
            if match:
                data['Weight'] = match.group(1).strip()
        elif 'High School' in text:
            match = re.search(r'High School[:\s]*(.*)', text, re.IGNORECASE)
            if match:
                data['High School'] = match.group(1).strip()
        elif 'Home Town' in text or 'Hometown' in text or 'City' in text:
            match = re.search(r'(?:Home Town|Hometown|City)[:\s]*(.*)', text, re.IGNORECASE)
            if match:
                data['City, ST'] = match.group(1).strip()
        elif 'Class' in text or 'Exp' in text:
            match = re.search(r'(?:Class|Exp)[:\s]*(.*)', text, re.IGNORECASE)
            if match:
                data['EXP'] = match.group(1).strip()

    # --- TEAM LOGIC ---
    data['Team'] = "NA"
    logo = soup.select_one('.primary-team-logo')
    if logo and logo.get('alt'):
        data['Team'] = logo.get('alt')
    elif soup.select_one('.ni-school-name a'):
        data['Team'] = soup.select_one('.ni-school-name a').text.strip()

    # Additional fallback team detection
    if data['Team'] == "NA":
        # try img alt patterns
        img = soup.find('img', alt=True)
        if img and img.get('alt') and len(img.get('alt')) < 60:
            data['Team'] = img.get('alt').strip()
    if data['Team'] == "NA":
        candidate = soup.find(lambda t: t.name == 'a' and t.get('href') and '/team/' in t.get('href'))
        if candidate:
            data['Team'] = candidate.get_text(strip=True)

    data['Transfer Team Name'] = "NA"
    banner = soup.select_one('.qa-team-name')
    if banner:
        data['Transfer Team Name'] = banner.text.strip()

    # --- PARSE TRANSFER ---
    data['Transfer Stars'] = "NA"
    data['Transfer Rating'] = "NA"
    data['Transfer Year'] = "NA"
    data['Transfer Overall Rank'] = "NA"
    data['Transfer Position Rank'] = "NA"

    transfer_section = _find_section_by_heading(soup, "As a Transfer")
    if transfer_section:
        # stars
        star_count = _count_stars(transfer_section)
        data['Transfer Stars'] = str(star_count) if star_count > 0 else "0"
        # rating (guarded)
        rating_el = transfer_section.select_one('.rating') or transfer_section.find(lambda t: t.name in ['strong','span','div'] and re.match(r'^\d{2,3}$', (t.get_text(strip=True) or '')))
        if rating_el:
            data['Transfer Rating'] = clean_text(rating_el.get_text(strip=True))
        # try to capture a transfer year if present (e.g., "Class of 2026" or "(2026)")
        year_match = re.search(r'(20\d{2})', transfer_section.get_text(" ", strip=True))
        if year_match:
            data['Transfer Year'] = year_match.group(1)
        # ranks: parse li/h5/strong patterns but normalize labels
        for li in transfer_section.select('li'):
            label_tag = li.select_one('h5') or li.select_one('label') or li.select_one('span')
            val_tag = li.select_one('strong') or li.select_one('span.value') or li.find(text=True)
            if label_tag and val_tag:
                label = _normalize_label(label_tag.get_text(strip=True))
                val = clean_text(val_tag.get_text(strip=True))
                if label == 'OVR':
                    data['Transfer Overall Rank'] = val
                elif label in VALID_POSITIONS:
                    data['Transfer Position Rank'] = val

    # --- PARSE PROSPECT ---
    data['Prospect Stars'] = "NA"
    data['Prospect Rating'] = "NA"
    data['Prospect Position Rank'] = "NA"
    data['Prospect National Rank'] = "NA"

    prospect_section = _find_section_by_heading(soup, "As a Prospect")
    if prospect_section:
        header_text = prospect_section.get_text(" ", strip=True).upper()
        is_juco = 'JUCO' in header_text
        star_count = _count_stars(prospect_section)
        if is_juco:
            data['Prospect Stars'] = (f"{star_count} JUCO" if star_count > 0 else "0 JUCO")
        else:
            data['Prospect Stars'] = str(star_count) if star_count > 0 else "0"
        # rating
        rating_el = prospect_section.select_one('.rating') or prospect_section.find(lambda t: t.name in ['strong','span','div'] and re.match(r'^\d{2,3}$', (t.get_text(strip=True) or '')))
        if rating_el:
            rating_val = clean_text(rating_el.get_text(strip=True))
            data['Prospect Rating'] = (f"{rating_val} JUCO" if is_juco and rating_val != "NA" else rating_val)
        else:
            data['Prospect Rating'] = ("NA JUCO" if is_juco else "NA")
        # ranks
        found_natl = False
        for li in prospect_section.select('li'):
            label_tag = li.select_one('h5') or li.select_one('label') or li.select_one('span')
            val_tag = li.select_one('strong') or li.select_one('span.value') or li.find(text=True)
            if label_tag and val_tag:
                label = _normalize_label(label_tag.get_text(strip=True))
                val = clean_text(val_tag.get_text(strip=True))
                if label == 'NATL':
                    data['Prospect National Rank'] = (f"{val} JUCO" if is_juco else val)
                    found_natl = True
                elif label in VALID_POSITIONS:
                    data['Prospect Position Rank'] = (f"{val} JUCO" if is_juco else val)
        if is_juco and not found_natl:
            data['Prospect National Rank'] = "JUCO"

    return data

async def scrape_profile(context, url, sem, failed_urls):
    async with sem:
        for attempt in range(MAX_RETRIES):
            page = await context.new_page()
            await page.route("**/*.{png,jpg,jpeg,svg,mp4,woff,woff2}", lambda route: route.abort())
            try:
                await random_delay()
                await page.goto(url, timeout=60000, wait_until="commit")
                try:
                    await page.wait_for_selector(".name, h1.name", timeout=15000)
                except:
                    pass

                content = await page.content()
                if "Player Profile" not in content and "name" not in content:
                    raise Exception("Blank content")

                player_id = extract_id_from_url(url)
                data = parse_profile(content, url, player_id)
                data['URL'] = url

                await page.close()
                print(f"   [SUCCESS] {data['Player Name']}")
                return data

            except Exception as e:
                print(f"   [ERROR] {url}: {e}")
                await page.close()
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(5)
                else:
                    failed_urls.append({'url': url, 'reason': str(e)})
                    return None

async def main():
    ua = UserAgent()
    print("--- Starting FINAL Scraper (Position Whitelist Logic) ---")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=ua.random, viewport={'width': 1920, 'height': 1080})
        page = await c

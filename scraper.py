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

# --- UTILS ---
def clean_text(text):
    if not text: return "NA"
    t = text.strip()
    if t in ["N/A", "", "-"]: return "NA"
    return t

def extract_id_from_url(url):
    match = re.search(r'-(\d+)(?:/|$)', url)
    return match.group(1) if match else "NA"

async def random_delay():
    await asyncio.sleep(random.uniform(1.0, 2.0))

# --- PARSING LOGIC (ROBUST VERSION) ---
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
    
    # Parse Header Metrics
    all_header_items = soup.select('.metrics-list li') + soup.select('.details li')
    for item in all_header_items:
        text = item.get_text(strip=True)
        if 'Pos' in text or 'Position' in text:
            match = re.search(r'(?:Pos|Position)[:\s]*(.*)', text, re.IGNORECASE)
            if match: data['Position'] = match.group(1).strip()
        elif 'Height' in text:
            match = re.search(r'Height[:\s]*(.*)', text, re.IGNORECASE)
            if match: data['Height'] = f"'{match.group(1).strip()}"
        elif 'Weight' in text:
            match = re.search(r'Weight[:\s]*(.*)', text, re.IGNORECASE)
            if match: data['Weight'] = match.group(1).strip()
        elif 'High School' in text:
            match = re.search(r'High School[:\s]*(.*)', text, re.IGNORECASE)
            if match: data['High School'] = match.group(1).strip()
        elif 'Home Town' in text or 'Hometown' in text or 'City' in text:
            match = re.search(r'(?:Home Town|Hometown|City)[:\s]*(.*)', text, re.IGNORECASE)
            if match: data['City, ST'] = match.group(1).strip()
        elif 'Class' in text or 'Exp' in text:
            match = re.search(r'(?:Class|Exp)[:\s]*(.*)', text, re.IGNORECASE)
            if match: data['EXP'] = match.group(1).strip()

    # 2. TEAM PARSING
    data['Team'] = "NA"
    data['Transfer Team Name'] = "NA"
    
    # Destination (Transfer Team)
    commit_banner = soup.select_one('.commitment-banner .team-name') or \
                    soup.select_one('.header-commitment .team-name') or \
                    soup.select_one('.main-team-logo.is-committed')
    if commit_banner: 
        data['Transfer Team Name'] = clean_text(commit_banner.get_text(strip=True))

    # Origin Team (Current/Former)
    origin_node = soup.select_one('.crystal-ball .team-name') or \
                  soup.select_one('.prediction-box .team-name') or \
                  soup.select_one('.module-team-logo .team-name')
    
    if origin_node:
        data['Team'] = clean_text(origin_node.get_text(strip=True))
    else:
        # Fallback to primary logo if it's different from destination
        primary = soup.select_one('.primary-team-logo')
        if primary and primary.get('alt'):
            cand = clean_text(primary.get('alt'))
            if cand != data['Transfer Team Name']:
                data['Team'] = cand

    # 3. SECTION PARSING (The "Scan All Lists" Fix)
    all_uls = soup.find_all('ul')
    
    transfer_ul = None
    prospect_ul = None

    # Identify sections by their content keywords
    for ul in all_uls:
        text = ul.get_text(" ", strip=True)
        if "Transfer Rating" in text:
            transfer_ul = ul
        elif ("Natl" in text or "National" in text) and "Transfer" not in text:
            prospect_ul = ul
        elif "Pos" in text and "Transfer" not in text and prospect_ul is None:
            # Fallback for Prospect if no National rank but has Position rank
            if ul.find_previous(class_='icon-starsolid'):
                prospect_ul = ul

    # Helper function to extract data from a found list
    def extract_section_data(ul_node, is_juco_check=False):
        res = {'stars': '0', 'rating': 'NA', 'year': 'NA', 'ranks': {}}
        if not ul_node: return res
        
        container = ul_node.find_parent('section') or ul_node.find_parent('div')
        if not container: return res
        
        full_text = container.get_text()
        
        # Year
        ym = re.search(r'\((\d{4})\)', full_text)
        if ym: res['year'] = ym.group(1)
        
        # Stars
        is_juco = "JUCO" in full_text.upper()
        stars = container.select('.icon-starsolid.yellow')
        
        star_count = str(len(stars))
        if is_juco and is_juco_check:
            res['stars'] = f"{star_count} JUCO"
        else:
            res['stars'] = star_count
            
        # Rating
        rating = container.select_one('.rating') or container.select_one('.score')
        if rating:
            val = clean_text(rating.get_text())
            res['rating'] = f"{val} JUCO" if (is_juco and is_juco_check) else val
            
        # Ranks
        for li in ul_node.find_all('li'):
            lbl = li.select_one('h5, .rank-label')
            val = li.select_one('strong, .rank-value')
            if lbl and val:
                l_txt = lbl.get_text(strip=True).upper()
                v_txt = clean_text(val.get_text(strip=True))
                
                if 'OVR' in l_txt: res['ranks']['OVR'] = v_txt
                elif 'NATL' in l_txt or 'NATIONAL' in l_txt: res['ranks']['NATL'] = v_txt
                elif 'POS' in l_txt or 'QB' in l_txt or 'WR' in l_txt or 'S' in l_txt: res['ranks']['POS'] = v_txt
        
        # Apply JUCO label to ranks if needed
        if is_juco and is_juco_check:
             for k in res['ranks']: 
                 if res['ranks'][k] != "NA":
                     res['ranks'][k] = f"{res['ranks'][k]} JUCO"
                     
        return res

    # Extract Transfer
    t_data = extract_section_data(transfer_ul, is_juco_check=False)
    data['Transfer Stars'] = t_data['stars']
    data['Transfer Rating'] = t_data['rating']
    data['Transfer Year'] = t_data['year']
    data['Transfer Overall Rank'] = t_data['ranks'].get('OVR', 'NA')
    data['Transfer Position Rank'] = t_data['ranks'].get('POS', 'NA')
    
    # Extract Prospect
    p_data = extract_section_data(prospect_ul, is_juco_check=True)
    data['Prospect Stars'] = p_data['stars']
    data['Prospect Rating'] = p_data['rating']
    data['Prospect National Rank'] = p_data['ranks'].get('NATL', 'NA')
    data['Prospect Position Rank'] = p_data['ranks'].get('POS', 'NA')

    return data

async def scrape_profile(context, url, sem, failed_urls):
    async with sem:
        for attempt in range(MAX_RETRIES):
            page = await context.new_page()
            await page.route("**/*.{png,jpg,jpeg,svg,mp4,woff,woff2}", lambda route: route.abort())
            try:
                await random_delay()
                await page.goto(url, timeout=60000, wait_until="commit")
                try: await page.wait_for_selector(".name, h1.name", timeout=15000)
                except: pass
                
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
    print("--- Starting FINAL Scraper (Robust Version) ---")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=ua.random, viewport={'width': 1920, 'height': 1080})
        page = await context.new_page()
        
        # 1. LOAD LIST
        print(f"--- 1. Loading List: {BASE_URL} ---")
        await page.goto(BASE_URL, timeout=120000, wait_until="commit")
        try: await page.wait_for_selector(".rankings-page__name-link", timeout=30000)
        except: pass

        # 2. CLICK LOAD MORE
        print("--- 2. Expanding List ---")
        for i in range(300):
            try:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(1)
                load_more = page.locator("text=Load More Players").or_(page.locator(".showmore_lnk"))
                if await load_more.count() > 0 and await load_more.first.is_visible():
                    await load_more.first.click()
                    await asyncio.sleep(4)
                else:
                    await asyncio.sleep(2)
                    if await load_more.count() == 0: break
            except: break
        
        # 3. EXTRACT
        links = await page.eval_on_selector_all("a[href*='/player/']", "elements => elements.map(e => e.href)")
        links = list(set(

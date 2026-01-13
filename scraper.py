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
CONCURRENCY_LIMIT = 5 
MAX_RETRIES = 3
OUTPUT_FILE = f"transfer_portal_2026_TEST_RUN.csv"

# --- UTILS ---
def clean_text(text):
    if not text: return None
    return text.strip()

def normalize_rank(rank):
    if not rank or rank in ['-', '', 'N/A', None]: return 'NA'
    clean = re.sub(r'[^\d]', '', rank)
    return clean if clean else 'NA'

def extract_id_from_url(url):
    match = re.search(r'-(\d+)(?:/|$)', url)
    return match.group(1) if match else "NA"

async def random_delay():
    await asyncio.sleep(random.uniform(0.5, 1.5))

# --- PARSING LOGIC (THE FIX) ---
def parse_profile(html, url, player_id):
    soup = BeautifulSoup(html, 'lxml')
    data = {}
    
    # 1. HEADER INFO
    data['247 ID'] = player_id
    name_tag = soup.select_one('.name') or soup.select_one('h1.name')
    data['Player Name'] = clean_text(name_tag.text) if name_tag else "NA"
    
    # Initialize Defaults
    data['Position'] = "NA"
    data['Height'] = "NA"
    data['Weight'] = "NA"
    data['High School'] = "NA"
    data['City, ST'] = "NA"
    data['EXP'] = "NA"
    
    # Combined Search in Header lists
    all_header_items = soup.select('.metrics-list li') + soup.select('.details li')
    
    for item in all_header_items:
        text = item.get_text(" ", strip=True) 
        
        # KEYWORD REMOVAL LOGIC (Fixes the "NA" issue)
        if 'Pos' in text or 'Position' in text:
            val = re.sub(r'(Pos|Position)[:\s]*', '', text, flags=re.IGNORECASE).strip()
            data['Position'] = val
            
        elif 'Height' in text:
            val = re.sub(r'Height[:\s]*', '', text, flags=re.IGNORECASE).strip()
            data['Height'] = f"'{val}" 
            
        elif 'Weight' in text:
            val = re.sub(r'Weight[:\s]*', '', text, flags=re.IGNORECASE).strip()
            data['Weight'] = val
            
        elif 'High School' in text:
            val = re.sub(r'High School[:\s]*', '', text, flags=re.IGNORECASE).strip()
            data['High School'] = val
            
        elif 'Home Town' in text or 'Hometown' in text:
            val = re.sub(r'(Home Town|Hometown)[:\s]*', '', text, flags=re.IGNORECASE).strip()
            data['City, ST'] = val
            
        elif 'Class' in text or 'Exp' in text:
            val = re.sub(r'(Class|Exp)[:\s]*', '', text, flags=re.IGNORECASE).strip()
            data['EXP'] = val

    # TEAM
    data['Team'] = "NA"
    team_block = soup.select_one('.ni-school-name a')
    if team_block:
        data['Team'] = team_block.text.strip()
    else:
        pred = soup.select_one('.transfer-prediction .team-name')
        if pred: data['Team'] = pred.text.strip()

    # --- TRANSFER SECTION ---
    data['Transfer Stars'] = "0"
    data['Transfer Rating'] = "NA"
    data['Transfer Year'] = "2026"
    data['Transfer Overall Rank'] = "NA"
    data['Transfer Position Rank'] = "NA"
    data['Transfer Team Name'] = data['Team']

    t_sect = soup.select_one('.transfer-rankings')
    if t_sect:
        stars = t_sect.select('.icon-starsolid.yellow')
        data['Transfer Stars'] = len(stars)
        
        rating = t_sect.select_one('.rating')
        if rating: data['Transfer Rating'] = rating.text.strip()
        
        for li in t_sect.select('.ranks-list li'):
            txt = li.get_text(" ", strip=True)
            val_match = re.search(r'(\d+)', txt)
            if not val_match: continue
            val = val_match.group(1)
            
            if 'OVR' in txt or 'National' in txt:
                data['Transfer Overall Rank'] = val
            elif 'Pos' in txt or data['Position'] in txt:
                data['Transfer Position Rank'] = val

    # --- PROSPECT SECTION ---
    data['Prospect Stars'] = "0"
    data['Prospect Rating'] = "NA"
    data['Prospect Position Rank'] = "NA"
    data['Prospect National Rank'] = "NA"
    
    p_sect = soup.select_one('.prospect-rankings')
    if not p_sect:
        for h in soup.select('h2, h3, h4'):
            if 'High School' in h.text or 'JUCO' in h.text:
                p_sect = h.find_parent('section')
                break
    
    if p_sect:
        is_juco = "JUCO" in p_sect.get_text()
        stars = p_sect.select('.icon-starsolid.yellow')
        
        if is_juco and len(stars) == 0:
            data['Prospect Stars'] = "JUCO"
        else:
            data['Prospect Stars'] = len(stars)
            
        rating = p_sect.select_one('.rating')
        if rating: data['Prospect Rating'] = rating.text.strip()
        
        for li in p_sect.select('.ranks-list li'):
            txt = li.get_text(" ", strip=True)
            val_match = re.search(r'(\d+)', txt)
            if not val_match: continue
            val = val_match.group(1)
            
            if 'Natl' in txt or 'National' in txt:
                data['Prospect National Rank'] = val
            elif 'Pos' in txt or data['Position'] in txt:
                data['Prospect Position Rank'] = val

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
                    await asyncio.sleep(2)
                else:
                    failed_urls.append({'url': url, 'reason': str(e)})
                    return None

async def main():
    ua = UserAgent()
    print("--- Starting SMOKE TEST (15 Players) ---")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=ua.random, viewport={'width': 1920, 'height': 1080})
        page = await context.new_page()
        
        # 1. LOAD LIST
        print(f"--- 1. Loading List ---")
        await page.goto(BASE_URL, timeout=90000, wait_until="commit")
        try: await page.wait_for_selector(".rankings-page__name-link", timeout=30000)
        except: pass

        # 2. CLICK LOAD MORE (Only twice for speed)
        for i in range(2):
            try:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(1)
                load_more = page.locator("text=Load More Players").or_(page.locator(".showmore_lnk"))
                if await load_more.count() > 0 and await load_more.first.is_visible():
                    await load_more.first.click()
                    await asyncio.sleep(3)
                else: break
            except: break
        
        # 3. EXTRACT
        links = await page.eval_on_selector_all("a[href*='/player/']", "elements => elements.map(e => e.href)")
        links = list(set([l for l in links if "247sports.com/player/" in l]))
        
        # --- LIMIT TO 15 PLAYERS FOR TEST ---
        links = links[:15]
        print(f"   TEST MODE: Reduced to {len(links)} profiles.")
        
        await page.close()

        # 4. SCRAPE
        sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
        failed_urls = []
        tasks = [scrape_profile(context, link, sem, failed_urls) for link in links]
        results = await asyncio.gather(*tasks)
        
        df = pd.DataFrame([r for r in results if r])
        cols = [
            "247 ID", "Player Name", "Position", "Height", "Weight", "High School", "City, ST", "EXP", "Team",
            "Transfer Stars", "Transfer Rating", "Transfer Year", "Transfer Overall Rank", "Transfer Position Rank", "Transfer Team Name",
            "Prospect Stars", "Prospect Rating", "Prospect Position Rank", "Prospect National Rank"
        ]
        df = df.reindex(columns=cols)
        df.to_csv(OUTPUT_FILE, index=False)
        print(f"--- TEST COMPLETE. Saved {len(df)} rows. ---")
        
        if failed_urls: pd.DataFrame(failed_urls).to_csv("failed_urls.csv", index=False)
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())

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
CONCURRENCY_LIMIT = 4   # Safe speed for overnight run
MAX_RETRIES = 5         # High persistence for network blips
OUTPUT_FILE = f"transfer_portal_2026_FINAL_{datetime.now().strftime('%Y%m%d')}.csv"

# --- UTILS ---
def clean_text(text):
    if not text: return None
    return text.strip()

def extract_id_from_url(url):
    match = re.search(r'-(\d+)(?:/|$)', url)
    return match.group(1) if match else "NA"

async def random_delay():
    # 1-2 second delay to be polite and avoid blocks
    await asyncio.sleep(random.uniform(1.0, 2.0))

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
    
    # Header Parsing (Handles "CityWest Linn" glued text issue)
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

    # --- TEAM LOGIC ---
    
    # 1. "Team" (Current/Previous School)
    # Checks Logo Alt Text -> Link Text
    data['Team'] = "NA"
    logo = soup.select_one('.primary-team-logo')
    if logo and logo.get('alt'):
        data['Team'] = logo.get('alt')
    elif soup.select_one('.ni-school-name a'):
        data['Team'] = soup.select_one('.ni-school-name a').text.strip()

    # 2. "Transfer Team Name" (Destination School)
    # STRICT RULE: Only populate if there is a Commit/Signed Banner.
    data['Transfer Team Name'] = "NA"
    banner = soup.select_one('.qa-team-name')
    if banner:
        # Note: Captures full name (e.g. "Michigan Wolverines"). 
        # Mascots included to ensure accuracy of data scrape.
        data['Transfer Team Name'] = banner.text.strip()
    
    # --- RANKINGS SECTION IDENTIFICATION ---
    # Find specific text nodes to locate the correct Ranking Boxes
    transfer_node = soup.find(string=re.compile("As a Transfer"))
    prospect_node = soup.find(string=re.compile("As a Prospect"))

    # --- PARSE TRANSFER ("As a Transfer") ---
    data['Transfer Stars'] = "0"
    data['Transfer Rating'] = "NA"
    data['Transfer Year'] = "2026"
    data['Transfer Overall Rank'] = "NA"
    data['Transfer Position Rank'] = "NA"

    if transfer_node:
        t_container = transfer_node.find_parent('section') or transfer_node.find_parent('div')
        if t_container:
            stars = t_container.select('.icon-starsolid.yellow')
            data['Transfer Stars'] = len(stars)
            
            rating = t_container.select_one('.rating')
            if rating: data['Transfer Rating'] = rating.text.strip()
            
            for li in t_container.select('li'):
                text = li.get_text(" ", strip=True).upper()
                # OVR Rank
                if 'OVR' in text:
                    match = re.search(r'(\d+)', text)
                    if match: data['Transfer Overall Rank'] = match.group(1)
                # Position Rank (Matches "QB", "TE", etc from player position)
                elif data['Position'] and data['Position'].upper() in text.split():
                    match = re.search(r'(\d+)', text)
                    if match: data['Transfer Position Rank'] = match.group(1)

    # --- PARSE PROSPECT ("As a Prospect") ---
    data['Prospect Stars'] = "0"
    data['Prospect Rating'] = "NA"
    data['Prospect Position Rank'] = "NA"
    data['Prospect National Rank'] = "NA"
    
    if prospect_node:
        p_container = prospect_node.find_parent('section') or prospect_node.find_parent('div')
        if p_container:
            # Check for JUCO header
            is_juco = "JUCO" in p_container.get_text()
            stars = p_container.select('.icon-starsolid.yellow')
            
            # JUCO Handling: If JUCO section & 0 stars -> Label "JUCO"
            if is_juco and len(stars) == 0:
                data['Prospect Stars'] = "JUCO"
            else:
                data['Prospect Stars'] = len(stars)
                
            rating = p_container.select_one('.rating')
            if rating: 
                r_text = rating.text.strip()
                data['Prospect Rating'] = r_text if r_text != 'N/A' else 'NA'
            
            for li in p_container.select('li'):
                text = li.get_text(" ", strip=True).upper()
                if "N/A" in text: continue

                # National Rank
                if 'NATL' in text or 'NATIONAL' in text:
                    match = re.search(r'(\d+)', text)
                    if match: data['Prospect National Rank'] = match.group(1)
                # Position Rank
                elif data['Position'] and data['Position'].upper() in text.split():
                    match = re.search(r'(\d+)', text)
                    if match: data['Prospect Position Rank'] = match.group(1)

    return data

async def scrape_profile(context, url, sem, failed_urls):
    async with sem:
        for attempt in range(MAX_RETRIES):
            page = await context.new_page()
            # Block media for speed/stability
            await page.route("**/*.{png,jpg,jpeg,svg,mp4,woff,woff2}", lambda route: route.abort())
            try:
                await random_delay()
                await page.goto(url, timeout=60000, wait_until="commit")
                
                # Smart wait for name
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
    print("--- Starting FINAL GOLDEN COPY Scraper ---")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=ua.random, viewport={'width': 1920, 'height': 1080})
        page = await context.new_page()
        
        # 1. LOAD MAIN LIST
        print(f"--- 1. Loading Main List: {BASE_URL} ---")
        await page.goto(BASE_URL, timeout=120000, wait_until="commit")
        try: await page.wait_for_selector(".rankings-page__name-link", timeout=30000)
        except: pass

        # 2. CLICK LOAD MORE (The "Broad Search" Loop)
        # 300 cycles ensures we hit 1,500+ players easily
        print("--- 2. Expanding List (Clicking Load More) ---")
        for i in range(300):
            try:
                # Scroll to trigger button visibility
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(1)
                
                # Locate button (handles two common class names)
                load_more = page.locator("text=Load More Players").or_(page.locator(".showmore_lnk"))
                
                if await load_more.count() > 0 and await load_more.first.is_visible():
                    await load_more.first.click()
                    # 4 second pause is safer for overnight stability
                    await asyncio.sleep(4)
                else:
                    # Double check before quitting
                    await asyncio.sleep(2)
                    if await load_more.count() == 0:
                        print("   No more buttons. Full list loaded.")
                        break
            except Exception as e:
                print(f"   Loop minor error: {e}")
                break
        
        # 3. EXTRACT LINKS
        print("--- 3. Extracting Profile Links ---")
        links = await page.eval_on_selector_all("a[href*='/player/']", "elements => elements.map(e => e.href)")
        # Filter for unique 247sports player profiles
        unique_links = list(set([l for l in links if "247sports.com/player/" in l]))
        print(f"   Found {len(unique_links)} unique profiles to scrape.")
        
        await page.close()

        if len(unique_links) == 0:
            print("CRITICAL: No links found. Aborting.")
            await browser.close()
            return

        # 4. SCRAPE PROFILES
        print("--- 4. Scraping Profiles ---")
        sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
        failed_urls = []
        tasks = [scrape_profile(context, link, sem, failed_urls) for link in unique_links]
        
        # Gather all results
        results = await asyncio.gather(*tasks)
        valid_results = [r for r in results if r]
        
        # 5. SAVE
        df = pd.DataFrame(valid_results)
        cols = [
            "247 ID", "Player Name", "Position", "Height", "Weight", "High School", "City, ST", "EXP", "Team",
            "Transfer Stars", "Transfer Rating", "Transfer Year", "Transfer Overall Rank", "Transfer Position Rank", "Transfer Team Name",
            "Prospect Stars", "Prospect Rating", "Prospect Position Rank", "Prospect National Rank", "URL"
        ]
        # Reorder columns (ignore missing ones safely)
        df = df.reindex(columns=cols)
        df.to_csv(OUTPUT_FILE, index=False)
        print(f"--- SUCCESS. Saved {len(df)} rows to {OUTPUT_FILE} ---")
        
        if failed_urls:
            print(f"   Note: {len(failed_urls)} profiles failed.")
            pd.DataFrame(failed_urls).to_csv("failed_urls.csv", index=False)

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())

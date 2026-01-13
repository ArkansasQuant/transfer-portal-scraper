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
# OVERNIGHT SETTINGS
CONCURRENCY_LIMIT = 2   # Extremely safe to prevent timeouts
MAX_RETRIES = 5         # High tenacity: if a page fails, try 5 times before giving up
OUTPUT_FILE = f"transfer_portal_2026_{datetime.now().strftime('%Y%m%d')}.csv"

# --- UTILS ---
def clean_text(text):
    if not text: return None
    return text.strip()

def normalize_rank(rank):
    if not rank or rank in ['-', '', 'N/A', None]: return 'NA'
    # Remove dots, hashtags, "No." -> Keep only numbers
    clean = re.sub(r'[^\d]', '', rank)
    return clean if clean else 'NA'

def extract_id_from_url(url):
    match = re.search(r'-(\d+)(?:/|$)', url)
    return match.group(1) if match else "NA"

async def random_delay():
    # 1 to 3 seconds sleep to mimic human reading
    await asyncio.sleep(random.uniform(1.0, 3.0))

# --- PARSING LOGIC (STRICT MODE) ---
def parse_profile(html, url, player_id):
    soup = BeautifulSoup(html, 'lxml')
    data = {}
    
    # --- HEADER ---
    data['247 ID'] = player_id
    
    name_tag = soup.select_one('.name') or soup.select_one('h1.name')
    data['Player Name'] = clean_text(name_tag.text) if name_tag else "NA"
    
    # Metrics - Strict Scope
    data['Position'] = "NA"
    data['Height'] = "NA"
    data['Weight'] = "NA"
    
    metrics_list = soup.select('.metrics-list li')
    for m in metrics_list:
        text = m.get_text().strip()
        if ':' in text:
            label, val = text.split(':', 1)
            val = val.strip()
            if 'Pos' in label: data['Position'] = val
            elif 'Height' in label: data['Height'] = f"'{val}" # Excel format fix
            elif 'Weight' in label: data['Weight'] = val
    
    # Details - Strict Scope (Fixes 'Calculator' error)
    data['High School'] = "NA"
    data['City, ST'] = "NA"
    data['EXP'] = "NA"
    
    details_list = soup.select('.details li')
    for d in details_list:
        label_span = d.select_one('span')
        if label_span:
            label = label_span.get_text().strip()
            # Remove label from full text to get value
            full_text = d.get_text().strip()
            value = full_text.replace(label, "").strip()
            
            if 'High School' in label: data['High School'] = value
            elif 'Home Town' in label: data['City, ST'] = value
            elif 'Class' in label: data['EXP'] = value

    # Team
    data['Team'] = "NA"
    team_block = soup.select_one('.ni-school-name a')
    if team_block:
        data['Team'] = team_block.text.strip()
    else:
        pred_team = soup.select_one('.transfer-prediction .team-name')
        if pred_team: data['Team'] = pred_team.text.strip()

    # --- TRANSFER SECTION ---
    data['Transfer Stars'] = "0"
    data['Transfer Rating'] = "NA"
    data['Transfer Year'] = "2026"
    data['Transfer Overall Rank'] = "NA"
    data['Transfer Position Rank'] = "NA"
    data['Transfer Team Name'] = data['Team']

    # ONLY search inside .transfer-rankings
    t_sect = soup.select_one('.transfer-rankings')
    if t_sect:
        stars = t_sect.select('.icon-starsolid.yellow')
        data['Transfer Stars'] = len(stars)
        
        rating = t_sect.select_one('.rating')
        if rating: data['Transfer Rating'] = rating.text.strip()
        
        for item in t_sect.select('.ranks-list li'):
            h5 = item.select_one('h5')
            strong = item.select_one('strong')
            if h5 and strong:
                header = h5.text.strip()
                val = strong.text.strip()
                if 'OVR' in header or 'National' in header:
                    data['Transfer Overall Rank'] = normalize_rank(val)
                elif data['Position'] in header:
                    data['Transfer Position Rank'] = normalize_rank(val)

    # --- PROSPECT SECTION ---
    data['Prospect Stars'] = "0"
    data['Prospect Rating'] = "NA"
    data['Prospect Position Rank'] = "NA"
    data['Prospect National Rank'] = "NA"
    
    # ONLY search inside .prospect-rankings
    p_sect = soup.select_one('.prospect-rankings')
    
    # Fallback for older profiles
    if not p_sect:
        for sect in soup.select('section'):
            header = sect.select_one('h2, h3')
            if header and ('High School' in header.text or 'JUCO' in header.text):
                p_sect = sect
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
        
        for item in p_sect.select('.ranks-list li'):
            h5 = item.select_one('h5')
            strong = item.select_one('strong')
            if h5 and strong:
                header = h5.text.strip()
                val = strong.text.strip()
                if 'Natl' in header or 'National' in header:
                    data['Prospect National Rank'] = normalize_rank(val)
                elif data['Position'] in header:
                    data['Prospect Position Rank'] = normalize_rank(val)

    return data

async def scrape_profile(context, url, sem, failed_urls):
    async with sem:
        for attempt in range(MAX_RETRIES):
            page = await context.new_page()
            # Block heavy media (Images/Videos) to prevent freezing
            await page.route("**/*.{png,jpg,jpeg,svg,mp4,woff,woff2}", lambda route: route.abort())
            
            try:
                await random_delay()
                
                # 'commit' = Don't wait for ads to finish, just wait for connection
                await page.goto(url, timeout=60000, wait_until="commit")
                
                # Smart Wait: Wait for the NAME to appear.
                try:
                    await page.wait_for_selector(".name, h1.name", timeout=20000)
                except:
                    pass # If it times out, we try scraping anyway

                content = await page.content()
                
                if "Player Profile" not in content and "name" not in content:
                    raise Exception("Page blank/blocked")

                player_id = extract_id_from_url(url)
                data = parse_profile(content, url, player_id)
                data['URL'] = url
                
                await page.close()
                print(f"   [SUCCESS] {data['Player Name']}")
                return data

            except Exception as e:
                print(f"   [ERROR] {url} (Attempt {attempt+1}): {e}")
                await page.close()
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(5)
                else:
                    failed_urls.append({'url': url, 'reason': str(e)})
                    return None

async def main():
    ua = UserAgent()
    print("--- Starting GOLDEN COPY Overnight Scraper ---")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=ua.random, viewport={'width': 1920, 'height': 1080})
        page = await context.new_page()
        
        # --- 1. ROBUST LIST LOADING ---
        print(f"--- 1. Loading Main List: {BASE_URL} ---")
        await page.goto(BASE_URL, timeout=120000, wait_until="commit")
        
        # Wait for the table to actually appear
        try:
            await page.wait_for_selector(".rankings-page__name-link", timeout=30000)
        except:
            print("   Warning: Initial selector wait timed out, attempting to click anyway...")

        # --- 2. ENDLESS CLICKING LOOP ---
        # 300 clicks ensures we cover 15,000+ players (way more than needed)
        for i in range(300):
            try:
                # Scroll to bottom
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(2)
                
                # Check progress
                # Using the BROAD selector to count, just to be sure
                count = await page.locator("a[href*='/player/']").count()
                print(f"   [Cycle {i+1}] Links visible: {count}")
                
                # Find Button (Robust Search)
                load_more = page.locator("text=Load More Players").or_(page.locator(".showmore_lnk"))
                
                if await load_more.count() > 0 and await load_more.first.is_visible():
                    await load_more.first.click()
                    # Wait 4 seconds for slow overnight loading
                    await asyncio.sleep(4)
                else:
                    # Double check
                    await asyncio.sleep(4)
                    if await load_more.count() == 0 or not await load_more.first.is_visible():
                        print("   No more buttons. List collection complete.")
                        break
            except Exception as e:
                print(f"   Loop minor error: {e}")
                await asyncio.sleep(2)
        
        # --- 3. EXTRACT LINKS (BROAD SELECTOR) ---
        print("--- 2. Extracting Links (Broad Search) ---")
        # This is the CRITICAL fix: Use the broad selector that worked before
        links = await page.eval_on_selector_all(
            "a[href*='/player/']", 
            "elements => elements.map(e => e.href)"
        )
        # Filter for 247 profile links only
        unique_links = list(set([l for l in links if "247sports.com/player/" in l]))
        print(f"   Found {len(unique_links)} unique profiles.")
        
        await page.close()

        if len(unique_links) == 0:
            print("CRITICAL ERROR: No links found. Aborting.")
            await browser.close()
            return

        # --- 4. SCRAPE ---
        print(f"--- 3. Scraping {len(unique_links)} Profiles ---")
        sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
        failed_urls = []
        tasks = [scrape_profile(context, link, sem, failed_urls) for link in unique_links]
        
        results = await asyncio.gather(*tasks)
        valid_results = [r for r in results if r]
        
        df = pd.DataFrame(valid_results)
        cols = [
            "247 ID", "Player Name", "Position", "Height", "Weight", "High School", "City, ST", "EXP", "Team",
            "Transfer Stars", "Transfer Rating", "Transfer Year", "Transfer Overall Rank", "Transfer Position Rank", "Transfer Team Name",
            "Prospect Stars", "Prospect Rating", "Prospect Position Rank", "Prospect National Rank"
        ]
        df = df.reindex(columns=cols)
        df.to_csv(OUTPUT_FILE, index=False)
        print(f"--- SUCCESS. Saved {len(df)} rows. ---")
        
        if failed_urls:
            print(f"   Note: {len(failed_urls)} profiles failed after {MAX_RETRIES} attempts.")
            pd.DataFrame(failed_urls).to_csv("failed_urls.csv", index=False)

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())

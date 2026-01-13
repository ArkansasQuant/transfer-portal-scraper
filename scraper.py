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
# SLOW AND STEADY SETTINGS
CONCURRENCY_LIMIT = 2  # Only 2 tabs at once (Very safe)
MAX_RETRIES = 5        # If a page fails, try 5 times
OUTPUT_FILE = f"transfer_portal_2026_{datetime.now().strftime('%Y%m%d')}.csv"

# --- UTILS ---
def clean_text(text):
    if not text: return None
    return text.strip()

def normalize_rank(rank):
    if not rank or rank in ['-', '', 'N/A', None]: return 'NA'
    # Remove dots, "No.", hashtags, etc. Keep only numbers.
    clean = re.sub(r'[^\d]', '', rank)
    return clean if clean else 'NA'

def extract_id_from_url(url):
    match = re.search(r'-(\d+)(?:/|$)', url)
    return match.group(1) if match else "NA"

async def random_delay():
    # A polite delay to let the server breathe
    await asyncio.sleep(random.uniform(1.0, 3.0))

# --- PARSING LOGIC (STRICT MODE) ---
def parse_profile(html, url, player_id):
    soup = BeautifulSoup(html, 'lxml')
    data = {}
    
    # --- HEADER ---
    data['247 ID'] = player_id
    
    name_tag = soup.select_one('.name') or soup.select_one('h1.name')
    data['Player Name'] = clean_text(name_tag.text) if name_tag else "NA"
    
    # Metrics (Strictly scoped to .metrics-list)
    data['Position'] = "NA"
    data['Height'] = "NA"
    data['Weight'] = "NA"
    
    metrics_list = soup.select('.metrics-list li')
    for m in metrics_list:
        text = m.get_text().strip()
        # "Pos: QB" -> split by ':'
        if ':' in text:
            label, val = text.split(':', 1)
            val = val.strip()
            if 'Pos' in label: data['Position'] = val
            elif 'Height' in label: data['Height'] = f"'{val}" # Excel format fix
            elif 'Weight' in label: data['Weight'] = val
    
    # Details (Strictly scoped to .details)
    data['High School'] = "NA"
    data['City, ST'] = "NA"
    data['EXP'] = "NA"
    
    details_list = soup.select('.details li')
    for d in details_list:
        # Each li has a <span>Label</span> Value
        label_span = d.select_one('span')
        if label_span:
            label = label_span.get_text().strip()
            # Remove the label text from the full text to get the value
            full_text = d.get_text().strip()
            value = full_text.replace(label, "").strip()
            
            if 'High School' in label: data['High School'] = value
            elif 'Home Town' in label: data['City, ST'] = value
            elif 'Class' in label: data['EXP'] = value

    # Team (Priority List)
    data['Team'] = "NA"
    # 1. Look for the "Committed / Enrolled" main block
    team_block = soup.select_one('.ni-school-name a')
    if team_block:
        data['Team'] = team_block.text.strip()
    else:
        # 2. Look for Crystal Ball / Prediction
        pred_team = soup.select_one('.transfer-prediction .team-name')
        if pred_team: data['Team'] = pred_team.text.strip()

    # --- TRANSFER SECTION ---
    data['Transfer Stars'] = "0"
    data['Transfer Rating'] = "NA"
    data['Transfer Year'] = "2026"
    data['Transfer Overall Rank'] = "NA"
    data['Transfer Position Rank'] = "NA"
    data['Transfer Team Name'] = data['Team']

    # Strict Scope: Only look inside .transfer-rankings
    t_sect = soup.select_one('.transfer-rankings')
    if t_sect:
        # Count stars strictly inside this box
        stars = t_sect.select('.icon-starsolid.yellow')
        data['Transfer Stars'] = len(stars)
        
        rating = t_sect.select_one('.rating')
        if rating: data['Transfer Rating'] = rating.text.strip()
        
        # Ranks
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
    
    # Strict Scope: Only look inside .prospect-rankings
    p_sect = soup.select_one('.prospect-rankings')
    
    # Fallback: Sometimes just a generic section if they are older
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
            # Still block heavy media to save memory, but allow basic styles
            await page.route("**/*.{png,jpg,jpeg,svg,woff,woff2,mp4}", lambda route: route.abort())
            
            try:
                await random_delay()
                
                # "commit" is fast, but we add a specific selector wait to be safe
                await page.goto(url, timeout=60000, wait_until="commit")
                
                # Wait for the NAME to appear. If this happens, the data is there.
                try:
                    await page.wait_for_selector(".name, h1.name", timeout=15000)
                except:
                    # If selector times out, we check if content loaded anyway
                    pass

                content = await page.content()
                
                # Safety Check: Did we get blocked or get a blank page?
                if "Player Profile" not in content and "name" not in content:
                    raise Exception("Page likely blank or blocked")

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
                    await asyncio.sleep(5) # Longer wait on failure
                else:
                    failed_urls.append({'url': url, 'reason': str(e)})
                    return None

async def main():
    ua = UserAgent()
    print("--- Starting SLOW & STEADY Scraper ---")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        # Larger viewport to ensure no mobile layout issues
        context = await browser.new_context(user_agent=ua.random, viewport={'width': 1920, 'height': 1080})
        
        page = await context.new_page()
        
        # --- 1. LOAD MAIN LIST ---
        print(f"--- 1. Loading Main List: {BASE_URL} ---")
        
        # Allow up to 3 minutes just to load the initial page
        await page.goto(BASE_URL, timeout=180000, wait_until="domcontentloaded")
        
        # --- 2. CLICK UNTIL DONE ---
        # 300 clicks * 50 players = 15,000 capacity. Plenty.
        for i in range(300):
            try:
                # Scroll to bottom
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(2) # Give it 2 seconds to realize it's at the bottom
                
                # Check how many players we currently have
                count = await page.locator("a.rankings-page__name-link").count()
                print(f"   [Cycle {i+1}] Players Loaded: {count}")
                
                # Locate button
                load_more = page.locator(".showmore_lnk")
                
                if await load_more.count() > 0 and await load_more.first.is_visible():
                    # Click
                    await load_more.first.click()
                    # Wait 4 full seconds for new data. Slow is smooth.
                    await asyncio.sleep(4) 
                else:
                    # Double check: Wait 5 seconds and look again just in case
                    await asyncio.sleep(5)
                    if await load_more.count() == 0 or not await load_more.first.is_visible():
                        print("   No more buttons found. List complete.")
                        break
            except Exception as e:
                print(f"   Loop hiccup: {e}")
                await asyncio.sleep(5) # Wait and continue
        
        # --- 3. EXTRACT LINKS ---
        print("--- 2. Extracting Links ---")
        links = await page.eval_on_selector_all(
            "a.rankings-page__name-link", 
            "elements => elements.map(e => e.href)"
        )
        unique_links = list(set([l for l in links if "247sports.com/player/" in l]))
        print(f"   Found {len(unique_links)} unique profiles.")
        
        await page.close()

        # --- 4. SCRAPE PROFILES ---
        print(f"--- 3. Scraping {len(unique_links)} Profiles (Batch Size: {CONCURRENCY_LIMIT}) ---")
        sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
        failed_urls = []
        tasks = [scrape_profile(context, link, sem, failed_urls) for link in unique_links]
        
        # Progress indicator logic is implicit in the print statements
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
        print(f"--- DONE. Saved {len(df)} rows. ---")
        
        if failed_urls:
            print(f"   WARNING: {len(failed_urls)} profiles failed.")
            pd.DataFrame(failed_urls).to_csv("failed_urls.csv", index=False)

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())

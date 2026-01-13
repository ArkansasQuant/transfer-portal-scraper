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
OUTPUT_FILE = f"transfer_portal_2026_{datetime.now().strftime('%Y%m%d')}.csv"

# --- UTILS ---
def clean_text(text):
    if not text: return None
    return text.strip()

def normalize_rank(rank):
    # Removes non-numeric chars except for "NA" or "JUCO"
    if not rank or rank in ['-', '', 'N/A', None]: return 'NA'
    # Remove dots, hashtags, "No."
    clean = re.sub(r'[^\d]', '', rank)
    return clean if clean else 'NA'

def extract_id_from_url(url):
    match = re.search(r'-(\d+)(?:/|$)', url)
    return match.group(1) if match else "NA"

async def exponential_backoff(attempt):
    wait_time = (2 ** attempt) + random.uniform(0.5, 1.5)
    await asyncio.sleep(wait_time)

# --- PRECISE PARSING LOGIC ---
def parse_profile(html, url, player_id):
    soup = BeautifulSoup(html, 'lxml')
    data = {}
    
    # --- HEADER SECTION (Basic Info) ---
    data['247 ID'] = player_id
    
    # Name
    name_tag = soup.select_one('.name') or soup.select_one('h1.name')
    data['Player Name'] = clean_text(name_tag.text) if name_tag else "NA"
    
    # Metrics Strip (Pos, Height, Weight)
    data['Position'] = "NA"
    data['Height'] = "NA"
    data['Weight'] = "NA"
    
    metrics = soup.select('.metrics-list li')
    for m in metrics:
        text = m.text.strip()
        if 'Pos' in text: 
            data['Position'] = text.split(':')[-1].strip()
        elif 'Height' in text: 
            raw_ht = text.split(':')[-1].strip()
            data['Height'] = f"'{raw_ht}" # Excel fix
        elif 'Weight' in text: 
            data['Weight'] = text.split(':')[-1].strip()

    # Details Block (High School, Hometown, Class)
    data['High School'] = "NA"
    data['City, ST'] = "NA"
    data['EXP'] = "NA" # This was pulling 'Calculator'
    
    details = soup.select('.details li')
    for d in details:
        # We split by the span label to get just the value
        label = d.select_one('span')
        if label:
            label_text = label.text.strip()
            # Get text excluding the label
            val_text = d.get_text().replace(label_text, "").strip()
            
            if 'High School' in label_text: data['High School'] = val_text
            elif 'Home Town' in label_text: data['City, ST'] = val_text
            elif 'Class' in label_text: data['EXP'] = val_text

    # Current Team / Transfer Destination
    data['Team'] = "NA"
    # Look for the big team logo block
    team_block = soup.select_one('.ni-school-name a')
    if team_block:
        data['Team'] = team_block.text.strip()
    else:
        # Fallback to transfer prediction
        pred_team = soup.select_one('.transfer-prediction .team-name')
        if pred_team: data['Team'] = pred_team.text.strip()

    # --- SECTION 2: TRANSFER RANKINGS ---
    data['Transfer Stars'] = "0"
    data['Transfer Rating'] = "NA"
    data['Transfer Year'] = "2026"
    data['Transfer Overall Rank'] = "NA"
    data['Transfer Position Rank'] = "NA"
    data['Transfer Team Name'] = data['Team']

    # Strictly target the container with class 'transfer-rankings'
    t_sect = soup.select_one('.transfer-rankings')
    if t_sect:
        # Count stars ONLY inside this box
        stars = t_sect.select('.icon-starsolid.yellow')
        data['Transfer Stars'] = len(stars)
        
        rating = t_sect.select_one('.rating')
        if rating: data['Transfer Rating'] = rating.text.strip()
        
        # Ranks are inside <li> items
        rank_items = t_sect.select('.ranks-list li')
        for item in rank_items:
            h5 = item.select_one('h5')
            strong = item.select_one('strong')
            if h5 and strong:
                header = h5.text.strip()
                val = strong.text.strip()
                if 'OVR' in header or 'National' in header:
                    data['Transfer Overall Rank'] = normalize_rank(val)
                elif data['Position'] in header: # e.g. "QB" in "QB Rank"
                    data['Transfer Position Rank'] = normalize_rank(val)

    # --- SECTION 3: PROSPECT RANKINGS ---
    data['Prospect Stars'] = "0"
    data['Prospect Rating'] = "NA"
    data['Prospect Position Rank'] = "NA"
    data['Prospect National Rank'] = "NA"
    
    # Strictly target the container with class 'prospect-rankings'
    p_sect = soup.select_one('.prospect-rankings')
    
    # If standard section missing, check specifically for JUCO header
    if not p_sect:
        # Sometimes it's just a general section with "High School" header
        for sect in soup.select('section'):
            if 'High School' in sect.get_text() or 'JUCO' in sect.get_text():
                p_sect = sect
                break

    if p_sect:
        is_juco = "JUCO" in p_sect.get_text()
        
        # Stars ONLY inside this box
        stars = p_sect.select('.icon-starsolid.yellow')
        
        if is_juco and len(stars) == 0:
             data['Prospect Stars'] = "JUCO"
        else:
             data['Prospect Stars'] = len(stars)
             
        rating = p_sect.select_one('.rating')
        if rating: data['Prospect Rating'] = rating.text.strip()
        
        rank_items = p_sect.select('.ranks-list li')
        for item in rank_items:
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
            # Block images/css for speed
            await page.route("**/*.{png,jpg,jpeg,svg,css,woff,woff2}", lambda route: route.abort())
            try:
                # Random fast delay
                await asyncio.sleep(random.uniform(0.2, 0.8))
                
                # Fast timeout
                await page.goto(url, timeout=30000, wait_until="domcontentloaded")
                
                content = await page.content()
                if "Player Profile" not in content and "name" not in content:
                    raise Exception("Empty/Blocked")

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
                    await exponential_backoff(attempt)
                else:
                    failed_urls.append({'url': url, 'reason': str(e)})
                    return None

async def main():
    ua = UserAgent()
    print("--- Starting Precision Scraper ---")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=ua.random, viewport={'width': 1920, 'height': 1080})
        
        page = await context.new_page()
        # Allow CSS/Images on Main List only so "Load More" behaves correctly
        # (Sometimes hidden if CSS is blocked)
        
        print(f"--- 1. Loading Main List: {BASE_URL} ---")
        await page.goto(BASE_URL, timeout=60000, wait_until="domcontentloaded")

        # --- FORCE LOAD MORE LOOP ---
        # We scroll down, wait, then click. Repeat.
        previous_count = 0
        consecutive_failures = 0
        
        for i in range(200): # High limit to ensure we get all 1500
            try:
                # 1. Scroll to bottom to trigger lazy load
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(1)
                
                # 2. Count current players to see if we are making progress
                current_links = await page.locator("a.rankings-page__name-link").count()
                print(f"   [Cycle {i+1}] Players visible: {current_links}")
                
                if current_links == previous_count and current_links > 0:
                    consecutive_failures += 1
                else:
                    consecutive_failures = 0
                
                previous_count = current_links
                
                # If stuck for 5 cycles, we are probably done
                if consecutive_failures >= 5:
                    print("   No new players loaded for 5 cycles. Assuming end of list.")
                    break

                # 3. Find and Click Button
                load_more = page.locator(".showmore_lnk")
                if await load_more.count() > 0 and await load_more.first.is_visible():
                    await load_more.first.click()
                    # Wait for spinner to disappear
                    await page.wait_for_timeout(2000)
                else:
                    print("   'Load More' button not visible (or list complete).")
                    break
                    
            except Exception as e:
                print(f"   Loop error: {e}")
                break
        
        # --- EXTRACT LINKS ---
        print("--- 2. Extracting Links ---")
        # Use the specific class for player links to avoid menu links
        links = await page.eval_on_selector_all(
            "a.rankings-page__name-link", 
            "elements => elements.map(e => e.href)"
        )
        links = list(set(links))
        print(f"   Found {len(links)} unique profiles.")
        
        await page.close()

        # --- SCRAPE ---
        print(f"--- 3. Scraping {len(links)} Profiles ---")
        sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
        failed_urls = []
        tasks = [scrape_profile(context, link, sem, failed_urls) for link in links]
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
            pd.DataFrame(failed_urls).to_csv("failed_urls.csv", index=False)

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())

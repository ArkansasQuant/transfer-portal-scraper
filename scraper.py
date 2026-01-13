import asyncio
import random
import time
import pandas as pd
import re
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from datetime import datetime

# --- CONFIGURATION ---
BASE_URL = "https://247sports.com/season/2026-football/transferportalpositionranking/"
MAX_RETRIES = 3
CONCURRENCY_LIMIT = 5  # Number of profiles to scrape at once (prevents throttling)
OUTPUT_FILE = f"transfer_portal_2026_{datetime.now().strftime('%Y%m%d')}.csv"

# --- UTILS ---
def clean_text(text):
    if not text: return None
    return text.strip()

def normalize_rank(rank):
    if not rank or rank in ['-', '', 'N/A', None]:
        return 'NA'
    return rank

def extract_id_from_url(url):
    # Extracts ID like 46108915 from url
    match = re.search(r'-(\d+)(?:/|$)', url)
    return match.group(1) if match else "NA"

async def exponential_backoff(attempt):
    wait_time = (2 ** attempt) + random.uniform(0, 1)
    print(f"   Waiting {wait_time:.2f}s before retry...")
    await asyncio.sleep(wait_time)

# --- PARSING LOGIC ---
def parse_profile(html, url, player_id):
    soup = BeautifulSoup(html, 'lxml')
    data = {}
    
    # 1. Basic Info & Prospect Info
    data['247 ID'] = player_id
    data['Player Name'] = clean_text(soup.select_one('.name').text) if soup.select_one('.name') else "NA"
    
    # Metrics (Height, Weight, POS)
    metrics = soup.select('.metrics-list li')
    data['Position'] = "NA"
    data['Height'] = "NA"
    data['Weight'] = "NA"
    
    for m in metrics:
        text = m.text.strip()
        if 'Pos' in text: data['Position'] = text.replace('Pos', '').strip()
        elif 'Height' in text: data['Height'] = text.replace('Height', '').strip()
        elif 'Weight' in text: data['Weight'] = text.replace('Weight', '').strip()

    # Hometown / HS / Team
    data['High School'] = "NA"
    data['City, ST'] = "NA"
    data['Team'] = "NA" # Current Team
    data['EXP'] = "NA"

    details = soup.select('.details li')
    for d in details:
        text = d.text.strip()
        if 'High School' in text: data['High School'] = text.replace('High School', '').strip()
        elif 'Home Town' in text: data['City, ST'] = text.replace('Home Town', '').strip()
        elif 'Class' in text: data['EXP'] = text.replace('Class', '').strip()
    
    # Sometimes Team is in a different header spot
    team_tag = soup.select_one('.ni-school-name a')
    if team_tag:
        data['Team'] = team_tag.text.strip()

    # --- SECTION 2: AS A TRANSFER ---
    # We look for the "Transfer" header section
    data['Transfer Stars'] = "0"
    data['Transfer Rating'] = "NA"
    data['Transfer Year'] = "2026"
    data['Transfer Overall Rank'] = "NA"
    data['Transfer Position Rank'] = "NA"
    data['Transfer Team Name'] = "NA" # The team they transferred TO

    # Locate Transfer Section
    transfer_sect = soup.select_one('.transfer-rankings')
    if transfer_sect:
        # Stars (count gold stars)
        stars = transfer_sect.select('.icon-starsolid.yellow')
        data['Transfer Stars'] = len(stars)
        
        # Rating
        rating = transfer_sect.select_one('.rating')
        if rating: data['Transfer Rating'] = rating.text.strip()
        
        # Ranks
        # We need to find the specific <li> that matches the player's position
        rank_items = transfer_sect.select('.ranks-list li')
        for item in rank_items:
            header = item.select_one('h5')
            if not header: continue
            header_text = header.text.strip()
            value = item.select_one('strong')
            if not value: continue
            
            if 'OVR' in header_text:
                data['Transfer Overall Rank'] = normalize_rank(value.text.strip())
            # Check if header matches player position (e.g. "QB" == "QB")
            elif data['Position'] in header_text:
                data['Transfer Position Rank'] = normalize_rank(value.text.strip())

    # Committed Team (often at top of page or in transfer section)
    # 247 usually shows the destination team logo/text in the header if committed
    commit_node = soup.select_one('.transfer-prediction .team-name') 
    if commit_node:
        data['Transfer Team Name'] = commit_node.text.strip()

    # --- SECTION 3: AS A PROSPECT ---
    data['Prospect Stars'] = "0"
    data['Prospect Rating'] = "NA"
    data['Prospect Position Rank'] = "NA"
    data['Prospect National Rank'] = "NA"
    
    # Check for JUCO
    is_juco = False
    prospect_sect = soup.select_one('.prospect-rankings') # Standard HS
    
    # If standard prospect section missing, look for JUCO specific indicators
    # Note: 247 often keeps the class name same but changes header text
    
    if prospect_sect:
        # Check JUCO flag in stars or headers
        if "JUCO" in prospect_sect.text or soup.select_one('.icon-juco'):
            is_juco = True
            
        stars = prospect_sect.select('.icon-starsolid.yellow')
        data['Prospect Stars'] = "JUCO" if is_juco and len(stars) == 0 else len(stars)
        
        rating = prospect_sect.select_one('.rating')
        if rating: data['Prospect Rating'] = rating.text.strip()
        
        rank_items = prospect_sect.select('.ranks-list li')
        for item in rank_items:
            header = item.select_one('h5')
            if not header: continue
            header_text = header.text.strip()
            value = item.select_one('strong')
            if not value: continue
            
            if 'Natl' in header_text or 'National' in header_text:
                data['Prospect National Rank'] = normalize_rank(value.text.strip())
            elif data['Position'] in header_text:
                data['Prospect Position Rank'] = normalize_rank(value.text.strip())

    if is_juco and data['Prospect National Rank'] == "NA":
         # If purely JUCO and no rank found, ensure logic follows request
         pass 

    return data

async def scrape_profile(context, url, sem, failed_urls):
    async with sem: # Limit concurrency
        for attempt in range(MAX_RETRIES):
            page = await context.new_page()
            try:
                # Random delay to be polite
                await asyncio.sleep(random.uniform(0.5, 2.0))
                
                response = await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                if response.status != 200:
                    raise Exception(f"Status {response.status}")
                
                content = await page.content()
                player_id = extract_id_from_url(url)
                
                # Parse
                data = parse_profile(content, url, player_id)
                data['URL'] = url # Keep record
                
                await page.close()
                print(f"   [SUCCESS] {data['Player Name']}")
                return data

            except Exception as e:
                print(f"   [ERROR] {url} (Attempt {attempt+1}/{MAX_RETRIES}): {e}")
                await page.close()
                if attempt < MAX_RETRIES - 1:
                    await exponential_backoff(attempt)
                else:
                    failed_urls.append({'url': url, 'reason': str(e), 'time': datetime.now().isoformat()})
                    return None

async def main():
    async with async_playwright() as p:
        # Browser setup
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
        
        page = await context.new_page()
        print(f"--- 1. Loading Main List: {BASE_URL} ---")
        await page.goto(BASE_URL, timeout=120000)
        
        # --- CLICK "LOAD MORE" LOOP ---
        while True:
            try:
                # Selector for the "Load More" button
                load_more = page.locator(".showmore_lnk") 
                if await load_more.is_visible():
                    print("   Clicking 'Load More Players'...")
                    await load_more.click()
                    # Wait for new content to load (waiting for spinner to go away or list to grow)
                    await page.wait_for_timeout(2000) 
                else:
                    print("   End of list reached (or no button found).")
                    break
            except Exception as e:
                print(f"   Load More loop interrupted: {e}")
                break
        
        # --- COLLECT LINKS ---
        print("--- 2. Extracting Player Links ---")
        # 247 links are usually in .rankings-page__name-link
        links = await page.eval_on_selector_all(
            "a.rankings-page__name-link", 
            "elements => elements.map(e => e.href)"
        )
        # Deduplicate
        links = list(set(links))
        print(f"   Found {len(links)} unique player profiles.")
        
        await page.close()

        # --- SCRAPE PROFILES ---
        print(f"--- 3. Scraping {len(links)} Profiles ---")
        sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
        failed_urls = []
        tasks = [scrape_profile(context, link, sem, failed_urls) for link in links]
        
        results = await asyncio.gather(*tasks)
        
        # Filter None results
        valid_results = [r for r in results if r]
        
        # --- SAVE ---
        df = pd.DataFrame(valid_results)
        
        # Ensure column order matches request
        cols = [
            "247 ID", "Player Name", "Position", "Height", "Weight", "High School", "City, ST", "EXP", "Team",
            "Transfer Stars", "Transfer Rating", "Transfer Year", "Transfer Overall Rank", "Transfer Position Rank", "Transfer Team Name",
            "Prospect Stars", "Prospect Rating", "Prospect Position Rank", "Prospect National Rank"
        ]
        # Reindex checks if cols exist, fills missing with NaN
        df = df.reindex(columns=cols)
        
        df.to_csv(OUTPUT_FILE, index=False)
        print(f"--- COMPLETE. Saved {len(df)} rows to {OUTPUT_FILE} ---")
        
        if failed_urls:
            pd.DataFrame(failed_urls).to_csv("failed_urls.csv", index=False)
            print(f"   WARNING: {len(failed_urls)} pages failed. See failed_urls.csv")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())

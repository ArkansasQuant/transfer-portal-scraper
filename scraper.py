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
# We will use a broader "Wait" to ensure the list is actually there
MAX_RETRIES = 3
CONCURRENCY_LIMIT = 5
OUTPUT_FILE = f"transfer_portal_2026_{datetime.now().strftime('%Y%m%d')}.csv"

# --- UTILS ---
def clean_text(text):
    if not text: return None
    return text.strip()

def normalize_rank(rank):
    if not rank or rank in ['-', '', 'N/A', None]: return 'NA'
    return rank

def extract_id_from_url(url):
    match = re.search(r'-(\d+)(?:/|$)', url)
    return match.group(1) if match else "NA"

async def exponential_backoff(attempt):
    wait_time = (2 ** attempt) + random.uniform(1, 3)
    print(f"   Waiting {wait_time:.2f}s before retry...")
    await asyncio.sleep(wait_time)

# --- PARSING LOGIC (Same as before) ---
def parse_profile(html, url, player_id):
    soup = BeautifulSoup(html, 'lxml')
    data = {}
    
    # 1. Basic Info
    data['247 ID'] = player_id
    data['Player Name'] = clean_text(soup.select_one('.name').text) if soup.select_one('.name') else "NA"
    
    metrics = soup.select('.metrics-list li')
    data['Position'] = "NA"
    data['Height'] = "NA"
    data['Weight'] = "NA"
    
    for m in metrics:
        text = m.text.strip()
        if 'Pos' in text: data['Position'] = text.replace('Pos', '').strip()
        elif 'Height' in text: data['Height'] = text.replace('Height', '').strip()
        elif 'Weight' in text: data['Weight'] = text.replace('Weight', '').strip()

    data['High School'] = "NA"
    data['City, ST'] = "NA"
    data['Team'] = "NA"
    data['EXP'] = "NA"

    details = soup.select('.details li')
    for d in details:
        text = d.text.strip()
        if 'High School' in text: data['High School'] = text.replace('High School', '').strip()
        elif 'Home Town' in text: data['City, ST'] = text.replace('Home Town', '').strip()
        elif 'Class' in text: data['EXP'] = text.replace('Class', '').strip()
    
    team_tag = soup.select_one('.ni-school-name a')
    if team_tag: data['Team'] = team_tag.text.strip()

    # 2. Transfer
    data['Transfer Stars'] = "0"
    data['Transfer Rating'] = "NA"
    data['Transfer Year'] = "2026"
    data['Transfer Overall Rank'] = "NA"
    data['Transfer Position Rank'] = "NA"
    data['Transfer Team Name'] = "NA"

    transfer_sect = soup.select_one('.transfer-rankings')
    if transfer_sect:
        stars = transfer_sect.select('.icon-starsolid.yellow')
        data['Transfer Stars'] = len(stars)
        rating = transfer_sect.select_one('.rating')
        if rating: data['Transfer Rating'] = rating.text.strip()
        
        rank_items = transfer_sect.select('.ranks-list li')
        for item in rank_items:
            header = item.select_one('h5')
            if not header: continue
            header_text = header.text.strip()
            value = item.select_one('strong')
            if not value: continue
            
            if 'OVR' in header_text: data['Transfer Overall Rank'] = normalize_rank(value.text.strip())
            elif data['Position'] in header_text: data['Transfer Position Rank'] = normalize_rank(value.text.strip())

    commit_node = soup.select_one('.transfer-prediction .team-name') 
    if commit_node: data['Transfer Team Name'] = commit_node.text.strip()

    # 3. Prospect
    data['Prospect Stars'] = "0"
    data['Prospect Rating'] = "NA"
    data['Prospect Position Rank'] = "NA"
    data['Prospect National Rank'] = "NA"
    
    is_juco = False
    prospect_sect = soup.select_one('.prospect-rankings')
    
    if prospect_sect:
        if "JUCO" in prospect_sect.text or soup.select_one('.icon-juco'): is_juco = True   
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
            
            if 'Natl' in header_text or 'National' in header_text: data['Prospect National Rank'] = normalize_rank(value.text.strip())
            elif data['Position'] in header_text: data['Prospect Position Rank'] = normalize_rank(value.text.strip())

    return data

async def scrape_profile(context, url, sem, failed_urls):
    async with sem:
        for attempt in range(MAX_RETRIES):
            page = await context.new_page()
            try:
                # Slower, more human-like pause
                await asyncio.sleep(random.uniform(1.0, 3.0))
                
                response = await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                if response.status != 200:
                    # Sometimes 403 means blocked, but sometimes we can still scrape content
                    print(f"   [WARN] Status {response.status} for {url}")
                
                content = await page.content()
                
                # Validation: Did we actually get a profile?
                if "Player Profile" not in content and "name" not in content:
                    raise Exception("Profile content missing (Blocked?)")

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
    user_agent_str = ua.random
    print(f"--- Using Identity: {user_agent_str} ---")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=user_agent_str,
            viewport={'width': 1920, 'height': 1080}
        )
        
        page = await context.new_page()
        print(f"--- 1. Loading Main List: {BASE_URL} ---")
        
        try:
            await page.goto(BASE_URL, timeout=60000, wait_until="domcontentloaded")
            # Take a screenshot to debug if it fails
            await page.screenshot(path="debug_landing_page.png")
        except Exception as e:
            print(f"CRITICAL: Could not load main page. {e}")
            await page.screenshot(path="debug_failure.png")
            await browser.close()
            return

        # --- CLICK "LOAD MORE" LOOP ---
        # We will try up to 50 clicks (approx 2500 players capacity)
        for i in range(50):
            try:
                # Look for button by TEXT or Class (more robust)
                load_more = page.locator("a.showmore_lnk, text='Load More'")
                
                if await load_more.count() > 0 and await load_more.first.is_visible():
                    print(f"   [Click {i+1}] Loading more players...")
                    await load_more.first.click()
                    # Wait for network idle or simple timeout
                    await page.wait_for_timeout(random.randint(1500, 3000))
                else:
                    print("   No more 'Load More' buttons found.")
                    break
            except Exception as e:
                print(f"   Load More loop interrupted: {e}")
                break
        
        # --- COLLECT LINKS (BROADER SELECTOR) ---
        print("--- 2. Extracting Player Links ---")
        
        # This selector grabs ANY link that has "/player/" in the URL. 
        # This fixes the issue if they changed the class name.
        links = await page.eval_on_selector_all(
            "a[href*='/player/']", 
            "elements => elements.map(e => e.href)"
        )
        
        # Filter duplicates and ensure they are 247 links
        links = list(set([l for l in links if "247sports.com/player/" in l]))
        print(f"   Found {len(links)} unique player profiles.")
        
        if len(links) == 0:
            print("!!! ERROR: Found 0 links. The page might be blocked. Check 'debug_landing_page.png' artifact !!!")
            await browser.close()
            return

        await page.close()

        # --- SCRAPE PROFILES ---
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
        print(f"--- COMPLETE. Saved {len(df)} rows to {OUTPUT_FILE} ---")
        
        if failed_urls:
            pd.DataFrame(failed_urls).to_csv("failed_urls.csv", index=False)

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())

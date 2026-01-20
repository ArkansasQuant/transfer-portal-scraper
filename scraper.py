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

# Valid Football Positions (Still useful for reference, though new parser is more flexible)
VALID_POSITIONS = {
    'QB', 'RB', 'WR', 'TE', 'OT', 'IOL', 'OC', 'DL', 'EDGE', 
    'LB', 'CB', 'S', 'ATH', 'K', 'P', 'LS', 'RET'
}

# --- UTILS ---
def clean_text(text):
    if not text: return None
    # normalize N/A, -, empty strings to "NA"
    t = text.strip()
    if t in ["N/A", "", "-"]: return "NA"
    return t

def extract_id_from_url(url):
    match = re.search(r'-(\d+)(?:/|$)', url)
    return match.group(1) if match else "NA"

async def random_delay():
    await asyncio.sleep(random.uniform(1.0, 2.0))

# --- PARSING LOGIC (UPDATED) ---
def parse_profile(html, url, player_id):
    soup = BeautifulSoup(html, 'lxml')
    data = {}
    
    # --- 1. BASIC HEADER INFO ---
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

    # --- 2. TEAM PARSING (Origin vs Destination) ---
    data['Team'] = "NA"
    data['Transfer Team Name'] = "NA"

    # A. Transfer Team (The Destination) - Usually in the top Commit Banner
    commit_banner = soup.select_one('.commitment-banner .team-name') or \
                    soup.select_one('.header-commitment .team-name') or \
                    soup.select_one('.main-team-logo.is-committed') 
    
    if commit_banner:
        data['Transfer Team Name'] = clean_text(commit_banner.get_text(strip=True))

    # B. Team Name (The Origin/Current) - The "Red Box" or standard team logo
    # Strategy: Look for the specific team module often found in the right rail or top blocks
    origin_node = soup.select_one('.crystal-ball .team-name') or \
                  soup.select_one('.prediction-box .team-name') or \
                  soup.select_one('.module-team-logo .team-name')
    
    # Fallback: If no dedicated module, check the primary logo if it differs from Transfer Team
    if not origin_node:
        primary_logo_alt = soup.select_one('.primary-team-logo')
        if primary_logo_alt and primary_logo_alt.get('alt'):
             candidate = clean_text(primary_logo_alt.get('alt'))
             # Only set as Origin if it's not the same as the one we just found for Transfer
             if candidate != data['Transfer Team Name']:
                 data['Team'] = candidate
    else:
        data['Team'] = clean_text(origin_node.get_text(strip=True))
        
    # Edge Case: If still NA, try generic red box class
    if data['Team'] == "NA":
        red_box = soup.select_one('.team-header .team-name')
        if red_box:
            data['Team'] = clean_text(red_box.get_text(strip=True))

    # --- 3. SECTION PARSING HELPERS ---
    def get_section_stats(header_text_match, soup_obj):
        """Finds a section by header text and parses stars, rating, ranks."""
        # Find the header element containing specific text (e.g. "As a Transfer")
        header_node = soup_obj.find(string=re.compile(header_text_match))
        
        stats = {
            'stars': '0', 
            'rating': 'NA', 
            'year': 'NA',
            'ranks': {'OVR': 'NA', 'NATL': 'NA', 'POS': 'NA'}
        }
        
        if not header_node:
            return stats

        # Traverse up to the container (section or div) to isolate context
        container = header_node.find_parent('section') or header_node.find_parent('div', class_='rankings-section') or header_node.find_parent('div')
        if not container:
            return stats

        # A. Detect Year (Specific to Transfer section mostly)
        header_full_text = container.get_text()
        year_match = re.search(r'\((\d{4})\)', header_full_text)
        if year_match:
            stats['year'] = year_match.group(1)

        # B. Stars (Count ONLY within this container)
        yellow_stars = container.select('.icon-starsolid.yellow')
        count = len(yellow_stars)
        if "JUCO" in header_full_text.upper():
             stats['stars'] = f"{count} JUCO"
        else:
             stats['stars'] = str(count)

        # C. Rating
        rating_node = container.select_one('.score') or container.select_one('.rating')
        if rating_node:
            val = clean_text(rating_node.get_text())
            if "JUCO" in header_full_text.upper():
                stats['rating'] = f"{val} JUCO"
            else:
                stats['rating'] = val

        # D. Ranks
        rank_items = container.select('ul.list-ranks li') or container.select('ul.ranks-list li') or container.select('li')
        
        for li in rank_items:
            # Try various selectors for label/value pairs inside the list item
            label_node = li.select_one('h5') or li.select_one('.rank-label')
            val_node = li.select_one('strong') or li.select_one('.rank-value')
            
            if label_node and val_node:
                raw_label = label_node.get_text(strip=True).upper()
                val = clean_text(val_node.get_text(strip=True))
                
                if 'OVR' in raw_label:
                    stats['ranks']['OVR'] = val
                elif 'NATL' in raw_label or 'NATIONAL' in raw_label:
                    stats['ranks']['NATL'] = val
                elif 'ST' in raw_label: # Skip State ranks
                    pass
                else:
                    # Assume any other label (Pos, QB, WR, S, Safety) is the Position Rank
                    stats['ranks']['POS'] = val

        return stats

    # --- 4. EXECUTE PARSING ---
    
    # A. Transfer Data
    t_stats = get_section_stats("As a Transfer", soup)
    data['Transfer Stars'] = t_stats['stars']
    data['Transfer Rating'] = t_stats['rating']
    data['Transfer Year'] = t_stats['year'] 
    data['Transfer Overall Rank'] = t_stats['ranks']['OVR']
    data['Transfer Position Rank'] = t_stats['ranks']['POS']

    # B. Prospect Data
    # Try "As a Prospect" first
    p_stats = get_section_stats("As a Prospect", soup)
    
    # Fallback if "As a Prospect" is missing (sometimes labeled "High School" or implied)
    if p_stats['stars'] == '0' and p_stats['rating'] == 'NA':
         p_stats = get_section_stats("High School", soup)

    data['Prospect Stars'] = p_stats['stars']
    data['Prospect Rating'] = p_stats['rating']
    data['Prospect National Rank'] = p_stats['ranks']['NATL']
    
    # Prospect Position Rank Logic
    if p_stats['ranks']['POS'] != 'NA':
        val = p_stats['ranks']['POS']
        if "JUCO" in data['Prospect Stars']:
            data['Prospect Position Rank'] = f"{val} JUCO"
            # Tag National Rank as JUCO if present
            if data['Prospect National Rank'] != "NA":
                 data['Prospect National Rank'] = f"{data['Prospect National Rank']} JUCO"
        else:
            data['Prospect Position Rank'] = val
    else:
        data['Prospect Position Rank'] = "NA"

    # JUCO Edge Case: If JUCO but no National Rank found, allow "NA"
    if "JUCO" in data['Prospect Stars'] and data['Prospect National Rank'] == 'NA':
        data['Prospect National Rank'] = "NA" 

    return data

async def scrape_profile(context, url, sem, failed_urls):
    async with sem:
        for attempt in range(MAX_RETRIES):
            page = await context.new_page()
            # Block media to speed up
            await page.route("**/*.{png,jpg,jpeg,svg,mp4,woff,woff2}", lambda route: route.abort())
            try:
                await random_delay()
                await page.goto(url, timeout=60000, wait_until="commit")
                
                # Wait for key elements to ensure data is loaded
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
    print("--- Starting FINAL Scraper (Fixed Transfer/Prospect Logic) ---")
    
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
        
        # 3. EXTRACT LINKS
        links = await page.eval_on_selector_all("a[href*='/player/']", "elements => elements.map(e => e.href)")
        links = list(set([l for l in links if "247sports.com/player/" in l]))
        print(f"   Found {len(links)} profiles.")
        await page.close()

        # 4. SCRAPE PROFILES
        sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
        failed_urls = []
        tasks = [scrape_profile(context, link, sem, failed_urls) for link in links]
        results = await asyncio.gather(*tasks)
        
        # 5. SAVE
        df = pd.DataFrame([r for r in results if r])
        cols = [
            "247 ID", "Player Name", "Position", "Height", "Weight", "High School", "City, ST", "EXP", "Team",
            "Transfer Stars", "Transfer Rating", "Transfer Year", "Transfer Overall Rank", "Transfer Position Rank", "Transfer Team Name",
            "Prospect Stars", "Prospect Rating", "Prospect Position Rank", "Prospect National Rank", "URL"
        ]
        # Reindex checks for missing columns and ensures order
        df = df.reindex(columns=cols)
        df.to_csv(OUTPUT_FILE, index=False)
        print(f"--- DONE. Saved {len(df)} rows to {OUTPUT_FILE} ---")
        
        if failed_urls: pd.DataFrame(failed_urls).to_csv("failed_urls.csv", index=False)
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())

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

# --- SMART PARSER (Looks for text labels, not just classes) ---
def find_value_by_label(soup, label_patterns, parent_tag='li'):
    """Finds a label (like 'High School') and returns the text next to it."""
    # Try finding the specific text
    for pattern in label_patterns:
        # Search for elements containing the label text
        elements = soup.find_all(string=re.compile(pattern, re.IGNORECASE))
        for el in elements:
            # Usually the value is in the parent <li> or a sibling <span>
            parent = el.find_parent(parent_tag)
            if parent:
                # Remove the label text to get the value
                text = parent.get_text(" ", strip=True)
                # Remove the label itself from the text (case insensitive)
                clean_val = re.sub(pattern, "", text, flags=re.IGNORECASE).strip()
                # Remove trailing/leading colons or punctuation
                clean_val = clean_val.strip(":").strip()
                if clean_val: return clean_val
    return "NA"

def parse_profile(html, url, player_id):
    soup = BeautifulSoup(html, 'lxml')
    data = {}
    
    # 1. Basic Info
    data['247 ID'] = player_id
    
    # Name is usually in a simpler header
    name_tag = soup.select_one('.name') or soup.select_one('h1.name')
    data['Player Name'] = clean_text(name_tag.text) if name_tag else "NA"
    
    # Metrics - Try both class method and fuzzy find method
    metrics_list = soup.select('.metrics-list li')
    data['Position'] = "NA"
    data['Height'] = "NA"
    data['Weight'] = "NA"
    
    # Strategy: Parse the metrics strip at top
    if metrics_list:
        for m in metrics_list:
            text = m.text.strip()
            if 'Pos' in text: data['Position'] = text.replace('Pos', '').strip()
            elif 'Height' in text: 
                # Add ' to force Excel text format
                raw_ht = text.replace('Height', '').strip()
                data['Height'] = f"'{raw_ht}" 
            elif 'Weight' in text: data['Weight'] = text.replace('Weight', '').strip()

    # Smart Search for Details
    # We look for "High School", "Home Town", "Class" anywhere in the list
    data['High School'] = find_value_by_label(soup, ["High School"])
    data['City, ST'] = find_value_by_label(soup, ["Home Town", "Hometown"])
    data['EXP'] = find_value_by_label(soup, ["Class", "Exp"])
    
    # Team - often complex. Look for 'Committed' or 'Enrolled' blocks
    data['Team'] = "NA"
    team_tag = soup.select_one('.ni-school-name a') # Standard
    if team_tag: 
        data['Team'] = team_tag.text.strip()
    else:
        # Fallback: Look for "Committed to" text
        commit_node = soup.select_one('.transfer-prediction .team-name')
        if commit_node: data['Team'] = commit_node.text.strip()

    # --- SECTION 2: AS A TRANSFER ---
    # We define specific search logic for the Transfer Section
    data['Transfer Stars'] = "0"
    data['Transfer Rating'] = "NA"
    data['Transfer Year'] = "2026"
    data['Transfer Overall Rank'] = "NA"
    data['Transfer Position Rank'] = "NA"
    data['Transfer Team Name'] = data['Team'] # Often same

    # Locate Transfer Section by finding the header "Transfer"
    transfer_sect = None
    for sect in soup.select('section'):
        if 'Transfer' in sect.get_text() and 'Rankings' in sect.get_text():
            transfer_sect = sect
            break
            
    if transfer_sect:
        # Stars
        stars = transfer_sect.select('.icon-starsolid.yellow')
        data['Transfer Stars'] = len(stars)
        
        # Rating
        rating = transfer_sect.select_one('.rating')
        if rating: data['Transfer Rating'] = rating.text.strip()
        
        # Ranks - Look for OVR and Pos inside this section
        data['Transfer Overall Rank'] = find_value_by_label(transfer_sect, ["OVR", "Natl", "National"])
        
        # Position rank is tricky, it matches the player position
        pos_rank = find_value_by_label(transfer_sect, [data['Position']])
        # If the fuzzy finder just found the position name itself (e.g. "QB"), ignore it
        if pos_rank != data['Position']:
            data['Transfer Position Rank'] = normalize_rank(pos_rank)

    # --- SECTION 3: AS A PROSPECT ---
    data['Prospect Stars'] = "0"
    data['Prospect Rating'] = "NA"
    data['Prospect Position Rank'] = "NA"
    data['Prospect National Rank'] = "NA"
    
    prospect_sect = None
    # Look for "Prospect" or "High School" section
    for sect in soup.select('section'):
        header = sect.select_one('h2, h3, h4')
        if header and ('Prospect' in header.text or 'High School' in header.text):
            prospect_sect = sect
            break

    if prospect_sect:
        is_juco = "JUCO" in prospect_sect.get_text()
        stars = prospect_sect.select('.icon-starsolid.yellow')
        
        if is_juco and len(stars) == 0:
            data['Prospect Stars'] = "JUCO"
        else:
            data['Prospect Stars'] = len(stars)

        rating = prospect_sect.select_one('.rating')
        if rating: data['Prospect Rating'] = rating.text.strip()
        
        data['Prospect National Rank'] = find_value_by_label(prospect_sect, ["Natl", "National"])
        
        # Position Rank
        pos_rank = find_value_by_label(prospect_sect, [data['Position'], "Pos"])
        if pos_rank != data['Position']:
            data['Prospect Position Rank'] = normalize_rank(pos_rank)

    return data

async def scrape_profile(context, url, sem, failed_urls):
    async with sem:
        for attempt in range(MAX_RETRIES):
            page = await context.new_page()
            try:
                await asyncio.sleep(random.uniform(0.5, 1.5))
                response = await page.goto(url, timeout=60000)
                
                content = await page.content()
                if "Player Profile" not in content and "name" not in content:
                    raise Exception("Profile empty")

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
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=ua.random, viewport={'width': 1920, 'height': 1080})
        page = await context.new_page()
        
        print(f"--- 1. Loading Main List: {BASE_URL} ---")
        await page.goto(BASE_URL, timeout=90000)

        # --- FORCE LOAD MORE ---
        # We try 100 times. We also SCROLL to the bottom to trigger the button visibility.
        for i in range(100):
            try:
                # 1. Scroll to bottom
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(1) 
                
                # 2. Check for button
                load_more = page.locator(".showmore_lnk")
                if await load_more.count() > 0 and await load_more.first.is_visible():
                    print(f"   [Click {i+1}] Clicking Load More...")
                    # Force Click using JS (More reliable than mouse click)
                    await page.evaluate("document.querySelector('.showmore_lnk').click()")
                    # Wait for data to load
                    await page.wait_for_timeout(random.randint(2000, 4000))
                else:
                    print("   No more buttons found.")
                    break
            except Exception as e:
                print(f"   Loop break: {e}")
                break
        
        # --- EXTRACT LINKS ---
        print("--- 2. Extracting Links ---")
        links = await page.eval_on_selector_all("a[href*='/player/']", "elements => elements.map(e => e.href)")
        links = list(set([l for l in links if "247sports.com/player/" in l]))
        print(f"   Found {len(links)} profiles.")

        await page.close()

        # --- SCRAPE ---
        print("--- 3. Scraping Details ---")
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

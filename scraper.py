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

# ⭐ TEST MODE
TEST_MODE = True  # Change to False for full scrape
TEST_LIMIT = 50

# --- UTILS ---
def clean_text(text):
    if not text: return None
    return text.strip()

def extract_id_from_url(url):
    match = re.search(r'-(\d+)(?:/|$)', url)
    return match.group(1) if match else "NA"

async def random_delay():
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
    
    # Header Parsing
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
    data['Team'] = "NA"
    logo = soup.select_one('.primary-team-logo')
    if logo and logo.get('alt'):
        data['Team'] = logo.get('alt')
    elif soup.select_one('.ni-school-name a'):
        data['Team'] = soup.select_one('.ni-school-name a').text.strip()

    data['Transfer Team Name'] = "NA"
    banner = soup.select_one('.qa-team-name')
    if banner:
        data['Transfer Team Name'] = banner.text.strip()
    
    # --- PARSE TRANSFER ("As a Transfer") ---
    data['Transfer Stars'] = "0"
    data['Transfer Rating'] = "NA"
    data['Transfer Year'] = "2026"
    data['Transfer Overall Rank'] = "NA"
    data['Transfer Position Rank'] = "NA"

    transfer_node = soup.find(string=re.compile("As a Transfer"))
    if transfer_node:
        t_container = transfer_node.find_parent('section') or transfer_node.find_parent('div')
        if t_container:
            # Stars - correct selector
            stars = t_container.select('span.icon-starsolid.yellow')
            if stars:
                star_count = len(stars)
                data['Transfer Stars'] = str(min(star_count, 5))
            
            # Rating - use regex to extract just the number
            rating_block = t_container.select_one('.rank-block')
            if rating_block:
                rating_text = rating_block.get_text(strip=True)
                # Extract just the first number (before year in parentheses)
                match = re.search(r'^(\d+)', rating_text)
                if match:
                    data['Transfer Rating'] = match.group(1)
            
            # Ranks - from <strong> tags
            for li in t_container.select('li'):
                bold_tag = li.find('b')
                if not bold_tag:
                    continue
                
                bold_text = bold_tag.get_text(strip=True).upper()
                strong_tag = li.find('strong')
                if not strong_tag:
                    continue
                
                rank_number = strong_tag.get_text(strip=True)
                
                # OVR Rank
                if 'OVR' in bold_text:
                    data['Transfer Overall Rank'] = rank_number
                
                # Position Rank - exact match
                elif data['Position'] and bold_text == data['Position'].upper():
                    data['Transfer Position Rank'] = rank_number

    # --- PARSE PROSPECT ("As a Prospect") ---
    data['Prospect Stars'] = "0"
    data['Prospect Rating'] = "NA"
    data['Prospect Position Rank'] = "NA"
    data['Prospect National Rank'] = "NA"
    
    prospect_node = soup.find(string=re.compile("As a Prospect"))
    if prospect_node:
        p_container = prospect_node.find_parent('section') or prospect_node.find_parent('div')
        if p_container:
            # Check for JUCO
            is_juco = "JUCO" in p_container.get_text()
            
            # Stars
            stars = p_container.select('span.icon-starsolid.yellow')
            if stars:
                star_count = len(stars)
                data['Prospect Stars'] = str(min(star_count, 5))
            elif is_juco and len(stars) == 0:
                pass
            
            # Rating - use regex to extract just the number
            rating_block = p_container.select_one('.rank-block')
            if rating_block:
                rating_text = rating_block.get_text(strip=True)
                match = re.search(r'^(\d+)', rating_text)
                if match:
                    r_text = match.group(1)
                    data['Prospect Rating'] = r_text if r_text != 'N/A' else 'NA'
            
            # Ranks - from <strong> tags, skip state ranks
            for li in p_container.select('li'):
                bold_tag = li.find('b')
                if not bold_tag:
                    continue
                
                bold_text = bold_tag.get_text(strip=True).upper()
                strong_tag = li.find('strong')
                if not strong_tag:
                    continue
                
                rank_number = strong_tag.get_text(strip=True)
                
                # National Rank
                if 'NATL' in bold_text or 'NATIONAL' in bold_text:
                    data['Prospect National Rank'] = rank_number
                
                # Skip 2-letter state codes
                elif len(bold_text) == 2 and bold_text.isalpha():
                    continue
                
                # Position Rank - capture ANY position (not just current)
                elif data['Prospect Position Rank'] == 'NA':
                    data['Prospect Position Rank'] = rank_number
            
            # Set JUCO if detected
            if is_juco and data['Prospect National Rank'] == 'NA':
                data['Prospect National Rank'] = 'JUCO'

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
    
    if TEST_MODE:
        print("="*80)
        print(f"🧪 TEST MODE - Scraping first {TEST_LIMIT} players")
        print("="*80)
    
    print("--- Starting Scraper ---")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=ua.random, viewport={'width': 1920, 'height': 1080})
        page = await context.new_page()
        
        print(f"--- 1. Loading Main List ---")
        await page.goto(BASE_URL, timeout=120000, wait_until="commit")
        try: await page.wait_for_selector(".rankings-page__name-link", timeout=30000)
        except: pass

        if not TEST_MODE:
            print("--- 2. Expanding List (Clicking Load More) ---")
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
                        if await load_more.count() == 0:
                            print("   No more buttons. Full list loaded.")
                            break
                except Exception as e:
                    print(f"   Loop minor error: {e}")
                    break
        else:
            print("--- 2. TEST MODE: Loading only first page ---")
            await asyncio.sleep(2)
        
        print("--- 3. Extracting Profile Links ---")
        links = await page.eval_on_selector_all("a[href*='/player/']", "elements => elements.map(e => e.href)")
        unique_links = list(set([l for l in links if "247sports.com/player/" in l]))
        
        if TEST_MODE:
            unique_links = unique_links[:TEST_LIMIT]
            print(f"   🧪 Limited to {len(unique_links)} profiles")
        else:
            print(f"   Found {len(unique_links)} unique profiles to scrape.")
        
        await page.close()

        if len(unique_links) == 0:
            print("CRITICAL: No links found. Aborting.")
            await browser.close()
            return

        print("--- 4. Scraping Profiles ---")
        sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
        failed_urls = []
        tasks = [scrape_profile(context, link, sem, failed_urls) for link in unique_links]
        
        results = await asyncio.gather(*tasks)
        valid_results = [r for r in results if r]
        
        df = pd.DataFrame(valid_results)
        cols = [
            "247 ID", "Player Name", "Position", "Height", "Weight", "High School", "City, ST", "EXP", "Team",
            "Transfer Stars", "Transfer Rating", "Transfer Year", "Transfer Overall Rank", "Transfer Position Rank", "Transfer Team Name",
            "Prospect Stars", "Prospect Rating", "Prospect Position Rank", "Prospect National Rank", "URL"
        ]
        df = df.reindex(columns=cols)
        
        output_filename = f"TEST_{OUTPUT_FILE}" if TEST_MODE else OUTPUT_FILE
        df.to_csv(output_filename, index=False)
        
        print("="*80)
        print(f"{'🧪 TEST COMPLETE' if TEST_MODE else 'SUCCESS'} - Saved {len(df)} rows to {output_filename}")
        print("="*80)
        
        if failed_urls:
            print(f"   Note: {len(failed_urls)} profiles failed.")
            failed_file = f"TEST_failed_urls.csv" if TEST_MODE else "failed_urls.csv"
            pd.DataFrame(failed_urls).to_csv(failed_file, index=False)

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())

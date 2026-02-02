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

# ⭐ TEST MODE - Set to True to only scrape first 50 players
TEST_MODE = True  # Change to False for full scrape
TEST_LIMIT = 50   # Number of players to test with

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

def rating_to_stars(rating):
    """Convert rating to star count as fallback"""
    if not rating or rating == 'NA':
        return 'NA'
    
    try:
        rating_num = int(rating)
        if rating_num >= 98:
            return '5'
        elif rating_num >= 90:
            return '4'
        elif rating_num >= 80:
            return '3'
        elif rating_num >= 70:
            return '2'
        elif rating_num > 0:
            return '1'
        else:
            return 'NA'
    except:
        return 'NA'

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
            if match: data['Height'] = match.group(1).strip()
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
        data['Transfer Team Name'] = banner.text.strip()
    
    # --- PARSE TRANSFER ("As a Transfer") ---
    data['Transfer Stars'] = "NA"
    data['Transfer Rating'] = "NA"
    data['Transfer Year'] = "2026"
    data['Transfer Overall Rank'] = "NA"
    data['Transfer Position Rank'] = "NA"

    # Find Transfer section by exact header
    transfer_section = soup.find(string=re.compile(r"247SPORTS TRANSFER RANKINGS"))
    if transfer_section:
        t_container = transfer_section.find_parent('section') or transfer_section.find_parent('div', class_='ranking-section')
        if t_container:
            
            # METHOD 1: Count actual star elements (NOT string counting)
            star_elements = t_container.select('i.icon-starsolid, i[class*="star"], span[class*="star"]')
            if star_elements:
                star_count = len(star_elements)
                star_count = min(star_count, 5)  # Safety cap at 5 stars
                data['Transfer Stars'] = str(star_count)
            
            # METHOD 2: Find rating (pattern: "94 (2026)" or standalone)
            rating_text = t_container.get_text()
            rating_match = re.search(r'\b(9[0-9]|8[5-9]|[67][0-9])\s*\((\d{4})\)', rating_text)
            if rating_match:
                data['Transfer Rating'] = rating_match.group(1)
                data['Transfer Year'] = rating_match.group(2)
            else:
                # Alternative: Look for rating as standalone number
                rating_elem = t_container.select_one('.score, .rating-number, .rating')
                if rating_elem:
                    rating_val = rating_elem.get_text(strip=True)
                    if rating_val and rating_val.replace('.', '').isdigit():
                        data['Transfer Rating'] = rating_val
            
            # Fallback: Calculate stars from rating if not found
            if data['Transfer Stars'] == 'NA' and data['Transfer Rating'] != 'NA':
                data['Transfer Stars'] = rating_to_stars(data['Transfer Rating'])
            
            # METHOD 3: Extract ranks from list items with bold labels
            for li in t_container.select('li'):
                # Check if this <li> has a <b> tag (the label)
                bold_tag = li.find('b')
                if not bold_tag:
                    continue
                
                label = bold_tag.get_text(strip=True).upper()
                full_text = li.get_text(strip=True)
                
                # Extract the number that follows the label
                number_match = re.search(r'\b(\d+)\b', full_text.replace(label, '', 1))
                
                if not number_match:
                    continue
                
                if 'OVR' in label:
                    data['Transfer Overall Rank'] = number_match.group(1)
                
                # Position rank: label must exactly match player's position
                elif data['Position'] != 'NA' and label == data['Position'].upper():
                    data['Transfer Position Rank'] = number_match.group(1)

    # --- PARSE PROSPECT ("As a Prospect") ---
    data['Prospect Stars'] = "NA"
    data['Prospect Rating'] = "NA"
    data['Prospect Position Rank'] = "NA"
    data['Prospect National Rank'] = "NA"
    
    # Find Prospect section (but NOT the Transfer section)
    prospect_section = soup.find(string=re.compile(r"247SPORTS(?!.*TRANSFER)", re.IGNORECASE))
    if prospect_section:
        p_container = prospect_section.find_parent('section') or prospect_section.find_parent('div', class_='ranking-section')
        if p_container:
            
            # Check for JUCO first
            is_juco = "247SPORTSJUCO" in p_container.get_text() or "JUCO" in prospect_section
            
            # Count star elements
            star_elements = p_container.select('i.icon-starsolid, i[class*="star"], span[class*="star"]')
            if star_elements:
                star_count = len(star_elements)
                star_count = min(star_count, 5)  # Safety cap
                data['Prospect Stars'] = str(star_count)
            
            # Find rating (usually first number in section)
            rating_text = p_container.get_text()
            # Look for 2-digit number that's likely a rating (60-99)
            rating_match = re.search(r'\b(9[0-9]|8[0-9]|7[0-9]|6[0-9])\b', rating_text)
            if rating_match:
                data['Prospect Rating'] = rating_match.group(1)
            else:
                rating_elem = p_container.select_one('.score, .rating-number, .rating')
                if rating_elem:
                    rating_val = rating_elem.get_text(strip=True)
                    if rating_val and rating_val.replace('.', '').isdigit():
                        data['Prospect Rating'] = rating_val
            
            # Fallback: Calculate stars from rating if not found
            if data['Prospect Stars'] == 'NA' and data['Prospect Rating'] != 'NA':
                data['Prospect Stars'] = rating_to_stars(data['Prospect Rating'])
            
            # Extract ranks from list items
            # CRITICAL FIX: Don't match against current position - capture ANY position rank found
            for li in p_container.select('li'):
                bold_tag = li.find('b')
                if not bold_tag:
                    continue
                
                label = bold_tag.get_text(strip=True).upper()
                full_text = li.get_text(strip=True)
                number_match = re.search(r'\b(\d+)\b', full_text.replace(label, '', 1))
                
                if not number_match:
                    continue
                
                # National Rank
                if 'NATL' in label or 'NATIONAL' in label:
                    if not is_juco:  # Don't overwrite JUCO
                        data['Prospect National Rank'] = number_match.group(1)
                
                # Skip 2-letter state abbreviations (GA, TX, KS, OR, etc.)
                elif len(label) == 2 and label.isalpha():
                    continue  # This is a state rank, ignore it
                
                # Position Rank - capture FIRST non-state, non-national rank
                # This handles position changes (e.g., recruited as S, plays WR)
                elif data['Prospect Position Rank'] == 'NA':
                    data['Prospect Position Rank'] = number_match.group(1)
            
            # Set JUCO in National Rank field if detected
            if is_juco and data['Prospect National Rank'] == 'NA':
                data['Prospect National Rank'] = "JUCO"

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
                print(f"   [SUCCESS] {data['Player Name']} (ID: {player_id})")
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
        print(f"🧪 TEST MODE ENABLED - Will scrape only first {TEST_LIMIT} players")
        print("="*80)
    
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
        if TEST_MODE:
            print(f"--- 2. TEST MODE: Loading first {TEST_LIMIT} players only ---")
            # In test mode, we only need the first page (50 players)
            await asyncio.sleep(2)  # Wait for initial load
        else:
            print("--- 2. Expanding List (Clicking Load More) ---")
            # 300 cycles ensures we hit 1,500+ players easily
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
        
        # TEST MODE: Limit to first N players
        if TEST_MODE:
            unique_links = unique_links[:TEST_LIMIT]
            print(f"   🧪 TEST MODE: Limited to {len(unique_links)} profiles")
        else:
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
        
        # Add test mode prefix to filename if in test mode
        output_filename = OUTPUT_FILE
        if TEST_MODE:
            output_filename = f"TEST_{OUTPUT_FILE}"
        
        df.to_csv(output_filename, index=False)
        
        print("="*80)
        if TEST_MODE:
            print(f"🧪 TEST COMPLETE - Saved {len(df)} rows to {output_filename}")
            print("="*80)
            print("\n📊 RESULTS SUMMARY:")
            print(f"   Total scraped: {len(df)}")
            print(f"   Failed: {len(failed_urls)}")
            
            # Show sample of results
            if len(df) > 0:
                print("\n✅ Sample Results (first 5 players):")
                print("-" * 80)
                for idx, row in df.head(5).iterrows():
                    print(f"\n{row['Player Name']} ({row['Position']})")
                    print(f"   Transfer: {row['Transfer Stars']}⭐ | Rating: {row['Transfer Rating']} | Rank: {row['Transfer Overall Rank']}")
                    print(f"   Prospect: {row['Prospect Stars']}⭐ | Rating: {row['Prospect Rating']} | Pos Rank: {row['Prospect Position Rank']} | Natl: {row['Prospect National Rank']}")
            
            print("\n" + "="*80)
            print("📝 To run full scrape, set TEST_MODE = False at the top of the file")
            print("="*80)
        else:
            print(f"--- SUCCESS. Saved {len(df)} rows to {output_filename} ---")
        
        if failed_urls:
            print(f"   Note: {len(failed_urls)} profiles failed.")
            failed_filename = "failed_urls.csv" if not TEST_MODE else "TEST_failed_urls.csv"
            pd.DataFrame(failed_urls).to_csv(failed_filename, index=False)

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())

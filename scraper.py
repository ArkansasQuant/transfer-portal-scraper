import asyncio
import random
import pandas as pd
import re
import os
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from datetime import datetime
from fake_useragent import UserAgent

# --- CONFIGURATION ---
BASE_URL_TEMPLATE = "https://247sports.com/season/{year}-football/transferportalpositionranking/"

# ⭐ YEARS TO SCRAPE (in order - newest first)
YEARS = [2026, 2025, 2024]  # Start with recent 3 years
# YEARS = [2026, 2025, 2024, 2023, 2022, 2021]  # Uncomment for all 6 years

CONCURRENCY_LIMIT = 4
MAX_RETRIES = 5
OUTPUT_FILE = f"transfer_portal_{min(YEARS)}-{max(YEARS)}_{datetime.now().strftime('%Y%m%d')}.csv"

# ⭐ TEST MODE (controlled by GitHub Actions or defaults to True)
TEST_MODE = os.getenv('TEST_MODE', 'true').lower() == 'true'
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
    # Current Team (Origination)
    data['Team'] = "NA"
    team_header = soup.select_one('.team-info-section header h2')
    if team_header:
        data['Team'] = team_header.text.strip()
    
    # Transfer Destination Team
    data['Transfer Team Name'] = "NA"
    commit_banner = soup.select_one('.commit-banner span')
    if commit_banner:
        team_text = commit_banner.text.strip()
        if team_text and team_text != "Commit":
            data['Transfer Team Name'] = team_text
    
    # --- PARSE TRANSFER AND PROSPECT BY TITLE ---
    data['Transfer Stars'] = "0"
    data['Transfer Rating'] = "NA"
    data['Transfer Year'] = "NA"
    data['Transfer Overall Rank'] = "NA"
    data['Transfer Position Rank'] = "NA"
    data['Transfer Position'] = "NA"
    
    data['Prospect Stars'] = "0"
    data['Prospect Rating'] = "NA"
    data['Prospect Position Rank'] = "NA"
    data['Prospect National Rank'] = "NA"
    data['Prospect Position'] = "NA"
    
    # Find all rankings sections
    all_rankings = soup.select('section.rankings-section')
    
    for section in all_rankings:
        title_tag = section.select_one('h3.title')
        if not title_tag:
            continue
            
        title = title_tag.get_text(strip=True)
        
        # TRANSFER SECTION
        if "Transfer" in title:
            # Stars
            stars = section.select('span.icon-starsolid.yellow')
            if stars:
                data['Transfer Stars'] = str(min(len(stars), 5))
            
            # Rating and Year (extract year from rating block)
            rating_block = section.select_one('.rank-block')
            if rating_block:
                rating_text = rating_block.get_text(strip=True)
                # Extract rating (number before year)
                match = re.search(r'^(\d+)', rating_text)
                if match:
                    data['Transfer Rating'] = match.group(1)
                # Extract year from (YYYY) format
                year_match = re.search(r'\((\d{4})\)', rating_text)
                if year_match:
                    data['Transfer Year'] = year_match.group(1)
            
            # Ranks and Position
            for li in section.select('li'):
                bold_tag = li.find('b')
                if not bold_tag:
                    continue
                
                bold_text = bold_tag.get_text(strip=True).upper()
                strong_tag = li.find('strong')
                if not strong_tag:
                    continue
                
                rank_number = strong_tag.get_text(strip=True)
                
                if 'OVR' in bold_text:
                    data['Transfer Overall Rank'] = rank_number
                elif data['Transfer Position Rank'] == 'NA':
                    # This is the position rank
                    data['Transfer Position Rank'] = rank_number
                    data['Transfer Position'] = bold_text
        
        # PROSPECT SECTION
        elif title == "247Sports" or "JUCO" in title:
            is_juco = "JUCO" in title
            
            # Stars - check for JUCO
            if is_juco:
                data['Prospect Stars'] = "JUCO"
            else:
                stars = section.select('span.icon-starsolid.yellow')
                if stars:
                    data['Prospect Stars'] = str(min(len(stars), 5))
            
            # Rating
            rating_block = section.select_one('.rank-block')
            if rating_block:
                rating_text = rating_block.get_text(strip=True)
                match = re.search(r'^(\d+)', rating_text)
                if match:
                    data['Prospect Rating'] = match.group(1)
            
            # Ranks and Position
            for li in section.select('li'):
                bold_tag = li.find('b')
                if not bold_tag:
                    continue
                
                bold_text = bold_tag.get_text(strip=True).upper()
                strong_tag = li.find('strong')
                if not strong_tag:
                    continue
                
                rank_number = strong_tag.get_text(strip=True)
                
                # Check link URL to distinguish position vs state ranks
                link_tag = li.find('a')
                link_url = link_tag.get('href', '') if link_tag else ''
                
                # National Rank
                if 'NATL' in bold_text or 'NATIONAL' in bold_text:
                    data['Prospect National Rank'] = rank_number
                # State rank - skip it
                elif 'State=' in link_url:
                    continue
                # Position Rank - check URL for Position= or positionKey=
                elif ('Position=' in link_url or 'positionKey=' in link_url) and data['Prospect Position Rank'] == 'NA':
                    data['Prospect Position Rank'] = rank_number
                    data['Prospect Position'] = bold_text

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
                return data

            except Exception as e:
                await page.close()
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(5)
                else:
                    failed_urls.append({'url': url, 'reason': str(e)})
                    return None

async def scrape_year(year, p, ua):
    """Scrape all players for a specific year"""
    print("\n" + "="*80)
    print(f"📅 SCRAPING YEAR: {year}")
    print("="*80)
    
    base_url = BASE_URL_TEMPLATE.format(year=year)
    
    browser = await p.chromium.launch(headless=True)
    context = await browser.new_context(user_agent=ua.random, viewport={'width': 1920, 'height': 1080})
    page = await context.new_page()
    
    print(f"--- 1. Loading {year} Transfer Portal List ---")
    await page.goto(base_url, timeout=120000, wait_until="commit")
    try: await page.wait_for_selector(".rankings-page__name-link", timeout=30000)
    except: pass

    if not TEST_MODE:
        print(f"--- 2. Expanding {year} List (Clicking Load More) ---")
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
                        print(f"   ✅ {year}: Full list loaded")
                        break
            except Exception as e:
                print(f"   Loop error: {e}")
                break
    else:
        print(f"--- 2. TEST MODE: Loading only first page of {year} ---")
        await asyncio.sleep(2)
    
    print(f"--- 3. Extracting {year} Profile Links ---")
    links = await page.eval_on_selector_all("a[href*='/player/']", "elements => elements.map(e => e.href)")
    unique_links = list(set([l for l in links if "247sports.com/player/" in l]))
    
    if TEST_MODE:
        unique_links = unique_links[:TEST_LIMIT]
        print(f"   🧪 {year}: Limited to {len(unique_links)} profiles")
    else:
        print(f"   ✅ {year}: Found {len(unique_links)} unique profiles")
    
    await page.close()

    if len(unique_links) == 0:
        print(f"   ⚠️ {year}: No links found")
        await browser.close()
        return [], []

    print(f"--- 4. Scraping {year} Profiles ---")
    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
    failed_urls = []
    tasks = [scrape_profile(context, link, sem, failed_urls) for link in unique_links]
    
    results = await asyncio.gather(*tasks)
    valid_results = [r for r in results if r]
    
    print(f"   ✅ {year}: Scraped {len(valid_results)} players")
    if failed_urls:
        print(f"   ⚠️ {year}: {len(failed_urls)} profiles failed")
    
    await browser.close()
    return valid_results, failed_urls

async def main():
    ua = UserAgent()
    
    print("="*80)
    if TEST_MODE:
        print(f"🧪 TEST MODE - Scraping {TEST_LIMIT} players per year")
    else:
        print(f"🚀 FULL MODE - Scraping all players")
    print(f"📅 Years to scrape: {', '.join(map(str, YEARS))}")
    print("="*80)
    
    all_results = []
    all_failed = []
    
    async with async_playwright() as p:
        for year in YEARS:
            year_results, year_failed = await scrape_year(year, p, ua)
            all_results.extend(year_results)
            all_failed.extend([{**f, 'year': year} for f in year_failed])
    
    # Create final DataFrame
    if len(all_results) == 0:
        print("\n❌ No data scraped. Exiting.")
        return
    
    df = pd.DataFrame(all_results)
    cols = [
        "247 ID", "Player Name", "Position", "Height", "Weight", "High School", "City, ST", "EXP", "Team",
        "Transfer Stars", "Transfer Rating", "Transfer Year", "Transfer Overall Rank", "Transfer Position Rank", "Transfer Position", "Transfer Team Name",
        "Prospect Stars", "Prospect Rating", "Prospect Position Rank", "Prospect Position", "Prospect National Rank", "URL"
    ]
    df = df.reindex(columns=cols)
    
    output_filename = f"TEST_{OUTPUT_FILE}" if TEST_MODE else OUTPUT_FILE
    df.to_csv(output_filename, index=False)
    
    print("\n" + "="*80)
    print(f"{'🧪 TEST COMPLETE' if TEST_MODE else '✅ SUCCESS'}")
    print(f"📊 Total players scraped: {len(df)}")
    print(f"📁 Saved to: {output_filename}")
    print("="*80)
    
    # Show breakdown by year
    if 'Transfer Year' in df.columns:
        year_counts = df['Transfer Year'].value_counts().sort_index(ascending=False)
        print("\n📅 Players by Transfer Year:")
        for year, count in year_counts.items():
            print(f"   {year}: {count} players")
    
    if all_failed:
        print(f"\n⚠️ Total failed: {len(all_failed)} profiles")
        failed_file = f"TEST_failed_urls.csv" if TEST_MODE else "failed_urls.csv"
        pd.DataFrame(all_failed).to_csv(failed_file, index=False)
        print(f"   Failed URLs saved to: {failed_file}")

if __name__ == "__main__":
    asyncio.run(main())

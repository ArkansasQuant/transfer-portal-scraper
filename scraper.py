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
MAX_RETRIES = 5
OUTPUT_FILE = f"transfer_portal_2026_FINAL_{datetime.now().strftime('%Y%m%d')}.csv"

# --- UTILS ---
def clean_text(text):
    if not text: return "NA"
    t = text.strip()
    return "NA" if t in ["N/A", "", "-"] else t

def extract_id_from_url(url):
    match = re.search(r'-(\d+)(?:/|$)', url)
    return match.group(1) if match else "NA"

async def random_delay():
    await asyncio.sleep(random.uniform(1.0, 1.5))

# --- PARSING LOGIC (THE FIX) ---
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
    
    # --- RANKINGS SECTION IDENTIFICATION ---
    transfer_node = soup.find(string=re.compile("As a Transfer"))
    prospect_node = soup.find(string=re.compile("As a Prospect"))

    # --- PARSE TRANSFER ("As a Transfer") ---
    data['Transfer Stars'] = "0"
    data['Transfer Rating'] = "NA"
    data['Transfer Year'] = "2026"
    data['Transfer Overall Rank'] = "NA"
    data['Transfer Position Rank'] = "NA"

    if transfer_node:
        # FIX: Look DOWN for the next list, instead of UP for a parent container
        # This prevents the "Scope Leak" where we grabbed Prospect ranks by mistake.
        t_list = transfer_node.find_next('ul')
        
        # Rating & Stars: Look for immediate siblings
        rating_tag = transfer_node.find_next(class_='rating')
        if rating_tag: 
            data['Transfer Rating'] = clean_text(rating_tag.text)

        first_star = transfer_node.find_next(class_='icon-starsolid')
        if first_star and first_star.parent:
            data['Transfer Stars'] = len(first_star.parent.select('.icon-starsolid.yellow'))
            
        # Ranks
        if t_list:
            for li in t_list.select('li'):
                label_tag = li.select_one('h5')
                val_tag = li.select_one('strong')
                if label_tag and val_tag:
                    label = label_tag.get_text(strip=True).upper()
                    val = clean_text(val_tag.get_text(strip=True))
                    
                    if 'OVR' in label:
                        data['Transfer Overall Rank'] = val
                    elif label not in ['NATL', 'NATIONAL', 'ST', 'STATE']: 
                        # Negative Logic: If it's not OVR/NATL, it IS the Position Rank
                        data['Transfer Position Rank'] = val

    # --- PARSE PROSPECT ("As a Prospect") ---
    data['Prospect Stars'] = "0"
    data['Prospect Rating'] = "NA"
    data['Prospect Position Rank'] = "NA"
    data['Prospect National Rank'] = "NA"
    
    if prospect_node:
        p_list = prospect_node.find_next('ul')
        
        # Check JUCO Context
        p_container = prospect_node.find_parent('section') or prospect_node.find_parent('div')
        is_juco = False
        if p_container and "JUCO" in p_container.get_text().upper():
            is_juco = True
        
        rating_tag = prospect_node.find_next(class_='rating')
        if rating_tag:
            val = clean_text(rating_tag.text)
            data['Prospect Rating'] = f"{val} JUCO" if (is_juco and val != "NA") else val
            
        first_star = prospect_node.find_next(class_='icon-starsolid')
        if first_star and first_star.parent:
            count = len(first_star.parent.select('.icon-starsolid.yellow'))
            if is_juco and count == 0:
                 data['Prospect Stars'] = "0 JUCO"
            else:
                 data['Prospect Stars'] = f"{count} JUCO" if is_juco else count

        if p_list:
            for li in p_list.select('li'):
                label_tag = li.select_one('h5')
                val_tag = li.select_one('strong')
                if label_tag and val_tag:
                    label = label_tag.get_text(strip=True).upper()
                    val = clean_text(val_tag.get_text(strip=True))
                    
                    if 'NATL' in label or 'NATIONAL' in label:
                        data['Prospect National Rank'] = f"{val} JUCO" if is_juco else val
                    # Filter out State abbreviations to find Position Rank
                    elif label not in ['OVR', 'ST', 'STATE', 'AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']:
                        data['Prospect Position Rank'] = f"{val} JUCO" if is_juco else val

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
    print("--- Starting FINAL Scraper (Restored Loop + Neighbor Parsing) ---")
    
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
        # 300 cycles ensures we hit 1,500+ players easily
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
        
        # 3. EXTRACT LINKS
        print("--- 3. Extracting Profile Links ---")
        links = await page.eval_on_selector_all("a[href*='/player/']", "elements => elements.map(e => e.href)")
        unique_links = list(set([l for l in links if "247sports.com/player/" in l]))
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
        
        results = await asyncio.gather(*tasks)
        valid_results = [r for r in results if r]
        
        # 5. SAVE
        df = pd.DataFrame(valid_results)
        cols = [
            "247 ID", "Player Name", "Position", "Height", "Weight", "High School", "City, ST", "EXP", "Team",
            "Transfer Stars", "Transfer Rating", "Transfer Year", "Transfer Overall Rank", "Transfer Position Rank", "Transfer Team Name",
            "Prospect Stars", "Prospect Rating", "Prospect Position Rank", "Prospect National Rank", "URL"
        ]
        df = df.reindex(columns=cols)
        df.to_csv(OUTPUT_FILE, index=False)
        print(f"--- SUCCESS. Saved {len(df)} rows to {OUTPUT_FILE} ---")
        
        if failed_urls:
            print(f"   Note: {len(failed_urls)} profiles failed.")
            pd.DataFrame(failed_urls).to_csv("failed_urls.csv", index=False)

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())

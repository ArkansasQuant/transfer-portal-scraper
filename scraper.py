import asyncio
import random
import pandas as pd
import re
import os
import urllib.parse
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from datetime import datetime
from fake_useragent import UserAgent

# --- CONFIGURATION ---
BASE_URL_TEMPLATE = "https://247sports.com/season/{year}-football/transferportalpositionranking/"
TOP_URL_TEMPLATE = "https://247sports.com/season/{year}-football/TransferPortalTop/"

# ⭐ YEARS TO SCRAPE (controlled by GitHub Actions dropdown or defaults to 2024-2026)
YEAR_RANGE = os.getenv('YEAR_RANGE', '2024-2026')
if YEAR_RANGE == '2021-2023':
    YEARS = [2023, 2022, 2021]
elif YEAR_RANGE == '2024-2026':
    YEARS = [2026, 2025, 2024]
elif YEAR_RANGE == 'all':
    YEARS = [2026, 2025, 2024, 2023, 2022, 2021]
elif YEAR_RANGE in ['2021', '2022', '2023', '2024', '2025', '2026']:
    YEARS = [int(YEAR_RANGE)]
else:
    YEARS = [2026, 2025, 2024]  # Default fallback

CONCURRENCY_LIMIT = 4
MAX_RETRIES = 2

# ⭐ TEST MODE (controlled by GitHub Actions or defaults to True)
TEST_MODE = os.getenv('TEST_MODE', 'true').lower() == 'true'
TEST_LIMIT = 50

MODE_LABEL = "TEST" if TEST_MODE else "FULL"
OUTPUT_FILE = f"transfer_portal_{min(YEARS)}-{max(YEARS)}_{MODE_LABEL}_{datetime.now().strftime('%Y%m%d')}.csv"

# ⭐ DIAGNOSTICS MODE (saves problem HTML files for debugging)
DIAGNOSTICS_MODE = True
MAX_DIAGNOSTIC_SAMPLES = 5  # Save up to 5 problem HTML files per year

# ⭐ LOAD MORE SAFETY CAPS
MAX_LOAD_MORE_ITERATIONS = 1000   # Hard safety cap
MAX_STALLED_ITERATIONS = 25       # Exit after this many iterations with no new links AND no visible button
CLICK_WAIT_SECONDS = 2.5          # How long to wait after a successful click

# --- UTILS ---
def clean_text(text):
    if not text: return None
    return text.strip()

def extract_id_from_url(url):
    match = re.search(r'-(\d+)(?:/|$)', url)
    return match.group(1) if match else "NA"

def normalize_player_url(url):
    """
    Normalize 247Sports player URLs for deduplication.
    Handles: www vs non-www, http vs https, query params.
    KEEPS the /college-XXXXX/ suffix — this determines which transfer year's
    data is displayed on the profile page.
    """
    url = url.split('?')[0].split('#')[0]
    url = url.replace('://www.', '://')
    url = url.replace('http://', 'https://')
    if not url.endswith('/'):
        url += '/'
    return url

async def random_delay():
    await asyncio.sleep(random.uniform(0.3, 0.5))

def save_diagnostic_html(html, filename):
    """Save problematic HTML for debugging"""
    try:
        os.makedirs('diagnostic_html', exist_ok=True)
        filepath = f"diagnostic_html/{filename}"
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html)
        return filepath
    except Exception as e:
        print(f"   ⚠️ Failed to save diagnostic HTML: {e}")
        return None

# --- PARSING LOGIC ---
def parse_profile(html, url, player_id, scraping_year):
    """
    Parse player profile HTML.
    scraping_year: The year we're scraping from - used as Transfer Year.
    """
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

    # --- TEAM LOGIC (RESTORED CORRECT SELECTORS) ---
    # Current Team (Origination) — from the historical team-info section
    data['Team'] = "NA"
    team_header = soup.select_one('.team-info-section header h2')
    if team_header:
        data['Team'] = team_header.text.strip()
    
    # Transfer Destination Team — from the commit banner
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
            stars = section.select('span.icon-starsolid.yellow')
            if stars:
                data['Transfer Stars'] = str(min(len(stars), 5))
            
            rating_block = section.select_one('.rank-block')
            if rating_block:
                rating_text = rating_block.get_text(strip=True)
                match = re.search(r'^(\d+)', rating_text)
                if match:
                    data['Transfer Rating'] = match.group(1)
                year_match = re.search(r'\((\d{4})\)', rating_text)
                if year_match:
                    data['Transfer Year'] = year_match.group(1)
            
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
                    data['Transfer Position Rank'] = rank_number
                    data['Transfer Position'] = bold_text
        
        # PROSPECT SECTION
        elif title == "247Sports" or "JUCO" in title:
            is_juco = "JUCO" in title
            
            if is_juco:
                data['Prospect Stars'] = "JUCO"
            else:
                stars = section.select('span.icon-starsolid.yellow')
                if stars:
                    data['Prospect Stars'] = str(min(len(stars), 5))
            
            rating_block = section.select_one('.rank-block')
            if rating_block:
                rating_text = rating_block.get_text(strip=True)
                match = re.search(r'^(\d+)', rating_text)
                if match:
                    data['Prospect Rating'] = match.group(1)
            
            for li in section.select('li'):
                bold_tag = li.find('b')
                if not bold_tag:
                    continue
                bold_text = bold_tag.get_text(strip=True).upper()
                strong_tag = li.find('strong')
                if not strong_tag:
                    continue
                rank_number = strong_tag.get_text(strip=True)
                
                link_tag = li.find('a')
                link_url = link_tag.get('href', '') if link_tag else ''
                
                if 'NATL' in bold_text or 'NATIONAL' in bold_text:
                    data['Prospect National Rank'] = rank_number
                elif 'State=' in link_url:
                    continue
                elif ('Position=' in link_url or 'positionKey=' in link_url) and data['Prospect Position Rank'] == 'NA':
                    data['Prospect Position Rank'] = rank_number
                    data['Prospect Position'] = bold_text
    
    # Always use scraping year — profile page shows MOST RECENT transfer only,
    # so a player who appears on multiple years' portal lists needs the list year, not the profile year.
    data['Transfer Year'] = str(scraping_year)

    return data

async def scrape_profile(context, url, sem, failed_urls, scraping_year, diagnostic_tracker):
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
                data = parse_profile(content, url, player_id, scraping_year)
                data['URL'] = url
                
                if DIAGNOSTICS_MODE:
                    track_diagnostics(data, content, scraping_year, diagnostic_tracker)
                
                await page.close()
                return data

            except Exception as e:
                await page.close()
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(2)
                else:
                    failed_urls.append({'url': url, 'reason': str(e), 'year': scraping_year})
                    return None

def track_diagnostics(data, html, year, tracker):
    """Track field completeness and save problematic HTML samples"""
    if year not in tracker['by_year']:
        tracker['by_year'][year] = {
            'total': 0,
            'fields': {},
            'problem_samples': []
        }
    
    tracker['by_year'][year]['total'] += 1
    
    fields_to_track = [
        'Transfer Stars', 'Transfer Rating', 'Transfer Year', 'Transfer Overall Rank',
        'Transfer Position Rank', 'Transfer Position', 'Transfer Team Name',
        'Prospect Stars', 'Prospect Rating', 'Prospect Position Rank',
        'Prospect Position', 'Prospect National Rank'
    ]
    
    has_issues = False
    missing_fields = []
    
    for field in fields_to_track:
        if field not in tracker['by_year'][year]['fields']:
            tracker['by_year'][year]['fields'][field] = {'filled': 0, 'na': 0}
        
        value = data.get(field, 'NA')
        if value == 'NA' or value == '0':
            tracker['by_year'][year]['fields'][field]['na'] += 1
            missing_fields.append(field)
            has_issues = True
        else:
            tracker['by_year'][year]['fields'][field]['filled'] += 1
    
    if has_issues and len(tracker['by_year'][year]['problem_samples']) < MAX_DIAGNOSTIC_SAMPLES:
        player_id = data.get('247 ID', 'unknown')
        player_name = data.get('Player Name', 'unknown')
        filename = f"problem_{year}_{player_id}.html"
        saved_path = save_diagnostic_html(html, filename)
        
        if saved_path:
            tracker['by_year'][year]['problem_samples'].append({
                'player': player_name,
                'id': player_id,
                'url': data.get('URL', 'unknown'),
                'missing_fields': missing_fields,
                'html_file': filename
            })

# --- OVERLAY CLEANUP HELPER ---
OVERLAY_CLEANUP_JS = """
() => {
    const selectors = [
        '[id^="bx-campaign"]',
        '.bxc.bx-type-overlay',
        '.bxc',
        '.IL_BASE',
        '[id="IL_INSEARCH"]',
        '[id="d_IL_INSEARCH"]',
        '.bx-slab',
        '.bx-overlay',
        '[id*="bouncex"]',
        '[class*="bouncex"]'
    ];
    selectors.forEach(sel => {
        try {
            document.querySelectorAll(sel).forEach(el => el.remove());
        } catch(e) {}
    });
}
"""

async def get_link_count(page):
    """Count player profile links currently on the page."""
    try:
        return await page.eval_on_selector_all(
            "a[href*='/player/']",
            "elements => elements.length"
        )
    except:
        return 0

async def try_click_load_more(page):
    """
    Try multiple strategies to click the Load More button.
    Returns True if a click was attempted successfully.
    """
    # Strategy 1: Playwright locator click
    try:
        load_more = page.locator("text=Load More Players").or_(page.locator(".showmore_lnk"))
        if await load_more.count() > 0:
            try:
                await load_more.first.scroll_into_view_if_needed(timeout=3000)
            except:
                pass
            try:
                await load_more.first.click(timeout=5000)
                return True
            except:
                # Strategy 2: Force click
                try:
                    await load_more.first.click(timeout=5000, force=True)
                    return True
                except:
                    pass
    except:
        pass
    
    # Strategy 3: Direct JS click — most robust for overlay interference
    try:
        clicked = await page.evaluate("""
            () => {
                const candidates = [
                    ...document.querySelectorAll('.transfer-group-loadMore'),
                    ...document.querySelectorAll('.showmore_lnk'),
                    ...[...document.querySelectorAll('button, a')].filter(el => 
                        el.textContent && el.textContent.trim().includes('Load More'))
                ];
                for (const btn of candidates) {
                    if (btn.offsetParent !== null || btn.getBoundingClientRect().height > 0) {
                        btn.click();
                        return true;
                    }
                }
                // Last resort — click even if hidden
                if (candidates.length > 0) {
                    candidates[0].click();
                    return true;
                }
                return false;
            }
        """)
        return clicked
    except:
        return False

async def load_more_button_exists(page):
    """Check if a Load More button exists in the DOM (regardless of visibility)."""
    try:
        count = await page.evaluate("""
            () => {
                const candidates = [
                    ...document.querySelectorAll('.transfer-group-loadMore'),
                    ...document.querySelectorAll('.showmore_lnk'),
                    ...[...document.querySelectorAll('button, a')].filter(el => 
                        el.textContent && el.textContent.trim().includes('Load More'))
                ];
                return candidates.length;
            }
        """)
        return count > 0
    except:
        return False

async def extract_expected_total(page):
    """
    Extract expected total player count from the page.
    ONLY looks at elements likely to contain the count — never grabs year numbers from h1.
    """
    # Look for explicit total indicators only
    selectors_to_try = [
        ".rankings-page__header .total",
        ".result-count",
        "[class*='total-count']",
        "[class*='player-count']",
    ]
    
    for selector in selectors_to_try:
        try:
            el = page.locator(selector).first
            if await el.count() > 0:
                text = await el.text_content()
                # Look for numbers that aren't years (exclude 19xx/20xx)
                matches = re.findall(r'\b(\d{3,5})\b', text or '')
                for m in matches:
                    n = int(m)
                    # Filter: must be at least 100, and not a year
                    if n >= 100 and not (1900 <= n <= 2099):
                        return n
        except:
            continue
    
    return None

# --- SCRAPE YEAR ---
async def scrape_year(year, p, ua, diagnostic_tracker):
    """Scrape all players for a specific year"""
    print("\n" + "="*80)
    print(f"📅 SCRAPING YEAR: {year}")
    print("="*80)
    
    browser = await p.chromium.launch(headless=True)
    context = await browser.new_context(user_agent=ua.random, viewport={'width': 1920, 'height': 1080})
    page = await context.new_page()
    
    # Block ad/overlay scripts that intercept clicks
    await page.route("**/*bouncex*", lambda route: route.abort())
    await page.route("**/*bounceexchange*", lambda route: route.abort())
    await page.route("**/*integralas*", lambda route: route.abort())
    await page.route("**/*IL_INSEARCH*", lambda route: route.abort())
    
    # ---------------------------------------------------------------
    # STEP 1a: Load TransferPortalTop page (top ~247 players)
    # ---------------------------------------------------------------
    top_url = TOP_URL_TEMPLATE.format(year=year)
    print(f"--- 1a. Loading {year} TransferPortalTop (top ~247) ---")
    top_links = []
    try:
        await page.goto(top_url, timeout=120000, wait_until="domcontentloaded")
        await page.wait_for_selector("li.transfer-player h3 a, .rankings-page__name-link", timeout=30000)
        await asyncio.sleep(2)
        
        for selector in [
            "li.transfer-player h3 a",
            ".rankings-page__name-link",
        ]:
            try:
                found = await page.eval_on_selector_all(
                    selector,
                    "elements => elements.map(e => e.href)"
                )
                if found:
                    print(f"   📎 Top page '{selector}' → {len(found)} links")
                    top_links.extend(found)
            except:
                pass
        
        top_links = [normalize_player_url(l) for l in top_links if "247sports.com/player/" in l]
        top_links = list(dict.fromkeys(top_links))  # Dedupe, preserve order
        print(f"   ✅ {year}: TransferPortalTop captured {len(top_links)} unique links")
    except Exception as e:
        print(f"   ⚠️ {year}: TransferPortalTop failed: {e}")
    
    # ---------------------------------------------------------------
    # STEP 1b: Load the full transfer portal list page
    # ---------------------------------------------------------------
    base_url = BASE_URL_TEMPLATE.format(year=year)
    print(f"--- 1b. Loading {year} Transfer Portal Full List ---")
    await page.goto(base_url, timeout=120000, wait_until="domcontentloaded")
    try:
        await page.wait_for_selector(".rankings-page__name-link, li.transfer-player h3 a", timeout=30000)
    except:
        pass
    
    # Capture expected total (from explicit count elements only)
    expected_total = await extract_expected_total(page)
    if expected_total:
        print(f"   📊 {year}: Page reports {expected_total} total players")
    else:
        print(f"   ℹ️ {year}: No explicit total count on page (will rely on link growth tracking)")

    # ---------------------------------------------------------------
    # STEP 2: Expand full list with ROBUST Load More loop
    # ---------------------------------------------------------------
    if not TEST_MODE:
        print(f"--- 2. Expanding {year} List (Robust Load More) ---")
        
        # Initial overlay dismissal and scroll
        await asyncio.sleep(2)
        await page.evaluate(OVERLAY_CLEANUP_JS)
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(1)
        
        last_link_count = await get_link_count(page)
        print(f"   📊 {year}: Starting link count: {last_link_count}")
        
        stalled_count = 0           # Iterations with no growth
        clicks_made = 0
        clicks_attempted = 0
        consecutive_click_failures = 0
        
        for i in range(MAX_LOAD_MORE_ITERATIONS):
            # Dismiss overlays every iteration (cheap, prevents click interception)
            try:
                await page.evaluate(OVERLAY_CLEANUP_JS)
            except:
                pass
            
            # Scroll to bottom every few iterations to trigger lazy loading
            if i % 2 == 0:
                try:
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                except:
                    pass
            
            # Check if Load More button exists in DOM (not just visible)
            button_exists = await load_more_button_exists(page)
            
            if button_exists:
                clicked = await try_click_load_more(page)
                clicks_attempted += 1
                
                if clicked:
                    clicks_made += 1
                    consecutive_click_failures = 0
                    await asyncio.sleep(CLICK_WAIT_SECONDS)
                else:
                    consecutive_click_failures += 1
                    await asyncio.sleep(1.5)
                    
                    # If click failures pile up, try reloading the page section
                    if consecutive_click_failures >= 10:
                        print(f"   ⚠️ {year}: {consecutive_click_failures} consecutive click failures, forcing scroll")
                        await page.evaluate("window.scrollTo(0, 0)")
                        await asyncio.sleep(1)
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        await asyncio.sleep(2)
                        consecutive_click_failures = 0
            else:
                # No button — might be truly done, or button temporarily hidden
                await asyncio.sleep(1.5)
            
            # Check if link count has grown
            current_link_count = await get_link_count(page)
            
            if current_link_count > last_link_count:
                stalled_count = 0
                last_link_count = current_link_count
            else:
                stalled_count += 1
            
            # Progress logging every 10 clicks or every 50 iterations
            if (clicks_made > 0 and clicks_made % 10 == 0 and clicks_attempted != clicks_made - 1) or (i > 0 and i % 50 == 0):
                print(f"   📈 {year}: iter={i}, clicks={clicks_made}, links={current_link_count}, stalled={stalled_count}")
            
            # EXIT CONDITIONS:
            # 1. Button gone AND links haven't grown for MAX_STALLED_ITERATIONS
            if not button_exists and stalled_count >= MAX_STALLED_ITERATIONS:
                print(f"   ✅ {year}: Load More complete (no button, {stalled_count} stalled iterations)")
                print(f"      Total: {clicks_made} clicks, {current_link_count} links found")
                break
            
            # 2. If we've stalled for a VERY long time even with button present, bail
            if stalled_count >= MAX_STALLED_ITERATIONS * 2:
                print(f"   ⚠️ {year}: Stalled for {stalled_count} iterations despite button present. Exiting.")
                print(f"      Total: {clicks_made} clicks, {current_link_count} links found")
                break
        else:
            # Hit the iteration cap
            print(f"   ⚠️ {year}: Hit max iteration cap ({MAX_LOAD_MORE_ITERATIONS})")
            print(f"      Total: {clicks_made} clicks, {last_link_count} links found")
        
        # Final overlay cleanup and one more scroll to capture anything lazy-loaded
        try:
            await page.evaluate(OVERLAY_CLEANUP_JS)
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(2)
        except:
            pass
    else:
        print(f"--- 2. TEST MODE: Loading only first page of {year} ---")
        await asyncio.sleep(1)
    
    # ---------------------------------------------------------------
    # STEP 3: Extract player profile links (multiple selectors)
    # ---------------------------------------------------------------
    print(f"--- 3. Extracting {year} Profile Links ---")
    
    all_links = []
    for selector in [
        ".rankings-page__name-link",
        "li.transfer-player h3 a",
        ".rankings-page__list-item a[href*='/player/']",
        "li.transfer-player a[href*='/player/']",
    ]:
        try:
            found = await page.eval_on_selector_all(
                selector,
                "elements => elements.map(e => e.href)"
            )
            if found:
                print(f"   📎 '{selector}' → {len(found)} links")
                all_links.extend(found)
        except:
            pass
    
    # Last resort
    if len(all_links) == 0:
        all_links = await page.eval_on_selector_all(
            "a[href*='/player/']",
            "elements => elements.map(e => e.href)"
        )
        print(f"   📎 broad fallback → {len(all_links)} links")
    
    # Merge TransferPortalTop links FIRST (preserve priority)
    merged_links = []
    if top_links:
        merged_links.extend(top_links)
        print(f"   📎 {len(top_links)} links from TransferPortalTop (priority)")
    merged_links.extend(all_links)
    
    # Normalize and deduplicate
    unique_links = list(dict.fromkeys(
        normalize_player_url(l)
        for l in merged_links
        if "247sports.com/player/" in l
    ))
    
    # Count visible list items for validation
    try:
        visible_items = await page.eval_on_selector_all(
            ".rankings-page__list-item, li.transfer-player",
            "elements => elements.length"
        )
        print(f"   📊 {year}: Visible list items on page: {visible_items}")
    except:
        visible_items = None
    
    if TEST_MODE:
        unique_links = unique_links[:TEST_LIMIT]
        print(f"   🧪 {year}: Limited to {len(unique_links)} profiles")
    else:
        print(f"   ✅ {year}: Found {len(unique_links)} unique profiles")
    
    # Validate coverage
    if expected_total and not TEST_MODE:
        coverage = len(unique_links) / expected_total * 100
        if coverage >= 95:
            print(f"   ✅ {year}: Coverage {coverage:.1f}% ({len(unique_links)}/{expected_total})")
        elif coverage >= 80:
            print(f"   ⚠️ {year}: Coverage {coverage:.1f}% ({len(unique_links)}/{expected_total}) — some players may be missing")
        else:
            print(f"   ❌ {year}: Coverage {coverage:.1f}% ({len(unique_links)}/{expected_total}) — SIGNIFICANT DATA LOSS")
    
    if visible_items and not TEST_MODE:
        if len(unique_links) < visible_items:
            print(f"   ⚠️ {year}: {visible_items - len(unique_links)} visible items have no extractable link!")
    
    await page.close()

    if len(unique_links) == 0:
        print(f"   ⚠️ {year}: No links found")
        await browser.close()
        return [], []

    # ---------------------------------------------------------------
    # STEP 4: Scrape individual profiles
    # ---------------------------------------------------------------
    print(f"--- 4. Scraping {year} Profiles ---")
    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
    failed_urls = []
    tasks = [scrape_profile(context, link, sem, failed_urls, year, diagnostic_tracker) for link in unique_links]
    
    results = await asyncio.gather(*tasks)
    valid_results = [r for r in results if r]
    
    print(f"   ✅ {year}: Scraped {len(valid_results)} players")
    if failed_urls:
        print(f"   ⚠️ {year}: {len(failed_urls)} profiles failed")
    
    await browser.close()
    return valid_results, failed_urls

def generate_diagnostic_report(diagnostic_tracker, output_file="diagnostics_report.txt"):
    """Generate detailed diagnostic report"""
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("="*80 + "\n")
        f.write("🔍 DIAGNOSTIC REPORT\n")
        f.write("="*80 + "\n\n")
        
        for year in sorted(diagnostic_tracker['by_year'].keys(), reverse=True):
            year_data = diagnostic_tracker['by_year'][year]
            total = year_data['total']
            
            f.write(f"\n{'='*80}\n")
            f.write(f"📅 YEAR {year} ({total} players)\n")
            f.write(f"{'='*80}\n\n")
            
            f.write("FIELD COMPLETENESS:\n")
            f.write("-" * 80 + "\n")
            
            for field, counts in sorted(year_data['fields'].items()):
                filled = counts['filled']
                na = counts['na']
                pct = (filled / total * 100) if total > 0 else 0
                status = "✅" if pct >= 95 else ("⚠️" if pct >= 80 else "❌")
                f.write(f"{status} {field:30} {filled:4d}/{total:4d} ({pct:5.1f}%)\n")
            
            if year_data['problem_samples']:
                f.write(f"\n🔴 PROBLEMATIC PLAYERS (saved {len(year_data['problem_samples'])} samples):\n")
                f.write("-" * 80 + "\n")
                for sample in year_data['problem_samples']:
                    f.write(f"\nPlayer: {sample['player']} (ID: {sample['id']})\n")
                    f.write(f"URL: {sample['url']}\n")
                    f.write(f"HTML File: {sample['html_file']}\n")
                    f.write(f"Missing Fields: {', '.join(sample['missing_fields'][:5])}\n")
        
        f.write("\n" + "="*80 + "\n")
        f.write("📊 SUMMARY\n")
        f.write("="*80 + "\n")
        
        for year in sorted(diagnostic_tracker['by_year'].keys(), reverse=True):
            year_data = diagnostic_tracker['by_year'][year]
            total = year_data['total']
            problem_count = len(year_data['problem_samples'])
            status = "✅" if problem_count == 0 else ("⚠️" if problem_count < 5 else "❌")
            f.write(f"{status} {year}: {total} players scraped, {problem_count} problem samples saved\n")
    
    print(f"\n📋 Diagnostic report saved to: {output_file}")

async def main():
    ua = UserAgent()
    
    print("="*80)
    if TEST_MODE:
        print(f"🧪 TEST MODE - Scraping {TEST_LIMIT} players per year")
    else:
        print(f"🚀 FULL MODE - Scraping all players")
    print(f"📅 Years to scrape: {', '.join(map(str, YEARS))}")
    print(f"📅 Year range setting: {YEAR_RANGE}")
    if DIAGNOSTICS_MODE:
        print(f"🔍 DIAGNOSTICS MODE: Enabled (saving up to {MAX_DIAGNOSTIC_SAMPLES} problem HTML files per year)")
    print("="*80)
    
    all_results = []
    all_failed = []
    diagnostic_tracker = {'by_year': {}}
    
    async with async_playwright() as p:
        for year in YEARS:
            year_results, year_failed = await scrape_year(year, p, ua, diagnostic_tracker)
            all_results.extend(year_results)
            all_failed.extend(year_failed)
    
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
    
    # Dedup
    before_dedup = len(df)
    df = df.drop_duplicates()
    df = df.drop_duplicates(subset=['247 ID', 'Transfer Year'], keep='first')
    after_dedup = len(df)
    if before_dedup != after_dedup:
        print(f"   🧹 Removed {before_dedup - after_dedup} duplicate rows ({before_dedup} → {after_dedup})")
    
    output_filename = OUTPUT_FILE
    df.to_csv(output_filename, index=False)
    
    print("\n" + "="*80)
    print(f"{'🧪 TEST COMPLETE' if TEST_MODE else '✅ SUCCESS'}")
    print(f"📊 Total players scraped: {len(df)}")
    print(f"📁 Saved to: {output_filename}")
    print("="*80)
    
    if 'Transfer Year' in df.columns:
        year_counts = df['Transfer Year'].value_counts().sort_index(ascending=False)
        print("\n📅 Players by Transfer Year:")
        for year, count in year_counts.items():
            print(f"   {year}: {count} players")
    
    if all_failed:
        print(f"\n⚠️ Total failed: {len(all_failed)} profiles")
        failed_file = f"failed_urls_{min(YEARS)}-{max(YEARS)}_{MODE_LABEL}_{datetime.now().strftime('%Y%m%d')}.csv"
        pd.DataFrame(all_failed).to_csv(failed_file, index=False)
        print(f"   Failed URLs saved to: {failed_file}")
    
    if DIAGNOSTICS_MODE and diagnostic_tracker['by_year']:
        generate_diagnostic_report(diagnostic_tracker)

if __name__ == "__main__":
    asyncio.run(main())

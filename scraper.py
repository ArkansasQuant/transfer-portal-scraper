"""
247Sports Transfer Portal Scraper — multi-method, year-aware.

Year routing:
  - 2023-2026:  positionKey probe via /TransferPortalTop/?positionKey=N
  - 2022:       team crawl via /transferteamrankings/ → per-team transfer pages
  - 2021:       per-team /transferportal/ pages (slug discovery via /compositeteamrankings/)

The downstream profile scraping/parsing/dedup pipeline is identical regardless
of method.
"""
import asyncio
import random
import pandas as pd
import re
import os
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from datetime import datetime
from fake_useragent import UserAgent

# --- URL TEMPLATES ---
POSITION_URL_TEMPLATE = "https://247sports.com/season/{year}-football/TransferPortalTop/?positionKey={key}"
TOP_URL_TEMPLATE = "https://247sports.com/season/{year}-football/TransferPortalTop/"
TRANSFER_TEAM_RANKINGS_URL = "https://247sports.com/season/{year}-football/transferteamrankings/"
COMPOSITE_TEAM_RANKINGS_URL = "https://247sports.com/season/{year}-football/compositeteamrankings/"
PER_TEAM_TRANSFER_URL = "https://247sports.com/college/{slug}/season/{year}-football/transferportal/"

# --- YEAR ROUTING ---
POSITIONKEY_YEARS = {2023, 2024, 2025, 2026}
TRANSFER_TEAM_RANKINGS_YEARS = {2022}
PER_TEAM_TRANSFERPORTAL_YEARS = {2021}

# --- YEAR INPUT PARSING ---
YEAR_RANGE = os.getenv('YEAR_RANGE', '2024-2026')

def parse_year_input(raw):
    """Accept '2024', '2024-2026', '2021,2022', 'all', etc."""
    raw = (raw or '').strip()
    if raw == 'all':
        return [2026, 2025, 2024, 2023, 2022, 2021]
    if ',' in raw:
        return sorted({int(y.strip()) for y in raw.split(',') if y.strip().isdigit()}, reverse=True)
    if '-' in raw:
        try:
            lo, hi = [int(x) for x in raw.split('-')]
            return list(range(hi, lo - 1, -1))
        except ValueError:
            pass
    if raw.isdigit():
        return [int(raw)]
    return [2026, 2025, 2024]

YEARS = parse_year_input(YEAR_RANGE)

CONCURRENCY_LIMIT = 4
MAX_RETRIES = 2

TEST_MODE = os.getenv('TEST_MODE', 'true').lower() == 'true'
TEST_LIMIT = 50

MODE_LABEL = "TEST" if TEST_MODE else "FULL"
OUTPUT_FILE = f"transfer_portal_{min(YEARS)}-{max(YEARS)}_{MODE_LABEL}_{datetime.now().strftime('%Y%m%d')}.csv"

DIAGNOSTICS_MODE = True
MAX_DIAGNOSTIC_SAMPLES = 5

# --- UTILS ---
def clean_text(text):
    if not text: return None
    return text.strip()

def extract_id_from_url(url):
    match = re.search(r'-(\d+)(?:/|$)', url)
    return match.group(1) if match else "NA"

def normalize_player_url(url):
    """Normalize 247Sports player URLs for deduplication.
    KEEPS the /college-XXXXX/ suffix — determines which transfer year displays."""
    url = url.split('?')[0].split('#')[0]
    url = url.replace('://www.', '://')
    url = url.replace('http://', 'https://')
    if not url.endswith('/'):
        url += '/'
    return url

def absolutize(href):
    """247 sometimes returns absolute URLs in hrefs. Detect and don't double-prefix."""
    if not href:
        return None
    if href.startswith('http://') or href.startswith('https://'):
        return href
    if href.startswith('/'):
        return f"https://247sports.com{href}"
    return None

async def random_delay():
    await asyncio.sleep(random.uniform(0.3, 0.5))

def save_diagnostic_html(html, filename):
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
    """Parse player profile HTML. scraping_year is used as Transfer Year."""
    soup = BeautifulSoup(html, 'lxml')
    data = {}
    
    data['247 ID'] = player_id
    name_tag = soup.select_one('.name') or soup.select_one('h1.name')
    data['Player Name'] = clean_text(name_tag.text) if name_tag else "NA"
    
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

    # --- TEAM LOGIC (correct selectors) ---
    data['Team'] = "NA"
    team_header = soup.select_one('.team-info-section header h2')
    if team_header:
        data['Team'] = team_header.text.strip()
    
    data['Transfer Team Name'] = "NA"
    commit_banner = soup.select_one('.commit-banner span')
    if commit_banner:
        team_text = commit_banner.text.strip()
        if team_text and team_text != "Commit":
            data['Transfer Team Name'] = team_text
    
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
    
    all_rankings = soup.select('section.rankings-section')
    
    for section in all_rankings:
        title_tag = section.select_one('h3.title')
        if not title_tag:
            continue
        title = title_tag.get_text(strip=True)
        
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
    if year not in tracker['by_year']:
        tracker['by_year'][year] = {'total': 0, 'fields': {}, 'problem_samples': []}
    
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
                'player': player_name, 'id': player_id,
                'url': data.get('URL', 'unknown'),
                'missing_fields': missing_fields, 'html_file': filename
            })

# =============================================================================
# METHOD A: positionKey probe (used for 2023-2026)
# =============================================================================

async def discover_position_keys(context, year):
    """Probe positionKey 1-99 in parallel, keep keys that return >=3 player links."""
    print(f"   🔍 {year}: Probing positionKey URLs 1-99 in parallel...")
    KNOWN_KEYS = {14: 'WR', 25: 'S', 59: 'LB'}
    valid_keys = []
    
    async def probe(key):
        probe_page = await context.new_page()
        try:
            url = POSITION_URL_TEMPLATE.format(year=year, key=key)
            await probe_page.goto(url, timeout=30000, wait_until="domcontentloaded")
            await asyncio.sleep(1)
            count = await probe_page.evaluate("""
                () => document.querySelectorAll('a[href*="/player/"]').length
            """)
            await probe_page.close()
            return (key, count) if count >= 3 else None
        except Exception:
            try:
                await probe_page.close()
            except:
                pass
            return None
    
    BATCH_SIZE = 8
    keys_to_probe = list(range(1, 100))
    for i in range(0, len(keys_to_probe), BATCH_SIZE):
        batch = keys_to_probe[i:i+BATCH_SIZE]
        results = await asyncio.gather(*[probe(k) for k in batch])
        for r in results:
            if r:
                key, count = r
                label = KNOWN_KEYS.get(key, '')
                valid_keys.append((key, label))
                print(f"      ✓ positionKey={key} ({label or '?'}): {count} players")
    
    print(f"   ✅ {year}: Found {len(valid_keys)} valid position keys")
    return valid_keys

async def collect_links_from_position_page(page, year, position_key, label):
    url = POSITION_URL_TEMPLATE.format(year=year, key=position_key)
    try:
        await page.goto(url, timeout=60000, wait_until="domcontentloaded")
        await asyncio.sleep(2)
        try:
            for _ in range(5):
                btn = page.locator(".transfer-group-loadMore, .showmore_lnk, text=Load More Players").first
                if await btn.count() > 0 and await btn.is_visible():
                    try:
                        await btn.click(timeout=3000)
                        await asyncio.sleep(2)
                    except:
                        break
                else:
                    break
        except:
            pass
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(1)
        
        links = []
        for selector in ["li.transfer-player h3 a", ".rankings-page__name-link",
                         "li.transfer-player a[href*='/player/']", "a[href*='/player/']"]:
            try:
                found = await page.eval_on_selector_all(selector, "elements => elements.map(e => e.href)")
                if found:
                    links.extend(found)
                    if len(links) > 0:
                        break
            except:
                pass
        unique = list(dict.fromkeys(normalize_player_url(l) for l in links if "247sports.com/player/" in l))
        if len(unique) > 0:
            print(f"      📎 positionKey={position_key} ({label or '?'}): {len(unique)} players")
        return unique
    except Exception as e:
        print(f"      ⚠️ positionKey={position_key} failed: {e}")
        return []

async def collect_via_position_keys(context, year):
    """Method A: discover keys, iterate each, aggregate."""
    page = await context.new_page()
    
    print(f"--- 1. Top players page for {year} ---")
    top_links = []
    try:
        await page.goto(TOP_URL_TEMPLATE.format(year=year), timeout=60000, wait_until="domcontentloaded")
        await asyncio.sleep(2)
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(1)
        for selector in ["li.transfer-player h3 a", ".rankings-page__name-link", "a[href*='/player/']"]:
            try:
                found = await page.eval_on_selector_all(selector, "elements => elements.map(e => e.href)")
                if found:
                    top_links.extend(found)
                    break
            except:
                pass
        top_links = list(dict.fromkeys(normalize_player_url(l) for l in top_links if "247sports.com/player/" in l))
        print(f"   ✅ {year}: Top page captured {len(top_links)} players")
    except Exception as e:
        print(f"   ⚠️ {year}: Top page fetch failed: {e}")
    
    print(f"--- 2. Discover & iterate positionKeys for {year} ---")
    position_keys = await discover_position_keys(context, year)
    
    all_links = list(top_links)
    for pos_key, label in position_keys:
        links = await collect_links_from_position_page(page, year, pos_key, label)
        all_links.extend(links)
        await asyncio.sleep(0.5)
    
    await page.close()
    return list(dict.fromkeys(all_links))

# =============================================================================
# METHOD B: /transferteamrankings/ crawl (used for 2022)
# =============================================================================

async def collect_via_transfer_team_rankings(context, year):
    """Method B: load /transferteamrankings/, find each team's link, visit each
    team's transfer-class page, extract player links."""
    print(f"--- 1. Loading /transferteamrankings/ for {year} ---")
    page = await context.new_page()
    team_class_urls = []
    try:
        await page.goto(TRANSFER_TEAM_RANKINGS_URL.format(year=year),
                        timeout=60000, wait_until="domcontentloaded")
        await asyncio.sleep(3)
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(2)
        
        raw_hrefs = await page.evaluate("""
            () => {
                const out = new Set();
                document.querySelectorAll('a[href]').forEach(a => {
                    const h = a.getAttribute('href') || '';
                    if (h.includes('/college/') && h.includes('/recruiting/') && h.includes('Transfers')) {
                        out.add(h);
                    } else if (h.includes('/college/') && h.includes('-football/') && h.includes('Transfers')) {
                        out.add(h);
                    }
                });
                if (out.size < 30) {
                    document.querySelectorAll('a[href*="/college/"]').forEach(a => {
                        const h = a.getAttribute('href') || '';
                        out.add(h);
                    });
                }
                return [...out];
            }
        """)
        
        for h in raw_hrefs:
            full = absolutize(h)
            if full and '/college/' in full:
                team_class_urls.append(full)
        team_class_urls = list(dict.fromkeys(team_class_urls))
        print(f"   ✅ {year}: Found {len(team_class_urls)} candidate team URLs")
    except Exception as e:
        print(f"   ⚠️ {year}: transferteamrankings fetch failed: {e}")
    
    await page.close()
    
    if not team_class_urls:
        return []
    
    print(f"--- 2. Visiting {len(team_class_urls)} team pages for {year} ---")
    all_player_links = []
    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
    
    async def fetch_team(team_url):
        async with sem:
            tp = await context.new_page()
            try:
                await tp.goto(team_url, timeout=60000, wait_until="domcontentloaded")
                await asyncio.sleep(2)
                await tp.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(1)
                
                links = await tp.eval_on_selector_all(
                    "a[href*='/player/']",
                    "elements => elements.map(e => e.href)"
                )
                normalized = [normalize_player_url(l) for l in links if "247sports.com/player/" in l]
                if normalized:
                    print(f"      📎 {team_url[:70]}... → {len(set(normalized))} players")
                await tp.close()
                return normalized
            except Exception as e:
                try:
                    await tp.close()
                except:
                    pass
                return []
    
    results = await asyncio.gather(*[fetch_team(u) for u in team_class_urls])
    for r in results:
        all_player_links.extend(r)
    
    return list(dict.fromkeys(all_player_links))

# =============================================================================
# METHOD C: per-team /transferportal/ pages (used for 2021)
# 
# 247's /transferteamrankings/ doesn't exist for 2021 (redirects to compositeteamrankings 
# which is HS recruiting team rankings). Workaround: discover team slugs from 
# /compositeteamrankings/, then visit each team's /transferportal/ page directly.
# =============================================================================

async def collect_via_per_team_transferportal(context, year):
    """Method C: get team slugs from /compositeteamrankings/, then visit
    /college/{slug}/season/{year}-football/transferportal/ for each team."""
    print(f"--- 1. Discovering team slugs via /compositeteamrankings/ for {year} ---")
    page = await context.new_page()
    team_slugs = []
    
    page_num = 1
    while True:
        list_url = COMPOSITE_TEAM_RANKINGS_URL.format(year=year)
        if page_num > 1:
            list_url = f"{list_url}?Page={page_num}"
        try:
            await page.goto(list_url, timeout=60000, wait_until="domcontentloaded")
            await asyncio.sleep(3)
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(1)
            
            # Extract just the slug portion of /college/{slug}/season/...
            raw_slugs = await page.evaluate("""
                () => {
                    const out = new Set();
                    document.querySelectorAll('a[href*="/college/"]').forEach(a => {
                        const m = (a.getAttribute('href') || '').match(/\\/college\\/([^/]+)\\/season\\//);
                        if (m && m[1] && m[1] !== 'transfer-portal') {
                            out.add(m[1]);
                        }
                    });
                    return [...out];
                }
            """)
            
            new_slugs_this_page = [s for s in raw_slugs if s not in team_slugs]
            team_slugs.extend(new_slugs_this_page)
            print(f"   📄 Page {page_num}: +{len(new_slugs_this_page)} new team slugs (total {len(team_slugs)})")
            
            if len(new_slugs_this_page) == 0:
                break
            page_num += 1
            if page_num > 20:
                print(f"   ⚠️ {year}: Hit page cap at {page_num}, stopping")
                break
        except Exception as e:
            print(f"   ⚠️ {year}: Page {page_num} fetch failed: {e}")
            break
    
    await page.close()
    print(f"   ✅ {year}: Collected {len(team_slugs)} team slugs across {page_num} pages")
    
    if not team_slugs:
        return []
    
    # For each team, visit the per-team transferportal page directly
    print(f"--- 2. Visiting {len(team_slugs)} per-team /transferportal/ pages ---")
    all_player_links = []
    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
    
    async def fetch_team_transfers(slug):
        async with sem:
            tp = await context.new_page()
            url = PER_TEAM_TRANSFER_URL.format(slug=slug, year=year)
            try:
                await tp.goto(url, timeout=60000, wait_until="domcontentloaded")
                await asyncio.sleep(2)
                # Scroll to load all transfers (per-team list isn't paginated, but lazy-renders)
                for _ in range(3):
                    await tp.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(1)
                
                # Page is filtered to transfers only — every player link IS a transfer
                links = await tp.eval_on_selector_all(
                    "a[href*='/player/']",
                    "elements => elements.map(e => e.href)"
                )
                normalized = [normalize_player_url(l) for l in links if "247sports.com/player/" in l]
                normalized = list(dict.fromkeys(normalized))
                
                if normalized:
                    print(f"      📎 {slug}: {len(normalized)} transfers")
                await tp.close()
                return normalized
            except Exception as e:
                try:
                    await tp.close()
                except:
                    pass
                return []
    
    results = await asyncio.gather(*[fetch_team_transfers(s) for s in team_slugs])
    for r in results:
        all_player_links.extend(r)
    
    return list(dict.fromkeys(all_player_links))

# =============================================================================
# YEAR ROUTING
# =============================================================================

async def collect_links_for_year(context, year):
    """Route each year to the appropriate collection method."""
    if year in POSITIONKEY_YEARS:
        print(f"   🧭 {year}: routing to POSITIONKEY method")
        return await collect_via_position_keys(context, year)
    elif year in TRANSFER_TEAM_RANKINGS_YEARS:
        print(f"   🧭 {year}: routing to TRANSFER_TEAM_RANKINGS method")
        return await collect_via_transfer_team_rankings(context, year)
    elif year in PER_TEAM_TRANSFERPORTAL_YEARS:
        print(f"   🧭 {year}: routing to PER_TEAM_TRANSFERPORTAL method")
        return await collect_via_per_team_transferportal(context, year)
    else:
        print(f"   🧭 {year}: no explicit route, defaulting to POSITIONKEY method")
        return await collect_via_position_keys(context, year)

# =============================================================================
# SCRAPE YEAR
# =============================================================================

async def scrape_year(year, p, ua, diagnostic_tracker):
    print("\n" + "="*80)
    print(f"📅 SCRAPING YEAR: {year}")
    print("="*80)
    
    browser = await p.chromium.launch(headless=True)
    context = await browser.new_context(user_agent=ua.random, viewport={'width': 1920, 'height': 1080})
    
    await context.route("**/*bouncex*", lambda route: route.abort())
    await context.route("**/*bounceexchange*", lambda route: route.abort())
    await context.route("**/*integralas*", lambda route: route.abort())
    await context.route("**/*IL_INSEARCH*", lambda route: route.abort())
    
    unique_links = await collect_links_for_year(context, year)
    
    if TEST_MODE:
        unique_links = unique_links[:TEST_LIMIT]
        print(f"   🧪 {year}: Limited to {len(unique_links)} profiles for test")
    else:
        print(f"   ✅ {year}: {len(unique_links)} unique player profiles to scrape")
    
    if len(unique_links) == 0:
        print(f"   ⚠️ {year}: No links found")
        await browser.close()
        return [], []
    
    print(f"--- Scraping {year} Profiles ---")
    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
    failed_urls = []
    tasks = [scrape_profile(context, link, sem, failed_urls, year, diagnostic_tracker) for link in unique_links]
    results = await asyncio.gather(*tasks)
    valid_results = [r for r in results if r]
    
    print(f"   ✅ {year}: Scraped {len(valid_results)} players ({len(failed_urls)} failed)")
    
    await browser.close()
    return valid_results, failed_urls

# =============================================================================
# DIAGNOSTICS
# =============================================================================

def generate_diagnostic_report(diagnostic_tracker, output_file="diagnostics_report.txt"):
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("="*80 + "\n🔍 DIAGNOSTIC REPORT\n" + "="*80 + "\n\n")
        for year in sorted(diagnostic_tracker['by_year'].keys(), reverse=True):
            year_data = diagnostic_tracker['by_year'][year]
            total = year_data['total']
            f.write(f"\n{'='*80}\n📅 YEAR {year} ({total} players)\n{'='*80}\n\n")
            f.write("FIELD COMPLETENESS:\n" + "-"*80 + "\n")
            for field, counts in sorted(year_data['fields'].items()):
                filled = counts['filled']
                pct = (filled / total * 100) if total > 0 else 0
                status = "✅" if pct >= 95 else ("⚠️" if pct >= 80 else "❌")
                f.write(f"{status} {field:30} {filled:4d}/{total:4d} ({pct:5.1f}%)\n")
            if year_data['problem_samples']:
                f.write(f"\n🔴 PROBLEMATIC PLAYERS (saved {len(year_data['problem_samples'])} samples):\n" + "-"*80 + "\n")
                for sample in year_data['problem_samples']:
                    f.write(f"\nPlayer: {sample['player']} (ID: {sample['id']})\n")
                    f.write(f"URL: {sample['url']}\n")
                    f.write(f"HTML File: {sample['html_file']}\n")
                    f.write(f"Missing Fields: {', '.join(sample['missing_fields'][:5])}\n")
        f.write("\n" + "="*80 + "\n📊 SUMMARY\n" + "="*80 + "\n")
        for year in sorted(diagnostic_tracker['by_year'].keys(), reverse=True):
            year_data = diagnostic_tracker['by_year'][year]
            total = year_data['total']
            problem_count = len(year_data['problem_samples'])
            status = "✅" if problem_count == 0 else ("⚠️" if problem_count < 5 else "❌")
            f.write(f"{status} {year}: {total} players scraped, {problem_count} problem samples saved\n")
    print(f"\n📋 Diagnostic report saved to: {output_file}")

# =============================================================================
# MAIN
# =============================================================================

async def main():
    ua = UserAgent()
    print("="*80)
    if TEST_MODE:
        print(f"🧪 TEST MODE - Scraping {TEST_LIMIT} players per year")
    else:
        print(f"🚀 FULL MODE - Year-aware multi-method scraping")
    print(f"📅 Years to scrape: {', '.join(map(str, YEARS))}")
    print(f"📅 Year input: {YEAR_RANGE}")
    if DIAGNOSTICS_MODE:
        print(f"🔍 DIAGNOSTICS MODE: Enabled")
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
    
    before = len(df)
    df = df.drop_duplicates()
    df = df.drop_duplicates(subset=['247 ID', 'Transfer Year'], keep='first')
    if before != len(df):
        print(f"   🧹 Removed {before - len(df)} duplicate rows ({before} → {len(df)})")
    
    df.to_csv(OUTPUT_FILE, index=False)
    
    print("\n" + "="*80)
    print(f"{'🧪 TEST COMPLETE' if TEST_MODE else '✅ SUCCESS'}")
    print(f"📊 Total players scraped: {len(df)}")
    print(f"📁 Saved to: {OUTPUT_FILE}")
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

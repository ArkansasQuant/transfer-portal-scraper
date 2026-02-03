import asyncio
import random
import pandas as pd
import re
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import sys

# --- CONFIGURATION ---
SAMPLE_SIZE = 20  # Number of players to validate
BROWSER_HEADLESS = True

# --- PARSING LOGIC (Same as scraper) ---
def clean_text(text):
    if not text: return None
    return text.strip()

def parse_profile(html, url, player_id):
    """Same parsing logic as main scraper"""
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
    team_header = soup.select_one('.team-info-section header h2')
    if team_header:
        data['Team'] = team_header.text.strip()
    
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

    return data

async def scrape_profile_for_validation(page, url, player_id):
    """Scrape a single profile for validation"""
    try:
        await page.goto(url, timeout=30000, wait_until="commit")
        await page.wait_for_selector(".name, h1.name", timeout=10000)
        content = await page.content()
        return parse_profile(content, url, player_id)
    except Exception as e:
        print(f"   ❌ Failed to scrape {url}: {e}")
        return None

def compare_values(csv_val, actual_val, field_name):
    """Compare two values and return match status"""
    # Normalize values
    csv_str = str(csv_val).strip() if pd.notna(csv_val) else "NA"
    actual_str = str(actual_val).strip() if actual_val else "NA"
    
    # Handle height formatting (remove extra quotes)
    if field_name == "Height":
        csv_str = csv_str.replace("'", "").strip()
        actual_str = actual_str.replace("'", "").strip()
    
    match = csv_str == actual_str
    return match, csv_str, actual_str

async def validate_accuracy(csv_file, sample_size=20):
    """Main validation function"""
    
    print("="*80)
    print(f"🔍 ACCURACY VALIDATION")
    print("="*80)
    print(f"📁 CSV File: {csv_file}")
    print(f"📊 Sample Size: {sample_size} players")
    print("="*80)
    
    # Load CSV
    try:
        df = pd.read_csv(csv_file)
    except FileNotFoundError:
        print(f"❌ File not found: {csv_file}")
        return
    
    # Sample random players
    if len(df) < sample_size:
        sample_size = len(df)
        print(f"⚠️ CSV has only {len(df)} rows, sampling all")
    
    sample = df.sample(n=sample_size, random_state=42)
    
    # Fields to validate
    fields_to_check = [
        'Player Name', 'Position', 'Height', 'Weight', 'Team',
        'Transfer Stars', 'Transfer Rating', 'Transfer Overall Rank', 
        'Transfer Position Rank', 'Transfer Position', 'Transfer Team Name',
        'Prospect Stars', 'Prospect Rating', 'Prospect Position Rank',
        'Prospect Position', 'Prospect National Rank'
    ]
    
    # Scrape fresh data
    print("\n🌐 Re-scraping sample profiles...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=BROWSER_HEADLESS)
        page = await browser.new_page()
        
        results = []
        total_fields = 0
        matching_fields = 0
        mismatches = []
        
        for idx, row in sample.iterrows():
            url = row['URL']
            player_id = str(row['247 ID'])
            player_name = row['Player Name']
            
            print(f"   Scraping: {player_name}...", end=" ")
            
            actual_data = await scrape_profile_for_validation(page, url, player_id)
            
            if not actual_data:
                print("FAILED")
                continue
            
            print("✅")
            
            # Compare all fields
            player_mismatches = []
            player_total = 0
            player_matches = 0
            
            for field in fields_to_check:
                if field not in df.columns:
                    continue
                
                csv_val = row[field]
                actual_val = actual_data.get(field, "NA")
                
                match, csv_str, actual_str = compare_values(csv_val, actual_val, field)
                
                player_total += 1
                total_fields += 1
                
                if match:
                    player_matches += 1
                    matching_fields += 1
                else:
                    player_mismatches.append({
                        'field': field,
                        'csv': csv_str,
                        'actual': actual_str
                    })
            
            if player_mismatches:
                mismatches.append({
                    'player': player_name,
                    'id': player_id,
                    'url': url,
                    'mismatches': player_mismatches,
                    'accuracy': (player_matches / player_total * 100) if player_total > 0 else 0
                })
        
        await browser.close()
    
    # Report Results
    print("\n" + "="*80)
    print("📊 ACCURACY RESULTS")
    print("="*80)
    
    overall_accuracy = (matching_fields / total_fields * 100) if total_fields > 0 else 0
    
    print(f"\n✅ Overall Accuracy: {overall_accuracy:.1f}% ({matching_fields}/{total_fields} fields match)")
    print(f"📈 Players Checked: {len(sample)}")
    print(f"❌ Players with Mismatches: {len(mismatches)}")
    
    if mismatches:
        print("\n" + "="*80)
        print("⚠️ DETAILED MISMATCHES")
        print("="*80)
        
        for item in mismatches[:10]:  # Show first 10
            print(f"\n🔴 {item['player']} (ID: {item['id']})")
            print(f"   Accuracy: {item['accuracy']:.1f}%")
            print(f"   URL: {item['url']}")
            
            for mm in item['mismatches']:
                print(f"   ❌ {mm['field']:30} CSV: '{mm['csv']}' ≠ Actual: '{mm['actual']}'")
        
        if len(mismatches) > 10:
            print(f"\n   ... and {len(mismatches) - 10} more players with mismatches")
    
    # Final Grade
    print("\n" + "="*80)
    print("🎯 FINAL GRADE")
    print("="*80)
    
    if overall_accuracy >= 98:
        grade = "✅ EXCELLENT"
        comment = "Scraper is working perfectly!"
    elif overall_accuracy >= 95:
        grade = "✅ VERY GOOD"
        comment = "Minor issues, but scraper is reliable."
    elif overall_accuracy >= 90:
        grade = "⚠️ GOOD"
        comment = "Some fields need attention."
    elif overall_accuracy >= 80:
        grade = "⚠️ FAIR"
        comment = "Multiple fields have issues. Review scraper logic."
    else:
        grade = "❌ POOR"
        comment = "Scraper has serious accuracy problems!"
    
    print(f"{grade} - {overall_accuracy:.1f}% Accuracy")
    print(f"💬 {comment}")
    print("="*80)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
    else:
        # Auto-detect
        import glob
        test_files = glob.glob("TEST_*.csv")
        if test_files:
            csv_file = test_files[0]
            print(f"📁 Auto-detected: {csv_file}\n")
        else:
            print("Usage: python validate_accuracy.py <csv_file> [sample_size]")
            print("   or: Place in same directory as TEST_*.csv file")
            sys.exit(1)
    
    if len(sys.argv) > 2:
        SAMPLE_SIZE = int(sys.argv[2])
    
    asyncio.run(validate_accuracy(csv_file, SAMPLE_SIZE))

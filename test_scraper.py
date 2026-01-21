import asyncio
import re
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

# TEST URL: Sam Leavitt (Known to have both Transfer and Prospect data)
TEST_URL = "https://247sports.com/Player/Sam-Leavitt-46114138/"

def clean_text(text):
    if not text: return "NA"
    return text.strip().replace('\n', '').replace('\t', '')

def parse_profile_debug(html):
    soup = BeautifulSoup(html, 'lxml')
    data = {}
    print(f"--- DEBUGGING PROFILE ---")

    # 1. SCAN ALL LISTS ON THE PAGE
    # We look for ALL <ul> elements, as 247 puts data in lists.
    all_uls = soup.find_all('ul')
    print(f"Found {len(all_uls)} total 'ul' lists on page.")
    
    transfer_ul = None
    prospect_ul = None

    # 2. IDENTIFY SECTIONS BY CONTENT
    for i, ul in enumerate(all_uls):
        text_content = ul.get_text(" ", strip=True)
        
        # DEBUG: Print snippet of first few lists
        if i < 5: 
            print(f"   List #{i} content snippet: {text_content[:50]}...")

        # LOGIC: If "Transfer Rating" is inside the list, it IS the transfer list.
        if "Transfer Rating" in text_content:
            print(f"   >>> MATCH: List #{i} identified as TRANSFER SECTION")
            transfer_ul = ul
        
        # LOGIC: If "Natl" or "National" is in list AND it's not the transfer list
        elif ("Natl" in text_content or "National" in text_content) and "Transfer" not in text_content:
            print(f"   >>> MATCH: List #{i} identified as PROSPECT SECTION")
            prospect_ul = ul
            
        # Fallback for Prospect: If we haven't found prospect yet, look for 'Pos' without 'Transfer'
        elif "Pos" in text_content and "Transfer" not in text_content and prospect_ul is None:
             # Check if it has stars nearby to confirm it's a ranking list
             if ul.find_previous(class_='icon-starsolid'):
                 print(f"   >>> MATCH: List #{i} identified as PROSPECT SECTION (Fallback)")
                 prospect_ul = ul

    # 3. PARSE TRANSFER
    print("\n--- PARSING TRANSFER ---")
    if transfer_ul:
        # Get Container for Stars (Go up to parent div/section)
        t_container = transfer_ul.find_parent('section') or transfer_ul.find_parent('div')
        
        # A. Stars
        stars = t_container.select('.icon-starsolid.yellow')
        print(f"   Stars Found: {len(stars)}")
        
        # B. Rating
        # Look for the 'score' or 'rating' class specifically inside this container
        rating = t_container.select_one('.rating') or t_container.select_one('.score')
        print(f"   Rating Found: {rating.get_text(strip=True) if rating else 'None'}")
        
        # C. Year (Look for (202X) in the container text)
        year_match = re.search(r'\((\d{4})\)', t_container.get_text())
        print(f"   Year Found: {year_match.group(1) if year_match else 'None'}")
        
        # D. Ranks
        for li in transfer_ul.find_all('li'):
            label = li.select_one('h5, .rank-label')
            value = li.select_one('strong, .rank-value')
            if label and value:
                print(f"   Rank Item: {label.get_text(strip=True)} -> {value.get_text(strip=True)}")
    else:
        print("   [!] NO TRANSFER LIST FOUND")

    # 4. PARSE PROSPECT
    print("\n--- PARSING PROSPECT ---")
    if prospect_ul:
         # Get Container
        p_container = prospect_ul.find_parent('section') or prospect_ul.find_parent('div')
        
        # A. Stars
        stars = p_container.select('.icon-starsolid.yellow')
        is_juco = "JUCO" in p_container.get_text().upper()
        print(f"   Stars Found: {len(stars)} {'(JUCO)' if is_juco else ''}")
        
        # B. Ranks
        for li in prospect_ul.find_all('li'):
            label = li.select_one('h5, .rank-label')
            value = li.select_one('strong, .rank-value')
            if label and value:
                print(f"   Rank Item: {label.get_text(strip=True)} -> {value.get_text(strip=True)}")
    else:
        print("   [!] NO PROSPECT LIST FOUND")

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        print(f"Navigating to {TEST_URL}...")
        await page.goto(TEST_URL, wait_until="commit")
        
        # Wait for profile to load
        try:
            await page.wait_for_selector("ul", timeout=10000)
        except:
            print("Timed out waiting for lists.")
            
        content = await page.content()
        parse_profile_debug(content)
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())

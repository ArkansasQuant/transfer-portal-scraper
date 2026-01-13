import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

# The 4 specific profiles with unique nuances
TEST_URLS = [
    "https://247sports.com/player/sam-leavitt-46108915/college-312305/",  # 1. Standard
    "https://247sports.com/player/drew-mestemaker-46154239/college-326311/", # 2. 0 Stars / NA
    "https://247sports.com/player/cam-coleman-46115877/college-307129/",  # 3. Has National Rank
    "https://247sports.com/player/mike-sandjo-46133331/"                   # 4. JUCO Nuance
]

async def analyze_profile(page, url):
    print(f"\n{'='*60}")
    print(f"DIAGNOSING: {url}")
    
    try:
        await page.goto(url, timeout=60000, wait_until="commit")
        # Smart wait for name to ensure content loaded
        try: await page.wait_for_selector(".name", timeout=15000)
        except: pass
        
        content = await page.content()
        soup = BeautifulSoup(content, 'lxml')
        
        # 1. CHECK HEADER (City, ST, EXP)
        print("\n--- A. HEADER DETAILS ---")
        details = soup.select('.details li')
        if details:
            for li in details:
                # Print the raw text to see exactly what "City" looks like
                print(f"   [LI]: {li.get_text(strip=True)}")
        else:
            print("   [WARNING] No .details li found. Dumping all UL text:")
            for ul in soup.select('ul'):
                if 'High School' in ul.get_text():
                    print(f"   [UL]: {ul.get_text(strip=True)[:100]}...")

        # 2. CHECK TEAM
        print("\n--- B. TEAM INFO ---")
        team_block = soup.select_one('.ni-school-name')
        if team_block:
            print(f"   [MAIN TEAM BLOCK]: {team_block.get_text(strip=True)}")
        else:
            print("   [MISSING] No .ni-school-name found.")
            
        # 3. CHECK TRANSFER SECTION
        print("\n--- C. TRANSFER SECTION ---")
        # Find section by text since class might vary
        t_header = soup.find(lambda tag: tag.name in ['h3', 'h4', 'h5'] and 'Transfer' in tag.get_text())
        if t_header:
            section = t_header.find_parent('section')
            if section:
                print(f"   [FOUND SECTION]: {t_header.get_text(strip=True)}")
                stars = section.select('.icon-starsolid.yellow')
                print(f"   [STARS COUNT]: {len(stars)}")
                
                # Print all rank labels
                for li in section.select('li'):
                    print(f"   [RANK ITEM]: {li.get_text(strip=True)}")
            else:
                print("   [ERROR] Found header but no parent section.")
        else:
            print("   [MISSING] No header containing 'Transfer' found.")

        # 4. CHECK PROSPECT / JUCO SECTION
        print("\n--- D. PROSPECT/JUCO SECTION ---")
        # Look for JUCO or High School headers
        p_header = soup.find(lambda tag: tag.name in ['h3', 'h4', 'h5'] and ('High School' in tag.get_text() or 'JUCO' in tag.get_text()))
        if p_header:
            section = p_header.find_parent('section')
            if section:
                print(f"   [FOUND SECTION]: {p_header.get_text(strip=True)}")
                stars = section.select('.icon-starsolid.yellow')
                print(f"   [STARS COUNT]: {len(stars)}")
                
                # Print all rank labels to see if 'Natl' exists
                for li in section.select('li'):
                    print(f"   [RANK ITEM]: {li.get_text(strip=True)}")
        else:
            print("   [MISSING] No 'High School' or 'JUCO' header found.")

    except Exception as e:
        print(f"   [CRASH] Could not load page: {e}")

async def main():
    ua = UserAgent()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=ua.random)
        page = await context.new_page()
        
        for url in TEST_URLS:
            await analyze_profile(page, url)
            
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())

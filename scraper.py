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
MAX_RETRIES = 3
OUTPUT_FILE = f"transfer_portal_2026_{datetime.now().strftime('%Y%m%d')}.csv"

# --- UTILS ---
def clean_text(text):
    if not text: return None
    return text.strip()

def normalize_rank(rank):
    # Removes non-numeric chars except for "NA" or "JUCO"
    if not rank or rank in ['-', '', 'N/A', None]: return 'NA'
    # Remove dots, hashtags, "No."
    clean = re.sub(r'[^\d]', '', rank)
    return clean if clean else 'NA'

def extract_id_from_url(url):
    match = re.search(r'-(\d+)(?:/|$)', url)
    return match.group(1) if match else "NA"

async def exponential_backoff(attempt):
    wait_time = (2 ** attempt) + random.uniform(0.5, 1.5)
    await asyncio.sleep(wait_time)

# --- PARSING LOGIC (Precision Mode) ---
def parse_profile(html, url, player_id):
    soup = BeautifulSoup(html, 'lxml')
    data = {}
    
    # --- HEADER SECTION ---
    data['247 ID'] = player_id
    
    name_tag = soup.select_one('.name') or soup.select_one('h1.name')
    data['Player Name'] = clean_text(name_tag.text) if name_tag else "NA"
    
    # Metrics
    data['Position'] = "NA"
    data['Height'] = "NA"
    data['Weight'] = "NA"
    
    metrics = soup.select('.metrics-list li')
    for m in metrics:
        text = m.text.strip()
        if 'Pos' in text: 
            data['Position'] = text.split(':')[-1].strip()
        elif 'Height' in text: 
            raw_ht = text.split(':')[-1].strip()
            data['Height'] = f"'{raw_ht}" # Excel fix
        elif 'Weight' in text: 
            data['Weight'] = text.split(':')[-1].strip()

    # Details (Fixes "Calculator" error)
    data['High School'] = "NA"
    data['City, ST'] = "NA"
    data['EXP'] = "NA"
    
    details = soup.select('.details li')
    for d in details:
        label = d.select_one('span')
        if label:
            label_text = label.text.strip()
            val_text = d.get_text().replace(label_text, "").strip()
            
            if 'High School' in label_text: data['High School'] = val_text
            elif 'Home Town' in label_text: data['City, ST'] = val_text
            elif 'Class' in label_text: data['EXP'] = val_text

    # Team
    data['Team'] = "NA"
    team_block = soup.select_one('.ni-school-name a')
    if team_block:
        data['Team'] = team_block.text.strip()
    else:
        pred_team = soup.select_one('.transfer-prediction .team-name')
        if pred_team: data['Team'] = pred_team.text.strip()

    # --- SECTION 2: TRANSFER RANKINGS ---
    data['Transfer Stars'] = "0"
    data['Transfer Rating'] = "NA"
    data['Transfer Year'] = "2026"
    data['Transfer Overall Rank'] = "NA"
    data['Transfer Position Rank'] = "NA"
    data['Transfer Team Name'] = data['Team']

    # Strict Scope
    t_sect = soup.select_one('.transfer-rankings')
    if t_sect:
        stars = t_sect.select('.icon-starsolid.yellow')
        data['Transfer Stars'] = len(stars)
        
        rating = t_sect.select_one('.rating')
        if rating: data['Transfer Rating'] = rating.text

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
        # FIX: Find the immediate next LIST (ul) relative to the header
        # This prevents grabbing the Prospect list below it
        t_list = transfer_node.find_next('ul')
        
        # FIX: Find the immediate next RATING relative to the header
        rating_tag = transfer_node.find_next(class_='rating')
        if rating_tag: 
            data['Transfer Rating'] = clean_text(rating_tag.text)

        # FIX: Find Stars (Find the first star block relative to the header)
        first_star = transfer_node.find_next(class_='icon-starsolid')
        if first_star:
            # Count only the stars in this specific container
            data['Transfer Stars'] = len(first_star.parent.select('.icon-starsolid.yellow'))
            
        # Parse Ranks (Only from t_list)
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
                        # Negative Logic: If not OVR/NATL, it is Position Rank
                        data['Transfer Position Rank'] = val

    # --- PARSE PROSPECT ("As a Prospect") ---
    data['Prospect Stars'] = "0"
    data['Prospect Rating'] = "NA"
    data['Prospect Position Rank'] = "NA"
    data['Prospect National Rank'] = "NA"
    
    if prospect_node:
        # FIX: Find the immediate next LIST (ul) relative to this header
        p_list = prospect_node.find_next('ul')
        
        # Check JUCO status (look at text near the header)
        p_container = prospect_node.find_parent('section') or prospect_node.find_parent('div')
        is_juco = False
        if p_container and "JUCO" in p_container.get_text().upper():
            is_juco = True
        
        # FIX: Find Rating relative to this header
        rating_tag = prospect_node.find_next(class_='rating')
        if rating_tag:
            val = clean_text(rating_tag.text)
            data['Prospect Rating'] = f"{val} JUCO" if (is_juco and val != "NA") else val
            
        # FIX: Find Stars relative to this header
        first_star = prospect_node.find_next(class_='icon-starsolid')
        if first_star:
            count = len(first_star.parent.select('.icon-starsolid.yellow'))
            if is_juco and count == 0:
                 data['Prospect Stars'] = "0 JUCO"
            else:
                 data['Prospect Stars'] = f"{count} JUCO" if is_juco else count

        # Parse Ranks (Only from p_list)
        if p_list:
            for li in p_list.select('li'):
                label_tag = li.select_one('h5')
                val_tag = li.select_one('strong')
                if label_tag and val_tag:
                    label = label_tag.get_text(strip=True).upper()
                    val = clean_text(val_tag.get_text(strip=True))
                    
                    if 'NATL' in label or 'NATIONAL' in label:
                        data['Prospect National Rank'] = f"{val} JUCO" if is_juco else val
                    # Filter out States
                    elif label not in ['OVR', 'ST', 'STATE', 'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']:
                        data['Prospect Position Rank'] = f"{val} JUCO" if is_juco else val

    return data

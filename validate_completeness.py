"""
validate_completeness.py
========================
Quick validation script to check your CSV against known high-profile transfers.
Run this against your output CSV to identify missing players.

Usage:
    python validate_completeness.py transfer_portal_2024-2026_XXXXXXXX.csv
"""

import pandas as pd
import sys
import glob

# Known high-profile transfers by year — expand these lists as needed
# Format: ("Player Name", "Expected Origin Team or None")
KNOWN_TRANSFERS = {
    2024: [
        ("Cam Ward", "Washington State"),
        ("Will Howard", "Kansas State"),
        ("Treshaun Ward", "Kansas State"),
        ("Dylan Edwards", "Colorado"),
        ("Grayson Howard", "South Carolina"),
        ("Howard Sampson", "North Texas"),
    ],
    2025: [
        ("Carson Beck", "Georgia"),
        ("Nico Iamaleava", "Tennessee"),
        ("Walker Howard", "Ole Miss"),
        ("Billy Edwards Jr.", "Maryland"),
        ("Jayden Ballard", "Ohio State"),
        ("Madden Iamaleava", "Arkansas"),
    ],
    2026: [
        ("Dylan Raiola", "Nebraska"),
        ("Dylan Edwards", "Kansas State"),
        ("Malachi Henry", "Arkansas"),
        ("Fabian Duncan", "Allen University"),
        ("JJ Shelton", "Arkansas"),
        ("Jaylen Raynor", "Arkansas"),
    ],
    2023: [
        # Add known 2023 portal entries
    ],
    2022: [
        # Add known 2022 portal entries
    ],
    2021: [
        # Add known 2021 portal entries
    ],
}


def validate_csv(csv_path):
    df = pd.read_csv(csv_path)
    
    print("=" * 70)
    print(f"COMPLETENESS VALIDATION: {csv_path}")
    print(f"Total rows: {len(df)} | Unique players: {df['247 ID'].nunique()}")
    print("=" * 70)

    years_in_data = sorted(df['Transfer Year'].unique(), reverse=True)

    for year in years_in_data:
        players = KNOWN_TRANSFERS.get(year, [])
        year_df = df[df['Transfer Year'] == year]
        
        print(f"\n📅 {year} ({len(year_df)} players in CSV)")
        print("-" * 50)

        if not players:
            print("  (No known players configured for spot-check)")
            continue

        missing = []
        found = []

        for name, team in players:
            # Search by last name first, then narrow by first name
            last_name = name.split()[-1]
            first_name = name.split()[0]
            
            match = df[df['Player Name'].str.contains(last_name, case=False, na=False)]
            if len(match) > 0:
                match = match[match['Player Name'].str.contains(first_name, case=False, na=False)]
            
            if len(match) > 0:
                row = match.iloc[0]
                found.append(name)
                print(f"  ✅ {name:25s} → Found (Team: {row['Team']}, Year: {row['Transfer Year']})")
            else:
                missing.append(name)
                print(f"  ❌ {name:25s} → MISSING")

        if missing:
            print(f"\n  ⚠️  {len(missing)}/{len(players)} known players MISSING from {year}")
        else:
            print(f"\n  ✅ All {len(players)} checked players found in {year}")

    # Overall stats
    print("\n" + "=" * 70)
    print("YEAR-BY-YEAR SUMMARY")
    print("=" * 70)
    for year in years_in_data:
        count = len(df[df['Transfer Year'] == year])
        unique = df[df['Transfer Year'] == year]['247 ID'].nunique()
        dupes = count - unique
        dupe_note = f" ({dupes} duplicates)" if dupes > 0 else ""
        print(f"  {year}: {count} rows, {unique} unique players{dupe_note}")
    
    # Ranking gap analysis
    print("\n" + "=" * 70)
    print("RANKING GAP ANALYSIS")
    print("=" * 70)
    for year in years_in_data:
        yr = df[df['Transfer Year'] == year]
        ranks = pd.to_numeric(yr['Transfer Overall Rank'], errors='coerce').dropna()
        if len(ranks) > 0:
            max_rank = int(ranks.max())
            all_ranks = set(ranks.astype(int))
            expected = set(range(1, max_rank + 1))
            gaps = expected - all_ranks
            gap_pct = len(gaps) / max_rank * 100 if max_rank > 0 else 0
            
            status = "✅" if gap_pct < 5 else ("⚠️" if gap_pct < 20 else "❌")
            print(f"  {status} {year}: ranks 1-{max_rank}, {len(all_ranks)} captured, {len(gaps)} gaps ({gap_pct:.1f}%)")
        else:
            print(f"  ⚠️ {year}: No ranking data available")


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        csv_path = sys.argv[1]
    else:
        # Auto-find the most recent CSV
        csvs = glob.glob("transfer_portal_*.csv") + glob.glob("TEST_transfer_portal_*.csv")
        if csvs:
            csv_path = sorted(csvs)[-1]
            print(f"Auto-detected: {csv_path}\n")
        else:
            print("Usage: python validate_completeness.py <csv_file>")
            print("No CSV files found in current directory.")
            sys.exit(1)
    
    validate_csv(csv_path)

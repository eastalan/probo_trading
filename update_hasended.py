import os
import csv
import re
import sys

def update_hasended_for_match(fotmob_url):
    matches_file = os.path.join("data", "event_data", "fotmob_matches.psv")
    if not os.path.exists(matches_file):
        print(f"Matches file not found: {matches_file}")
        return

    # Extract match id from URL
    match_id = None
    match = re.search(r'#(\d+)$', fotmob_url)
    if match:
        match_id = match.group(1)
    else:
        match = re.search(r'/(\d+)(?:/[^/#]*)?(?:#\d*)?$', fotmob_url)
        if match:
            match_id = match.group(1)
    if not match_id:
        print("Could not extract match ID from URL.")
        return

    # Read, update, and write back
    with open(matches_file, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f, delimiter="|"))
        fieldnames = rows[0].keys() if rows else []
    updated = False
    for row in rows:
        if row.get("MatchID") == match_id:
            row["HasEnded"] = "1"
            updated = True
    if updated:
        with open(matches_file, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="|")
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        print(f"Updated HasEnded=1 for MatchID {match_id} in fotmob_matches.psv")
    else:
        print(f"No matching MatchID {match_id} found in fotmob_matches.psv")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 update_hasended.py <fotmob_match_url>")
    else:
        update_hasended_for_match(sys.argv[1])
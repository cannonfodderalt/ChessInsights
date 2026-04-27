import csv
import sys
import argparse
import re
from typing import Dict

def parse_delta_pgn_raw(input_pgn: str, output_csv: str, batch_size: int = 100000) -> None:
    headers = [
        "Event", "Site", "Date", "Round", 
        "White", "Black", "Result", 
        "WhiteElo", "BlackElo", "WhiteTitle", "BlackTitle", 
        "ECO", "Source", "ImportDate", "TimeControl", "Moves"
    ]

    print(f"Opening {input_pgn} for raw text extraction...")
    
    # Pre-compile the regex for maximum C-level speed. 
    # This looks for lines like: [TagName "TagValue"]
    tag_pattern = re.compile(r'^\[([A-Za-z0-9_]+)\s+"(.*)"\]$')

    try:
        with open(input_pgn, "r", encoding="utf-8") as pgn_file, \
             open(output_csv, "w", newline="", encoding="utf-8") as csv_file:
            
            writer = csv.DictWriter(csv_file, fieldnames=headers)
            writer.writeheader()
            
            games_processed = 0
            
            # Initialize our empty game container
            current_game = {h: "" for h in headers}
            moves_buffer = []
            
            for line in pgn_file:
                line = line.strip()
                
                # Skip totally blank lines
                if not line:
                    continue
                
                # If we hit a new [Event...] tag, we know we are starting a brand new game
                if line.startswith("[Event "):
                    # If we have data in the buffer, write the PREVIOUS game to the CSV
                    if current_game.get("Event") != "" or moves_buffer:
                        current_game["Moves"] = " ".join(moves_buffer)
                        writer.writerow(current_game)
                        games_processed += 1
                        
                        if games_processed % batch_size == 0:
                            print(f"Processed {games_processed:,} games...")
                            
                        # Reset for the new game
                        current_game = {h: "" for h in headers}
                        moves_buffer = []

                # Check if the line is a Tag
                match = tag_pattern.match(line)
                if match:
                    tag_name, tag_value = match.groups()
                    # Only save it if it's one of the headers you actually want for SQL
                    if tag_name in current_game:
                        current_game[tag_name] = tag_value
                else:
                    # If it's not a tag and not blank, it must be the chess moves!
                    moves_buffer.append(line)

            # File ended. Make sure to write the very last game to the CSV!
            if current_game.get("Event") != "" or moves_buffer:
                current_game["Moves"] = " ".join(moves_buffer)
                writer.writerow(current_game)
                games_processed += 1

        print(f"\nSuccess! {games_processed:,} total games extracted at warp speed to {output_csv}")

    except FileNotFoundError:
        print(f"Error: Could not find the file '{input_pgn}'.")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lightning fast raw text PGN to CSV extractor.")
    parser.add_argument("input", help="Path to the source .pgn file")
    parser.add_argument("output", help="Path to the destination .csv file")
    args = parser.parse_args()
    
    parse_delta_pgn_raw(args.input, args.output)
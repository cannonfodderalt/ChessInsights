import azure.functions as func
import logging
import os
import io
import csv
import re
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="ProcessPGN")
def ProcessPGN(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function triggered by Azure Data Factory.')

    # 1. Get the filename from the URL query string
    file_name = req.params.get('filename')
    if not file_name:
        return func.HttpResponse("Please pass a filename in the query string", status_code=400)

    try:
        # 2. Connect to Azure Blob Storage
        connection_string = os.environ["AzureWebJobsStorage"]
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # 3. Read the raw PGN from the uploads container into memory
        logging.info(f"Downloading {file_name} from raw-pgn-uploads...")
        raw_container_client = blob_service_client.get_container_client("raw-pgn-uploads")
        blob_client = raw_container_client.get_blob_client(file_name)
        
        pgn_content = blob_client.download_blob().readall().decode('utf-8')
        pgn_stream = io.StringIO(pgn_content)

        # 4. Set up the output CSV stream in memory
        csv_stream = io.StringIO()
        
        headers = [
            "Event", "Site", "Date", "Round", 
            "White", "Black", "Result", 
            "WhiteElo", "BlackElo", "WhiteTitle", "BlackTitle", 
            "ECO", "Source", "ImportDate", "TimeControl", "Moves"
        ]
        
        writer = csv.DictWriter(csv_stream, fieldnames=headers)
        writer.writeheader()

        # 5. YOUR PARSING LOGIC (Adapted for in-memory streams)
        logging.info("Starting C-level regex extraction...")
        tag_pattern = re.compile(r'^\[([A-Za-z0-9_]+)\s+"(.*)"\]$')
        
        games_processed = 0
        current_game = {h: "" for h in headers}
        moves_buffer = []

        for line in pgn_stream:
            line = line.strip()
            
            if not line:
                continue
                
            if line.startswith("[Event "):
                if current_game.get("Event") != "" or moves_buffer:
                    current_game["Moves"] = " ".join(moves_buffer)
                    writer.writerow(current_game)
                    games_processed += 1
                    
                    if games_processed % 10000 == 0:
                        logging.info(f"Processed {games_processed:,} games...")
                        
                current_game = {h: "" for h in headers}
                moves_buffer = []

            match = tag_pattern.match(line)
            if match:
                tag_name, tag_value = match.groups()
                if tag_name in current_game:
                    current_game[tag_name] = tag_value
            else:
                moves_buffer.append(line)

        # Write the final game
        if current_game.get("Event") != "" or moves_buffer:
            current_game["Moves"] = " ".join(moves_buffer)
            writer.writerow(current_game)
            games_processed += 1

        logging.info(f"Success! {games_processed:,} total games extracted.")

        # 6. Upload the populated CSV buffer to the processed container
        processed_container_client = blob_service_client.get_container_client("processed-csv")
        csv_filename = file_name.replace(".pgn", ".csv")
        processed_blob_client = processed_container_client.get_blob_client(csv_filename)
        
        # Get the string value from the buffer and encode it back to bytes for Azure
        logging.info(f"Uploading clean data to {csv_filename}...")
        processed_blob_client.upload_blob(csv_stream.getvalue().encode('utf-8'), overwrite=True)

        return func.HttpResponse(f"Successfully processed {games_processed} games into {csv_filename}", status_code=200)

    except Exception as e:
        logging.error(f"Failed to process: {str(e)}")
        return func.HttpResponse(f"Server Error: {str(e)}", status_code=500)
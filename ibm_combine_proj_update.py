import pandas as pd
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time
import gc  # Garbage collector to free memory
import os

# Load the Excel file
file_path = '/Users/svavasseur/Python/all_players_with_positions_abbreviated.xlsx'
players_df = pd.read_excel(file_path)

# Define the column order
columns = ['Name', 'Player ID', 'Position', 'NFL Team', 'Score Projection',
           'Low Score', 'High Score', 'Bust', 'Breakout']

# Function to fetch JSON data from the URL (for both projections and classifiers)
def fetch_projections(player_id, projection_type):
    time.sleep(0.1)  # Adding a 100ms delay between requests
    if projection_type == 'score':
        url = f"https://watsonfantasyfootball.espn.com/espnpartner/dallas/projections/projections_{player_id}_ESPNFantasyFootball_2025.json"
    elif projection_type == 'classifiers':
        url = f"https://watsonfantasyfootball.espn.com/espnpartner/dallas/classifiers/classifiers_{player_id}_ESPNFantasyFootball_2025.json"
    
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return player_id, response.json()
        else:
            return player_id, None
    except Exception as e:
        print(f"Error fetching data for Player ID {player_id}: {e}")
        return player_id, None

# Function to extract the most recent score projection
def get_most_recent_projection(json_data, max_age_days=3):
    if not json_data:
        return None, None, None

    most_recent_timestamp = None
    most_recent_projection = None
    now = datetime.now()

    for projection in json_data:
        timestamp_str = projection.get('EXECUTION_TIMESTAMP', None)
        if not timestamp_str:
            continue
        try:
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            # Try parsing without microseconds
            try:
                timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                print(f"Failed to parse EXECUTION_TIMESTAMP: {timestamp_str}")
                continue

        # Check if the projection is within the acceptable age
        if (now - timestamp).days > max_age_days:
            continue

        if not most_recent_timestamp or timestamp > most_recent_timestamp:
            most_recent_timestamp = timestamp
            most_recent_projection = projection

    if most_recent_projection:
        simulation_projection = most_recent_projection.get('SCORE_PROJECTION', None)
        low_score = most_recent_projection.get('LOW_SCORE', None)
        high_score = most_recent_projection.get('HIGH_SCORE', None)
        return simulation_projection, low_score, high_score

    return None, None, None

# Function to extract the most recent bust and breakout models
def extract_most_recent_models(json_data, max_age_days=1):
    if not json_data:
        return None, None

    most_recent_bust_model = None
    most_recent_breakout_model = None
    most_recent_bust_timestamp = None
    most_recent_breakout_timestamp = None
    now = datetime.now()

    for model in json_data:
        model_type = model.get('MODEL_TYPE', '')
        timestamp_str = model.get('EXECUTION_TIMESTAMP', None)
        if not timestamp_str:
            continue
        try:
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            # Try parsing without microseconds
            try:
                timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                print(f"Failed to parse EXECUTION_TIMESTAMP: {timestamp_str}")
                continue

        # Check if the model is within the acceptable age
        if (now - timestamp).days > max_age_days:
            continue

        if model_type == 'bust_classifier':
            if not most_recent_bust_timestamp or timestamp > most_recent_bust_timestamp:
                normalized_result = model.get('NORMALIZED_RESULT', None)
                most_recent_bust_model = normalized_result
                most_recent_bust_timestamp = timestamp

        elif model_type == 'breakout_classifier':
            if not most_recent_breakout_timestamp or timestamp > most_recent_breakout_timestamp:
                normalized_result = model.get('NORMALIZED_RESULT', None)
                most_recent_breakout_model = normalized_result
                most_recent_breakout_timestamp = timestamp

    return most_recent_bust_model, most_recent_breakout_model

# Function to process a batch of players (fetch both score and classifier data)
def process_batch(batch_df, player_projections, max_age_days=3):
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {}
        for index, row in batch_df.iterrows():
            player_id = row['Player ID']
            futures[executor.submit(fetch_projections, player_id, 'score')] = (player_id, 'score', row)
            futures[executor.submit(fetch_projections, player_id, 'classifiers')] = (player_id, 'classifiers', row)

        for future in as_completed(futures):
            player_id, projection_type, row = futures[future]
            try:
                _, json_data = future.result()

                # Initialize player's data if not already present
                if player_id not in player_projections:
                    player_projections[player_id] = {
                        'Name': row['Name'],
                        'Player ID': player_id,
                        'Position': row['Position'],
                        'NFL Team': row['NFL Team'],
                        'Score Projection': None,
                        'Low Score': None,
                        'High Score': None,
                        'Bust': None,
                        'Breakout': None
                    }

                if projection_type == 'score':
                    simulation_projection, low_score, high_score = get_most_recent_projection(json_data, max_age_days)
                    player_projections[player_id]['Score Projection'] = simulation_projection
                    player_projections[player_id]['Low Score'] = low_score
                    player_projections[player_id]['High Score'] = high_score

                elif projection_type == 'classifiers':
                    bust_normalized_result, breakout_normalized_result = extract_most_recent_models(json_data, max_age_days)
                    player_projections[player_id]['Bust'] = bust_normalized_result
                    player_projections[player_id]['Breakout'] = breakout_normalized_result

            except Exception as e:
                print(f"Error processing {projection_type} data for Player ID {player_id}: {e}")

# Initialize a dictionary to store the player data with projections
player_projections = {}

# Timestamp for unique file names
current_date = datetime.now().strftime('%Y-%m-%d')
intermediate_file = f'/Users/svavasseur/Python/intermediate_results_{current_date}.csv'

# Process data in batches
batch_size = 100  # Adjust this to a reasonable batch size for your machine
for i in tqdm(range(0, len(players_df), batch_size), desc="Processing batches"):
    batch_df = players_df.iloc[i:i+batch_size]
    process_batch(batch_df, player_projections)

    # Save intermediate results to avoid storing everything in memory
    if len(player_projections) >= batch_size:
        temp_df = pd.DataFrame.from_dict(player_projections, orient='index')
        temp_df = temp_df[columns]  # Ensure columns are in the correct order
        temp_df.to_csv(intermediate_file, mode='a', header=not os.path.exists(intermediate_file),
                       index=False, columns=columns)  # Append to CSV
        player_projections.clear()  # Clear the dictionary to free up memory
        gc.collect()  # Force garbage collection

# After processing all batches, check if any data is left and save it
if player_projections:
    final_df = pd.DataFrame.from_dict(player_projections, orient='index')
    final_df = final_df[columns]  # Ensure columns are in the correct order
    final_df.to_csv(intermediate_file, mode='a', header=not os.path.exists(intermediate_file),
                    index=False, columns=columns)  # Save remaining data

# Load all intermediate results from CSV
final_df = pd.read_csv(intermediate_file)
final_df = final_df[columns]  # Ensure columns are in the correct order

# Convert numeric columns to appropriate data types
numeric_cols = ['Score Projection', 'Low Score', 'High Score', 'Bust', 'Breakout']
for col in numeric_cols:
    final_df[col] = pd.to_numeric(final_df[col], errors='coerce')

# Save the final combined data to a new Excel file with timestamp
output_file = f'/Users/svavasseur/Python/ibm_combined_projections_{current_date}.xlsx'
final_df.to_excel(output_file, index=False)

# Automatically delete the intermediate file after saving the Excel file
if os.path.exists(intermediate_file):
    os.remove(intermediate_file)
    print(f"Intermediate file '{intermediate_file}' deleted.")

print(f"Data saved to: {output_file}")

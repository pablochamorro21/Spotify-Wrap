import os
import random
import csv
import time
import fastavro
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import fastavro
from fastavro import writer, reader, parse_schema
import io
import pandas as pd
from faker import Faker
from datetime import datetime
import pytz
import pycountry
import requests
from bs4 import BeautifulSoup

folder_path = './'
song_df = pd.read_csv(folder_path + 'Spotify-2000.csv')
print(song_df.head(10))

#AVRO schema
spotify_wrap_schema = {

  "doc": "User listening history with song details.", # documentation/description of schema

  "name": "ListeningHistory", # name of record type the schema represents

  "namespace": "music", # helps to prevent name clashes with other schema

  "type": "record", # type of schema - record representing a complex structure

  # fields of the recod
  "fields": [
    {"name": "user_id", "type": "int"},

    {
      "name": "user_location",
      "type": {
        "type": "record",
        "name": "Location",
        "fields": [
          {"name": "latitude", "type": "float"},
          {"name": "longitude", "type": "float"},
          {"name": "country", "type": "string"},
          {"name": "city", "type": "string"},
          {"name": "timezone", "type": "string"}
        ]
      }
    },


    {"name": "interaction", "type": "string"},

    {"name": "song_name", "type": "string"},

    {"name": "artist", "type": "string"},

    {"name": "genre", "type": "string"},

    {"name": "year", "type": "int"},

    {"name": "popularity", "type": "int"},

    {"name": "danceability", "type": "float"},

    {"name": "energy", "type": "float"},

    {"name": "length_seconds", "type": "int"},

    {
      "name": "listening_time",
      "type": {
        "type": "record",
        "name": "ListeningPeriod",
        "fields": [
            {"name": "start_time", "type": "long"},
            {"name": "end_time", "type": "long"}
        ]
    }
    },

    {
      "name": "personality",
      "type": "string",
    }
  ]
}

utc = pytz.utc
start_time = datetime(2020, 1, 1, tzinfo=utc)

personalities = {
    "The early adopter": {"F/E": "Exploration", "T/N": "Newness", "L/V": "Variety", "C/U": "Commonality"},
    "The nomad": {"F/E": "Exploration", "T/N": "Newness", "L/V": "Loyalty", "C/U": "Uniqueness"},
    "The specialist": {"F/E": "Familiarity", "T/N": "Newness", "L/V": "Variety", "C/U": "Uniqueness"},
    "The enthusiast": {"F/E": "Familiarity", "T/N": "Newness", "L/V": "Loyalty", "C/U": "Commonality"},
    "The connoisseur": {"F/E": "Familiarity", "T/N": "Timelessness", "L/V": "Loyalty", "C/U": "Commonality"},
    "The deep diver": {"F/E": "Familiarity", "T/N": "Timelessness", "L/V": "Variety", "C/U": "Uniqueness"},
    "The fanclubber": {"F/E": "Familiarity", "T/N": "Newness", "L/V": "Variety", "C/U": "Commonality"},
    "The top charter": {"F/E": "Exploration", "T/N": "Timelessness", "L/V": "Loyalty", "C/U": "Commonality"},
    "The replayer": {"F/E": "Familiarity", "T/N": "Timelessness", "L/V": "Loyalty", "C/U": "Uniqueness"},
    "The jukeboxer": {"F/E": "Familiarity", "T/N": "Timelessness", "L/V": "Variety", "C/U": "Commonality"},
    "The voyager": {"F/E": "Exploration", "T/N": "Newness", "L/V": "Loyalty", "C/U": "Commonality"},
    "The devotee": {"F/E": "Familiarity", "T/N": "Newness", "L/V": "Loyalty", "C/U": "Uniqueness"},
    "The maverick": {"F/E": "Exploration", "T/N": "Timelessness", "L/V": "Loyalty", "C/U": "Uniqueness"},
    "The time traveler": {"F/E": "Exploration", "T/N": "Timelessness", "L/V": "Variety", "C/U": "Uniqueness"},
    "The musicologist": {"F/E": "Exploration", "T/N": "Timelessness", "L/V": "Variety", "C/U": "Commonality"},
    "The adventurer": {"F/E": "Exploration", "T/N": "Newness", "L/V": "Variety", "C/U": "Uniqueness"}
}


fake = Faker()
user_song_history = {}
records = []
user_country_mapping = {}

def dynamic_song_selection(user_id, personality, history, song_df):
    year_preference = random.choice([1990, 2000, 2010, 2020])
    filtered_df = song_df[song_df['Year'] == year_preference]

    if user_id in history:
        filtered_df = filtered_df[~filtered_df['Title'].isin(history[user_id])]

    if filtered_df.empty:
        filtered_df = song_df

    return filtered_df.sample(n=1)

def simulate_interaction():
    interactions = ['played', 'paused', 'skipped', 'repeated', 'added_to_playlist']
    probabilities = [0.5, 0.2, 0.15, 0.1, 0.05]
    return random.choices(interactions, weights=probabilities, k=1)[0]

#This is same as dynamic song selection but takes into account personality
def dynamic_song_selection_2(user_id, personality, user_song_history, df):
    characteristics = personalities[personality]
    feature = random.choice(list(characteristics.keys()))
    value = characteristics[feature]

    if user_id not in user_song_history or not user_song_history[user_id]:
        # If the user has no song history, select a random song based on artist
        random_song = df[df['Artist'].isin(df['Artist'].unique())].sample(n=1)
        user_song_history[user_id] = [random_song]

        return random_song

    # User has song history, apply filtering logic based on personality
    if feature == "F/E":
        filter_condition = df['Artist'].isin(user_song_history[user_id][0]['Artist']) if value == "Familiarity" else ~df['Artist'].isin(user_song_history[user_id][0]['Artist'])
    elif feature == "T/N":
        filter_condition = df['Year'] == user_song_history[user_id][0]['Year'].values[0] if value == "Newness" else ~(df['Year'] == user_song_history[user_id][0]['Year'].values[0])
    elif feature == "L/V":
        filter_condition = df['Title'].isin(user_song_history[user_id][0]['Title']) if value == "Loyalty" else ~df['Title'].isin(user_song_history[user_id][0]['Title'])
    elif feature == "C/U":
        filter_condition = df['Popularity'] > user_song_history[user_id][0]['Popularity'].values[0] if value == "Commonality" else df['Popularity'] < user_song_history[user_id][0]['Popularity'].values[0]

    filtered_df = df[filter_condition].sample(n=1)
    user_song_history[user_id].append(filtered_df)

    # Return the same structure as dynamic_song_selection
    return filtered_df.sample(n=1)

def parse_duration(duration_str):
    # Remove commas and any other non-numeric characters
    duration_str = ''.join(c for c in duration_str if c.isdigit() or c == ':')

    # Split the string into minutes and seconds
    parts = duration_str.split(':')

    if len(parts) == 2:
        minutes, seconds = map(int, parts)
    elif len(parts) == 1:
        minutes = 0
        seconds = int(parts[0])
    else:
        raise ValueError("Invalid duration format: " + duration_str)

    return minutes * 60 + seconds

def generate_valid_country_code():
    while True:
        country_code = fake.country_code(representation="alpha-2")
        # Ensure the country code is compatible with local_latlng
        if fake.local_latlng(country_code=country_code):
            return country_code
        
def generate_user_location(user_id,user_country_mapping):
    if user_id in user_country_mapping:
        # If the user already has a country assigned, use that country
        random_country = user_country_mapping[user_id]
    else:
        # Generate a new country for the user
        random_country = generate_valid_country_code()
        user_country_mapping[user_id] = random_country

    location = fake.local_latlng(country_code=random_country)
    latitude, longitude, city, country, timezone = location

    return {
        "user_id": user_id,
        "latitude": latitude,
        "longitude": longitude,
        "country": country,
        "city": city,
        "timezone": timezone
    }

def generate_record(user_id, personality, df, user_location, start_time, n_records, n_records2):

    user_song_history.setdefault(user_id, [])
    characteristics = personalities[personality]
    feature, value = random.choice(list(characteristics.items()))

    # Refactor personality-based filtering into a function
    def filter_songs(df, feature, value, history):
        if feature == "F/E":
            exploration_threshold = 0.8
            is_exploring = random.random() > exploration_threshold if value == "Exploration" else random.random() < exploration_threshold
            artist_in_history = df['Artist'].isin(history[user_id]) if user_id in history else pd.Series([False] * len(df))
            return df[artist_in_history] if is_exploring else df[~artist_in_history]
        elif feature == "T/N":
            newness_threshold = 0.8
            is_new = random.random() < newness_threshold if value == "Newness" else random.random() > newness_threshold
            return df[df['Year'] == 2019] if is_new else df[df['Year'] != 2019]
        elif feature == "L/V":
            loyalty_threshold = 0.8
            is_loyal = random.random() < loyalty_threshold if value == "Loyalty" else random.random() > loyalty_threshold
            title_in_history = df['Title'].isin(history[user_id]) if user_id in history else pd.Series([False] * len(df))
            return df[title_in_history] if is_loyal else df[~title_in_history]
        elif feature == "C/U":
            commonality_threshold = 0.8
            is_common = random.random() < commonality_threshold if value == "Commonality" else random.random() > commonality_threshold
            return df[df['Popularity'] > 70] if is_common else df[df['Popularity'] <= 70]
        return df

    filtered_df = filter_songs(df, feature, value, user_song_history)

    if filtered_df.empty:
        filtered_df = df

    song_row = filtered_df.sample(n=1).iloc[0]
    user_song_history[user_id].append(song_row['Title'])

    # Time adjustments
    if start_time is not None:
        n_records += 1
        n_records2 += 1
        if n_records > 20:
            start_time += timedelta(days=1)
            n_records = 0
        if n_records2 > 400:
            start_time += relativedelta(months=1)
            n_records2 = 0

    # Interaction handling
    interaction = random.choices(['played', 'paused', 'shuffle'], weights=[0.7, 0.2, 0.1], k=1)[0]
    interaction_duration = random.randint(30, 210) if interaction != 'paused' else random.randint(30, 90)
    end_time = start_time + timedelta(seconds=interaction_duration)

    # Format time consistently
    start_time_ms = int(record["listening_time"]["start_time"].timestamp() * 1000)
    end_time_ms = int(record["listening_time"]["end_time"].timestamp() * 1000)
    #start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S") - does not work bc avro schema says we use a timestamp, not this format
    #end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")


    # Return structured record
    return {
        "user_id": user_id,
        "user_location": user_location,
        "interaction": interaction,
        "song_name": song_row['Title'],
        "artist": song_row['Artist'],
        "genre": song_row['Top Genre'],
        "year": int(song_row['Year']),
        "popularity": int(song_row['Popularity']),
        "danceability": float(song_row.get('Danceability', 0)),  # Assuming these fields are now included and handled as floats
        "energy": float(song_row.get('Energy', 0)),
        "length_seconds": int(song_row['Length (Duration)']),
        "listening_time": (start_time_ms, end_time_ms),
        "personality": personality
    }


def simulate_user_session(user_id, start_time, duration_hours, personality, user_song_history, song_df,user_country_mapping):
    session_end_time = start_time + timedelta(hours=duration_hours)
    current_time = start_time
    session_records = []
    user_location = generate_user_location(user_id, user_country_mapping)

    while current_time < session_end_time:
        song_row = dynamic_song_selection(user_id, personality, user_song_history, song_df).iloc[0]
        interaction = simulate_interaction()

        if interaction == 'skipped':
            listen_duration = random.randint(5, 30)
        elif interaction == 'repeated':
            listen_duration = random.randint(360, 600)
        else:
            listen_duration = random.randint(180, 300)

        end_time = current_time + timedelta(seconds=listen_duration)

        start_time_ms = int(start_time.timestamp() * 1000)
        end_time_ms = int(session_end_time.timestamp() * 1000)

            # Generate realistic user location
        user_location = generate_user_location(user_id, user_country_mapping)


        record = {
            "user_id": user_id,
            "user_location": user_location,
            "interaction": interaction,
            "song_name": song_row['Title'],
            "artist": song_row['Artist'],
            "genre": song_row['Top Genre'],
            "year": song_row['Year'],
            "popularity": song_row['Popularity'],
            "danceability": song_row.get('Danceability', 0),
            "energy": song_row.get('Energy', 0),
            "length_seconds": parse_duration(song_row['Length (Duration)']),
            "listening_time": {"start_time": start_time_ms, "end_time": end_time_ms},
            "personality": personality
        }
        session_records.append(record)

        if user_id not in user_song_history:
            user_song_history[user_id] = []
        user_song_history[user_id].append(song_row['Title'])

        current_time = end_time

    return session_records


def main(num_sessions_per_user, session_duration_hours_range, n_users):
    records = []  # Make sure this is initialized here if not globally defined
    for user_id in range(1, n_users + 1):  # Adjust for the correct range if necessary
        personality = random.choice(list(personalities.keys()))
        user_sessions_records = []
        for _ in range(num_sessions_per_user):
            start_time = fake.date_time_between(start_date="-1y", end_date="now", tzinfo=utc)
            session_duration_hours = random.randint(*session_duration_hours_range)
            session_records = simulate_user_session(user_id, start_time, session_duration_hours, personality, user_song_history, song_df, user_country_mapping)
            user_sessions_records.extend(session_records)
        records.extend(user_sessions_records)

    # Serialize data and save to a file
    avro_file_name = 'spotify_wrapped_data.avro'
    file_path = os.path.join(os.path.dirname(__file__), avro_file_name)
    with open(file_path, 'wb') as out_file:
        fastavro.writer(out_file, spotify_wrap_schema, records)
    
    # Get the file size
    file_size = os.path.getsize(file_path)
    print(f"Serialized data saved to {file_path}")
    print(f"Serialized data length: {file_size} bytes")

if __name__ == "__main__":
    # Example execution
    num_sessions_per_user = 3
    session_duration_hours_range = (1, 3)
    n_users = 100
    main(num_sessions_per_user, session_duration_hours_range, n_users)

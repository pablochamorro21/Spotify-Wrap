### **Overview**

- Our project aims to build a system that can track and analyze how people use Spotify in real-time, much like Spotify Wrapped does yearly, but that works continuously. 
- This system will handle data related to user activities on music tracks, including what songs they play, skip, like, and how long they listen to them. 
- Essentially, we're setting up the basics for a tool that can offer instant insights into listeners' habits and preferences, which could help make music streaming services more personal for users. 
- This first step is about creating a framework that can capture and analyse music streaming data on the fly.
- First step: Develop a AVRO Schema for the data feed.
- Second step: Generate synthetic data simulating the streaming of several users. 

### **Dependencies**

Ensure Python 3.x is installed on your system, as the scripts are designed to be compatible with Python 3, specifically tested on Python 3.9.7.

- **fastavro**: A library for working with AVRO files, used for efficient data serialization and deserialization.
- **pandas**: Provides powerful data structures and tools for data manipulation and analysis.
- **Faker**: Used for generating fake data, such as names, addresses, and timestamps, to simulate realistic user profiles.
- **pytz**: A Python library for accurate and cross-platform timezone calculations.
- **pycountry**: Utilizes ISO country codes and names, enhancing geographic data realism.
- **requests**: Allows HTTP requests to be sent, useful for web scraping or API interactions.
- **beautifulsoup4**: A library for parsing HTML and XML documents, often used in web scraping.
- **datetime, timedelta, and relativedelta (from dateutil)**: Built-in and external libraries for handling and manipulating dates and times.

To install these dependencies, execute the following command in your terminal or command prompt:
```
pip install fastavro pandas Faker pytz pycountry requests beautifulsoup4 python-dateutil
```
- Note: The datetime, csv, io, random modules are part of the Python Standard Library and do not require separate installation.


### **Spotify Wrapped AVRO Schema Design**

- Objective: representing user listening histories in the Spotify Wrapped project. 
- It captures detailed information about user interactions with songs, including metadata about the songs played, user location, interaction types, and temporal details of listening sessions.
- Below is the AVRO schema defined in JSON format (can be found in the github repo as `spotify_schema.json`):

```json
{
  "doc": "User listening history with song details.",
  "name": "ListeningHistory",
  "namespace": "music",
  "type": "record",
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
    {"name": "personality", "type": "string"}
  ]
}
``` 

## Field Descriptions

- **user_id**: Unique identifier for the user (Integer).
- **user_location**: Nested record capturing the user's geographical location at the time of the listening event. Includes latitude, longitude, country, city, and timezone.
- **interaction**: Type of interaction with the song (e.g., played, paused, skipped).
- **song_name**: Title of the song.
- **artist**: Name of the artist who performed the song.
- **genre**: Genre of the song.
- **year**: Release year of the song.
- **popularity**: Popularity score of the song on Spotify.
- **danceability**: Numerical measure of the song's danceability.
- **energy**: Numerical measure of the song's energy.
- **length_seconds**: Duration of the song in seconds.
- **listening_time**: Nested record indicating the start and end timestamps of the listening period.
- **personality**: The user's listening personality type.

## Rationale Behind the Schema

- **Documentation**: The `doc` attribute provides a description of the schema's purpose - to document users' listening histories along with detailed song information.
- **Name and Namespace**: They establish a unique identity, preventing conflicts with other schemas and ensuring data integrity.
- **Record Type**: The schema is defined as a record, the most suitable AVRO type for complex data structures.
- **User ID**: The `user_id` for now is a simple integer.
- **User Location**: Includes a nested `Location` record, capturing detailed geographical information - latitude, longitude, country, city, and timezone.
- **Interaction**: A string, describes the users' actions with the songs, such as 'played', 'paused', or 'skipped'.
- **Song Metadata**: Fields like `song_name`, `artist`, `genre`, `year`, and `popularity` deliver essential metadata about the songs for content analysis, recommendation algorithms…
- **Song Characteristics**: Attributes such as `danceability`, `energy`, and `length_seconds` are stored as numerical values, a quantifiable descriptor of the song.
- **Listening Time**: `ListeningPeriod` record with `start_time` and `end_time` fields, this captures the duration of each listening session in a structured format.
- **Personality**: Categorization of users based on their listening habits or preferences, influencing the music recommendation logic.

### **Technical Report**

## Rationale Behind the AVRO Format Choice
AVRO is the most appropriate format for the Spotify Wrapped project due to its schema evolution, data compression, serialization efficiency, and tool ecosystem support.
- **Schema Evolution**: Enables schema modifications over time without invalidating existing data, crucial for the evolving data model of Spotify Wrapped.
- **Data Compression**: Offers effective data compression to handle growing volumes of listening history data cost-effectively without impacting performance.
- **Serialization Efficiency**: Designed for compact binary serialization, reducing storage and network demands, vital for the high-volume data flow in Spotify Wrapped.
- **Big Data Tool Integration**: Compatible with key big data tools like Hadoop, Spark, and Kafka, facilitating easy data pipeline creation for analytics and machine learning.
- **Rich Data Structures**: Supports complex data types, including nested records, ideal for detailed representation of user interactions and song metadata.
- **Language Neutrality**: Language-agnostic, ensuring Spotify Wrapped data can be shared and processed across different systems and programming languages.

## Initialization

- **utc**: A timezone-aware datetime object initialized with UTC timezone to ensure consistent time representations across the dataset.
- **personalities**: A dictionary that outlines various user listening personalities along with their characteristics, influencing song selection and user interaction simulations.
- **song_df**: A pandas DataFrame derived from the original dataset, filtered to include only relevant columns for the simulation (e.g., Title, Artist, Genre).
- **user_song_history**: A dictionary utilized to maintain a record of songs each user has interacted with, helping to simulate realistic listening patterns.
- **records**: An accumulating list where records of user interactions are stored, ready for serialization.
- **user_country_mapping**: A dictionary that maps each user to a country, used to generate consistent and realistic user locations.

## Functions

- **Dynamic Song Selection**: A function that selects a song based on the user's historical interactions and year preference, aiming to mimic realistic music discovery and loyalty behaviors.
- **Simulate Interaction**: Randomly determines the type of interaction (e.g., play, pause, skip) a user has with a song, based on predefined probabilities to reflect varied user behaviors.
- **Dynamic Song Selection 2**: An enhanced version of the song selection function that additionally takes into account the user's personality when filtering song choices, for more personalized interactions.
- **Parse Duration**: Converts the duration of songs from string format to an integer representing the total seconds, facilitating calculations related to listening times.
- **Generate Valid Country Code**: A utility function to generate country codes that are valid and compatible with the `faker` library's `local_latlng` function, ensuring realistic geographic data.
- **Generate User Location**: Leverages the `faker` library to generate plausible user locations, including latitude, longitude, country, city, and timezone, based on generated country codes.
- **Generate Record**: Constructs a comprehensive record of a user's interaction with a song, incorporating details like user location, interaction type, song metadata, listening period, and user personality.
- **Simulate User Session**: Orchestrates the generation of multiple interaction records over a simulated session, dynamically selecting songs and documenting user interactions, locations, and listening times.
- **Main Execution**: The main function that orchestrates the entire simulation process, generating sessions for multiple users, creating interaction records, and serializing the data into AVRO format for analysis.

## Usage

To initiate the simulation, execute the main function specifying the desired parameters: number of sessions per user, range of session durations (in hours), and total number of users to simulate. This process generates a synthetic dataset.

## Design and Implementation of the Synthetic Data Generation Scripts
- The synthetic data generation scripts simulates user interactions with songs, including details on what songs are played, when they're played, and the nature of interactions users have with them. Key aspects of the script's design and implementation include:
- **User Location Generation**: For each `user_id`, the script generates a single user location, capturing latitude, longitude, country, city, and timezone. This provides a geographic context to each user's streaming data.
- **Limitation - Inability to Simulate Travel**: A notable limitation of the current implementation is its inability to simulate users listening from multiple locations. Each user is associated with a single location throughout the dataset, which may not fully mirror real-world behavior where users might listen from various places.
- **Personality-based Song Selection**: At the simulation's start, each `user_id` is assigned one of several predefined personality types. These personality types influence the choice of songs and the nature of interactions, adding a layer of personalization to the simulated data.
- **Predefined Probabilities for User Interactions**: User interactions with the streaming service, including playing, pausing, skipping, and repeating songs, are determined based on predefined probabilities. This approach models the variability in user behavior across the platform.
- **Variable Listening Durations**: The duration of each listening interaction is contingent on the type of interaction. For instance, a "skipped" song will feature a shorter listening duration compared to a "repeated" song, reflecting intuitive user behaviors

## Implementation Details
- **Dynamic Song Selection**: Functions are provided for selecting songs based on user history and preferences, taking into account the year of release and the user's personality. 
- **Interaction Simulation**: The scripts simulate various types of user interactions with songs, assigning different probabilities to each action (stochastic approac)
- **Data Serialization**: After generating the synthetic interactions, the scripts serialize the data into AVRO format, adhering to the predefined schema. 

## How the synthetic data meets the project requirements
- **Geographic variation**: Unique location per user, with global diversity, by use of the Faker library to produce realistic latitude, longitude, country, city, and timezone data. The inclusion of country and city information in user locations enables analyses that consider local musical preferences, regional trends, and the impact of timezone on listening habits.
- **Realistic user behaviour**: ssigning personality types to users introduces realistic variability in music preferences and listening habits. Each personality type influences song selection and interaction types
- **Temporal dynamics**: By simulating user sessions with start and end times, the dataset incorporates temporal dynamics, reflecting how listening habits change over the course of a day, week, or longer periods.The dynamic song selection process, which considers both the user's history and year preference, ensures that the dataset reflects temporal trends in music popularity and discovery. 

## Challenges encountered and solutions adopted
- **Generating realistic user locations**: To overcome this challenge, we utilized the Faker library, which can generate fake but realistic data, including geographic information. For each user ID, we generated a country code using pycountry for broader geographic diversity and then used Faker to generate latitude, longitude, city, and timezone information corresponding to this country code. 
- **Reflecting user interactions accurately**:  We addressed this by defining a set of user personalities, each associated with specific music preferences and interaction probabilities. By mapping these personalities to user IDs, we could tailor song selection and interaction types based on the assigned personality, using random choice functions weighted by predefined probabilities. 
- **Handling timestamps and time zones**: We tackled this by ensuring all datetime operations were timezone-aware, using Python's pytz library. For each user session, we generated start times and dynamically calculated end times based on the duration of interactions, ensuring these were consistent with the user's assigned timezone. To handle the representation of start and end times in the dataset, we stored timestamps in UNIX format (milliseconds since the epoch), which allowed for straightforward conversion and comparison regardless of the time zone. 


### **Other things**

## **For reference: Listener Personalities**
- **Familiarity vs. Exploration (F/E):** Whether the user prefers playing songs from favorite artists (Familiarity) or discovering new artists (Exploration), quantified using an "affinity" score.
- **Timelessness vs. Newness (T/N):** This measures the preference for new releases versus older music.
- **Loyalty vs. Variety (L/V):** Evaluates if a user listens to the same songs repeatedly (Loyalty) or prefers a wide range of different songs (Variety)
- **Commonality vs. Uniqueness (C/U):** Looks at whether users listen to popular music known by many (Commonality) or seek out lesser-known tracks (Uniqueness). Listening to the top global song on Spotify daily would score towards Commonality, while playing only obscure artists scores towards Uniqueness.

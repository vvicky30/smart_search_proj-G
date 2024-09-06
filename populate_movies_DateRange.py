import requests
import psycopg2
import os
import time
import json
from tqdm import tqdm

# Replace with your TMDB API key
TMDB_API_KEY = os.getenv('TMDB_API_KEY')

# PostgreSQL connection details
DB_HOST = 'localhost'
DB_NAME = 'movies_db'
DB_USER = 'postgres'
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Connect to PostgreSQL
conn = psycopg2.connect(
    host=DB_HOST,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)
cur = conn.cursor()

# Function to fetch movie data from TMDB
def fetch_movies_from_tmdb(page=1, start_date=None, end_date=None):
    url = 'https://api.themoviedb.org/3/discover/movie'
    params = {
        'api_key': TMDB_API_KEY,
        'sort_by': 'popularity.desc',
        'page': page,
        'primary_release_date.gte': start_date,
        'primary_release_date.lte': end_date
    }
    
    for attempt in range(5):
        response = requests.get(url, params=params)
        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 1))
            print(f"Rate limit exceeded, retrying after {retry_after} seconds")
            time.sleep(retry_after)
            continue
        elif response.status_code != 200:
            response.raise_for_status()
        else:
            break
    else:
        print("Failed to fetch data even after 5 attempts")
        return []
        
    data = response.json()
    return data['results']

# Function to insert movie data into PostgreSQL
def insert_movie_data(movies):
    try:
        for movie in movies:
            movie_name = movie['title']
            tmdb_rating = movie.get('vote_average', None)
            
            if tmdb_rating is not None and tmdb_rating > 9.99:
                tmdb_rating = 9.99

            genre_ids = movie.get('genre_ids', [])
            release_year = movie['release_date'].split('-')[0] if 'release_date' in movie and movie['release_date'] else None
            overview = movie.get('overview', '')
            genres = fetch_genres(genre_ids)
            genre = ', '.join(genres)
            director_name = fetch_director(movie['id'])
            top_5_actors = fetch_top_actors(movie['id'])

            # Check if the movie already exists
            cur.execute("""
                SELECT id FROM movies.movies
                WHERE movie_name = %s AND release_year = %s
            """, (movie_name, release_year))
            
            existing_movie = cur.fetchone()
            if existing_movie:
                print(f"Movie '{movie_name}' from {release_year} already exists. Skipping insertion.")
                continue

            cur.execute("""
                INSERT INTO movies.movies (movie_name, tmdb_rating, genre, release_year, director_name, top_5_actors)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (movie_name, tmdb_rating, genre, release_year, director_name, top_5_actors))
            
            movie_id = cur.fetchone()[0]
        
            cur.execute("""
                INSERT INTO movies.movie_overviews (movie_id, movie_name, overview)
                VALUES (%s, %s, %s)
            """, (movie_id, movie_name, overview))

        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error inserting movie data: {e}")

# Function to fetch genre names
def fetch_genres(genre_ids):
    genre_names = []
    url = f"https://api.themoviedb.org/3/genre/movie/list?api_key={TMDB_API_KEY}&language=en-US"
    response = requests.get(url)
    genres = response.json().get('genres', [])
    for genre in genres:
        if genre['id'] in genre_ids:
            genre_names.append(genre['name'])
    return genre_names

# Function to fetch director name
def fetch_director(movie_id):
    url = f'https://api.themoviedb.org/3/movie/{movie_id}/credits'
    params = {'api_key': TMDB_API_KEY}
    response = requests.get(url, params=params)
    data = response.json()
    crew = data.get('crew', [])
    for member in crew:
        if member['job'] == 'Director':
            return member['name']
    return 'Unknown'

# Function to fetch top 5 actors
def fetch_top_actors(movie_id):
    url = f'https://api.themoviedb.org/3/movie/{movie_id}/credits'
    params = {'api_key': TMDB_API_KEY}
    response = requests.get(url, params=params)
    data = response.json()
    cast = data.get('cast', [])[:5]
    top_actors = '{' + ','.join([json.dumps(actor['name']) for actor in cast]) + '}'
    return top_actors

# Fetch and insert movie data
total_pages = 299  # Set to 500 pages per date range
batch_size = 100
pause_time = 60
start_page = 100
# List of date ranges to cover all movies
date_ranges = [
    ("2000-01-01", "2005-12-31"),
    ("2006-01-01", "2010-12-31"),
    ("2011-01-01", "2015-12-31"),
    ("2016-01-01", "2023-12-31")
]

for start_date, end_date in date_ranges:
    print(f"Fetching movies from {start_date} to {end_date}")
    for start_page in range(start_page, total_pages + 1, batch_size):
        end_page = min(start_page + batch_size - 1, total_pages)

        all_movies = []
        for page in tqdm(range(start_page, end_page + 1), desc=f"Pages {start_page}-{end_page}"):
            movies = fetch_movies_from_tmdb(page, start_date, end_date)
            all_movies.extend(movies)
        insert_movie_data(all_movies)
        print(f"Processed pages {start_page} to {end_page}")
        time.sleep(pause_time)

# Close the connection
cur.close()
conn.close()

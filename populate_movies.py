import requests
import psycopg2
import os
import time
import json # Importing json module
from tqdm import tqdm #TQDM:Python library that provides a convenient way to add progress bars to loops and iterable objects.
# Replace with your TMDB API key
TMDB_API_KEY = os.getenv('TMDB_API_KEY')

# PostgreSQL connection details
DB_HOST = 'localhost'
DB_NAME = 'movies_db'
DB_USER = 'postgres'
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Connect to PostgreSQL (connection to 'movies_db' of postgresql using postgre database credentials)
conn = psycopg2.connect(
    host=DB_HOST,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)
cur = conn.cursor()   # cur object created by calling cursor constructor of conn(connection that we already eshtablished)

# Function to fetch movie data from TMDB
def fetch_movies_from_tmdb(page=1): # by default page value is 1
    url = 'https://api.themoviedb.org/3/discover/movie'
    params = {
        'api_key': TMDB_API_KEY,
        'sort_by': 'popularity.desc',
        'page': page
    }
    
    for attempt in range(5): # retry upto 5 times ; if API requests overwehlemed
        response = requests.get(url, params=params)
        if response.status_code == 429: # it means API's rate-Limit exceeded
            retry_after = int(response.headers.get('Retry-After', 1))  # fething value of retry_after variale from Api's request response' header
            print(f"Rate limit exceeded, retrying after {retry_after} seconds")
            time.sleep(retry_after) # sleeping for retry_after (var-value-seconds)
            continue  # then again continue to the for loop for making next attempt 
        elif response.status_code != 200: # got some other status_code rather than status 200 (sucess_code)
            response.raise_for_status() # raise that status
        else: # means if all is fine ; no rate-limit exceed ; no other unsuccefull status then:
            break #break the for lopps of attempts
    else: #even after attempt for 5 times 
        print("Failed to fetch data even after 5 attempts")
        return []  # return nothing 
        
    data = response.json()  #converting response into json     
    return data['results']  # fetching 'results' from json-data

# Function to insert movie data into PostgreSQL
def insert_movie_data(movies):
    try:
        
        for movie in movies:  # iterate over movies ;as we saved json_data(which's fetched from TMDB-site using 'fetch_movies_from_tmdb'fn ) in 'movies' variable 
            movie_name = movie['title']  # fethcing movie's name through 'title' tag
            tmdb_rating = movie.get('vote_average', None) # fetching tmdb_rating through 'vote_average' tag
            #Cap the Ratings at 9.99 in Your Script: Ensuring that any tmdb_rating value above 9.99 is capped at 9.99 before insertion.
            if tmdb_rating is not None and tmdb_rating > 9.99:
                tmdb_rating = 9.99  # capped at 99.9  //to avoid this error: psycopg2.errors.NumericValueOutOfRange 
            # this is we're doing because: in the 'movies' table definition restricts the rating to a maximum value of 9.99, but the check constraint allows values up to 10, which is inconsistent with the precision.
            #like this : TMDB_rating DECIMAL(3, 2) CHECK (TMDB_rating >= 0 AND TMDB_rating <= 10)

            genre_ids = movie.get('genre_ids', []) #fetching genre_ids through 'genre_ids' tag ; as this will be useful for finding out genres of this specific movies   
            release_year = movie['release_date'].split('-')[0] if 'release_date' in movie and movie['release_date'] else None
               #fetching release_year of this movie through 'release_date' tag (if this tag is exist for this movie) 
            overview = movie.get('overview', '') # fetching overview of this movie(in text form '') through 'overview' tag 

            genres = fetch_genres(genre_ids) # fetching genre using fetch_genres -fn (where genre_ids used ss argument)
            genre = ', '.join(genres) # formatting genres of this movie // all genres of the movies seperated by coma.

            director_name = fetch_director(movie['id'])# fetching director_name of this movie using fetch_director fn(where movie id used as arguments) 
            top_5_actors = fetch_top_actors(movie['id'])# fetching top_5_actors's name of this movie using fetch_top_actors-fn(where movie id used as arguments) 

            cur.execute("""
                INSERT INTO movies.movies (movie_name, tmdb_rating, genre, release_year, director_name, top_5_actors)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (movie_name, tmdb_rating, genre, release_year, director_name, top_5_actors))
            #The RETURNING id clause is very important here. It tells PostgreSQL to return the value of the id column of the newly inserted row.
            # This is useful because id is an SERIAL type column, meaning it is automatically incremented and assigned by PostgreSQL.
            # By using RETURNING id,you can get the unique identifier of the row you just inserted without having to query the table again.
            movie_id = cur.fetchone()[0] #cur.fetchone() returns a single tuple containing the returned values from the INSERT statement(above). 
            #Since we used RETURNING id, this tuple will contain just one element, which is the id of the newly inserted row.
            #[0] is used to access the first (and only) element of this tuple, which is the id.
            #Thus, movie_id = cur.fetchone()[0] retrieves the id of the newly inserted movie and stores it in the variable movie_id.
        
            # Insert overview into movie_overviews table
            cur.execute("""
                INSERT INTO movies.movie_overviews (movie_id, movie_name, overview)
                VALUES (%s, %s, %s)
            """, (movie_id, movie_name, overview))

        conn.commit() #commiting results into the 'movies_db' 
    except Exception as e:
        conn.rollback()
        print(f"Error inserting movie data: {e}")
        
# Function to fetch genre names (where genre_ids corresponds to current movie used as argunents)
def fetch_genres(genre_ids):
    genre_names = [] # this is list variable where list of genres stored 
    url = f"https://api.themoviedb.org/3/genre/movie/list?api_key={TMDB_API_KEY}&language=en-US"
    response = requests.get(url) 
    genres = response.json().get('genres', []) # converting genres into json data , and fetching all lists of genre available usng 'genre' tag.  
    for genre in genres: # now iterating over total genres list  
        if genre['id'] in genre_ids: # if genre_id from total genre list has genre_ids of the current movie then: 
            genre_names.append(genre['name'])  # we append that genre_name to the list through 'name' tag 
    return genre_names # returning genre_names(list) for the movie

# Function to fetch director name using movie_id as argument (where this movie_ids corresponds to current movie (ongoing))
def fetch_director(movie_id):
    url = f'https://api.themoviedb.org/3/movie/{movie_id}/credits'  # inserting movie_id in url
    params = {'api_key': TMDB_API_KEY}   #inserting api_key(TMDB_API_KEY) in parameters for url request 
    response = requests.get(url, params=params)
    data = response.json() #converting data into json data
    crew = data.get('crew', []) #getting crew-data (in list '[]' format) of this movie  from json_data using 'crew' tag
    for member in crew: # iterate over all the crew of this movie using 'member' as iterator
        if member['job'] == 'Director':  # if member's job equal to 'Director' then:
            return member['name'] # return that member's name 
    return 'Unknown' # else return 'unknown' for director's name

# Function to fetch top 5 actors of this movie (with corresponding movie_id)
def fetch_top_actors(movie_id):
    url = f'https://api.themoviedb.org/3/movie/{movie_id}/credits' # inserting movie_id in url
    params = {'api_key': TMDB_API_KEY} #inserting api_key(TMDB_API_KEY) in parameters for url request
    response = requests.get(url, params=params)
    data = response.json() #converting data into json data
    cast = data.get('cast', [])[:5]   # now getting top 5 cast from json_data using 'cast' tag in form of list-format '[]'
    #top_actors = ', '.join([actor['name'] for actor in cast]) 
       
    # Convert list of actor names to PostgreSQL array literal format:   //as in database 'top_5_actors'(attribute/column_name) is of type text[](or array literal fromat of postgresql)
    top_actors = '{' + ','.join([json.dumps(actor['name']) for actor in cast]) + '}'
    #using json.dumps to format the actor names will handle the psycopg2.errors.InvalidTextRepresentation error.
    # The error occurs because the actor names contain special characters that need to be properly escaped in the PostgreSQL array literal. 
    # By using json.dumps,you ensure that any special characters are correctly escaped, thus preventing the error.
    return top_actors # return top_actors 

# Fetch and insert movie data
#total_pages = 3000  # Set to 3000 pages for initial testing
total_pages = 600  # Set to 3000 pages for initial testing
batch_size = 100  # Set a batch size for processing
pause_time = 60  # Pause for 60 seconds after each batch

#start_page = 1  # start page was set to 1 in the biegning
#now when 400 pages of data fetched , and due to some error program terminated
#so here start page updated to 401, as movie data from page 1 to 400 already bieng saved in postgre-sql database, 
# to avoiding data redundence & ambiguity we'll n0w start from page no : 401
start_page = 501  # Set to 401 to start from page 401

for start_page in range(start_page, total_pages + 1, batch_size):
    end_page = min(start_page + batch_size - 1, total_pages)

    all_movies = []  # Initialize an empty list to collect movies from a batch of pages.
    for page in tqdm(range(start_page, end_page + 1), desc=f"Pages {start_page}-{end_page}"):
        movies = fetch_movies_from_tmdb(page)
        all_movies.extend(movies)  # Add the movies from the current page to the list
    insert_movie_data(all_movies) # Insert all collected movies in one go
    #insert_movie_data(all_movies): Insert all collected movies from the batch into the database in one go.
    #This reduces the number of database commits, which can be time-consuming and resource-intensive.
    # Error Handling: By processing data in batches, we can handle errors more gracefully. If an error occurs, we can debug and re-run the batch without affecting previously processed data.
    print(f"Processed pages {start_page} to {end_page}")
    time.sleep(pause_time)  # Pause to avoid rate limits

# Close the connection
cur.close()
conn.close()

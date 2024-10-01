from config import SessionLocal
import os
import re
from sqlalchemy.sql import text #we should use SQLAlchemy's text construct to ensure the raw SQL string is interpreted correctly. 
#probably for the potential ERROR like :
#The error message we can potentially received indicates that SQLAlchemy is having trouble interpreting the raw-SQL-string in the session.execute command.
# This can happen when SQLAlchemy needs a clear indication that you're passing a raw SQL string.
from decimal import Decimal

#Initialization:
from openai import OpenAI

# set-uping here the openAI_api key from environment-variable name using OS
# This creates a new instance or object of the OpenAI(class_name) client by calling its corresponding constructor with API_KEy as args, 
# which allows us to interact with the OpenAI API
client = OpenAI(
    api_key = os.getenv('OPENAI_API_KEY') # saving the key to aopenai's api_key object
)  
'''
    #gpt-3.5-turbo is the name of a specific variant of OpenAI's GPT-3 model.
    #gpt-3.5-turbo excels at understanding and generating human-like text, 
    # making it suitable for tasks like text completion, summarization, translation, and question-answering.

Tokens are the units of text that the GPT-3 model processes. A token can be as short as one character or as long as one word. 
For example, the word "GPT-3" is one token, and the phrase "OpenAI's GPT-3" consists of four tokens.
When you send a query to the GPT-3 model, the text is broken down into tokens. The model processes these tokens to generate a response.
Token Limit:Each model has a limit on the number of tokens it can process in a single request. For gpt-3.5-turbo, the limit is typically 4096 tokens,
which includes both the input tokens (your prompt) and the output tokens (the model's response).

Model Usage: When you make API calls, you specify which model you want to use in your requests. Your credits will be used according to the model's pricing and the number of tokens processed.

we don’t typically need to purchase each model separately; instead, we can use our credits to access any of the models available in the API, 
with pricing varying depending on the model and amount of usage (in tokens including tokens of input & output as well).
Always check OpenAI’s current pricing and model details for the most accurate information.
'''
def complete_correct(movie_name):
    prompt = f"Correct the spelling or complete the movie name: '{movie_name}'"
    
    try:
        completion = client.completions.create(
            model= "gpt-3.5-turbo-instruct",
            prompt=prompt,
            max_tokens=8,  # Limiting the response to ensure it's concise   //by setting max tokens to 8 here we're ensuting it should gives us the comple/correct name of the movie only
                                         # without any eleborated sentences or instructional sentences. like "correct movie title is 'Jack the Giant Slayer' "  
        )
        # Extract the first line or word assuming it's the movie name
        corrected_movie_name = completion.choices[0].text.strip().split('\n')[0]  # Get the first line of response
        print(f"Corrected/Completed Movie Name: '{corrected_movie_name}'")  # Just for debugging
        return corrected_movie_name
    except Exception as e:
        print(f"ERROR in correcting/completing movie name: {e}")
        return movie_name  # If there's an error, return the original movie name ; the movie name which's entered by user while querying 
# Function to correct and complete actor/actress names
def complete_correct_actors(actor_name):
    prompt = f"Correct the spelling or complete the actor/actress name: '{actor_name}'" # auxilliary prompt for directing chat-gpt for completing and correct the spelling of actor/actress
    
    try:
        completion = client.completions.create(
            model="gpt-3.5-turbo-instruct",
            prompt=prompt,
            max_tokens=6  # Limiting the response to ensure it's concise
        )
        corrected_actor_name = completion.choices[0].text.strip().split('\n')[0] # Get the first line of response
        print(f"Corrected/Completed Actor/Actress Name: '{corrected_actor_name}'")
        return corrected_actor_name
    except Exception as e:
        print(f"ERROR in correcting/completing actor/actress name: {e}")
        return actor_name # if there is error in completing and correcting the actor/actress name then return original actor-name which was used by user while querying.

#This function will correct and complete the director’s name, similar to how you handle the actor/actress names.
def complete_correct_director(director_name):
    prompt = f"Correct the spelling or complete the director's name: '{director_name}'"
    try:
        completion = client.completions.create(
            model="gpt-3.5-turbo-instruct",
            prompt=prompt,
            max_tokens=6
        )
        corrected_director_name = completion.choices[0].text.strip().split('\n')[0]
        print(f"Corrected/Completed Director Name: '{corrected_director_name}'")
        return corrected_director_name
    except Exception as e:
        print(f"ERROR in correcting/completing director name: {e}")
        return director_name #in the case of error:  return original director name as it's which is written by user while querying

def complete_correct_genre(genre_name):
    prompt = f"Correct the spelling or complete the genre name: '{genre_name}'"
    
    try:
        completion = client.completions.create(
            model="gpt-3.5-turbo-instruct",
            prompt=prompt,
            max_tokens=3  # limiting token to 3 as it will correct and complete the genre in the genre_list one by one  
        )
        genre_name1 = completion.choices[0].text.strip().split('\n')[0]
        corrected_genre_name = genre_name1.title()# This will capitalize each word's first letter.
        print(f"Corrected/Completed Genre Name: '{corrected_genre_name}'")
        return corrected_genre_name
    except Exception as e:
        print(f"ERROR in correcting/completing genre name: {e}")
        return genre_name #in the case of error:  return original genre  as it's, which is written by user while querying

    
def process_query(query): 
    # this function is take raw user's query and extract text from it .
    # Normalize the query
    query = query.lower()#Normalize the query : converts the entire query string to lowercase. This is useful because it standardizes the input, 
    #making it easier to process and compare, as SQL queries and text searches are often case-insensitive.

    # Define special cases:
    #This defines a list of regular expressions (regex) to identify specific patterns in the query. 
    #Each regex pattern is a string that describes a search pattern.
    special_cases = [
        r'overview of (.+) movie', #capturing movie_name for retrieving its overviews
        r'overview of (.+)',  #Alternate-query of overview of movies: capturing movie_name for retrieving its overviews
        r'top 5 movies from year (\d{4})', # this will capture the release year for searching top 5 movies corresponds to it.
        r'movies of (actor|actress) (.+) from (\d{4}) to (\d{4})',  # regex for actor/actress with date-range
        # this will capture the actress'/actor's name for searching his top 7 movies
        r'movies of (actor|actress) (.+)',  # Flexible match for both actor and actress
        #r'movies of (.+)',  # Catch-all for other cases like actor/actress with misspelling  //fallback for other cases
        r'movies of director (.+) from (\d{4}) to (\d{4})',  # regex for director with date-range
        r'movies of director (.+) with ratings? (above|below) (\d+(\.\d+)?)',  #regex for director with rating filter  
        r'movies of director (.+)',  #regex for director 
        r'genres? like (.+) with ratings? (above|below) (\d+(\.\d+)?)',  # regex pattern for capturing genre and fassion of rating(above or below) and capturing decimal value(rating)
        r'genres? like (.+)'  # regex pattern to capture genres //genres?: Matches "genre" or "genres" (the ? makes the "s" optional). 
    ]
  
    #this loop iterates through each regex pattern in the special_cases list.
    #re.search(case, query) attempts to match the regex pattern (case) to the query.
    for case in special_cases:
        match = re.search(case, query)
        if match:  #If a match is found (if match), match.group(1) 
            #extracts the relevant part of the query based on the capturing group defined in the regex pattern (the part within parentheses ()).
            if case == r'top 5 movies from year (\d{4})':
                # If the case is for capturing the year, return the year directly without correction
                return match.group(1)
            elif case == r'movies of (actor|actress) (.+) from (\d{4}) to (\d{4})':
                actor_name = match.group(2) # capturing actor/actress' name 
                from_date = match.group(3) # capturing from_date
                to_date = match.group(4)  # capturing to_date
                corrected_actor_name = complete_correct_actors(actor_name) # correcting and completing actor's name with the help of function 
                return corrected_actor_name, from_date, to_date  # Return all three captured and corrected values.
            elif case in [r'movies of (actor|actress) (.+)', r'movies of (.+)']:
                actor_name = match.group(2) #if case == r'movies of (actor|actress) (.+)' else match.group(1)   # capture actor/actress name 
                corrected_actor_name = complete_correct_actors(actor_name)# then comple and correct the actor/actress name by passing captured name to the function: complete_correct_actors.
                return corrected_actor_name # retrun corrected name.
            elif case == r'movies of director (.+)':  # <-- Handling director name
                director_name = match.group(1)
                corrected_director_name = complete_correct_director(director_name)
                return corrected_director_name
            elif case == r'movies of director (.+) from (\d{4}) to (\d{4})':  # <-- Handling director and date range
                director_name = match.group(1) # capturing director's name 
                from_date = match.group(2) # capeturing from_date
                to_date = match.group(3) #capturing to_date
                corrected_director_name = complete_correct_director(director_name)# correcting and completing director's name with the help of function 
                return corrected_director_name, from_date, to_date# returning director's name , from_date and to_date 
            elif case == r'movies of director (.+) with ratings? (above|below) (\d+(\.\d+)?)':
                director_name = match.group(1)   # for director
                rating_comparison = match.group(2)  # 'above' or 'below'
                rating_value = Decimal(match.group(3))  # Rating number (e.g., 8 or 6.47 in decimal as well are allowed)
                # Correct director_name using complete_correct_director
                corrected_director_name = complete_correct_director(director_name)
                return corrected_director_name, rating_comparison, rating_value
            elif case == r'genres? like (.+) with ratings? (above|below) (\d+(\.\d+)?)':
                genre_string = match.group(1)   # for genre
                rating_comparison = match.group(2)  # 'above' or 'below'
                rating_value = Decimal(match.group(3))  # Rating number (e.g., 8 or 6.47 in decimal as well are allowed)
            
                # Correct genres using complete_correct_genre
                genre_list = re.split(r',\s*|\s+and\s+', genre_string) # Split by commas (",") or "and"
                corrected_genres = [complete_correct_genre(genre) for genre in genre_list]
                return corrected_genres, rating_comparison, rating_value
            elif case == r'genres? like (.+)': # <-- Handling queries related to genre 
                genre_string = match.group(1) # capturing strings regarding genre_name i.e. Adventure, crime, drama and war
                genre_list = re.split(r',\s*|\s+and\s+', genre_string)  # Split by commas (",") or "and"
                #uses a list comprehension to iterate through each genre in genre_list and applies the complete_correct_genre function to each one.
                # This means each genre name will be individually corrected or completed, 
                corrected_genres = [complete_correct_genre(genre) for genre in genre_list] # resulting in a new list called corrected_genres that contains the corrected versions of all the genres provided in the user's query.
                return corrected_genres # finally return the list of corrected gnere. 
            else:
                # For movie names in context of retriving overviews, use the complete_correct function
                processed_query = match.group(1)
                corrected_query = complete_correct(processed_query) # for completing and correcting the movie , 
                                                       #here we're passing the relevent text(captured with the help of regex pattern) to the complete_correct function 
                return corrected_query  # then finally returning the corrected query

            
    # Fallback to default prompt processing if explicit actor/actress query check and regex cases based match won't satisfied by user query:-
    #first of all structuring our prompt with the help of this auxilliary-prompt
    prompt = f"Extract the main keyword or complete the movie name for database search from the following user query:\n\nUser Query: \"{query}\"\n\nKeyword:-"
    print(f"Processing query with prompt: {prompt}")
    try:
        completion = client.completions.create(
            model= "gpt-3.5-turbo-instruct",  # Specifies the model to use (e.g., text-davinci-003 or "gpt-3.5-turbo-instruct")
            prompt=prompt,              # The input text for the model to process
            max_tokens=8              # The maximum number of tokens in the response // here Limit the response(by 8 tokens) to ensure it's concise
        )
        # Extract the generated text from the 'completion' (from 'completion' which's generated while langchain interpreting the raw-user's query)
        #where each reponses in the form of messages(variable where 'roles' and their corresponding 'query' used in from of disctionary) 
        # we have to acess user's raw query through 'choices' and its zero index ; then throguh message var with 'content'. 
        processed_query = completion.choices[0].text.strip()
        print(f"Processed Query: '{processed_query}'")  # Debuging: Ensure this is what you expect
        return processed_query
    except Exception as e:
        print(f"ERROR in processing query: {e}")
        return ""  # return nothing in this case (if error occured)

#this function takes raw user's query ; then extract the crucial text from response generated by langchain while interpreting raw-user's query 
# and then based on processed-query this function will then search against 'smart_search_db' through keyword functionality of postgre_sql
def search_movies(query):
    # Process the query using OpenAI
    processed_query = process_query(query)
    if not processed_query:
        print("No processed query to search for.")
        return []
    # Strip extra quotes from the processed query
    processed_query = processed_query.strip('"')
    #In SQL, single quotes are used to enclose string literals, and if your string contains an apostrophe (like in "Ocean's Eleven" movie),
    # we need to escape it by doubling the single quote (''), not by replacing it with double quotes(").
    # Replace single quotes in the processed query with two single quotes
    processed_query = processed_query.replace("'", "''")
    # so here we handle the quotes(in processed query) using regular expression : Using re.findall: This method is better suited for handling complex cases 
    # where you need to differentiate between single words and phrases enclosed in quotes. for getting suitable keywords
    #The regular expression r'"(.*?)"|(\S+)' matches either:
    #A quoted phrase: "(.*?)", where .*? is a non-greedy match for any character inside quotes.
    #A non-whitespace sequence of characters: \S+.
    #re.findall returns a list of tuples. Each tuple contains two elements:
    #The first element is the quoted phrase (if it exists).
    #The second element is the non-whitespace sequence (if it exists).
    #For example, if processed_query is 'Godfather movie', the result of re.findall would be:[('', 'Godfather'), ('', 'movie')]
    #If processed_query is 'my name is "Dr. DRE"', the result would be: [('', 'my'), ('', 'name'), ('', 'is'), ('Dr. DRE', '')]
    #keywords = re.findall(r'"(.*?)"|(\S+)', processed_query)
    #we have to Loop Over Tuples: for keyword in keywords; it iterates over each tuple in the keywords list.
    #For each tuple keyword, it checks the first element (keyword[0]).; If keyword[0] is not an empty string, it selects keyword[0].
    #If keyword[0] is an empty string, it selects the second element keyword[1].; Filter Non-Empty: if keyword[0] or keyword[1]
    #The result is a list of non-empty strings extracted from the tuples. For the given examples:
    #For processed_query as 'Godfather movie': The final keywords list will be ['Godfather', 'movie'].
    #For processed_query as 'my name is "Dr. DRE"': The final keywords list will be ['my', 'name', 'is', 'Dr. DRE'].
    #keywords = [keyword[0] if keyword[0] else keyword[1] for keyword in keywords if keyword[0] or keyword[1]]  # Filter out empty keywords //removing those empty elements from the keywords list
    #if not keywords:  # if whole list of keywords is empty then return nothing
        #print("No valid keywords found in the processed query.")
        #return []
    
    #A list comprehension [f"content ILIKE '%{keyword}%'" for keyword in keywords] is used to create a list of conditions. 
    # For each keyword in the keywords list, it creates a condition like content ILIKE '%keyword%'.
    #The join(" OR ") method combines these conditions with the OR operator, making a single string.
    #For example, if keywords is ["Master", "Of", "Puppets", "Metallica"], the resulting keyword_query will be "content ILIKE '%Master%' OR content ILIKE '%Of%' OR content ILIKE '%Puppets%' OR content ILIKE '%Metallica%'".
    keyword_query = " OR ".join([f"movie_name ILIKE '%{keyword}%'" for keyword in [processed_query]])
    final_query = f"SELECT * FROM movies.movies WHERE {keyword_query} LIMIT 10"
    print(f"Executing SQL Query... : {final_query}")  # debugging purpose
    
    # Generate and execute the SQL query
    session = SessionLocal()  # calling SessionLocal's constructor for creating instance or object 
    #try and except the code here while excecuting queries to avoid the unwanted program termination due to postgre-sql side error.
    try:
        # now executing final query with sql alchemy's session object and it's execute fn along with sql alchemy's text constructor
        results = session.execute(text(final_query)).fetchall()
        print(f"Number of results found: {len(results)}")  # Debugging: Check number of results found
        return results # returning results-var
    except Exception as e: # if above try-block not excecuted ; then except the corresponding error ; then print corresponding error
        print(f"ERROR in searching documents: {e}")
        results = []  # no results will be saved in results variable 

# Function to search for movie overviews based on the processed query
# similarly like above function:
def search_overview(query):
    processed_query = process_query(query)
    if not processed_query:
        print("No processed query to search for.")
        return []
    # Strip extra quotes(if any) from the processed query
    processed_query = processed_query.strip('"')
    #In SQL, single quotes are used to enclose string literals, and if your string contains an apostrophe (like in "Ocean's Eleven" movie),
    # we need to escape it by doubling the single quote (''), not by replacing it with double quotes(").
    # Replace single quotes in the processed query with two single quotes
    processed_query = processed_query.replace("'", "''")

    #keywords = re.findall(r'"(.*?)"|(\S+)', processed_query)
    #keywords = [keyword[0] if keyword[0] else keyword[1] for keyword in keywords if keyword[0] or keyword[1]]
    #if not keywords:
        #print("No valid keywords found in the processed query.")
        #return []

    keyword_query = " OR ".join([f"m.movie_name ILIKE '%{keyword}%'" for keyword in [processed_query]])
    #Foreign Key Join: In the search_overview function here, joined the movies and movie_overviews tables using the movie_id foreign key.
    # retriving movie_name and overview only
    final_query = f"""
        SELECT m.movie_name, o.overview
        FROM movies.movies m
        JOIN movies.movie_overviews o ON m.id = o.movie_id
        WHERE {keyword_query}
        LIMIT 7
    """
    print(f"Executing SQL Query... : {final_query}")

    session = SessionLocal()
    try:
        results = session.execute(text(final_query)).fetchall()
        print(f"Number of results found: {len(results)}")
        return results
    except Exception as e:
        print(f"ERROR in searching documents: {e}")
        return []

#Function search_top_movies: This function is designed to find the top 5 movies from a specific year based on user input.
def search_top_movies(query):
    processed_query = process_query(query)
    if not processed_query:
        print("No processed query to search for.")
        return []
    #using regular expression we here tries to find the year like ex: 2000 or 2006 or 1992 etc to match and grouping them to featch
    match = re.search(r'(\d{4})', processed_query)
    if not match:
        print("No valid year found in the processed query.")
        return []

    year = match.group(1)  # fetching group from 'match' to the variable 'year' here 
    #seearching in movies table for retriving movie information of the top(order by tmdb_rating DESCending) 5(limit =5) movies where release year = fetched year 
    final_query = f"""
        SELECT * FROM movies.movies
        WHERE release_year = {year}
        ORDER BY tmdb_rating DESC
        LIMIT 5
    """
    print(f"Executing SQL Query... : {final_query}")
    
    session = SessionLocal()
    try:
        results = session.execute(text(final_query)).fetchall()
        print(f"Number of results found: {len(results)}")
        return results
    except Exception as e:
        print(f"ERROR in searching documents: {e}")
        return []
    
# Function to search movies by actor or actress name
def search_movies_by_actor(query):
    actor_name = process_query(query)
    if not actor_name:
        print("No actor/actress name to search for.")
        return []
    # Strip extra quotes(if any) from the processed query
    # Replace single quotes in the processed query with two single quotes
    actor_name = actor_name.strip('"').replace("'", "''")
    
    #The error we’re encountering is due to attempting to use the ILIKE operator with a PostgreSQL array (text[]), which isn't supported directly. 
    # The ILIKE operator works with text columns, but your top_5_actors column is likely a text[] array, not a plain text field.
    #To fix this, you need to modify the query to check each element of the array. In PostgreSQL, you can use the ANY operator to match values within arrays.
    #instead of using this in query " top_5_actors ILIKE '%{actor_name}%'"...."  we will use " '%{actor_name}%' ILIKE ANY(top_5_actors)"
    keyword_query = f"'{actor_name}' ILIKE ANY(top_5_actors)"
    #The '%{actor_name}%' part was only useful for pattern matching in strings, but since top_5_actors is an array, using ILIKE ANY() already checks for any occurrence of the actor_name in the array.
    # Thus, no wildcards are needed.
    final_query = f"""
        SELECT * FROM movies.movies
        WHERE {keyword_query}
        ORDER BY tmdb_rating DESC
        LIMIT 7
    """
    print(f"Executing SQL Query... : {final_query}")

    session = SessionLocal()
    try:
        results = session.execute(text(final_query)).fetchall()
        print(f"Number of results found: {len(results)}")
        return results
    except Exception as e:
        print(f"ERROR in searching for movies by actor/actress: {e}")
        return []

def search_movies_by_actor_and_date_range(query):
    result = process_query(query)
    if not result or len(result) < 3: # len(result) < 3 means process_query fn returned not all three values like actor's name, from_date and to_date 
                                       #or one value among them is missing then we print this error
        print("No valid actor/actress name or date range found.")
        return [] # in this case fn will return nothing 
    
    actor_name, from_date, to_date = result # unpack the tuple values and save it to corresponding variables
    actor_name = actor_name.strip('"').replace("'", "''")
    
    final_query = f"""
        SELECT * FROM movies.movies
        WHERE '{actor_name}' ILIKE ANY(top_5_actors) 
        AND release_year BETWEEN {from_date} AND {to_date}
        ORDER BY tmdb_rating DESC
        LIMIT 10;
    """
    
    print(f"Executing SQL Query... : {final_query}")
    session = SessionLocal()
    try:
        results = session.execute(text(final_query)).fetchall()
        print(f"Number of results found: {len(results)}")
        return results
    except Exception as e:
        print(f"ERROR in searching for movies by actor/actress and date range: {e}")
        return []


def search_movies_by_director(query):
    director_name = process_query(query)
    if not director_name:
        print("No director name to search for.")
        return []
    
    director_name = director_name.strip('"').replace("'", "''")
    final_query = f"""
        SELECT * FROM movies.movies
        WHERE director_name ILIKE '%{director_name}%'
        ORDER BY tmdb_rating DESC
        LIMIT 10;
    """
    print(f"Executing SQL Query... : {final_query}")
    session = SessionLocal()
    try:
        results = session.execute(text(final_query)).fetchall()
        print(f"Number of results found: {len(results)}")
        return results
    except Exception as e:
        print(f"ERROR in searching for movies by director: {e}")
        return []
#This function will query the PostgreSQL database to find movies directed by the given director and within the specified date range:
def search_movies_by_director_and_date_range(query):
    result = process_query(query)
    
    if not result or len(result) < 3:  # len(result) < 3 means process_query fn returned not all three values like director's name, from_date and to_date 
                                       #or one value among them is missing then we print this error  
        print("No valid director name or date range found.")
        return []  # search function will return nothing 
    
    director_name, from_date, to_date = result # unpack the tuple values and save it to corresponding variables
    director_name = director_name.strip('"').replace("'", "''")
    
    final_query = f"""
        SELECT * FROM movies.movies
        WHERE director_name ILIKE '%{director_name}%'
        AND release_year BETWEEN {from_date} AND {to_date}
        ORDER BY tmdb_rating DESC
        LIMIT 10;
    """
    
    print(f"Executing SQL Query... : {final_query}")
    session = SessionLocal()
    try:
        results = session.execute(text(final_query)).fetchall()
        print(f"Number of results found: {len(results)}")
        return results
    except Exception as e:
        print(f"ERROR in searching for movies by director and date range: {e}")
        return []

#This function will query the PostgreSQL database to find movies directed by the given director_name and with rating filter (ex: with ratings above/below 8):
def searching_by_director_and_rating(query):
    result = process_query(query)
    
    if not result or len(result) < 3:  # len(result) < 3 means process_query fn returned not all three values like director's name, rating operator and rating. 
                                       #or one value among them is missing then we print this error  
        print("No valid director name or rating found.")
        return []  # search function will return nothing 
    
    director_name, rating_operator, rating = result # unpack the tuple values and save it to corresponding variables
    director_name = director_name.strip('"').replace("'", "''")
    # Construct rating condition (above or below)
    rating_condition = f"tmdb_rating {'>' if rating_operator == 'above' else '<'} {rating}"
     # Construct the SQL query with rating conditions and director
    final_query = f"""
        SELECT * FROM movies.movies
        WHERE director_name ILIKE '%{director_name}%'
        AND {rating_condition}
        ORDER BY tmdb_rating DESC
        LIMIT 10;
    """
    
    print(f"Executing SQL Query... : {final_query}")
    session = SessionLocal()
    try:
        results = session.execute(text(final_query)).fetchall()
        print(f"Number of results found: {len(results)}")
        return results
    except Exception as e:
        print(f"ERROR in searching for movies by director and date range: {e}")
        return []

def search_movies_by_genres(query):
    genres = process_query(query) # storing corrected gnere_list in genres 
    if not genres or not isinstance(genres, list):
        print("No genre names to search for.")
        return []
    # making genre conditions:This part creates a list of SQL conditions for each genre in the genres list. where genres is a list of genre strings (e.g., ["adventure", "war", "drama"]).
    #For each genre in this list, the expression creates a string condition:
    #genre ILIKE '%adventure%'
    #genre ILIKE '%war%'
    #genre ILIKE '%drama%' 
    genre_conditions = " AND ".join([f"genre ILIKE '%{genre}%'" for genre in genres]) #the join function combines them into a single string.
          #" AND " is the separator used to join the conditions, meaning that the final string will look like this:genre ILIKE '%adventure%' AND genre ILIKE '%war%' AND genre ILIKE '%drama%'
          #Example Scenario:
          #Movie 1: "Action, Adventure"
          #Movie 2: "War, Drama"
          #Movie 3: "Action, War, Drama"
          #If the user searches for genres like "action, war":

         #With 'OR':
         #Results: Movie 1 ("Action, Adventure"), Movie 2 ("War, Drama"), and Movie 3 ("Action, War, Drama") because each movie matches at least one of the genres.
         #With 'AND':
         #Results: Only Movie 3 ("Action, War, Drama") because it's the only movie that contains both "action" and "war" in the genre list.
    final_query = f"""
        SELECT * FROM movies.movies
        WHERE {genre_conditions}
        ORDER BY tmdb_rating DESC
        LIMIT 20;
    """
    
    print(f"Executing SQL Query... : {final_query}")
    session = SessionLocal()
    try:
        results = session.execute(text(final_query)).fetchall()
        print(f"Number of results found: {len(results)}")
        return results
    except Exception as e:
        print(f"ERROR in searching for movies by genres: {e}")
        return []

def searching_by_genre_rating(query):
    result = process_query(query)
    # Unpack genres, rating_operator, and rating from result
    if not result or len(result) < 3:
        # If process_query didn't return all three values (genres, rating_operator, rating)
        print("No valid genres or rating information found.")
        return []

    genres, rating_operator, rating = result

    # Check if genres is a valid list
    if not genres or not isinstance(genres, list):
        print("No valid genre name found.")
        return []
    # Construct genre conditions
    genre_conditions = " AND ".join([f"genre ILIKE '%{genre}%'" for genre in genres])
    
    # Construct rating condition (above or below)
    rating_condition = f"tmdb_rating {'>' if rating_operator == 'above' else '<'} {rating}"
    
    # Final SQL query to search for movies
    final_query = f"""
        SELECT * FROM movies.movies
        WHERE {genre_conditions}
        AND {rating_condition}
        ORDER BY tmdb_rating DESC
        LIMIT 10;
    """
    
    print(f"Executing SQL Query... : {final_query}")
    session = SessionLocal()
    
    try:
        results = session.execute(text(final_query)).fetchall()
        print(f"Number of results found: {len(results)}")
        return results
    except Exception as e:
        print(f"ERROR in searching for movies by genres and rating: {e}")
        return []


#The Decimal issue occurs because SQLAlchemy uses Python's Decimal type for precise decimal arithmetic, which is why it is displaying Decimal('value').
#To convert it to a float or string for display purposes, 
#Resolving the problem: This function takes the results and formats any Decimal values to floats.
#With this change, the Decimal values will be converted to floats, making the output cleaner and easier to read.
def format_results(results):
    formatted_results = []
    for result in results:
        formatted_result = []
        for field in result:
            if isinstance(field, Decimal): # if its has field specify with results 'decimal' 
                formatted_result.append(float(field))  # then append it to the formatted results in float type 
            else:
                formatted_result.append(field) # else append it to the formatted results in as it's without float  type 
        formatted_results.append(tuple(formatted_result)) # now append these formatted results as tuple in relatively global formatted _results  
    return formatted_results 

#This function formats the results into the neat and clean format.
#It takes the list of movie details and movie overviews, and constructs a strings with the formatted results.
def display_results(movies, overviews):
    
    movie_str = f"{len(movies)} Movies Found :-\n\n"
    for i, movie in enumerate(movies, 1):
        movie_str += (
            f"{i}. {movie[1]} ({movie[4]})\n\n"
            f"Rating: {movie[2]}\n"
            f"Genre: {movie[3]}\n"
            f"Director: {movie[5]}\n"
            f"Top Actors: {', '.join(movie[6])}\n\n"
        )

    overview_str = "Movie Overviews :-\n\n"
    for i, overview in enumerate(overviews, 1):
        overview_str += f"{i}. {overview[0]}: \"{overview[1]}\"\n\n"
    
    
    if overviews : # if overviews passed in fn 
        if movies and  overviews: # along with overviews movies also passed then 
            return movie_str + overview_str   # then display both  
        return overview_str #else display overviews only
    elif movies :# if movies passed in fn 
        if movies and  overviews: # along with movies, overviews also passed then
            return movie_str + overview_str   # then display both      
        return movie_str  # else display movies only
    else: # else , meaning nothing paased in this display function then 
        return [] #return nothing 


def main():
    while True:
        user_query = input("Enter your query (or 'thanks, I am done here' to exit): ").strip()
        if user_query.lower() == "thanks, i am done here":
            print("Thank you! Have a great day!")
            break

        if "overview" in user_query.lower():
            overview_results = search_overview(user_query)
            if not overview_results:
                print("No results found for overview.")
            formatted_overview_results = format_results(overview_results)
            display_str = display_results([], formatted_overview_results)
            print(display_str)
        elif "top 5 movies" in user_query.lower() and "from year" in user_query.lower():
            top_movies_results = search_top_movies(user_query)
            if not top_movies_results:
                print("No results found for top movies.")
            formatted_top_movies_results = format_results(top_movies_results)
            display_str = display_results(formatted_top_movies_results, [])
            print(display_str)
        elif ("movies of actor" in user_query.lower() or "movies of actress" in user_query.lower()) and "from" in user_query.lower() and "to" in user_query.lower(): #check for the actor/actress and date rang
            movies_by_actor_and_date_results = search_movies_by_actor_and_date_range(user_query)
            if not movies_by_actor_and_date_results:
                print("No results found for actor within the specified date range.")
            formatted_movies_by_actor_and_date_results = format_results(movies_by_actor_and_date_results)
            display_str = display_results(formatted_movies_by_actor_and_date_results, [])
            print(display_str)
        elif "movies of actor" in user_query.lower() or "movies of actress" in user_query.lower(): # <-- Added condition for actor/actress movies search
            movies_by_actor_results = search_movies_by_actor(user_query)
            if not movies_by_actor_results:
                print("No results found for actor/actress.")
            formatted_movies_by_actor_results = format_results(movies_by_actor_results)
            display_str = display_results(formatted_movies_by_actor_results, [])
            print(display_str)
        elif ("movies of director" in user_query.lower()) and ("with ratings above" in user_query.lower() or "with ratings below" in user_query.lower()): # <-- Added condition for director search alongwith rating filter.
            movies_by_director_and_rating_results = searching_by_director_and_rating(user_query) 
            if not movies_by_director_and_rating_results:
                print("No results found for the specified director and rating.")
            formatted_movies_by_director_and_rating_results = format_results(movies_by_director_and_rating_results)
            display_str = display_results(formatted_movies_by_director_and_rating_results,[])
            print(display_str)
        elif "movies of director" in user_query.lower() and "from" in user_query.lower() and "to" in user_query.lower():# <-- Added condition for director search alongwith date range
            #condition for director and date range
            movies_by_director_and_date_results = search_movies_by_director_and_date_range(user_query) 
            if not movies_by_director_and_date_results:
                print("No results found for director within the specified date range.")
            formatted_movies_by_director_and_date_results = format_results(movies_by_director_and_date_results)
            display_str = display_results(formatted_movies_by_director_and_date_results, [])
            print(display_str) 
        elif ("genres like" in user_query.lower() or "genre like" in user_query.lower()) and ("with ratings above" in user_query.lower() or "with ratings below" in user_query.lower()):                    
            movies_by_genre_rating_results = searching_by_genre_rating(user_query)
            if not movies_by_genre_rating_results:
                print("No results found for the specified genres and rating.")
            
            formatted_movies_by_genre_rating_results = format_results(movies_by_genre_rating_results)
            display_str = display_results(formatted_movies_by_genre_rating_results, [])
            print(display_str)
        elif "genres like" in user_query.lower() or  "genre like" in user_query.lower():
            movies_by_genres_results = search_movies_by_genres(user_query)
            if not movies_by_genres_results:
                print("No results found for the specified genres.")
            formatted_movies_by_genres_results = format_results(movies_by_genres_results)
            display_str = display_results(formatted_movies_by_genres_results, [])
            print(display_str)

        elif "movies of director" in user_query.lower():  # <-- Added condition for director search
            movies_by_director_results = search_movies_by_director(user_query)
            if not movies_by_director_results:
                print("No results found for director.")
            formatted_movies_by_director_results = format_results(movies_by_director_results)
            display_str = display_results(formatted_movies_by_director_results, [])
            print(display_str)
            
        else:
            results = search_movies(user_query)
            if not results:
                print("No results found.")
            formatted_results = format_results(results)
            #overview_query = f"Can you give me the overview of {formatted_results[0][1]}" if formatted_results else ""
            #overview_results = search_overview(overview_query) if overview_query else []
            #formatted_overview_results = format_results(overview_results)
            display_str = display_results(formatted_results, [])
            print(display_str)

               
# Example usage
if __name__ == "__main__":
    main()
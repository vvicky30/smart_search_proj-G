CREATE TABLE movies.movies (
    id SERIAL PRIMARY KEY,  --id is of type serial number acts as primary key in the table
    movie_name VARCHAR(255) NOT NULL, -- their should be movie name , it just can't be empty or NULL
    TMDB_rating DECIMAL(3, 2) CHECK (TMDB_rating >= 0 AND TMDB_rating <= 10), --TMDB_rating would be og type float/decimal(2 points after decimal); rating should be equal or grater than zero AND less than 10
    genre VARCHAR(100),
    release_year INT CHECK (release_year >= 1888), --release year of integer type ; it should be greater than equal to year:1888
    director_name VARCHAR(255),
    top_5_actors TEXT[] -- An array of text to store top 5 actors ; we need to Convert list of actor names to PostgreSQL array literal format
);

CREATE TABLE movies.movie_overviews (
    movie_id INT PRIMARY KEY,
    movie_name VARCHAR(255) NOT NULL,
    overview TEXT,
    FOREIGN KEY (movie_id) REFERENCES movies.movies(id) -- using movie_id as a foreign key in the movie_overviews table that references the id column in the movies table allows you to correctly select the corresponding overview of a movie along with its other information like director name, release year, etc. from the movies table.
);

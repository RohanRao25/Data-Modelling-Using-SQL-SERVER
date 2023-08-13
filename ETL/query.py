"""This file contains all the queries required for the Load operation"""

genreTableInsertQuery = f"INSERT INTO IMDB_MASTER.genre (Genre_ID, Genre_Name) VALUES (?, ?)"

genreTableDeleteQuery = "DELETE FROM IMDB_MASTER.genre"

starsTableDeleteQuery = "DELETE FROM IMDB_MASTER.stars"

starsTableInsertQuery = f"INSERT INTO IMDB_MASTER.stars(Star_ID, Name) VALUES(?, ?)"

genreMovieRelTableDeleteQuery = "DELETE FROM IMDB.MOVIE_GENRE"

genreMovieRelTableINSERTQuery = "INSERT INTO IMDB.MOVIE_GENRE (genre_id, movie_id) VALUES (? ,?)"

starsMovieRelTableDeleteQuery = "DELETE FROM IMDB.MOVIE_STAR"

starsMovieRelTableINSERTQuery = "INSERT INTO IMDB.MOVIE_STAR (movie_id, star_id) VALUES (? ,?)"

moviesTableDeleteQuery = "DELETE FROM IMDB.MOVIE"

moviesTableInsertQuery = "INSERT INTO IMDB.MOVIES (movie_id, title, director, rating, certificate_rating, runtime, votes) VALUES (?, ?,?,?,?,?,?)"
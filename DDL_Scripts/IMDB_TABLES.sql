USE IMDB_data
GO
CREATE SCHEMA IMDB
GO
CREATE TABLE IMDB.MOVIE (
movie_id int,
title varchar(100),
director varchar(100),
rating FLOAT,
certificate_rating VARCHAR(10),
runtime int,
votes bigint,
CONSTRAINT PK_M_ID PRIMARY KEY (movie_id)
) ON [PRIMARY]

GO 
CREATE TABLE IMDB.MOVIE_GENRE (
genre_id int,
movie_id int
CONSTRAINT FK_G_ID FOREIGN KEY (genre_id)
REFERENCES imdb_master.genre (genre_id),
CONSTRAINT FK_M_ID FOREIGN KEY (movie_id)
REFERENCES imdb.movie (movie_id)
) ON [PRIMARY]
GO
CREATE TABLE IMDB.MOVIE_STAR (
movie_id int,
star_id int
CONSTRAINT FK_MS_M_ID FOREIGN KEY (movie_id)
REFERENCES imdb.movie (movie_id),
CONSTRAINT FK_MS_S_ID FOREIGN KEY (star_id)
REFERENCES imdb_master.stars (star_id)
)ON [PRIMARY]
GO

SELECT * FROM IMDB.;


USE IMDB_data;
GO
CREATE SCHEMA IMDB_MASTER
GO
CREATE TABLE IMDB_MASTER.genre(
Genre_ID int,
Genre_Name varchar(100)
Constraint G_ID PRIMARY KEY (Genre_ID)
) ON [PRIMARY]
GO


--EXEC sp_help 'imdb_master.genre'; -- this helps in describing the table structure since sql server doesn't have a describe command

CREATE TABLE imdb_master.stars(
Star_ID int,
Name varchar(100)
Constraint S_ID PRIMARY KEY (Star_ID)
) ON [PRIMARY];
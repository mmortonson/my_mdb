## Personal movie database

Interactive script for managing a personal movie database.
Uses the [OMDb API] to obtain movie metadata to add to the database,
and SQL to search and edit the contents of the database.

Features:

- Add movies you own and specify their formats (e.g. blu ray, iTunes, etc.)
  and other information like series name and date(s) viewed.
- Delete movies from the database.
- Search for movies that match given criteria (e.g. runtime or time since
  last viewing).

To run from the command line: `./my_mdb.py` followed by the name of
your movie database file. If this is your first time running the program
or you need to create a new database for some other reason, just use a new 
file name and a new database will be automatically created.


[OMDb API]: http://www.omdbapi.com

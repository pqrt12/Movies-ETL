# Movies-ETL
# challenge
The [challenge.py](https://github.com/pqrt12/Movies-ETL/blob/master/challenge.py) is a Python script to automatically extract movie data from Wikipedia, Kaggle and MovieLens; clean and transform the incoming data and finally load into PostgreSQL.

The script may be invoked in a command line shell with three full file names as parameters, in the order:
- a Wikipedia movie JSON file,
- a Kaggle megadata movie cvs file,
- and a MovieLens ratings cvs file.
If no input parameters are presented, the script will ask user input instead.

These data files are huge and cannot be saved in github. It is expected they should be properly downloaded seperately, and accessible via the given filename (include path) to this script.

The script may also be useful by directly calling the main function "movies_etl" with the same three filename. It returns "0" if run successfully, or "-1" if failed.

To successfully run the script, several python modules would be imported; they must be presented:
    sys
    JSON
    pandas
    numpy
    re
    time
    sqlalchemy

A "db_password" in config.py must be available, with a format like:
    db_password="myPostgrePassword"
This is needed to access PostgreSQL.

A Database "movie_data" should have been created in PostgreSQL, and running. After running this script, two tables "movies" and "rating" would be created if not existed, or replaced.

All three input files are assumed no file type change, and no column name change after being converted to DataFrame. If a change in those "Key", the script may need to be modified accordingly.

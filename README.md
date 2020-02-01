# Data Modeling and ETL processing for Shopify data

This project aims at `Data Modeling` and `ETL processing` for tracking users' songplay activity for Sparkify using relational databases.

## Table of Contents

- [Technologies used](#Technologies-used)
- [Source data information](#Source-data-information)
- [Data Model](#Data-Model)
- [ETL Processing](#ETL-Processing)
- [Project Files](#Project-Files)
- [References](#References)

## Technologies used

- Python
- PostgreSQL

## Source data information

There are two set of source files available for processing. `song_data` and `log_data`.

### song_data
set of files that contains metadata about songs.
- single file contains metadata about a single song.
- json format.
- sample data:

![Song Data Sample](https://www.lucidchart.com/publicSegments/view/edf0ddcc-868e-42a5-8942-bfad22f20fc0/image.png)


### log_data
set of files that contains transactional data about the songs played in the app by various users.
- single file contains transactional data for a given day. a list of json data records are present in the file.
- json format.
- sample data:

![Log Data Sample](https://www.lucidchart.com/publicSegments/view/a60a0666-5cf8-4ab8-a575-b6b64bb6c598/image.png)


## Data Model
    Data is modeled in form of STAR schema postgreSQL tables.
    `SPARKIFYDB` database contains the below tables:

    `SONGPLAYS` table is a Fact table containing all the songs played by various users of the app.
    `TIME`,`SONGS`,`USERS` and `ARTISTS` tables are Dimentsion tables containing metadata about the songplay data.
    
Below is the ER diagram which provides more details about the database tables:
![Sparkify Relational Data Model](https://www.lucidchart.com/publicSegments/view/6b022f3c-4036-4f3a-8855-40d502463dc3/image.jpeg)

## ETL Processing

### Data Profile:

* songs and artists tables are populated from the `song_data` source file. 
* Data profiled shows that `year, artist_location, artist_latitude and artist_longitude` are not always present. 
* An Artist could have multiple songs. some song_names have special characters in the source file encoded in utf8 format and target database is set to the same in order to preserve data accuracy. 
* Users, time and songplay tables are populated from the `log_data` source file(except for user_id and artist_id which are linked from songs and artists table respectively).
* A user could have multiple songplays.

### Data Processing
* Pandas Dataframes are used to hold the data from the json files.
* Transformations and data cleansing is done as mentioned in the next section before inserting data.
* sql queries are used to insert data into the tables and are invoked from the python script using the psycopg2 package for postGRE sql.

### Transformations and data cleansing:

* few of the songs do not have year attribute in the source files. for such records year attribute is set to null before adding it to the database.
* 'ts' contains timestamp in milliseconds format and is converted to datetime before inserting into time/songplays table.
* few of the artists do not have location, latitude and longitude attributes in the source file. for such records location, latitude and longitude are set to null before adding it to the database.
* song_data file contains data about a single file and is processed file by file.
* log_data file contains data multiple songplay records and is processed file by file.
* before adding data to users table - data from the dataframe is deduped before inserting records. (Although there is a ON CONFLICT clause in the table database - this is done for faster processing).
* data from log_file is sorted based on 'ts' - so that level attribute for a user will be latest when inserted in the tables.


## Project Files
`create_tables.py` and `sql_queries.py` are used to create the required database schema as per the model and has insert queries for the respective tables.
`etl.py` is used for processing the source JSON files, data transformations and data cleansing and invokation to insert the data into PostgreSQL tables.

## References

* https://pandas.pydata.org/pandas-docs/stable/reference/index.html
* https://docs.python.org/3/library/datetime.html
* https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_json.html
* https://www.lucidchart.com
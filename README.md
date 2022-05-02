# DataEngineering
Project files for Data Lake excercise
# Purpose of this database:
<p> This project is designed for a fictional company called sparkify. 
  <br> The objective is to to process the big data (song and log data) generated by the users using spark. 
  <br> This will enable in generating further analytical insights. </p>
# Schema
<p>The schema of the datalake is as follows: <br>
## Fact Table
songplays - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
## Dimension Tables
users - users in the app
user_id, first_name, last_name, gender, level
songs - songs in music database
song_id, title, artist_id, year, duration
artists - artists in music database
artist_id, name, location, lattitude, longitude
time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday </p>

# ELT pipeline:
<p>
The ELT pipeline first processes the song data and the log_data to create the Dimension Tables <br>
The songplays fact table is then created by joining and filtering the relevant dimension tables </p>

# DataEngineering
Project files for Data Lake excercise
# Purpose of this database:
<p> This project is designed for a fictional company called sparkify. 
  <br> The objective is to to process the big data (song and log data) generated by the users using spark. 
  <br> This will enable in generating further analytical insights. </p>
  
# Schema
The schema of the datalake is as follows: <br>

## Fact Table
<ol>
<li> songplays - records in log data associated with song plays i.e. records with page `NextSong` <br>
<ol> 
  <li> songplay_id, <li> start_time, <li> user_id, <li> level, <li> song_id, <li> artist_id, <li> session_id, <li> location, <li> user_agent</ol>
  </ol>
  
## Dimension Tables
<ol>
<li> users - users in the app
  <ol>
    <li> user_id, <li> first_name, <li> last_name, <li> gender, <li> level
    </ol>
  <li> songs - songs in music database
    <ol>
<li> song_id, <li> title, <li> artist_id, <li> year, <li> duration
    </ol>
<li> artists - artists in music database
  <ol>
<li> artist_id, <li> name, <li> location, <li> lattitude, <li> longitude
</ol>
    <li> time - timestamps of records in songplays broken down into specific units
      <ol>
<li> start_time, <li> hour, <li> day, <li> week, <li> month, <li> year, <li> weekday 
      </ol></ol>

# ELT pipeline:
<p>
The ELT pipeline first processes the song data and the log_data to create the Dimension Tables <br>
The songplays fact table is then created by joining and filtering the relevant dimension tables </p>

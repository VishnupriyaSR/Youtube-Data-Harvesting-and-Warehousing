# Youtube-Data-Harvesting-and-Warehousing

**Inrtoduction**

YouTube is one of the most popular websites on the planet. As of May 2019, more than 500 hours of video content is uploaded to the platform every single minute.

With over 2 billion users, the video-sharing platform is generating billions of views with over 1 billion hours of videos watched every single day. 

This project extracts the particular youtube channel data by using the youtube channel id, processes the data, and stores it in the MongoDB database. It has the option to migrate the data to MySQL from MongoDB then analyse the data and give the results depending on the customer queries.

**Technologies Used:**

The following technologies are used in this project:

**1.Python:** The programming language used for building the application and scripting tasks.

**2.Streamlit:** A Python library used for creating interactive web applications and data visualizations.

**3.YouTube API:** Google API is used to retrieve channel and video data from YouTube.

**4.MongoDB:** MongoDB is a source-available, cross-platform, document-oriented database program.

**5.SQL (Postgre-SQL):** Postgres, is a free and open-source relational database management system (RDBMS) emphasizing extensibility and SQL compliance.

**Installation and Setup**

To run the YouTube Data Harvesting and Warehousing project, follow these steps:

**1.Install Python:** Install the Python programming language on your machine.

**2.Install Required Packages:** Install the necessary Python packages using pip or conda package manager. 
pip install google-api-python-client, pymongo,pandas,numpy,streamlit,psycopg2

**3.Import Libraries:**

**Youtube API libraries**

import googleapiclient.discovery

from googleapiclient.discovery import build

**MongoDB**

import pymongo

from pymongo import MongoClient

**SQL libraries**

import psycopg2

**pandas and numpy**

import pandas as pd

import numpy as np

**Dashboard libraries**

import streamlit as st

**4.Set Up Google API:** Set up a Google API project and obtain the necessary API credentials for accessing the YouTube API.
https://console.cloud.google.com/welcome?project=youtube-second-409106

**5.Configure Database:** Set up a MongoDB database and SQL database (PostgreSQLSQL) for storing the data.

**6.Configure Application:** Update the configuration file or environment variables with the necessary API credentials and database connection details.

**7.Run the Application:** Launch the Streamlit application using the command-line interface-streamlit run app.py

**User Guide**

**Step 1.Data collection**

Search channel_id, copy and paste on the input box.

**Step 2**. **Data Extraction**

Click the Extract data button and display the channel information based on particular channel_id and its corresponding channel datails,video details and comment 
details will be extracted from Youtube.

**Step 3.** **Data Storage**

Click the Upload Data to Mongodb button and store the retrieved data in a MongoDB database.

**Step 4**.**Data Migration**

Click the Migrate datas to SQL button and data from the data lake will be migrated to PostgreSQL database for efficient querying and analysis.

**Step 5**.**SQL queries**

Select a Question from the dropdown option you can get the results in Dataframe format.

**References**

Python Documentation: https://docs.python.org/

Streamlit Documentation: https://docs.streamlit.io/

YouTube API Documentation: https://developers.google.com/youtube

MongoDB Documentation: https://docs.mongodb.com/

PostgreSQL Documentation:https://www.postgresql.org/docs/



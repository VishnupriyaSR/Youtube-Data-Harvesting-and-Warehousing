#LIST OF CHANNELS
#channel_lists={
# channel1_id:"UCFwZEnF7MRFRFhZWZ0OmCQw", #Datum Learning
# channel2_id:"UCnz-ZXXER4jOvuED5trXfEA",  #techTFQ
# channe3_id:"UCTHAIlSOqquwOKnd_nCJgAQ", #My Lesson
# channel4_id:"UCITaV_WWRm6bPzYhJZ5Jnmw",  #harizone
# channel5_id:"UCwr-evhuzGZgDFrq_1pLt_A", #Error Makes Clever Academy
# channel6_id:"UCFAr3FQxRhSzVNOD3vq1gMQ", #learn with Lokesh Lalwani
# channel7_id:"UCJQJAI7IjbLcpsjWdSzYz0Q", #Thu Vu data analytics
# channel8_id:"UCcskSCtpiScqJrHTqrqrmbg", #digital Sculler
# channel9_id:"UCWv7vMbMWH4-V0ZXdmDpPBA", #programmin with mosh
# channel10_id:"UCw_LFe2pS8x3NyipGNJgeEA" #learn with lukas# }      

# Youtube API libraries
import googleapiclient.discovery
from googleapiclient.discovery import build

#PANDAS AND NUMPY
import pandas as pd
import numpy as np

#MONGODB LIBRARIES
import pymongo
from pymongo import MongoClient

#POSTgre-SQL Libraries
import psycopg2

#streamlit Libraries 
import streamlit as st
from PIL import Image

#To Access Youtube API
api_service_name = "youtube"
api_version = "v3"
api_key="AIzaSyBhfIpZOG17A8N_Nrn0klvngRqGXL3kWq8"
youtube = build(api_service_name, api_version,developerKey=api_key)

#Establishing connection:Port number Default: 27017 (MONGODB)
conn = pymongo.MongoClient('mongodb://localhost:27017/')
#create a database
db=conn.youtube_data_harvesting

#Connect to the POSTgre-SQL Server
mydb=psycopg2.connect(host="localhost",user="postgres",password="vishnu",database="youtube",port=5432)
cursor=mydb.cursor()

##FUNCTION TO RETRIEVE CHANNEL INFORMATION FROM CHANNEL-ID

def getchannel_details(channel_id):
     
    channel_data=[]
    
    request = youtube.channels().list(part="snippet,contentDetails,statistics",id=channel_id)
    response = request.execute()
  
    for i in response["items"]:
        data = dict(Channel_id = i["id"],
                    Channel_name=i['snippet']['title'],
                    Channel_description=i['snippet']['description'],
                    Channel_playlist_id=i['contentDetails']['relatedPlaylists']['uploads'],
                    Channel_scount=i['statistics']['subscriberCount'],
                    Channel_videocount=i['statistics']['videoCount'],
                    Channel_viewcount=i['statistics']['viewCount']                    
                    )

    channel_data.append(data)
    return channel_data


#Function to retrieve videoIDs from Channel Playlist-ID(Give Channe_ID as input to this function)

def getvideo_id(channel_id):
    
    request = youtube.channels().list(part="snippet,contentDetails,statistics",id=channel_id)
    response1 = request.execute() 
    playlist_id=response1['items'][0]['contentDetails']['relatedPlaylists']['uploads']  


    video_ids=[]
    
    next_page_token = None
    
    
    while True:
        
        request = youtube.playlistItems().list(part="snippet",playlistId=playlist_id,maxResults=50,
                                                        pageToken=next_page_token)
        response = request.execute()

        for i in range(len(response["items"])):
            video_ids.append(response['items'][i]['snippet']['resourceId']['videoId'])
        
        next_page_token = response.get('nextPageToken')

        if next_page_token is None:
            break
        
    return video_ids
                             
    
##FUNCTION TO RETRIEVE VIDEO DETAILS FROM VIDEO-ID

def getvideo_data(video_id):
    
    video_data=[]
    
       
    for v_id in video_id:
        response = youtube.videos().list(part="snippet,contentDetails,statistics",id=v_id).execute()
        
        for v in response["items"]:
            vdata=dict(
            Channel_id=v["snippet"]["channelId"],
            Channel_name=v["snippet"]["channelTitle"],
            Video_id=v["id"],
            Video_name=v["snippet"]["title"],
            Video_description=v["snippet"]["description"],
            Video_publishedAt=v["snippet"]["publishedAt"],
            Duration=v["contentDetails"]["duration"],
            Thumbnails=v["snippet"]["thumbnails"]["default"]["url"],
            Commentcount=v["statistics"].get("commentCount"),
            Likecount=v["statistics"].get("likeCount"),
            Viewcount=v["statistics"]["viewCount"],
            Favcount=v["statistics"]["favoriteCount"])

        video_data.append(vdata)
                                    
    return video_data

##FUNCTION TO RETRIEVE COMMENT DETAILS(GIVE VIDEO_ID AS INPUT)

def getcomment_data(video_id):
    comment_data=[]
    try:
        next_page_token=None
        
        while True:
            for vid in video_id:
                request = youtube.commentThreads().list(part="snippet",videoId=vid,maxResults=100,pageToken=next_page_token)
                response = request.execute()

                for c in response["items"]:
                    data=dict(
                            Comment_id=c["id"],
                            Video_id=c["snippet"]["videoId"],
                            Comment_author_name=c["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                            Comment_text=c["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                            Comment_publishedAt=c["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

                    comment_data.append(data)
                    
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break

        
    except:
        pass
    
    return comment_data

##FUNCTION TO RETRIEVE PLAYLIST INFORMATION FROM CHANNEL-ID

def getplaylist_data(channel_id):
    
    playlist_data=[]
    
    next_page_token=None
    
    
    while True:
        request = youtube.playlists().list(part="snippet,contentDetails",channelId=channel_id,maxResults=50,pageToken=next_page_token)                        
        response = request.execute()

        for item in response["items"]:
            data=dict(Playlist_id=item["id"],
                      Playlist_name=item["snippet"]["title"],
                      Playlist_published=item["snippet"]["publishedAt"],
                      Channel_id=item["snippet"]["channelId"],
                      Channel_name=item["snippet"]["channelTitle"],
                      Playlist_description=item["snippet"]["description"],
                      Video_count=item["contentDetails"]["itemCount"])

            playlist_data.append(data)
            
           
               
        next_page_token = response.get('nextPageToken')
        
        if next_page_token is None:
            break
        
    return playlist_data 

def channel_info(channel_id):
    channel_datas=getchannel_details(channel_id)
    playlist_details=getplaylist_data(channel_id)
    video_ids_list=getvideo_id(channel_id)
    video_datas=getvideo_data(video_ids_list)
    comment_details=getcomment_data(video_ids_list)
    
# store all the datas into MONGODB

    collection1=db.channel_details
    collection1.insert_many(channel_datas)

    collection2=db.video_datas
    collection2.insert_many(video_datas)

    collection3=db.comment_details
    collection3.insert_many(comment_details)
    
    collection4=db.playlist_details
    collection4.insert_many(playlist_details)
    
    return "upload completed successfully"


    
#Home page using Streamlit
icon = Image.open("YouTube-Symbol.png")
st.set_page_config(page_title= "YouTube Data Harvesting",
                   page_icon= icon,
                   layout= "centered",
                   initial_sidebar_state= "expanded",
                   menu_items=None)

st.title(':red[YouTube Data Harvesting and DatawareHousing using SQL,MongoDB and Streamlit]')

with st.sidebar:
    st.header("Technical Framework")
    st.write("Python Code")
    st.write("API Integration")
    st.write("MongoDB")
    st.write("Postgre-SQL")
    st.write("Streamlit")
    
st.header("Data Collection")

channel_id = st.text_input("Enter the Channel ID")
   
#Extracting Youtube datas(Channel_id as input given by the user)

st.header("Data Extraction")
if st.button("Extract Youtube Data"):
    channel_datas=getchannel_details(channel_id)
    st.table(channel_datas)
   
         
#Uploading datas to MongoDB
st.header("Data Storage in MONGODB")
if st.button("Upload data to MONGODB"):
    ch_ids=[]
    collection1=db.channel_details
    for ch in collection1.find({},{"_id":0}):
        ch_ids.append(ch["Channel_id"])
    if channel_id in ch_ids:
        st.success("Channel Details of given Channel-ID already exists")
    else:
        data= channel_info(channel_id)
        st.success(data)

#Migrate datas from MONGODB to SQL(Given Channel_id as Input)

st.header("Data Migration to SQL")

if st.button("Migrate datas to SQL"):
    conn = pymongo.MongoClient('mongodb://localhost:27017/')
    db=conn.youtube_data_harvesting
    
    def insert_into_channel(channel_id):
        collections = db.channel_details
        query = "insert into channel values(%s,%s,%s,%s,%s,%s,%s)"

        for i in collections.find({"Channel_id" : channel_id},{'_id' : 0}):
            cursor.execute(query,tuple(i.values()))
            mydb.commit()

    def insert_into_videos(channel_id):
        collections1 = db.video_datas
        query1 = """INSERT INTO videos VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

        for j in collections1.find({"Channel_id" : channel_id},{'_id' : 0}):
            cursor.execute(query1,tuple(j.values()))
            mydb.commit()



    def insert_into_comments(channel_id):
        collections1 = db.video_datas
        collections2 = db.comment_details
        query2 = """INSERT INTO comment VALUES(%s,%s,%s,%s,%s)"""

        for vid in collections1.find({"Channel_id" : channel_id},{'_id' : 0}):
            for co in collections2.find({"Video_id":vid['Video_id']},{'_id' : 0}):
                cursor.execute(query2,tuple(co.values()))
                mydb.commit()

    def insert_into_playlist(channel_id):
        collection3=db.playlist_details
        query="""insert into playlist values(%s,%s,%s,%s,%s,%s,%s)"""
        for p in collection3.find({"Channel_id" : channel_id},{'_id' : 0}):
            cursor.execute(query,tuple(p.values()))
            mydb.commit()
            
#inserting values into SQL database

    insert_into_channel(channel_id)
    insert_into_videos(channel_id)
    insert_into_comments(channel_id)
    insert_into_playlist(channel_id)
    st.success("Transformation to MySQL Successful !!")

st.header("SQL QUERIES")
question=st.selectbox("Select your question",("1.Names of all the videos and their corresponding channels",
                      "2.Channels have the most number of videos",
                      "3.Channels with top 10 most viewed videos",
                      "4.Comments made on each video, and their corresponding video names",
                      "5.Videos with the highest number of likes",
                      "6.Total number of likes for each video",
                      "7.Total number of views for each channel",
                      "8.Names of all the channels that have published videos in the year 2022",
                      "9.Average duration of all videos in each channel",
                      "10.Videos with the highest number of comments"
                     ))

if question=="1.Names of all the videos and their corresponding channels":
    query= "select video_name as VIDEO_NAME,channel_name as CHANNEL_NAME from videos"
    cursor.execute(query)
    mydb.commit()
    a1=cursor.fetchall()
    df=pd.DataFrame(a1,columns=["VIDEO_NAME","CHANNEL_NAME"])
    st.write(df)

if question=="2.Channels have the most number of videos":
    cursor.execute("select channel_name,channel_videocount from channel order by channel_videocount desc limit 10")
    mydb.commit()
    a1=cursor.fetchall()
    df=pd.DataFrame(a1,columns=["CHANNEL_NAME","TOTAL_VIDEOCOUNT"])
    st.write(df)

if question=="3.Channels with top 10 most viewed videos":
    cursor.execute("select channel_name,video_name,viewcount from videos order by viewcount desc limit 15")
    mydb.commit()
    a1=cursor.fetchall()
    df=pd.DataFrame(a1,columns=["CHANNEL_NAME","VIDEO_NAME","TOTAL VIEWS"])
    st.write(df)

if question=="4.Comments made on each video, and their corresponding video names":
    cursor.execute("select channel_name,video_name,commentcount from videos order by commentcount desc")
    mydb.commit()
    a1=cursor.fetchall()
    df=pd.DataFrame(a1,columns=["CHANNEL_NAME","VIDEO_NAME","TOTAL COMMENTS"])
    st.write(df)

if question=="5.Videos with the highest number of likes":
    cursor.execute("select channel_name,video_name,likecount from videos order by likecount desc limit 10")
    mydb.commit()
    a1=cursor.fetchall()
    df=pd.DataFrame(a1,columns=["CHANNEL_NAME","VIDEO_NAME","TOTAL LIKES"])
    st.write(df)


if question=="6.Total number of likes for each video":
    cursor.execute("select video_name,sum(likecount) as total_likes from videos group by video_name order by total_likes desc")
    mydb.commit()
    a1=cursor.fetchall()
    df=pd.DataFrame(a1,columns=["CHANNEL_NAME","TOTAL LIKES"])
    st.write(df)


if question=="7.Total number of views for each channel":
    cursor.execute("select channel_name,sum(viewcount) as total_views from videos group by channel_name order by total_views desc")
    mydb.commit()
    a1=cursor.fetchall()
    df=pd.DataFrame(a1,columns=["CHANNEL_NAME","TOTAL_VIEWS"])
    st.write(df)

if question=="8.Names of all the channels that have published videos in the year 2022":
    cursor.execute("select distinct(channel_name) from videos where video_publishedat::text like '2022%'")
    mydb.commit()
    a1=cursor.fetchall()
    df=pd.DataFrame(a1,columns=["CHANNEL_NAME"])
    st.write(df)

    
if question=="9.Average duration of all videos in each channel":
    cursor.execute("select channel_name,avg(duration) from videos group by channel_name")
    mydb.commit()
    a1=cursor.fetchall()
    df2=pd.DataFrame(a1,columns=["CHANNEL_NAME","AVERAGE_DURATION"])
#convert avreage_duration into string for displaying on Streamlit
    d=[]
    for i,j in df2.iterrows():
        channel_name=j["CHANNEL_NAME"]
        average_duration=j["AVERAGE_DURATION"]
        avg_dur_str=str(average_duration)
        d.append(dict(channel_name=channel_name,average_duration=avg_dur_str))
    df1=pd.DataFrame(d)
    st.write(df1)
        
    

if question=="10.Videos with the highest number of comments":
    cursor.execute("select video_name,sum(commentcount) as total_comments from videos group by video_name order by total_comments desc")
    mydb.commit()
    a1=cursor.fetchall()
    df=pd.DataFrame(a1,columns=["VIDEO_NAME","TOTAL_COMMENTS"])
    st.write(df)






    

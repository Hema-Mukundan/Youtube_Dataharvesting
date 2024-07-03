from googleapiclient import errors # type: ignore
import os
import streamlit as st
from googleapiclient.discovery import build # type: ignore
import pandas as pd
import json
import pymongo # type: ignore
import mysql.connector # type: ignore
import re
from datetime import datetime
from pandas import json_normalize

# YouTube API setup
api_key = 'AIzaSyAbUPGkECjLGTipq6KipdkOrfg5KXX5QBg'
youtube = build('youtube', 'v3', developerKey=api_key)

# MongoDB setup
client = pymongo.MongoClient("mongodb+srv://hema_mukundan:ALMh_gr43SdABra@cluster0.ivcubxu.mongodb.net/")
mydb = client["YT_Project"]
channel_collection = mydb['yt_channels']

# MySQL setup
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Data23Wrangl#",
    database='yt_db'
)
cursor = connection.cursor(buffered=True)

# Streamlit app setup
st.set_page_config(page_title="YouTube Data Harvesting and Warehousing", layout="wide")
st.title("YouTube Data Harvesting and Warehousing")

# Function to get channel information
def get_channel_info(channel_id):
    try:
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )
        response = request.execute()

        if 'items' in response and len(response['items']) > 0:
            channel = response['items'][0]
            uploads_playlist_id = channel['contentDetails']['relatedPlaylists']['uploads']
            if uploads_playlist_id:
                channel_info = {
                    'channel_id': channel_id,
                    'channel_name': channel['snippet']['title'],
                    'channel_description': channel['snippet']['description'],
                    'channel_views': channel['statistics']['viewCount'], 
                    'channel_subscriber_count': channel["statistics"]["subscriberCount"],
                    'channel_video_count': channel["statistics"]["videoCount"],
                    'channel_type': 'N/A',
                    'channel_status': 'N/A'
                }
                return channel_info, uploads_playlist_id
    except Exception as e:
        st.error(f"An error occurred: {e}")
    return None, None

# Function to get playlist information
def get_playlist_info(playlist_id):
    try:
        request = youtube.playlists().list(
            part="snippet",
            id=playlist_id
        )
        response = request.execute()
        
        if 'items' in response and len(response['items']) > 0:
            playlist = response['items'][0]
            playlist_info = {
                'playlist_id': playlist_id,
                'playlist_name': playlist['snippet']['title']
            }
            return playlist_info
    except Exception as e:
        st.error(f"An error occurred: {e}")
    return None

# Function to get videos in a playlist
def get_videos_in_playlist(playlist_id):
    try:
        request = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=playlist_id,
            maxResults=50
        )
        response = request.execute()
        
        videos = []
        for item in response['items']:
            video_id = item['contentDetails']['videoId']
            video_info = get_video_info(video_id)
            if video_info:
                videos.append(video_info)
        
        return videos
    except Exception as e:
        st.error(f"An error occurred while fetching videos: {e}")
        return []

# Function to get video information
def get_video_info(video_id):
    try:
        request = youtube.videos().list(
            part="snippet,statistics,contentDetails",
            id=video_id
        )
        response = request.execute()
        
        if 'items' in response and len(response['items']) > 0:
            video = response['items'][0]
            video_info = {
                'video_id': video_id,
                'video_name': video['snippet']['title'],
                'video_description': video['snippet']['description'],
                'published_date': video['snippet']['publishedAt'],
                'view_count': video['statistics'].get('viewCount', 0),
                'like_count': video['statistics'].get('likeCount', 0),
                'dislike_count': video['statistics'].get('dislikeCount', 0),
                'favorite_count': video['statistics'].get('favoriteCount', 0),
                'comment_count': video['statistics'].get('commentCount', 0),
                'duration': video['contentDetails']['duration'],
                'thumbnail': video['snippet']['thumbnails']['default']['url'],
                'caption_status': video['contentDetails']['caption']
            }
            return video_info
    except Exception as e:
        st.error(f"An error occurred while fetching video information: {e}")
    return None

# Function to get comments for a video from YouTube API
def get_comments(video_id):
    try:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=50
        )
        response = request.execute()

        comments = []
        for item in response['items']:
            snippet = item.get('snippet', {}).get('topLevelComment', {}).get('snippet', {})
            comment_id = item.get('id', '')
            comment_text = snippet.get('textDisplay', '')
            comment_author = snippet.get('authorDisplayName', '')
            comment_published_date = snippet.get('publishedAt', '')

            if comment_id and comment_text and comment_author and comment_published_date:
                comments.append({
                    'comment_id': comment_id,
                    'comment_text': comment_text,
                    'comment_author': comment_author,
                    'comment_published_date': comment_published_date
                })
        return comments
    except googleapiclient.errors.HttpError as e: # type: ignore
        error_content = json.loads(e.content.decode('utf-8'))
        if error_content.get('error', {}).get('errors', [{}])[0].get('reason') == 'commentsDisabled':
            st.warning(f"Comments are disabled for video ID: {video_id}")
        else:
            st.error(f"An error occurred while fetching comments: {e}")
        return []

# Function to migrate channel data to MongoDB
def migrate_data_to_mongodb(channel_id):
    existing_channel = channel_collection.find_one({'channel_id': channel_id})
    if existing_channel:
        st.warning(f"Channel ID {channel_id} already exists. Skipping migration to MongoDB.")
    else:
        channel_info, uploads_playlist_id = get_channel_info(channel_id)
        if channel_info and uploads_playlist_id:
            playlist_info = get_playlist_info(uploads_playlist_id)
            if playlist_info:
                channel_info['playlists'] = [playlist_info]
                videos = get_videos_in_playlist(uploads_playlist_id)
                playlist_info['videos'] = videos

            channel_collection.update_one(
                {'channel_id': channel_info['channel_id']},
                {'$set': channel_info},
                upsert=True
            )
            st.success(f"Data migration of channel ID {channel_id} to MongoDB completed successfully!")
        else:
            st.error("Channel or Playlist not found")

# Function to fetch channel data from MongoDB
def fetch_channel_data():
    return list(channel_collection.find({}))

def convert_duration(duration_str):
    # Initialize variables for hours, minutes, and seconds
    hours = 0
    minutes = 0
    seconds = 0

    # Regular expression to parse ISO 8601 duration
    pattern = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
    match = pattern.match(duration_str)

    if match:
        if match.group(1):  # hours
            hours = int(match.group(1))
        if match.group(2):  # minutes
            minutes = int(match.group(2))
        if match.group(3):  # seconds
            seconds = int(match.group(3))

    # Calculate total duration in seconds
    total_seconds = hours * 3600 + minutes * 60 + seconds
    return total_seconds

# Function to migrate data to MySQL
def migrate_data_to_mysql(channel_data):
    cursor = connection.cursor(buffered=True)

    try:
        for channel in channel_data:
            # Check if channel already exists in MySQL
            cursor.execute("SELECT * FROM channel WHERE channel_id = %s", (channel['channel_id'],))
            existing_channel = cursor.fetchone()

            if existing_channel:
                st.warning(f"Channel data for '{channel['channel_name']}' already exists. Skipping migration to MySQL Database.")
                continue

            if all(key in channel for key in ('channel_id', 'channel_name', 'channel_description', 'channel_views', 'channel_type', 'channel_status')):
                channel_insert_query = """
                    INSERT INTO channel (channel_id, channel_name, channel_description, channel_views, channel_type, channel_status)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    channel_name=VALUES(channel_name), channel_description=VALUES(channel_description), channel_views=VALUES(channel_views),
                    channel_type=VALUES(channel_type), channel_status=VALUES(channel_status);
                """
                cursor.execute(channel_insert_query, (
                    channel['channel_id'], channel['channel_name'], channel['channel_description'], channel['channel_views'],
                    channel['channel_type'], channel['channel_status']
                ))
                st.success(f"Channel data of '{channel['channel_name']}' migrated successfully to MySQL Database.")
            else:
                st.error(f"Missing data in channel: {channel}")
        
        connection.commit()

        for channel in channel_data:
            for playlist in channel.get('playlists', []):
                playlist_insert_query = """
                    INSERT INTO playlist (playlist_id, playlist_name, channel_id)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    playlist_name=VALUES(playlist_name), channel_id=VALUES(channel_id);
                """
                cursor.execute(playlist_insert_query, (
                    playlist['playlist_id'], playlist['playlist_name'], channel['channel_id']
                ))
                connection.commit()

                for video in playlist.get('videos', []):
                    formatted_duration = convert_duration(video['duration'])
                    formatted_datetime = datetime.strptime(video['published_date'], "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
                
                    video_insert_query = """
                        INSERT INTO video (video_id, playlist_id, video_name, video_description, published_date,
                        view_count, like_count, dislike_count, favorite_count, comment_count,
                        duration, thumbnail, caption_status)
                        VALUES (%s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        playlist_id=VALUES(playlist_id), video_name=VALUES(video_name),
                        video_description=VALUES(video_description),
                        published_date=VALUES(published_date), view_count=VALUES(view_count),
                        like_count=VALUES(like_count), dislike_count=VALUES(dislike_count),
                        favorite_count=VALUES(favorite_count), comment_count=VALUES(comment_count),
                        duration=VALUES(duration), thumbnail=VALUES(thumbnail),
                        caption_status=VALUES(caption_status);
                    """
                    cursor.execute(video_insert_query, (
                        video['video_id'], playlist['playlist_id'], video['video_name'], video['video_description'], formatted_datetime,
                        video['view_count'], video['like_count'], video['dislike_count'], video['favorite_count'],
                        video['comment_count'], formatted_duration, video['thumbnail'], video['caption_status']
                    ))
                    connection.commit()
                    comments = get_comments(video['video_id'])
                    for comment in comments:
                        formatted_datetime = datetime.strptime(video['published_date'], "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
                        comment_insert_query = """
                            INSERT INTO comment (comment_id, comment_text, comment_author, comment_published_date, video_id)
                            VALUES (%s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                            comment_text=VALUES(comment_text), comment_author=VALUES(comment_author),
                            comment_published_date=VALUES(comment_published_date), video_id=VALUES(video_id);
                        """
                        cursor.execute(comment_insert_query, (
                            comment['comment_id'], comment['comment_text'], comment['comment_author'],
                            formatted_datetime, video['video_id']
                        ))
                    connection.commit()
    except mysql.connector.Error as err:
        st.error(f"Error: {err}")
        connection.rollback()
    finally:
        cursor.close()

# # Streamlit app setup
# st.set_page_config(page_title="YouTube Data Harvesting and Warehousing", layout="wide")
# st.title("YouTube Data Harvesting and Warehousing")

tabs = st.tabs(["Search", "Migrate to MongoDB", "Migrate to MySQL", "Channel Insights"])

# Tab 1: Search for YouTube Channel
with tabs[0]:
    channel_id = st.text_input("Enter YouTube Channel ID")
    if st.button("Search"):
        channel_info, uploads_playlist_id = get_channel_info(channel_id)
        if channel_info:
            st.write("Channel found:", channel_info)
        else:
            st.write("Invalid Channel ID or Channel not found")

# Tab 2: Migrate to MongoDB
with tabs[1]:
    channel_id = st.text_input("Enter YouTube Channel ID for MongoDB Migration")
    if st.button("Migrate to MongoDB"):
        migrate_data_to_mongodb(channel_id)
        st.write("MongoDB Channel Collection:")
        channel_data = fetch_channel_data()

        # # Convert complex data types to JSON strings for display
        # for channel in channel_data:
        #     channel['playlists'] = json.dumps(channel.get('playlists', []))

        # st.write(pd.DataFrame(channel_data))
        if channel_data:
            for channel in channel_data:
                # Convert ObjectId to string and any other non-serializable fields
                if '_id' in channel:
                    channel['_id'] = str(channel['_id'])
                # Display each channel's data as JSON
                st.json(channel)
        else:
            st.info("No data found in MongoDB collection.")

# Tab 3: Migrate to MySQL
with tabs[2]:
    channel_data = fetch_channel_data()
    if channel_data:
        unique_channel_names = list(set([channel['channel_name'] for channel in channel_data]))
        selected_channel_name = st.selectbox("Select Channel to Migrate to MySQL", unique_channel_names)
        if st.button("Migrate to MySQL"):
            selected_channel = [channel for channel in channel_data if channel['channel_name'] == selected_channel_name][0]
            migrate_data_to_mysql([selected_channel])
            cursor = connection.cursor()
            cursor.execute("SELECT * FROM channel")
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=[i[0] for i in cursor.description])
            st.write(df)
            cursor.close()
    else:
        st.write("No channel data available for migration")

# Tab 4: Channel Insights
with tabs[3]:
    st.write("This section will contain channel insights based on a questionnaire.")
    questionnaire_options = [
        "Select",
        "Names of all videos and their corresponding channels",
        "Channels with the most number of videos",
        "Top 10 most viewed videos and their respective channels",
        "Number of comments on each video",
        "Videos with the highest number of likes",
        "Total number of likes and dislikes for each video",
        "Total number of views for each channel",
        "Channels that have published videos in the year 2022",
        "Average duration of all videos in each channel",
        "Videos with the highest number of comments"
    ]
    selected_questionnaire = st.selectbox("Insights", questionnaire_options)

    # Define SQL queries based on selection
    def get_query(option):
        if option == "Names of all videos and their corresponding channels":
            return """
            SELECT video.video_name, channel.channel_name 
            FROM video
            JOIN playlist ON video.playlist_id = playlist.playlist_id
            JOIN channel ON playlist.channel_id = channel.channel_id;
            """
        elif option == "Channels with the most number of videos":
            return """
            SELECT channel.channel_name, COUNT(video.video_id) AS video_count
            FROM channel
            LEFT JOIN playlist ON channel.channel_id = playlist.channel_id
            LEFT JOIN video ON playlist.playlist_id = video.playlist_id
            GROUP BY channel.channel_name
            ORDER BY video_count DESC;
            """
        elif option == "Top 10 most viewed videos and their respective channels":
            return """
            SELECT video.video_name, video.view_count, channel.channel_name
            FROM video
            JOIN playlist ON video.playlist_id = playlist.playlist_id
            JOIN channel ON playlist.channel_id = channel.channel_id
            ORDER BY video.view_count DESC
            LIMIT 10;

            """
        elif option == "Number of comments on each video":
            return """
            SELECT video_name, comment_count
            FROM video;
            """
        elif option == "Videos with the highest number of likes":
            return """
            SELECT v.video_name, v.like_count, c.channel_name
            FROM video AS v
            INNER JOIN playlist AS p ON v.playlist_id = p.playlist_id
            INNER JOIN channel AS c ON p.channel_id = c.channel_id
            ORDER BY v.like_count DESC
            LIMIT 10;
            """
        elif option == "Total number of likes and dislikes for each video":
            return """
            SELECT video_name, SUM(like_count) AS Total_Likes, SUM(dislike_count) AS Total_Dislikes
            FROM video
            GROUP BY video_name;
            """
        elif option == "Total number of views for each channel":
            return """
            SELECT channel_name, SUM(channel_views) AS Total_Views
            FROM channel
            GROUP BY channel_name;
            """
        elif option == "Channels that have published videos in the year 2022":
            return """
            SELECT c.channel_name, v.published_date
            FROM video AS v
            INNER JOIN playlist AS p ON v.playlist_id = p.playlist_id
            INNER JOIN channel AS c ON p.channel_id = c.channel_id
            WHERE YEAR(published_date) = 2022;
            """
        elif option == "Average duration of all videos in each channel":
            return """
            SELECT channel_name, AVG(duration) AS Avg_Duration
            FROM video
            INNER JOIN playlist ON video.playlist_id = playlist.playlist_id
            INNER JOIN channel ON playlist.channel_id = channel.channel_id
            GROUP BY channel_name;
            """
        elif option == "Videos with the highest number of comments":
            return """
            SELECT v.video_name, c.channel_name, v.comment_count
            FROM video AS v
            JOIN playlist AS p ON v.playlist_id = p.playlist_id
            JOIN channel AS c ON p.channel_id = c.channel_id
            ORDER BY v.comment_count DESC
            LIMIT 10;
            """
        return None

    # Button to execute query
    if st.button("Execute Query"):
        if selected_questionnaire != 'Select':
            query = get_query(selected_questionnaire)
            if query:
                # Execute the query and fetch results
                try:
                    cursor.execute(query)
                    result = cursor.fetchall()
                    # Fetch column names from cursor description
                    columns = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(result, columns=columns)
                    # Display results
                    st.write(f"Results for: {selected_questionnaire}")
                    st.dataframe(df)
                except Exception as e:
                    st.error(f"Error executing query: {e}")
            else:
                st.error("Invalid query option selected.")
        else:
            st.warning("Please select a valid query option.")

# Close MySQL connection
cursor.close()
connection.close()
# youtube_scraper.py

import pandas as pd
from datetime import datetime, timedelta, timezone
import scrapetube
import logging
import json
import requests 
import re 

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_timedelta(time_str):
    """Convert a time string to a timedelta."""
    if 'hour' in time_str:
        hours = int(time_str.split()[0])
        return timedelta(hours=hours)
    elif 'day' in time_str:
        days = int(time_str.split()[0])
        return timedelta(days=days)
    elif 'week' in time_str:
        weeks = int(time_str.split()[0])
        return timedelta(weeks=weeks)
    elif 'year' in time_str:
        years = int(time_str.split()[0])
        return timedelta(days=years * 365)  # Approximation for years
    else:
        return timedelta.max  # Return a large timedelta for "out of range" cases

def get_recent_videos_for_handle(handle, hours=24):
    """Retrieve recent videos for a specified YouTube handle."""
    try:
        videos = scrapetube.get_channel(channel_username=handle)
    except Exception as e:
        logging.error(f"Failed to retrieve videos for {handle}: {e}")
        return pd.DataFrame()  # Return empty DataFrame on failure

    video_data = []
    
    for video in videos:
        publish_time_str = video['publishedTimeText']['simpleText']
        timedelta_since_publish = get_timedelta(publish_time_str)

        # Stop processing if older than specified hours
        if timedelta_since_publish > timedelta(hours=hours):
            break
        
        video_dict = {
            'videoPublishTime': publish_time_str,
            'videoID': video['videoId'],
            'videoTitle': video['title']['runs'][0]['text']
        }
        video_data.append(video_dict)

    return pd.DataFrame(video_data)

def get_recent_videos_for_handles(handles, hours=24):
    """Retrieve recent videos for multiple YouTube handles."""
    if isinstance(handles, str):
        handles = [handles]
    
    df_list = []
    for handle in handles:
        df = get_recent_videos_for_handle(handle, hours)
        if not df.empty:
            df['handle'] = handle  # Add handle column
            df_list.append(df)
    
    if df_list:
        return pd.concat(df_list, ignore_index=True)
    else:
        return pd.DataFrame()  # Return empty DataFrame if no videos found

    
def get_video_transcript(video_id):
    r = requests.get(f'https://www.tldwyoutube.com/api/analyze?v={video_id}') 
    s = r.json()['transcript'] 
    lst = s.split('\n') 
    # Cleaning the list 
    cleaned_list = [line for line in lst if line and not (line[0].isdigit() or "-->" in line)] 
    
    # Combine the cleaned lines into a single string 
    cleaned_string = ' '.join(cleaned_list)
    return cleaned_string


def scrape_youtube(youtube_handles, hours=80):
    """Main function to run the video retrieval and transcript collection."""
    recent_videos_df = get_recent_videos_for_handles(youtube_handles, hours)
    recent_videos_df['videoTranscript'] = recent_videos_df['videoID'].apply(get_video_transcript)

    logging.info("Retrieved recent videos and transcripts.")
    return recent_videos_df

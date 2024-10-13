# youtube_scraper.py

import pandas as pd
from datetime import datetime, timedelta, timezone
import scrapetube
from youtube_transcript_api import YouTubeTranscriptApi
import logging
from youtube_scraper_api.working.data import fetch_video_info
from youtube_scraper_api.working.transcript import fetch_transcript
import json

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

# def get_video_transcript(video_id):
#     """Retrieve the transcript for a specific video ID."""
#     try:
#         video_transcript_json = YouTubeTranscriptApi.get_transcript(video_id)
#         return ' '.join([i['text'] for i in video_transcript_json])
#     except Exception as e:
#         logging.error(f"Failed to retrieve transcript for {video_id}: {e}")
#         return None  # Return None if transcript retrieval fails
    
def load_api_key_from_json(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
        return data['youtube_api_key']
    
def get_video_transcript(video_id):
    api_key = load_api_key_from_json('youtube_scraper_api/testing/config.json')
    details = fetch_video_info(video_id, api_key)
    transcript = fetch_transcript(video_id)
    return transcript


def scrape_youtube(youtube_handles, hours=24):
    """Main function to run the video retrieval and transcript collection."""
    recent_videos_df = get_recent_videos_for_handles(youtube_handles, hours)
    recent_videos_df['videoTranscript'] = recent_videos_df['videoID'].apply(get_video_transcript)

    logging.info("Retrieved recent videos and transcripts.")
    return recent_videos_df

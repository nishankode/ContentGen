import logging
import pandas as pd
from youtube_scraper import scrape_youtube
from twitter_thread_prompt import create_twitter_thread_prompt
from openai_module import get_openai_completion
from email_sender import send_daily_digest

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    try:
        logging.info("Script started")

        # Load email and YouTube handle mappings from the JSON file
        email_handles = {
            "mdnishan006@gmail.com": ["backstagewithmillionaires", "mreflow"],
            "mnsn.n006@gmail.com": ["tahirmajithia", "rebelagent1223", "walikhanenglish"]
        }

        recent_videos_dfs = {}

        # Process each email and corresponding YouTube handles
        for email, handles in email_handles.items():
            logging.info(f"Fetching videos for handles: {handles} for email: {email}")

            recent_videos_df = scrape_youtube(handles, hours=24)
            logging.info(f"Retrieved {len(recent_videos_df)} videos for {email}")

            # Generating the prompt from transcript
            recent_videos_df['twitterThreadPrompt'] = recent_videos_df['videoTranscript'].apply(lambda x : create_twitter_thread_prompt(x))
            logging.info(f"Generated Twitter thread prompts for {email}")
            logging.info(str(recent_videos_df.to_dict(orient='records')))

            # Uncomment this line to generate the thread using OpenAI
        #     recent_videos_df['twitterThread'] = recent_videos_df['twitterThreadPrompt'].apply(lambda x : get_openai_completion(x))
        #     logging.info(f"Generated Twitter threads using OpenAI for {email}")

        #     # Store the DataFrame with the corresponding email
        #     recent_videos_dfs[email] = recent_videos_df

        # # Send the daily email digest for each email and corresponding DataFrame
        # for email, df in recent_videos_dfs.items():
        #     logging.info(f"Sending daily digest to {email}")
        #     send_daily_digest(df, [email])
        #     logging.info(f"Daily digest sent to {email}")

        # logging.info("Script completed successfully")

    except FileNotFoundError as e:
        logging.error(f"File not found: {e}")
    except pd.errors.EmptyDataError:
        logging.error("The CSV file is empty")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()

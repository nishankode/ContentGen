import pandas as pd
import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import logging

def load_email_credentials():
    """Load email credentials from environment variables."""
    sender_email = os.environ.get('GMAIL_USER')
    sender_password = os.environ.get('GMAIL_APP_PASSWORD')

    if not sender_email or not sender_password:
        raise ValueError("Email or password not set in environment variables.")

    # Log the email ID (but not the password)
    logging.info(f"Loaded email credentials for: {sender_email}")
    
    return sender_email, sender_password

def read_data(csv_file):
    """Read the CSV file and return a DataFrame."""
    df = pd.read_csv(csv_file)
    return df

def compose_email_content(df):
    """Compose the HTML email content from the DataFrame."""
    html_template = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Daily Twitter Threads Digest</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                line-height: 1.6;
                color: #333;
                max-width: 800px;
                margin: 0 auto;
                padding: 20px;
            }}
            h1 {{
                color: #1DA1F2;
                border-bottom: 2px solid #1DA1F2;
                padding-bottom: 10px;
            }}
            .thread-container {{
                background-color: #f8f8f8;
                border: 1px solid #e1e8ed;
                border-radius: 5px;
                padding: 15px;
                margin-bottom: 30px;
            }}
            .video-info {{
                background-color: #ffffff;
                padding: 15px;
                border-radius: 5px;
                margin-bottom: 15px;
            }}
            .twitter-thread {{
                background-color: #ffffff;
                border: 1px solid #e1e8ed;
                border-radius: 5px;
                padding: 15px;
            }}
            .tweet {{
                border-bottom: 1px solid #e1e8ed;
                padding: 10px 0;
            }}
            .tweet:last-child {{
                border-bottom: none;
            }}
            .channel-name {{
                font-weight: bold;
                color: #14171a;
            }}
            .video-title {{
                font-size: 1.2em;
                color: #1DA1F2;
            }}
            .publish-time {{
                color: #657786;
                font-style: italic;
            }}
        </style>
    </head>
    <body>
        <h1>Daily Twitter Threads Digest</h1>
        
        {thread_containers}
    </body>
    </html>
    """

    THREAD_CONTAINER_TEMPLATE = """
    <div class="thread-container">
        <div class="video-info">
            <p class="video-title">Video Title: {video_title}</p>
            <p class="channel-name">Channel Handle: @{channel_name}</p>
            <p class="publish-time">Published: {publish_time}</p>
        </div>
        
        <div class="twitter-thread">
            <h2>Twitter Thread:</h2>
            {twitter_thread}
        </div>
    </div>
    """

    thread_containers = ""
    for index, row in df.iterrows():
        twitter_thread_html = "".join(f'<div class="tweet"><p>{tweet}</p></div>' for tweet in row['twitterThread'].split('\n'))
        
        thread_container = THREAD_CONTAINER_TEMPLATE.format(
            channel_name=row['handle'],
            video_title=row['videoTitle'],
            publish_time=row['videoPublishTime'],
            twitter_thread=twitter_thread_html
        )
        thread_containers += thread_container

    html_content = html_template.format(thread_containers=thread_containers)
    return html_content

def send_email(html_content, recipient_emails, sender_email, sender_password):
    """Send the email to the recipients."""
    smtp_server = "smtp.gmail.com"
    smtp_port = 587

    try:
        # Connect to the SMTP server
        logging.info("Connecting to SMTP server...")
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        logging.info("TLS started. Attempting login...")
        server.login(sender_email, sender_password)
        logging.info("Login successful.")

        # Create the email message
        message = MIMEMultipart("alternative")
        message["From"] = sender_email
        message["To"] = ", ".join(recipient_emails)  # Set To header for all recipients
        message["Subject"] = f"Daily Twitter Threads Digest - {datetime.now().strftime('%Y-%m-%d')}"

        # Attach HTML part
        message.attach(MIMEText(html_content, "html"))

        # Send the email
        server.send_message(message)
        logging.info("Email sent successfully to all recipients.")

    except smtplib.SMTPAuthenticationError as e:
        logging.error("Authentication failed. Please check your email and App Password.")
        logging.error(f"Error details: {str(e)}")
        logging.info("Make sure you're using an App Password, not your regular Gmail password.")
        logging.info("To set up an App Password, visit: https://myaccount.google.com/apppasswords")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    finally:
        # Close the SMTP connection
        if 'server' in locals():
            server.quit()
        logging.info("SMTP connection closed.")


def send_daily_digest(df, recipient_emails):
    """Read data, compose the email, and send it."""
    sender_email, sender_password = load_email_credentials()
    html_content = compose_email_content(df)
    send_email(html_content, recipient_emails, sender_email, sender_password)

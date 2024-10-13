# openai_module.py

from dotenv import load_dotenv
import os
from openai import OpenAI

def get_openai_completion(prompt):
    """Load OpenAI API key and create a chat completion."""
    # Load environment variables from .env file
    load_dotenv()
    
    # Retrieve the OpenAI API key
    openai_api_key = os.getenv('OPENAI_API_KEY')
    
    # Initialize OpenAI client
    client = OpenAI(api_key=openai_api_key)
    
    # Create chat completion
    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}]
    )
    
    # Get the result
    result = completion.choices[0].message.content
    return result

# llm/chatgpt_connection_info.py

from pydantic import BaseModel


class ChatGPTConnectionInfo(BaseModel):
    api_key: str
    base_url: str = "https://api.openai.com/v1"

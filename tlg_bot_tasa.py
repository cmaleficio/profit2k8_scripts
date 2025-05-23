import os
import requests
from dotenv import load_dotenv, dotenv_values

load_dotenv()
# Cargar las variables de entorno desde el archivo .env
config = dotenv_values(".env")  


TLG_API_KEY = config.get("TLG_API_KEY")
TLG_CHAT_ID = config.get("TLG_CHAT_ID")

def bot_send_text(message):
    url = "https://api.telegram.org/bot"+TLG_API_KEY+"/sendMessage"
    payload = {
        "chat_id": TLG_CHAT_ID,
        "text": message
    }

    return requests.post(url, data=payload)

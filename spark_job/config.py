from dotenv import load_dotenv
import logging
import os
from typing import Optional

load_dotenv()

#cluster info
IP=os.getenv('IP')
PORT=os.getenv('PORT')

TOPIC = 'client_ticket'
KAFKA_BROKER = 'redpanda-0:9092'

#ticket infos for generation
DEMANDES = ['support', 'billing', 'technical', 'sales']
PRIORITY = ['low', 'medium', 'high', 'urgent']
TYPES = [
    "Can't access account",
    "Issue with invoice",
    "Bug in application",
    "Need a product demo"
]



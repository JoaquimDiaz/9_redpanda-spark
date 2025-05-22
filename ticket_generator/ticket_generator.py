"""
Ticket Generator

This script simulates customer support ticket creation and streams ticket data
to a Kafka (or Redpanda) topic. It generates synthetic tickets at a user-defined
interval using randomized fields such as ticket ID, client ID, priority, and type.

Main Features:
--------------
- Configurable via command-line arguments:
    --interval  : Time delay (in seconds) between messages [default: 2]
    --ip        : Kafka broker IP address [default from config]
    --port      : Kafka broker port [default from config]
    --topic     : Kafka topic name [default from config]
    --log-level : Log level [default INFO]
- Structured logging with timestamped INFO-level output.

Usage:
------
python ticket_generator.py --interval 1.5 --ip 127.0.0.1 --port 9092 --topic support-tickets
"""

import json
import random
from datetime import datetime, timezone 
from kafka import KafkaProducer 
import logging 
import argparse
import time
from typing import Dict, Any, Optional
import uuid

import config

logger = logging.getLogger(__name__)

def setup_logging(level: Optional[int] = logging.INFO) -> None:
    """
    Setup basicConfig for logging, with output to the console.
    """
    logging.basicConfig(
        level=level,
        format="%(levelname)s [%(module)s|L%(lineno)d] %(asctime)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    )

def main(argv):

    parser = argparse.ArgumentParser(description="Ticket generator for RedPanda POC")
    parser.add_argument("--interval", type=float, default=2)
    parser.add_argument("--ip", type=str, default=config.IP)
    parser.add_argument("--port", type=int, default=config.PORT)
    parser.add_argument("--topic", type=str, default=config.TOPIC)
    parser.add_argument("--log-level", type=str, default='INFO')

    args = parser.parse_args(argv)

    log_level = getattr(logging, args.log_level, logging.INFO)

    setup_logging(log_level)
    logger = logging.getLogger(__name__)

    producer = create_producer(
        ip = args.ip,
        port = args.port
    )

    send_tickets(
        producer=producer,
        topic=args.topic,
        time_interval=args.interval
    )

def create_producer(ip: str, port: int) -> KafkaProducer:
    """ Create a KafkaProducer connected to the given IP and port. """
    kafka_broker = f"{ip}:{port}"
    return KafkaProducer(
        bootstrap_servers=kafka_broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def send_tickets(
        producer: KafkaProducer,
        topic: str,
        time_interval: Optional[float] = 2
    )-> None:
    """ """
    try:
        while True:
            ticket = generate_ticket()
            logger.info(f"Sending ticket: {ticket}")
            producer.send(topic=topic, value=ticket)
            time.sleep(time_interval)  
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        producer.flush()
        producer.close()

def generate_ticket() -> Dict[str, Any]:
    ticket = {
        "ticket_id": str(uuid.uuid4()),
        "client_id": f"C{random.randint(1000, 9999)}",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "demande": random.choice(config.DEMANDES),
        "type": random.choice(config.TYPES),
        "priorite": random.choice(config.PRIORITY)
    }
    return ticket

if __name__ == "__main__":
    import sys
    main(sys.argv[1:]) 
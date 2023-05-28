import json

from flask import Flask, request
from kafka import KafkaProducer
import logging
import concurrent.futures
import psycopg2
from dotenv import dotenv_values

app = Flask(__name__)

# Load environment variables from .env file
env_variables = dotenv_values()

# Constants
BATCH_SIZE = env_variables['BATCH_SIZE']
INSERT_INTERVAL = env_variables['INSERT_INTERVAL']

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# PostgreSQL connection details
postgres_config = {
    'host': "localhost",
    'port': '5432',
    'database': "rocket_data",
    'user': env_variables['POSTGRES_USER'],
    'password': env_variables['POSTGRES_PASSWORD']
}

# SQL insert queries for different message types
insert_queries = {'RocketLaunched': """
INSERT INTO rocket_launched (channel, messageNumber, messagetype, messageTime, type, launchSpeed, mission)
VALUES (%s, %s, %s, %s, %s, %s, %s)
""",
                  'RocketSpeedIncreased': """
INSERT INTO speed_change ( channel, messageNumber, messagetype,
messageTime, changeValue)
VALUES (%s, %s, %s, %s, %s)""",
                  'RocketSpeedDecreased': """
INSERT INTO speed_change ( channel, messageNumber, messagetype,
messageTime, changeValue)
VALUES (%s, %s, %s, %s, %s)""",
                  'RocketExploded': """
INSERT INTO rocket_exploded (channel, messageNumber, messageType, messageTime, reason)
VALUES (%s, %s, %s, %s, %s)""",
                  'RocketMissionChanged': """
INSERT INTO mission_changed (channel, messageNumber, messageType, messageTime, newMission)
VALUES (%s, %s, %s, %s, %s)""",
                  }


@app.route('/')
def hello_world():
    return "<h1>Lunar Rocket Launcher</h1>"


@app.route('/messages', methods=['POST', 'GET'])
def get_data():
    logging.info("Launch data packages being received")

    TOPIC_NAME = "rocket-launch"
    KAFKA_SERVER = "localhost:9092"

    # Create a Kafka producer
    producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER,
                             api_version=(0, 11, 15))

    data = request.json
    json_data = json.dumps(data)
    encoded_data = str.encode(json_data)

    # Send data to the Kafka topic
    producer.send(TOPIC_NAME, encoded_data)
    producer.flush()
    logging.info("Data Flushed")

    message_type = data['metadata']['messageType']
    insert_query = insert_queries.get(message_type)

    if insert_query:
        try:
            # Connect to PostgreSQL
            conn = psycopg2.connect(**postgres_config)
            cursor = conn.cursor()

            logging.info("Postgres conn and cursor ok")

            insert_tuple = data_to_insert(data, message_type)

            # Use a thread pool executor to execute the insert query asynchronously
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(cursor.execute, insert_query, insert_tuple)
                conn.commit()
            future.result()

            logging.info("Message added to PostgreSQL")
        except Exception as e:
            logging.error(f"Error inserting message into PostgreSQL: {str(e)}")
            return 'Error inserting message into PostgreSQL', 500

        finally:
            cursor.close()
            conn.close()

    else:
        logging.error(f"Unknown message type: {message_type}")
        return 'Unknown message type', 400

    return 'Package received'

# Treat data to put in the correct PostgreSQL table
def data_to_insert(data: dict, message_type: str) -> tuple:
    metadata_params = (data.get('metadata').get('channel'),
                       data.get('metadata').get('messageNumber'),
                       message_type,
                       data.get('metadata').get('messageTime'))

    if message_type == 'RocketLaunched':
        message_params = (
            data.get('message', {}).get('type'),
            data.get('message', {}).get('launchSpeed'),
            data.get('message', {}).get('mission')
        )

    elif message_type == 'RocketSpeedIncreased' or message_type == 'RocketSpeedDecreased':
        message_params = (
            data.get('message', {}).get('by')
        )

    elif message_type == 'RocketExploded':
        message_params = (
            data.get('message', {}).get('reason')
        )

    elif message_type == 'RocketMissionChanged':
        message_params = (
            data.get('message', {}).get('newMission')
        )

    else:
        # Handle unknown message type
        logging.error(f"Unknown message type: {message_type}")
    if not isinstance(message_params, tuple):
        message_params = (message_params,)
    insert_params = metadata_params + message_params
    return insert_params


if __name__ == '__main__':
    app.run(debug=True, port=8088)

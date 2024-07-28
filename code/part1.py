import csv
import json
import asyncio
import random

from datetime import datetime
from itertools import cycle

from aiokafka import AIOKafkaProducer

from faker import Faker

# Create a Faker instance
fake = Faker()

# Kafka topic
topic = 'spotify'

class DataGenerator:
    """Generates data to be produced into Kafka."""

    STUDENT_NAME = "Kostis Konstantinos"

    def __init__(self, spotify_songs_handle, fake_instance, fake_names_num=20):
        self.spotify_songs_handle = spotify_songs_handle
        self.fake_instance = fake_instance
        self.fake_names_num = fake_names_num
        self.load_spotify_songs()
        self.create_fake_names()

    def load_spotify_songs(self):
        """Load spotify songs"""
        with open(self.spotify_songs_handle) as f:
            reader = csv.DictReader(f)
            data = []
            for row in reader:
                song = dict(name=row['name'], danceability=float(row['danceability']))
                data.append(song)

        self.songs = data

    def create_fake_names(self):
        """Persist a list of fake names"""
        self.fake_names = []
        for _ in range(0, self.fake_names_num):
            self.fake_names.append(self.fake_instance.name())
        
        self.name_iterator = cycle(self.fake_names)

    def current_time_millis(self):
        date= datetime.utcnow() - datetime(1970, 1, 1)
        seconds =(date.total_seconds())
        milliseconds = round(seconds*1000)

        return milliseconds

    def generate_sample(self, fake):
        """ Create a sample payload (user, song, timestamp)

            Args:
                fake: (Boolean) Indicate if a fake or not fake person should be fetched
        """
        # get a user (either from fake list or student name)
        user = None

        if fake:
            user = next(self.name_iterator)
        else:
            user = self.STUDENT_NAME

        # randomly pick a spotify song
        idx = random.randint(0, len(self.songs)-1)
        selected_song = self.songs[idx]

        # assemble the payload (user, song, current time)
        time_millis = self.current_time_millis()
        payload=dict(name=user, song=selected_song['name'],event_timestamp=str(time_millis))

        return payload


def serializer(value):
    return json.dumps(value).encode()

async def produce():
    producer = AIOKafkaProducer(
        bootstrap_servers='localhost:29092',
        value_serializer=serializer,
        compression_type="gzip")
    
    await producer.start()

    # The number of fake people
    people_num = 60

    # The number of seconds to wait before producing the next batch of data
    wait_time = 5

    # The data generator instance
    generator = DataGenerator(spotify_songs_handle='spotify-songs.csv', fake_instance=fake, fake_names_num=people_num)

    # Produce messages for ever, until exit signal :-)
    while True:
        # produce a message per fake person
        for _ in range(0, people_num):
            payload = generator.generate_sample(True)
            await producer.send(topic, payload)

        # produce a message for myself :-)
        payload = generator.generate_sample(False)
        await producer.send(topic, payload)

        # wait for wait_time seconds
        await asyncio.sleep(wait_time)

    await producer.stop()

loop = asyncio.get_event_loop()
result = loop.run_until_complete(produce())

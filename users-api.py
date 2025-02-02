from faker import Faker
from kafka import KafkaProducer
import json
import time
import random

def create_user():
    fake = Faker()
    return {
        'name': fake.name(),
        'last_name': fake.last_name(),
        'age': fake.random_int(min=18, max=80, step=1),
        'address': fake.address(),
        'email': fake.email()
    }

def preprocess_user(user):
    user_str = json.dumps(user).encode('utf-8')
    return user_str

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)
 
def on_send_error(excp):
    log.error('I am an errback', exc_info=excp)

if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='localhost:29092', 
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    while True:
        user = create_user()
        reps = random.randint(1, 10)
        for i in range(reps):
            future = producer.send('codespotify-topic', user)  # Send message
            try:
                record_metadata = future.get(timeout=10)  # Ensure message is sent
                print(f"Message sent to {record_metadata.topic} partition {record_metadata.partition}")
            except Exception as e:
                print(f"Error sending message: {e}")

        producer.flush()  # Ensure all messages are delivered
        time.sleep(3)
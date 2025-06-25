import json
from confluent_kafka import SerializingProducer
import psycopg2
import requests
import random

BASE_URL = 'https://randomuser.me/api/'

random.seed(236)

def generate_candidate_data():
    response = requests.get(BASE_URL)
    if response.status_code == 200:
        results = response.json()['results']
        if len(results) == 0:
            return 'Warning: No results found in API response'

        user_data = results[0]

        return {
            'id': user_data['login']['uuid'],
            'full_name': f"{user_data['name']['first']} {user_data['name']['last']}",
            'gender': user_data['gender'],
            'age': user_data['dob']['age'],
            'email': user_data['email'],
            'phone': user_data['phone'],
            'city': user_data['location']['city'],
            'country': user_data['location']['country'],
            'nationality': user_data['nat'],
            'picture_url': user_data['picture']['large'],
            'registered_at': user_data['registered']['date']
        }
    else:
        return 'Error fetching data from API'

def insert_candidate(conn, cur, candidate):
    cur.execute('''
        INSERT INTO candidate (id, full_name, gender, age, email, phone, city, country, nationality, picture_url, registered_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ''', 
                (candidate['id'], candidate['full_name'], candidate['gender'], candidate['age'], 
                 candidate['email'], candidate['phone'], candidate['city'], candidate['country'], 
                 candidate['nationality'], candidate['picture_url'], candidate['registered_at']))
    conn.commit()

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

if __name__ == '__main__':
    producer = SerializingProducer({'bootstrap.servers': 'broker:29092'})
    try:
        conn = psycopg2.connect('host=postgres dbname=recruitment user=postgres password=secret')
        cur = conn.cursor()

        # Fetch existing candidates
        cur.execute('SELECT * FROM candidate')

        candidates = cur.fetchall()
        print(candidates)

        for i in range(1000):
            
            candidate = generate_candidate_data()

            if not isinstance(candidate, dict):
                print(f"Skipping due to error: {candidate}")
                continue

            insert_candidate(conn, cur, candidate)

            producer.produce(
                'candidates_topic',
                key = candidate['id'],
                value = json.dumps(candidate),
                on_delivery = delivery_report
            )

            print('Produced candidate {}, data: {}'.format(candidate['id'], candidate))

            producer.flush()

    except Exception as e:
        print(e)
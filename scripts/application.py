import random
import time
import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException, SerializingProducer
import json

conf = {
    'bootstrap.servers': 'broker:29092',
}

consumer = Consumer(conf | {
    'group.id': 'recruitment_group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})

def generate_score(age, experience_years):
    base_score = experience_years * 8
    if 25 <= age <= 35:
        base_score += 10
    noise = random.randint(-5, 5)
    return min(max(base_score + noise, 30), 100)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

producer = SerializingProducer(conf)

if __name__ == '__main__':
    try:
        conn = psycopg2.connect('host=postgres dbname=recruitment user=postgres password=secret')
        cur = conn.cursor()

        position_query = cur.execute(
            '''
            SELECT row_to_json(col)
            FROM (  
                SELECT * FROM position
            ) col
            '''
        )

        positions = [position[0] for position in cur.fetchall()]
        # Rename 'id' to 'position_id' and 'name' to 'position_name'
        for position in positions:
            position['position_id'] = position.pop('id')
            position['position_name'] = position.pop('name')

        if len(positions) == 0:
            print('Warning: No positions found in the database')
        else:   
            print(positions)

        consumer.subscribe(['candidates_topic'])

        try:
            while True:
                msg = consumer.poll(timeout = 1.0)
                if msg is None:
                    continue
                elif msg.error():
                    if msg.error() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"Error occurred: {msg.error()}")
                        break
                else:
                    candidate = json.loads(msg.value().decode('utf-8'))
                    chosen_position = random.choice(positions)
                    experience_years = random.randint(0, 10)
                    application = candidate | chosen_position | {
                        'status': 'submitted',
                        'score': generate_score(candidate['age'], experience_years),
                        'experience_years': experience_years,
                    }

                    try:
                        print('User {} is applying for position {}'.format(
                            candidate['id'], chosen_position['position_id']
                        ))
                        cur.execute(
                            '''
                            INSERT INTO application (candidate_id, position_id, status, score, experience_years)
                            VALUES (%s, %s, %s, %s, %s)
                            ''',
                            (candidate['id'], chosen_position['position_id'], application['status'], 
                             application['score'], application['experience_years'])
                        )
                        conn.commit()

                        producer.produce(
                            topic = 'applications_topic',
                            key = str(application['id']),
                            value = json.dumps(application),
                            on_delivery = delivery_report
                        )

                        producer.flush(0)

                    except Exception as e:
                        print(f"Error inserting application: {e}")
                        conn.rollback()
                        continue

                time.sleep(0.5)

        except KafkaException as e:
            print(f"Kafka error: {e}")
        
    except Exception as e:
        print(e)
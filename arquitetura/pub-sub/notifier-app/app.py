import json
import logging
import requests
from confluent_kafka import Consumer, KafkaError


TOKEN = '7556072798:AAGI2xXjAUL1dJNZZ6-LGUYsagds70lak8M'
CHAT_ID = '5464078318'
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TOKEN}/sendMessage"

c = Consumer({
    'bootstrap.servers': 'kafka1:19091,kafka2:19092,kafka3:19093',
    'group.id': 'notifier-group',
    'client.id': 'notifier-client',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
})

c.subscribe(['/notificacao'])


def send_telegram_message(message):
    payload = {
        'chat_id': CHAT_ID,
        'text': message
    }
    try:
        response = requests.post(TELEGRAM_API_URL, data=payload)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao mandar mensagem pro telegram: {e}")


try:
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            data = json.loads(msg.value())
            filename = data['filename']
            operation = data['operation']

            notification_message = f'O arquivo {filename} foi {operation}.'
            send_telegram_message(notification_message)
            logging.info(f"Notificação: {notification_message}")
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            logging.warning(f'End of partition reached: {msg.topic()} {msg.partition()}')
        else:
            logging.error(f'Error: {msg.error().str()}')

except KeyboardInterrupt:
    pass
finally:
    c.close()

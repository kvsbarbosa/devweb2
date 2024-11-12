import json
import logging
import telegram
import asyncio
from confluent_kafka import Consumer, KafkaError

api_key = '7556072798:AAGI2xXjAUL1dJNZZ6-LGUYsagds70lak8M'
user_id = '5464078318'

async def send_telegram_message(message):
    bot = telegram.Bot(token=api_key)
    await bot.send_message(chat_id=user_id, text=message)

c = Consumer({
    'bootstrap.servers': 'kafka1:19091,kafka2:19092,kafka3:19093',
    'group.id': 'notifier-group',
    'client.id': 'notifier-client',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
})

c.subscribe(['/notificacao'])

def consume_and_notify():
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
                logging.info(f"Notificação: {notification_message}")

                asyncio.run(send_telegram_message(notification_message))
            elif msg.error().code() == KafkaError._PARTITION_EOF:
                logging.warning(f'End of partition reached: {msg.topic()} {msg.partition()}')
            else:
                logging.error(f'Error: {msg.error().str()}')

    except KeyboardInterrupt:
        pass
    finally:
        c.close()

consume_and_notify()

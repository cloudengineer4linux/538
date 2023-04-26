import os
import redis
import json
import google
from google.auth import jwt
from google.cloud import pubsub_v1
from flask import Flask, render_template, request

app = Flask(__name__)

redis_host = 'redis.cloud.enginner.example'
redis_port = '6379'
redis_client = redis.StrictRedis(host=redis_host, port=redis_port)
#value = redis_client.incr('counter', 1)

@app.route('/')
def home():

	id = "associate-cloud-engineer2"      # Prencher com o Project ID a ser conectado
	topic = "python-4linux-topic"   # Preencher com o nome do tópico a ser criado
	sub = "python-4linux-sub"     # Preencher com o some da subscription a ser criada
	message = request.args.get('message') # Variavel recebida através da página HTML

	if message != None:
		# Authenticate account
		service_account_info = json.load(open("credenciais.json")) # Necessário um arquivo de credenciais com esse nome no diretório

		# Authenticate Subscriber
		audience = "https://pubsub.googleapis.com/google.pubsub.v1.Subscriber"
		credentials = jwt.Credentials.from_service_account_info(service_account_info, audience=audience)
		subscriber = pubsub_v1.SubscriberClient(credentials=credentials)

		# Authenticate Publisher
		publisher_audience = "https://pubsub.googleapis.com/google.pubsub.v1.Publisher"
		credentials_pub = credentials.with_claims(audience=publisher_audience)
		publisher = pubsub_v1.PublisherClient(credentials=credentials_pub)

		# Create Topic if it doesn't exists
		try:
			topic_name = f'projects/{id}/topics/{topic}'
			publisher.create_topic(name = topic_name)
		except google.api_core.exceptions.AlreadyExists:
			pass

		# Create Suscription if it doesn't exists
		try:
			topic_name = f'projects/{id}/topics/{topic}'
			subscription_name = f'projects/{id}/subscriptions/{sub}'

			def callback(message):
				print(message.data)
				message.ack()

			with pubsub_v1.SubscriberClient(credentials=credentials) as subscriber:
				subscriber.create_subscription(name = subscription_name, topic = topic_name)
				future = subscriber.subscribe(subscription_name, callback)
		except google.api_core.exceptions.AlreadyExists:
			pass

		# Publish
		future = publisher.publish(topic_name, message.encode("utf-8"))
		future.result()


	return render_template('index.html')


@app.route('/healthz')
def healthz():
	return ''


if __name__ == '__main__':
    app.run(host="127.0.0.1", port=8080, debug=True)

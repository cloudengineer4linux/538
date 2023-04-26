import os
from flask import Flask, render_template
import redis

app = Flask(__name__)

redis_host = os.environ.get('REDISHOST', 'localhost')
redis_port = int(os.environ.get('REDISPORT', 6379))
redis_client = redis.StrictRedis(host=redis_host, port=redis_port)

@app.route('/')
def index():
        value = redis_client.incr('counter', 1)
        return render_template('index.html')

if __name__ == '__main__':
        app.run(host="127.0.0.1", port=8080, debug=True)

from datetime import datetime
import json

from flask import Flask, request
from flask.templating import render_template
from kafka import KafkaProducer

app = Flask(__name__)


@app.route('/')
def index():
    return render_template('order.html')


@app.route('/order', methods=['POST'])
def order():
    products = request.form.getlist('product')
    email = request.form.get('email')
    email_event = {'type': 'order_confirmation',
                   'email': email,
                   'products': products,
                   'send_offset': datetime.now().isoformat()}
    producer = KafkaProducer(bootstrap_servers='localhost:29092',
                             value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'))
    producer.send('emails', value=email_event)
    producer.close()
    return 'ok!'


if __name__ == '__main__':
    app.run(debug=True)

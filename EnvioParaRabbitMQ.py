import pika
import time 
from py4j.tests.java_gateway_test import sleep


ler  = open('limpo04.txt', 'r')
lerDados = ler.readlines()
ler.close()
credentials = pika.PlainCredentials('psd', 'psd')
connection = pika.BlockingConnection(pika.ConnectionParameters(
               'localhost', 5672, 'psd', credentials))
channel = connection.channel()
channel.exchange_declare(exchange='amq.topic',
                 exchange_type='topic', durable=True)
timestemp = lerDados[0].split(',')[0]
timestemp = int(timestemp)
cont =0 
for i in lerDados:
    time = i.split(',')[0]
    time = int(time)   
    if time == timestemp+60:
        cont = 0
        timestemp+=60
        sleep(60)
        print timestemp
    channel.basic_publish(
            exchange='amq.topic',  # amq.topic as exchange
            routing_key='hello',   # Routing key used by producer
            body= i
        )
    cont+=1
connection.close()
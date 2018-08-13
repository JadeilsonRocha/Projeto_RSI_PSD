import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from mqtt import MQTTUtils
from __builtin__ import False
import paho.mqtt.client as mqtt

def filtro(line):
    if line != "" or line != None:
        line_list = line.split(",")
        return (line_list[2].replace("\r\n",""), [line_list[0]])
    
def agrupaTS(a,b):
    return a + b
    
def getTimeMac(tuple):
    maximo = int(max(tuple[1]))
    minimo = int(min(tuple[1]))
    diferenca = ((maximo - minimo)/60.0)
    diferenca = round(diferenca,2)
    if diferenca >=17.5 and diferenca < 40:
        return (minimo, 1)
    
def getUltimo(x):
    y = x[-1]
    print y
    return y

def limpeza(tuple2):
    if tuple2 != None:
        return True
    return False

def contar(total):
    topic_pub='v1/devices/me/telemetry'
    client = mqtt.Client()
    timestamp = int(total[0])*1000
    print timestamp
    timestamp = timestamp-10800
    print timestamp
    client.username_pw_set("BAWQlw3NANhVI4egpb5m")
    client.connect('127.0.0.1', 18830, 1)
    msg = '{"ts":"'+ str(timestamp) +'", "values":{"Qtd-Pessoas-RU-04":"'+ str(total[1]) + '"}}'
    if client.publish(topic_pub, msg):
        return  total
    return total

def getTimeQtd(tp1, tp2):    
    ts = []
    ts.append(tp1[0])
    ts.append(tp2[0])
    
    return (max(ts), int(tp1[1]) + int(tp2[1]))
    
if __name__ == "__main__":

    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: mqtt_wordcount.py < > <>"
        exit(-1)

    sc = SparkContext(appName="PythonStreamingMQTTWordCount")
    ssc = StreamingContext(sc, 120)

    brokerUrl = sys.argv[1]
    topic = sys.argv[2]
    print (brokerUrl)
    print (topic)
    lines = MQTTUtils.createStream(ssc, brokerUrl, topic)
    counts = lines.map(filtro)
    windowedWordCounts = counts.reduceByKeyAndWindow(agrupaTS, None, 2760, 120)
    windowedWordCounts = windowedWordCounts.map(getTimeMac)
    windowedWordCounts = windowedWordCounts.filter(limpeza)
    windowedWordCounts = windowedWordCounts.reduce(getTimeQtd)
    windowedWordCounts = windowedWordCounts.map(contar)
    windowedWordCounts.pprint()
    
    ssc.start()
ssc.awaitTermination()
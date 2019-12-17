from kafka import KafkaProducer
import datetime, time as t, random, threading
import urllib.request

def send_at():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    topic = 'bus'
    while True:

        contents = urllib.request.urlopen("http://countdown.api.tfl.gov.uk/interfaces/ura/instant_V1?DirectionID=1&returnlist=DirectionID,StopPointName,EstimatedTime,LineName").read()
        station = []
        bus = []
        time = []
        data1 = contents.decode('utf-8')
        data1 = data1.split("\n")
        # print(data1)
        bad_chars = ['"', '[', ']', '\r']
        for d in data1[1:]:
            for i in bad_chars:
                d = d.replace(i, '')
            d = d.split(',')
            #print(d)
            msg = '%s--%s--%s' % (d[1], d[2], d[4])
            print(msg)
            producer.send(topic, msg.encode('ascii'))
        print("#####################################################################")
        t.sleep(240)



if __name__ == "__main__":
    send_at()
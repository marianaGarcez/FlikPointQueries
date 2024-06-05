from json import dumps
from kafka import KafkaProducer
import pandas as pd



kafka_nodes = "kafka:9092"
myTopic = "aisdata"

def gen_data():
  df = pd.read_csv('aisdk_20110301_2000.csv')

  prod = KafkaProducer(bootstrap_servers=kafka_nodes, value_serializer=lambda x:dumps(x).encode('utf-8'))

  for index, row in df.iterrows():
    my_data = {'t': row['t'], 'mmsi': row['mmsi'], 'lon': row['lon'], 'lat': row['lat'], 'speed': row['speed'], 'course': row['course']}
    print(my_data)
    prod.send(topic=myTopic, value=my_data)

  prod.flush()

if __name__ == "__main__":
  gen_data()
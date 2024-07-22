from confluent_kafka import Producer
import json
import pandas as pd

config = {
    'bootstrap.servers': '192.168.0.241:29092',  # адрес Kafka сервера
    'client.id': 'simple-producer'
}

producer = Producer(**config)

def conn():
    from clickhouse_driver import Client

    dbname = 'default'

    with open(f"/Users/uliamosienko/Desktop/dag_test — копия/Utils/wb_key_ch_pegas.json") as json_file:
        data = json.load(json_file)

    client = Client(data['server'][0]['host'],
                    user=data['server'][0]['user'],
                    password=data['server'][0]['password'],
                    verify=False,
                    database=dbname,
                    settings={"numpy_columns": True, 'use_numpy': True},
                    compression=True)

    rid = client.execute(f"""
            select rid
                , shk_id 
                , toString(dt) 
                , chrt_id       
                , employee_id   
                , src_office_id 
                , dst_office_id 
                , sm_id         
                , entry 
                , status_id        
            from rid_status
            where dt >= now() - interval 1 minute 
            limit 100
                """)
    return rid
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def send_message(data):
    try:
        # Асинхронная отправка сообщения
        producer.produce('ridstatus', data.encode('utf-8'), callback=delivery_report)
        producer.poll(0)  # Поллинг для обработки обратных вызовов
    except BufferError:
        print(f"Local producer queue is full ({len(producer)} messages awaiting delivery): try again")

if __name__ == '__main__':
    res = conn()
    list_to_json = [{"rid": x[0], "shk_id": x[1], "dt": x[2], "chrt_id": x[3], "employee_id": x[4], "src_office_id": x[5], "dst_office_id": x[6], "sm_id": x[7], "entry": x[8], "status_id": x[9]} for x in res]
    for i in list_to_json:
        send_message(json.dumps(i, ensure_ascii=False))

    producer.flush()

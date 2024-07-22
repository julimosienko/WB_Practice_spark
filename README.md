# WB_Practice_spark
1. Считать данные из вашей Кафки через спарк. Если нужно, залейте немного данных с пегас.
 - Поднимаем контейнеры: clickhouse, spark, kafka
![image](https://github.com/user-attachments/assets/c6203c93-bff1-4499-a946-f0ab2c802e5d)
- Подключаемся к кафке через Offset Explorer
![image](https://github.com/user-attachments/assets/527fb841-7b49-441d-a2e2-d652e81f0b47)
- Через [producer](https://github.com/julimosienko/WB_Practice_spark/blob/main/producer.py) заливаем данные из пегаса в топик кафки
  ![telegram-cloud-photo-size-2-5255854761650020527-y](https://github.com/user-attachments/assets/7b37ea5c-19b7-48ed-b3ea-050a264786cf)

2. Добавить какую-нибудь колонку. Записать в ваш клик в докере. (*Можно через csv импортировать в ваш клик справочник объемов nm_id с пегаса, чтобы оттуда брать объем номенклатуры.)
Подключаемся к локальному клику (default, default)
![image](https://github.com/user-attachments/assets/59ed0b05-02bb-48ca-80de-ad2d76b61ec8)

- Эскпортируем словарь OrderStatus из пегаса и импортируем в локальный клик
![telegram-cloud-photo-size-2-5253602961836334813-y](https://github.com/user-attachments/assets/6da0f5c2-83b1-490b-99f5-155c60d7fa16)
3. Выложить папку с docker-compose файлами для развертывания контейнеров. Должно быть 2 файла: docker-compose.yml, .env. [docker-compose](https://github.com/julimosienko/WB_Practice_spark/tree/main/docker-compose)
4. Запушить в свой гит получившийся таск спарк. Не пушить файл с паролями.
  ```spark-submit --master spark://spark-master:7077  \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --executor-cores 1 \
    --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp" \
    /opt/spark/Streams/rs_sync_edu.py
  ```
    
  [task](https://github.com/julimosienko/WB_Practice_spark/tree/main/pyspark)
  
5. Выложить в гит скрины с содержимым конечной папки в вашем клике.
   ![telegram-cloud-photo-size-2-5255854761650020521-y](https://github.com/user-attachments/assets/6aa4d564-ecea-4e85-be7d-ff1176193c31)
6. Выложить код структуру конечной таблицы в вашем клике.
![image](https://github.com/user-attachments/assets/cd6f68fe-8485-4bb6-bf8d-204a34f2b80e)
7. Выложить скрин веб интерфейса вашего спарк.
![telegram-cloud-photo-size-2-5255854761650020522-y](https://github.com/user-attachments/assets/8f3955de-f0a7-4dfd-942c-c43cd4038018)
8. Скрин работы вашего приложения из консоли контейнера.
![telegram-cloud-photo-size-2-5255854761650022052-y](https://github.com/user-attachments/assets/7bf9371e-3e9f-4d1d-b874-8b4cc8dea430)
![telegram-cloud-photo-size-2-5255854761650022060-y](https://github.com/user-attachments/assets/56159ff8-fb22-40d5-a150-50cc290fd84b)







  






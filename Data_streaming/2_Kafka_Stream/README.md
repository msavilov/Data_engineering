## Домашнее задание по Kafka Streams-2

### Задание:
Написать приложение, которое будет отправлять сообщение-алерт, если сумма денег заработанных по этому продукту (для каждой покупки сумма - это purchase.quantity * product.price) за последнюю минуту больше 3 000.

Для генерации данных использовать файлы purchase.avsc и product.avsc из [репозитория](https://github.com/netology-ds-team/kafka-streams-practice)


### Инструменты:
- Kafka Streams DSL
- Kafka Streams
- Kafka Connect Datagen
- Docker with Kafka cluster
- Conduktor

### Результат работы программы
![Результата работы программы](https://github.com/msavilov/Data_engineering/blob/main/Data_streaming/2_Kafka_Stream/DSL_screen.png)

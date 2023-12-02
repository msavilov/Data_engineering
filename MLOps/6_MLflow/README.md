## Домашняя работа к занятию “MLOps/DataOps”
### **Цель задания**

На основе полученных знаний необходимо настроить окружение и провести обучение модели.

### **Задание**:

1. Устанавливаем и настраиваем conda
2. Устанавливаем и настраиваем python3
3. Устанавливаем и настраиваем mlflow (Важно! Модель старая, поэтому лучше всего устанавливать mlflow 1 поколения, например, версию 1.20, и не скупитесь на память для инструмента, не меньше 6-8 GB должно быть только для него)
4. Настраиваем переменные:
export MLFLOW_TRACKING_URI=http://localhost
export MLFLOW_S3_ENDPOINT_URL=http://localhost:9000
5. Настраиваем MinIO
6. Берём модель отсюда [https://github.com/vppuzakov/mlflow-example]
7. Проводим её обучение:
mlflow models serve -m S3://mlflow/0/98bdf6ec158145908af39f86156c347f/artifacts/model -p 1234


###**Решение**
![MLflow](https://github.com/msavilov/Data_engineering/blob/main/MLOps/6_MLflow/MLflow.png)

## Домашняя работа к занятию “Deply ML”

### **Цель задания**

Закрепить полученные знания и научиться настраивать PyTorch посредством настройки окружения и проведения обучения модели c использованием torchserve-dashboard

### **Задание**:

1. Устанавливаем и настраиваем conda либо python3
2. Устанавливаем и настраиваем torchserve через pip3 либо conda
3. Устанавливаем и настраиваем torchserve-dashboard через pip3 либо conda
4. Загружаем обученную модель:
wget https://download.pytorch.org/models/densenet161-8d451a50.pth
5. Заархивируйте модель с помощью архиватора моделей.
6. Стартуем сервер из под torchserve-dashboard с параметром
--config_path ./torchserve.properties --model_store ./model_store --server.port 8501 -- --config_path ./torchserve.properties


### **Решение**

Оба варианта установки torchserve-dashboard не привели к успеху:

1) устновка через pip валится при запуске с ошибкой "TypeError: main_run() got multiple values for argument 'target'" - по всей видимости есть какой-то конфликт с версиями окружения

2) запуск через streamlit позволяет загрузить сам веб-интерфейс

![torchserve-dashboard](https://github.com/msavilov/Data_engineering/blob/main/MLOps/7_Deploy_ML/torchserve-dashboard.png)

Но при попытке его запусти Torchserve валится с ошибками в Java на версиях 11 и 17.
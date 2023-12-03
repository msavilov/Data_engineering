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

![torchserve-dashboard]()
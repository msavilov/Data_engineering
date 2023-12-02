## Домашняя работа к занятию “Мониторинг”

### **Цель задания**

Научиться пользоваться инструментами bugtrasing и мониторинга, чтобы понимать, каким образом можно мониторить ваши приложения и готовить логи.

### **Задание**:

Необходимо установить и настроить на базе образа Grafana + Prometheus + alertmanager:
1. Скачиваем образ из репозитория - https://github.com/stefanprodan/dockprom
2. Собираем
3. Поднимаем
4. Проверяем, что всё работает через docker ps
5. Логинимся в Prometheus и изучаем настроенные alerts, rules
6. Логинимся в Grafana
7. Смотрим, что настроено в дашбордах и explore

### **Решение**:

1.	Перечислите алерты, которые настроены в Prometheus alerts.
![Prometheus alerts](https://github.com/msavilov/Data_engineering/blob/main/MLOps/5_Monitoring/Prometheus%20alerts.png)
![Prometheus rules](https://github.com/msavilov/Data_engineering/blob/main/MLOps/5_Monitoring/Prometheus%20rules.png)

2.	Перечислите количество dashboards в Grafana, для какого ПО они?
![Grafana dashboards](https://github.com/msavilov/Data_engineering/blob/main/MLOps/5_Monitoring/Grafana%20dashboards.png)

3.	Сделайте скриншот работающего dashboards docker containers grafana, перечислите метрики, которые там есть.
![Grafana docker dashboard](https://github.com/msavilov/Data_engineering/blob/main/MLOps/5_Monitoring/Grafana%20docker%20dashboard.png)

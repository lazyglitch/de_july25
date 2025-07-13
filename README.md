# ETL для прогноза погоды

Это ETL-приложение способно:
- Извлекать данные из локальных JSON-файлов или внешнего [Open-Meteo API](https://open-meteo.com).
- Трансформировать данные.
- Загружать данные либо в CSV файл либо в БД PostgreSQL. 

## Структура проекта
```
de_july25/
├── docker-compose.yml
├── Dockerfile          # Для ETL приложения
├── requirements.txt    # Зависимости Python
├── etl_app.py          # ETL (загрузка/обработка/выгрузка данных)
├── preprocess.py       # Скрипт трансформирования данных в формат итоговой таблицы
├── config.py          	# Файл конфигурации
├── db_init/
│   └── create_table.sql    # Скрипт для создания БД и таблицы
├── data_inputs/         # Директрия для входных JSON файлов (монтируется)
│   └── test.json           # json для примера
└── data_outputs/        # Директрия для выходных CSV файлов (монтируется)
```

### ПО

Для работы с проектом понадобятся следующие инструменты:

1. **Git** для клонирования репозитория.
    -   [Скачать Git](https://git-scm.com/downloads)


2. **Docker Desktop** для запуска контейнеризированных приложений.
    -   [Скачать Docker Desktop](https://www.docker.com/products/docker-desktop/) (доступно для Windows, macOS, Linux).

3. Так же вместо Docker Desktop можно устанавить **Docker Engine** [(пример установики Docker Engine в Ubuntu)](https://docs.docker.com/engine/install/ubuntu/) 
  

### Загрузка проекта

1. **Клонируйте репозиторий**:
    откройте терминал / командную строку и выполните команду.
    ```
    git clone https://github.com/lazyglitch/de_july25.git
    ```
    (или загрузите проект архивом и распакуйте его)

2. **Перейдите в директорию проекта**:
    ```
    cd de_july25
    ```
3. **Запустите Docker** (откройте Docker Desktop)

## Конфигурация часового пояса

По умолчанию, часовой пояс приложения задан как "Asia/Novosibirsk". Это значение используется для получения прогноза по API (если не задаем даты вручную) и для наименования выходных CSV-файлов.

Вы можете изменить это значение в файле [`config.py`](./config.py).

Часовой пояс погоды (получаемый из JSON-ответа API) тоже = "Asia/Novosibirsk". 

## Запуск ETL приложения

 Приложение запускается как одноразовый контейнер, `--rm` автоматически удаляет контейнер после его завершения.

Посмотреть список доступных команд:
```
docker compose run --rm etl_app
```
Посмотреть опции конкретной команды:
```
docker compose run --rm etl_app [название команды] --help
```
### Извлечение из JSON

#### Положить данные из JSON в CSV
```
docker compose run --rm etl_app json-to-csv --fname test.json
```
*   `--fname test.json`: имя JSON-файла, который должен находиться в директории `data_inputs/` **(в других директориях приложение их не ищет!)**
    
    Пример файла: [`data_inputs/test.json`](./data_inputs/test.json)

#### Положить данные из JSON в БД
```
docker compose run --rm etl_app json-to-db --fname test.json
```
### Извлечение из API

#### Правила для дат при работе с API:
*   Дата начала не может быть более 3 месяцев назад от сегодняшней даты.
*   Дата окончания не может быть позже, чем через 16 дней от сегодняшней даты.
*   Начальная дата должна быть меньше или равна конечной дате.
*   Формат даты должен быть `YYYY-MM-DD`.

#### Положить данные из API в CSV

*   Использовать даты по умолчанию (прогноз на 7 дней с сегодняшней даты): (`--end-date` и `--start-date` будут проигнорированы)
    ```
    docker compose run --rm etl_app api-to-csv --use-default-date y
    ```
*   Указать конкретный диапазон дат:
    ```
    docker compose run --rm etl_app api-to-csv --start-date 2025-05-05 --end-date 2025-06-05
    ```
#### Положить данные из API в БД

*   Использовать даты по умолчанию (прогноз на 7 дней с сегодняшней даты):
    ```
    docker compose run --rm etl_app api-to-db --use-default-date y
    ```
*   Указать конкретный диапазон дат:
    ```
    docker compose run --rm etl_app api-to-db --start-date 2025-05-05 --end-date 2025-06-05
    ```
## Просмотр данных в БД

Вы можете подключиться к базе данных PostgreSQL, используя [PgAdmin](https://www.pgadmin.org/download/).

  [Туториал](https://dykraf.com/blog/how-to-connect-pgadmin4-and-postgresql-server-on-docker-container) (eng) по подключению PgAdmin к PostgreSQL в Docker.

**!!!**: БД хранит время в UTC-формате. При просмотре данных через SQL-запрос, если вы хотите чтобы время отображалось в часовом поясе прогноза, установите его в начале:
```
SET TIMEZONE TO 'Asia/Novosibirsk'; 
SELECT * FROM daily_weather;
```

## Завершение работы


Используйте команду ниже для остановки контейнеров, удаления образов и всех связанных томов:
```
docker compose down --rmi all -v
```


import requests
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pathlib import Path
import os
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert
import json
import sys
import argparse
from preprocess import transform_data
from config import USER_TIMEZONE, TABLE_NAME, BASE_API_URL
import pytz
# Получение и сохранение данных


# Настройки подключения к БД
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('POSTGRES_DB')
DB_USER = os.getenv('POSTGRES_USER')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_NAME}"
engine = create_engine(DATABASE_URL)


# Сохранение DataFrame в Postgresql
def save_to_db(df):

  with engine.connect() as conn:
    transaction = conn.begin() # Начинаем транзакцию
    try:
      metadata = MetaData()
      # Определяем какую таблицу используем и получаем ее metadata
      table = Table(TABLE_NAME, metadata, autoload_with=conn)
      for _, row in df.iterrows():
        insertion= insert(table).values(**row.to_dict())

        # on_conflict_do_nothing при вставке либо добавляет запись либо пропускает ее если произошла ошибка (дубликаты)
        on_conflict_do = insertion.on_conflict_do_update(
                         index_elements=['sunrise_iso', 'sunset_iso'],
                         set_=row.to_dict()
                         )

        conn.execute(on_conflict_do)

      transaction.commit()
      print("Вставка данных в БД завершена (дубликаты игнорируются)")
      # здесь могла бы быть реализация логики подсчета дубликатов и выводилось бы сообщение об их наличии, но увы

    except Exception as e:
      transaction.rollback() # Откатываем в случае ошибки
      print(f"Ошибка: {e}")
      print("Выгрузка данных отменена.")
      import sys
      sys.exit(1)

# Сохранине DataFrame в csv
def save_to_csv(df, output_directory = '/app/outputs'):
    #получаем текущую дату
    now = datetime.now(pytz.timezone(USER_TIMEZONE))
    
    timestamp_str = now.strftime("%Y-%m-%d %H-%M-%S")

    filename = f"open-meteo {timestamp_str}.csv"

    file_path = os.path.join(output_directory, filename)

    try:
        df.to_csv(file_path, index=False, encoding='utf-8')
        print(f"Файл сохранен в: data_output/{filename}")
    except Exception as e:
        print(f"Ошибка при сохранении файла: {e}")


# Получаем данные из JSON фала
def get_json_data(filename, input_directory = '/app/inputs'):
    
    filepath = os.path.join(input_directory, filename)

    json_path = Path(filepath)

    try:
        with open(json_path, "r", encoding="utf-8") as f:
            print(f"Загрузка данных из JSON файла: data_inputs/{filename}")
            data_json = json.load(f)
            print("JSON получен")
            return data_json
    except FileNotFoundError:
        print(f"Файл не найден: {json_path}")
        sys.exit(1)


# Проверяем даты для API
def check_api_parameters(start_date, end_date, use_default_date):

  if use_default_date == 'y':
    today = datetime.now(pytz.timezone(USER_TIMEZONE))
    start_d = today.strftime("%Y-%m-%d")  # Сегодняшняя дата
    end_d = (today + timedelta(days=6)).strftime("%Y-%m-%d")  # 6 дней от сегодняшней даты
    return start_d, end_d

  elif use_default_date == "n":
    try:
      start_date_dt = datetime.strptime(start_date, "%Y-%m-%d")
      end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")
      today = datetime.now()
      three_months_ago = today - relativedelta(months=3)

      if start_date_dt < three_months_ago:
        print("\nДата начала не может быть более 3 месяцев назад от сегодняшней даты")
        sys.exit(1)

      if end_date_dt > today + timedelta(days=15):
        print("\nДата окончания не может быть позже чем 16 дней от сегодняшней даты")
        sys.exit(1)

      if start_date_dt >= end_date_dt: # Проверяем, что начальная дата меньше конечной
        print("\nНачальная дата должна быть меньше или равна конечной дате")
        sys.exit(1)

      else:
        return start_date, end_date

    except ValueError:
      print("\nНеверный формат даты. Используйте YYYY-MM-DD.")
      sys.exit(1)

# Получает данные с API
def get_data_from_api(start_date, end_date):

  API_URL = f"{BASE_API_URL}&start_date={start_date}&end_date={end_date}"

  api_response = requests.get(API_URL)

  if api_response.status_code != 200:
    print(f"\nОшибка! Код ответа API: {api_response.status_code}")
    sys.exit(1)

  data_json = api_response.json()
  print('JSON получен')
  return data_json

# Проверка JSON
def validate_json(data_json):

  required_params = {
      'daily': {'time', 'sunrise', 'sunset', 'daylight_duration'},
      'hourly': {'time', 'temperature_2m', 'relative_humidity_2m', 'dew_point_2m',
                  'apparent_temperature', 'temperature_80m', 'temperature_120m',
                  'wind_speed_10m', 'wind_speed_80m', 'wind_direction_10m',
                  'wind_direction_80m', 'visibility', 'evapotranspiration',
                  'weather_code', 'soil_temperature_0cm', 'soil_temperature_6cm',
                  'rain', 'showers', 'snowfall'
                  }
  }
  # Проверка наличия timezone
  if 'timezone' not in data_json.keys():
     raise ValueError(f"Параметр 'timezone' отсутствует в ответе")
  
  for key, params in required_params.items():
    # Проверка наличия основного ключа
    if key not in data_json:
        raise ValueError(f"Параметр '{key}' отсутствует в ответе")

    data = data_json[key]

    # Проверка типа данных значения по основному ключу
    if not isinstance(data, dict):
        raise ValueError(f"Значение по ключу '{key}' должно быть словарем (dict)")

    # Проверка наличия ключей во вложенном словаре
    if not params.issubset(data.keys()):
        missing_params = params - set(data.keys())
        raise ValueError(f"В '{key}' отсутствуют параметры: {', '.join(missing_params)}")

    # Проверка типа данных значений во вложенном словаре
    for sub_key in data:
        if not isinstance(data[sub_key], list):
            raise ValueError(f"Значение по ключу '{sub_key}' в '{key}' должно быть списком (list)")

  print("JSON проверен: данные корректны")

# получение данных из json и их трансформация
def json_get_and_transform(args):
  json_filepath = args.fname
  raw_data = get_json_data(json_filepath)

  validate_json(raw_data)

  df = transform_data(raw_data)

  return df

# Вставить данные из json в базу
def json_to_db(args):
  try:
    df = json_get_and_transform(args)
    save_to_db(df)
  except Exception as e:
    print(f"Ошибка при выгрузке JSON в БД: {e}")
    sys.exit(1)


# Положить данные из json в csv
def json_to_csv(args):
  try:
    df = json_get_and_transform(args)
    save_to_csv(df)
  except Exception as e:
    print(f"Ошибка при выгрузке JSON в csv: {e}")
    sys.exit(1)

# получение данных из API и их трансформация
def api_get_and_transform(args):
  start_date = args.start_date
  end_date = args.end_date
  use_default_date = args.use_default_date
  
  start_d, end_d = check_api_parameters(start_date, end_date, use_default_date)
  raw_data = get_data_from_api(start_d, end_d)

  validate_json(raw_data)

  df = transform_data(raw_data)

  return df

# Вставить данные из API в базу
def api_to_db(args):
  try:
      df = api_get_and_transform(args)
      save_to_db(df)
  except Exception as e:
      print(f"Ошибка при выгрузке API в БД: {e}")
      sys.exit(1)

# Положить данные из API в csv
def api_to_csv(args):
  try:
    df = api_get_and_transform(args)
    save_to_csv(df)
  except Exception as e:
    print(f"Ошибка при выгрузке API в csv: {e}")
    sys.exit(1)


def main():
  # Вызов параметров
  parser = argparse.ArgumentParser(description="ETL приложение")

  subparsers = parser.add_subparsers(dest="command", help="sub-commands")

  parser_json_to_db = subparsers.add_parser("json-to-db", help="Вставить данные из JSON в базу данных")
  parser_json_to_db.add_argument("--fname", help="название_файла.json в папке data_inputs")
  parser_json_to_db.set_defaults(func=json_to_db)

  parser_json_to_csv = subparsers.add_parser("json-to-csv", help="Сохранить данные из JSON в CSV файл")
  parser_json_to_csv.add_argument("--fname", help=" название_файла.json в папке data_inputs")
  parser_json_to_csv.set_defaults(func=json_to_csv)

  parser_api_to_db = subparsers.add_parser("api-to-db", help="Вставить данные из API в базу данных")
  parser_api_to_db.add_argument("--start-date", help="Дата начала прогноза (например 2025-05-05)", default = None)
  parser_api_to_db.add_argument("--end-date", help="Дата конца прогноза (например 2025-06-05)", default = None)
  parser_api_to_db.add_argument("--use-default-date", help="Получить прогноз на 7 дней начиная с сегодняшней даты (y/n)", default="n")
  parser_api_to_db.set_defaults(func=api_to_db)

  parser_api_to_csv = subparsers.add_parser("api-to-csv", help="Сохранить данные из API в CSV файл")
  parser_api_to_csv.add_argument("--start-date", help="Дата начала прогноза (например 2025-05-05)", default = None)
  parser_api_to_csv.add_argument("--end-date", help="Дата конца прогноза (например 2025-05-05)", default = None)
  parser_api_to_csv.add_argument("--use-default-date", help="Получить прогноз на 7 дней начиная с сегодняшней даты (y/n)", default="n")
  parser_api_to_csv.set_defaults(func=api_to_csv)

  args = parser.parse_args()

  if hasattr(args, 'func'):
    args.func(args)
  else:
    parser.print_help()

if __name__ == "__main__":
    main()
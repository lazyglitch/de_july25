import pandas as pd
from datetime import datetime
import pytz
import warnings

# Трансформация данных

# Функция перевода unix-времени в iso формат + вычисление длины светового дня в часах
def unix_to_datetime(datetime_values, full_date, timzone = None):
    result = []
    # если надо получить полную дату
    if full_date:
        for i in datetime_values:
            result.append(datetime.fromtimestamp(i, pytz.timezone(timzone)).isoformat())  # timzone берется из json файла/ответа
    # Если надо получить только часы светового дня 
    # Часы вычисляются без округления, даже если 16ч 59 мин, то длина светового дня указывается = 16
    # Например, для интервала 04:59 и 21:58 длина светового дня будет 16ч (хотя прошло почти 17ч)
    else:
        for i in datetime_values:
            result.append(datetime.fromtimestamp(i, tz=pytz.utc).hour)

    return result # []


# Функция для предобработки ежедневных данных (меняет формат даты, переименовывает данные)
def preprocess_daily_data(daily_data, daily_data_keys, timzone):
    result = {}

    for key, val in daily_params.items():
        if key == "daylight_duration":
            result[val] = unix_to_datetime(daily_data[key], False)
        else:
            result[val] = unix_to_datetime(daily_data[key], True, timzone)

    return result  # {'param1': [], 'param2': [], ...}


# Функция которая делит массим на подмассивы заданного размера
# Используется для почасовых данных
# Данные передаются одним массивом без деления на дни поэтому разбивка на подмассивы по 24 элемента позволяет определить границы дней (24ч)
def split_list_to_intervals(input_list, interval=24):
    result = []
    for i in range(0, len(input_list), interval):
        result.append(input_list[i : i + interval])
    return result # [[], [], [], ...]


# Функция которая возвращает те параметры у которых есть заданная метрика/единица измерения
def choose_params(params, metric_type):
    result = {}

    for key, value in params.items():
        if metric_type in value[0]:
            result[key] = [metric_type, value[1]]

    return result


# Функция подсчета среднего(avg) и общего(total)
def calculate_params(params, data, days_count, duration, daylight_hrs=None):
    result = {}

    for key in params.keys():

        nested_lists = data[key]
        metrics = []

        for day in range(days_count):
            # daylight_hrs это интервалы светового дня
            start_hour, end_hour = (
                daylight_hrs[day] if daylight_hrs is not None else [0, 24]
            )

            if not (
                0 <= start_hour <= 24 and 0 <= end_hour <= 24 and start_hour < end_hour
            ):
                warnings.warn(f"Неверный интервал для дня: ({start_hour}, {end_hour}).")
                return None

            try:
                interval = nested_lists[day][start_hour:end_hour]
                # Замена None на 0
                interval_no_none = [value if value is not None else 0 for value in interval]

                if params[key][0] == "avg":
                    daily_metric = sum(interval_no_none) / len(interval_no_none)
                else:
                    daily_metric = sum(interval_no_none)
                metrics.append(round(daily_metric, params[key][1]))

            except IndexError as e:
                warnings.warn(f"Неверный индекс для параметра {key}, день {day}. {e}")
                return None

        result[f"{params[key][0]}_{key}_{duration}"] = metrics

    return result


# Функция подсчета интервала светового дня
# Чтобы определить интервал, из даты заката вычтем количество световых часов
# Если просто брать час рассвета и час заказа как границы интервала то длина этого интервала не всегда совпадает с длиной светового дня в часах 
# Это обусловлено способом вычисления длины светового дня (см. unix_to_datetime())
def calculate_daylight_intervals(data, days_count):
    result = {}

    for i in range(days_count):
        end_h = int(datetime.fromisoformat(data["sunset_iso"][i]).hour)
        start_h = end_h - data["daylight_hours"][i]
        result[i] = [start_h, end_h]

    return result  # словарь вида {day_id:[start_h, end_h)}


# Общая логика для конвертации значений в другие единицы измерения
def convert_values(data, rnd, convert_func):
    result = []

    for lst_id in range(len(data)):
        # иногда в данных значения = None, их приводим к 0
        converted_data = [round(convert_func(value if value is not None else 0), rnd) for value in data[lst_id]]
        result.append(converted_data)
    return result  # [[], [], ...]


# Узлы в метры в секунду
def knots_to_ms(data, rnd):
    return convert_values(data, rnd, lambda x: x * 0.514) # 1 knot = 0.51444444444 м/с


# Градусы Фаренгейта в градусы Цельсия
def fahrenheit_to_celsius(data, rnd):
    return convert_values(data, rnd, lambda x: (x - 32) * 5.0 / 9.0)


# Дюймы в миллиметры
def inch_to_millimeters(data, rnd):
    return convert_values(data, rnd, lambda x: x * 25.4)

# Функция выбора способа конвертации параметров в зависимости от нужной единицы измерения
def convert_params(params, data, cmap):
    result = {}
    for key, (metric, rnd) in params.items():
        converter = cmap.get(metric)
        if converter:
            result[f"{key}_{metric}"] = converter(data[key], rnd)
        else:
            warnings.warn(
                f"Неизвестная единица измерения: {metric} для параметра {key}"
            )
    return result
# единицы измерения: функция 


# Почасовые параметры
# признаки: [метрики/единицы измерения, количество знаков после запятой]
hourly_params = {
    "temperature_2m": [["avg", "celsius"], 2],
    "relative_humidity_2m": ["avg", None],
    "dew_point_2m": ["avg", 2],
    "apparent_temperature": [["avg", "celsius"], 2],
    "temperature_80m": [["avg", "celsius"], 2],
    "temperature_120m": [["avg", "celsius"], 2],
    "wind_speed_10m": [["avg", "m_per_s"], 2],
    "wind_speed_80m": [["avg", "m_per_s"], 2],
    "visibility": ["avg", 3],
    "soil_temperature_0cm": ["celsius", 2],
    "soil_temperature_6cm": ["celsius", 2],
    "rain": [["total", "mm"], 3],
    "showers": [["total", "mm"], 3],
    "snowfall": [["total", "mm"], 3],
}

# Дневные параметры
# признак: его новое название
daily_params = {
    "daylight_duration": "daylight_hours",
    "sunset": "sunset_iso",
    "sunrise": "sunrise_iso",
}
# единицы измерения: функция
convert_map = {
    'm_per_s': knots_to_ms,
    'celsius': fahrenheit_to_celsius,
    'mm': inch_to_millimeters
}

# Функция, которая объединяет все шаги трансформации параметров в формат итоговой таблицы (DataFrame)
def transform_data(data_json, 
                    daily_params = daily_params, 
                    hourly_params = hourly_params, 
                    convert_map = convert_map):
    # Определяем часовой пояс
    timezone = data_json['timezone']

    # Ежедневные данные
    daily_data = data_json["daily"].copy()
    # Предобрабатываем дневные данные
    daily_converted = preprocess_daily_data(daily_data, daily_params, timezone)
    print("Дневные данные успешно предобработаны")

    # Почасовые данные
    hourly_data = data_json["hourly"].copy()

    chunked_hourly_data = {}
    # Делим массив значений каждого параметра на подмассив из 24 значений (в сутках 24ч)
    for key in hourly_params.keys():
        chunked_hourly_data[key] = split_list_to_intervals(hourly_data[key], 24)
    print("Почасовые данные разбиты на подмассивы")
    # Определяем количеств дней прогноза
    days_count = len(daily_data["time"])
    # Определяем интервал светового дня для каждого дня
    daylight_h_intervals = calculate_daylight_intervals(daily_converted, days_count)

    hourly_calculated_24h = {}
    # Считаем avg и total для 24ч
    for metric in ["avg", "total"]:
        calculated_params = calculate_params(
            choose_params(hourly_params, metric), chunked_hourly_data, days_count, "24h"
        )
        hourly_calculated_24h.update(calculated_params)
    print("Предобработка почасовых данных: 33%")

    hourly_calculated_daylight = {}
    # Считаем avg и total для светового дня
    for metric in ["avg", "total"]:
        calculated_params = calculate_params(
            choose_params(hourly_params, metric),
            chunked_hourly_data,
            days_count,
            "daylight",
            daylight_h_intervals,
        )
        hourly_calculated_daylight.update(calculated_params)
    print("Предобработка почасовых данных: 66%")

    hourly_converted_data = {}
    # Считаем m_per_s, celsius и mm (почасовые данные)
    for metric in ["m_per_s", "celsius", "mm"]:
        calculated_params = convert_params(
                                            choose_params(hourly_params, metric), 
                                            chunked_hourly_data, 
                                            convert_map
                                            )
        hourly_converted_data.update(calculated_params)
    print("Почасовые данные успешно предобработаны")

    # Объединяем все нужные словари
    full_data = {
        **hourly_calculated_24h,
        **hourly_calculated_daylight,
        **hourly_converted_data,
        **daily_converted,
    }
    # Преобразование обработанных данных в DataFrame
    return pd.DataFrame(full_data)
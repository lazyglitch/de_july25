USER_TIMEZONE = "Asia/Novosibirsk" 

TABLE_NAME = 'daily_forecast' # такое же как в файле create_table.sql

BASE_API_URL = (
    "https://api.open-meteo.com/v1/forecast?"
    "latitude=55.0344&longitude=82.9434&"
    "daily=sunrise,sunset,daylight_duration&"
    "hourly=temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,temperature_80m,temperature_120m,wind_speed_10m,wind_speed_80m,wind_direction_10m,wind_direction_80m,visibility,evapotranspiration,weather_code,soil_temperature_0cm,soil_temperature_6cm,rain,showers,snowfall&"
    "timezone=auto&timeformat=unixtime&"
    "wind_speed_unit=kn&temperature_unit=fahrenheit&precipitation_unit=inch"
)
CREATE OR REPLACE TABLE clima.silver.clima
SELECT
  air_pressure_at_sea_level AS pressao_ar_nivel_mar,
  air_temperature AS temperatura_ar,
  cloud_area_fraction AS fracao_nuvens,
  relative_humidity AS umidade_relativa,
  time AS data_hora,
  wind_from_direction AS direcao_vento,
  wind_speed AS velocidade_vento,
  data_processamento AS data_processamento
FROM clima.bronze.weather
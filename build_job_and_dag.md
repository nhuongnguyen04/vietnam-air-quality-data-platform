Cách ingest dữ liệu:

AQICN (World Air Quality Index - api.waqi.info):
    - Tao job:
        - Ingest stations tu crawl.html (aqicn.org/city/vietnam/)
        - Tu stations lay cac measurements: https://api.waqi.info/feed/@{station_id}/?token=...
        - Tu measurements lay cac forecast: https://api.waqi.info/feed/@{station_id}/?token=...
        - Tien hanh tranform du lieu va insert vao ClickHouse theo schema trong init-clickhouse.sql
    => Luu y: Du lieu stations chi lay 1 lan
            - Moi lan ingest 1 gio du lieu pham vi 1 gio

Sensors.Community (api.sensor.community):
    - Tao job:
        - Ingest sensors metadata
        - Ingest measurements tu bounding box Vietnam

OpenWeather Air Pollution (api.openweathermap.org):
    - Tao job:
        - Ingest measurements tu cac thanh pho Vietnam

Dag:
    - Tao dag:
        - Dag chay 1 gio 1 lan
        - Dag chay song song job AQICN, Sensors.Community va OpenWeather

Cach orchestrate dbt:
    - Tao dag:
        - Sau khi ingest du lieu xong, chay dbt dbt build hoac run de tranform du lieu

Yeu cau:
    - Code ro rang, comment giai thich day du
    - Code dam bao tinh san sang cao, kha nang scale
    - Code dam bao tinh bao mat, khong lo du lieu
    - Code dam bao tinh hieu qua, khong lang phi tai nguyen
    - Co log day du, de debug

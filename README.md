# PDBS & DuckDB

## Instalace knihoven a stažení dat

```sh
python -m venv venv
venv\Scripts\activate
pip install kagglehub duckdb
curl -L -o used-car-price-dataset.zip  https://www.kaggle.com/api/v1/datasets/download/kreeshrajani/used-car-price-dataset
tar -xf used-car-price-dataset.zip
curl -L -o cars-2022-dataset.zip  https://www.kaggle.com/api/v1/datasets/download/tr1gg3rtrash/cars-2022-dataset
tar -xf cars-2022-dataset.zip
duckdb -ui
```

## DuckDB

Po zadání `duckdb -ui` se nám otevře notebook kde můžeme začít zadávat SQL příkazy. Začneme načtením dat.

```sql
create or replace table used_cars as 
select * from read_csv('used_car_dataset.csv',
  header = true,
    store_rejects = true,
                escape = '\',
  rejects_table = 'user_cars_reject_table',	
  rejects_scan= 'user_cars_scan_table'
)
```

```sql
create or replace table cars as 
select *, row_number() over () id
from read_csv('CARS_1.csv',
  header = true,
    store_rejects = true,
                escape = '\',
  rejects_table = 'cars_reject_table',	
  rejects_scan= 'cars_scan_table'
)
```

Údaje v tabulce `used_cars` jsou u ceny i najetých km v řetězci. Pro další zpracvování dat by bylo vhodné je normalizovat.

```sql
CREATE OR REPLACE TABLE used_cars_normalized AS
SELECT
  id,
  car_name,
  fuel_type,
  city,
  year_of_manufacture,
  CASE
    WHEN car_price_in_rupees ILIKE '%lakh%' THEN
      CAST(
        TRY_CAST(
          REPLACE(REGEXP_REPLACE(car_price_in_rupees, '[^0-9.]', '', 'g'), ',', '') AS DOUBLE
        ) * 100000 AS BIGINT
      )
    ELSE
      NULL
  END AS price_numeric,
  TRY_CAST(REPLACE(REGEXP_REPLACE(kms_driven, '[^0-9]', '', 'g'), ',', '') AS BIGINT) AS kms_numeric
FROM used_cars;
```

V dalím kroku by bylo vhodné data z obout tabulek propojit. Nicméně máme pouze názvy aut a není možné tabulky spojit jen na základě rovnosti.  Pro další práci si data nyní uložíme zpět do CSV, abychom je byli schopni načíst z Pythonu.

```sql
COPY user_cars TO 'cleaned_user_cars.csv' (HEADER, DELIMITER ',');
```

## DuckDB a Python

Nyní se přesuneme do prostředí Python, kde si data opět načteme s pomocí DuckDB. Python nám umožní vytvořit jednoduchou uživatelskou funkci (UDF), která bude mít na vstupu dva řetězce a vrátí počet společných slov. Tato funkce pak v DuckDB umožní spojení obou tabulek na základě společných slov.

```python
import duckdb
import numpy

con = duckdb.connect()

con.execute("""
    CREATE OR REPLACE TABLE used_cars AS
    SELECT * FROM read_csv_auto('cleaned_user_cars.csv');
""")

con.execute("""
    CREATE OR REPLACE TABLE cars AS
    SELECT * FROM read_csv_auto('CARS_1.csv');
""")

# Define the Python UDF to count common words
def common_word_count(a, b):
    if a is None or b is None:
        return 0
    return len(set(a.lower().split()) & set(b.lower().split()))

con.create_function('common_word_count', common_word_count, return_type=duckdb.typing.INTEGER)

con.execute("""
    CREATE OR REPLACE TABLE used_cars_joined AS
    SELECT u.*, c.*, common_word_count(u.car_name, c.car_name) AS common_words
    FROM used_cars u
    JOIN cars c
    ON common_word_count(u.car_name, c.car_name) > 2
""")

con.execute("""
    COPY used_cars_joined TO 'joined_user_cars.parquet' (FORMAT 'parquet');
""")
```

Provedení similarity self-joinu je relativně náročný úkol, jelikož databáze musí nejprve provést kartézský součin a pak nad každou dvojící provádí uvedenou funkci.

Výsledná data jsme tentokrát uložili do parquet formátu místo CSV.

# Úkoly

Zkontrolujte výsledná data v DukcDB UI a popište problémy, které při spojení obou tabulek nastaly. 

Zkuste vyřešit následující úkoly:
1. Upravte výsledek spojení tak, aby se na jeden řádek `used_cars` napojil vždy maximálně jeden řádek z tabulky `cars`.
2. Nyní většina řádků z `used_cars` chybí. Vyřeště to nějakým vhodným způsobem.
3. Vypište průměrnou cenu aut v jednotlivých Indických městech
4. Nalezněte model auta s největším množstvím inzerátů. Jeden záznam v cars odpovídá jednomu modelu auta.
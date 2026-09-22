import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# -----------------------------------------------------------------------------------------------------------------------------------------------------------------
# ----------------------           НАЧИНАЕМ СПАРК СЕССИИЮ           ---------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------
def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("scale-kaggle-sales")
        .getOrCreate()
    )





# -----------------------------------------------------------------------------------------------------------------------------------------------------------------
# ----------------------           ФУНКЦИЯ ЧТЕНИЯ ТАБЛИЦ ИЗ БД           ---------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------
def read_postgres_table(
    spark: SparkSession,
    table_name: str
) -> DataFrame:
    jdbc_url = (
        f"jdbc:postgresql://"
        f"{os.environ['DWH_POSTGRES_HOST']}:"
        f"{os.environ['DWH_POSTGRES_PORT']}/"
        f"{os.environ['DWH_POSTGRES_DB']}"
    )

    return (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("user", os.environ["DWH_POSTGRES_USER"])
        .option("password", os.environ["DWH_POSTGRES_PASSWORD"])
        .option("driver", "org.postgresql.Driver")
        .load()
    ) 





# -----------------------------------------------------------------------------------------------------------------------------------------------------------------
# ----------------------           ФУНКЦИЯ ЧТЕНИЯ CSV ФАЙЛА           ---------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------
def read_csv_table(
        spark: SparkSession,
        file_path: str
) -> DataFrame:
    return(
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(file_path)
    )





# -----------------------------------------------------------------------------------------------------------------------------------------------------------------
# ----------------------           ФУНКЦИЯ ТРАНСФОРМАЦИИ И ПОДГОТОВКИ К ЗАПИСИ           ---------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------
def transform_sales(
    sales_df: DataFrame,
    cities_df: DataFrame,
    city_profiles_df: DataFrame
    
) -> DataFrame:
# ----------------------           СОЗДАЕМ ВРЕМЕННЫЙ df С КОЛОНКАМИ ИЗ ТАБЛИЦЫ ГОРОДОВ           ---------------------------------------------------------------------------
    intermediate_city_directory_df = cities_df.select(
        "city_slug",
        "city_name",
        "latitude",
        "longitude"
    )

    intermediate_city_directory_join = intermediate_city_directory_df.join(
        city_profiles_df,
        on="city_slug",
        how="left"
    )


# ----------------------           СОЗДАЕМ ВРЕМЕННЫЙ df С КОЛОНКАМИ ИЗ ТАБЛИЦЫ ПРОДАЖ           ---------------------------------------------------------------------------
    intermediate_sales_reference = sales_df.select(
        F.col("money").alias("amount_money"),
        F.col("coffee_name").alias("product_name"),
        F.col("cash_type").alias("payment_type"),
        "date",
        "time"
    )
# ----------------------           ПОДГОТАВЛИВАЕМ СТРОКУ ДЛЯ ДАЛЬНЕЙШЕГО ПРЕОБРАЗОВАНИЯ В СТАБИЛЬНЫЙ ИДЕНТИФИКАТОР           ---------------------------------------------------------------------------
    intermediate_sales_reference_full = intermediate_sales_reference.withColumn(
        "source_sale_string",
        F.concat_ws("|", F.col("date").cast("string"), F.col("time").cast("string"), "product_name", F.col("amount_money").cast("string"), "payment_type")
    )

# ----------------------           ДЕЛАЕМ СТАБИЛЬНЫЙ ИДЕНТИФИКАТОР, ГДЕ У ОДНОЙ ПРОДАЖИ - ОДИНАКОВЫЙ ИДЕНТИФИКАТОР ВО ВСЕХ ГОРОДАХ           ---------------------------------------------------------------------------
    intermediate_sales_reference_append_id = intermediate_sales_reference_full.withColumn(
        "source_sale_id",
        F.sha2("source_sale_string", 256)
    )

# ----------------------           JOIN ПРОДАЖИ НА ГОРОД           ---------------------------------------------------------------------------
    expanded_sales_df = intermediate_sales_reference_append_id.crossJoin(
        intermediate_city_directory_join
    )

    expanded_sales_df = (
        expanded_sales_df

        # --- добавляем рублевые расчеты ---
        .withColumn("amount_rub", (F.col("amount_money") * 10 * F.col("price_multiplier")).cast("decimal(12,2)"))

        # --- добавляем id чека synthetic_sale_id ---
        .withColumn("synthetic_sale_id", F.sha2((F.concat_ws("|", F.col("source_sale_id"), F.col("city_slug"))), 256))

        # --- добавляем позицию чека ---
        .withColumn("line_number", F.lit(1))
        .withColumn("quantity", F.lit(1))
        .withColumn("synthetic_sale_item_id", F.sha2(F.concat_ws("|", F.col("synthetic_sale_id"), F.col("line_number").cast("string")), 256))

        # --- собираем местную дату и время, преобразуем результат во время, получаем момент продажи в utc ---
        .withColumn("sold_local_text", F.concat_ws(" ", F.col("date").cast("string"), F.date_format(F.col("time"), "HH:mm:ss.SSS")))
        .withColumn("sold_local_at", F.to_timestamp("sold_local_text", "yyyy-MM-dd HH:mm:ss.SSS"))
        .withColumn("sold_at", F.to_utc_timestamp("sold_local_at", F.col("timezone")))

        # --- добавляем признак происхождения ---
        .withColumn("is_synthetic", F.lit(True))
    )

    # --- выбираем необходимые строки ---
    final_sales_df = expanded_sales_df.select(
        "source_sale_id",
        "synthetic_sale_id", 
        "synthetic_sale_item_id", 
        "city_slug", 
        "timezone", 
        "date", 
        "sold_at", 
        "product_name", 
        "payment_type", 
        "amount_money", 
        "price_multiplier", 
        "amount_rub", 
        "quantity", 
        "line_number", 
        "is_synthetic"
    )


    return final_sales_df





# -----------------------------------------------------------------------------------------------------------------------------------------------------------------
# ----------------------           ФУНКЦИЯ ПРОВЕРКИ ДАННЫХ ПЕРЕД ЗАПИСЬЮ В БД           ---------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------
def validate_sales(
    result_df: DataFrame,
    expected_count: int 
) -> None:

    # --- считаем общее количество строк ---
    total_rows = result_df.count()
    
# ----------------------           ПРОВЕРКА НА КОЛИЧЕСТВО СТРОК           ---------------------------------------------------------------------------
    if total_rows != expected_count:
        raise ValueError(f"Ожидаемое количество строк: {expected_count}, а фактическое составляет: {total_rows}")




# ----------------------           ПРОВЕРКА НА ЗАПОЛНЕННОСТЬ ОБЯЗАТЕЛЬНОГО ПОЛЯ            ---------------------------------------------------------------------------
    list_of_columns_to_check = [
        "source_sale_id",
        "synthetic_sale_id",
        "synthetic_sale_item_id",
        "city_slug",
        "timezone",
        "sold_at", 
        "product_name", 
        "amount_rub", 
        "quantity",
        "line_number" 
        ]

    for col_name in list_of_columns_to_check:
        null_count = result_df.filter(F.col(col_name).isNull()).count()
        if null_count > 0:
            raise ValueError(f"В колонке '{col_name}' найдено {null_count} NULL-значений")


# ----------------------           ПРОВЕРКА НА УНИКАЛЬНОСТЬ ДВУХ КЛЮЧЕЙ            ---------------------------------------------------------------------------

    # --- считаем уникальное количество значений в столбце synthetic_sale_id ---
    uniqueness_synthetic_sale_id = result_df.select("synthetic_sale_id").distinct().count()
    # --- считаем уникальное количество значений в столбце synthetic_sale_item_id ---
    uniqueness_synthetic_sale_item_id = result_df.select("synthetic_sale_item_id").distinct().count()

    # --- делаем проверку на уникальность и отдаем ошибку в случае чего ---
    if uniqueness_synthetic_sale_id != total_rows:
        raise ValueError(f"synthetic_sale_id не уникален: строк {total_rows}, уникальных ID {uniqueness_synthetic_sale_id}")

    if uniqueness_synthetic_sale_item_id != total_rows:
        raise ValueError(f"uniqueness_synthetic_sale_item_id не уникален: строк {total_rows}, уникальных ID {uniqueness_synthetic_sale_item_id}")

    # --- проверяем колокнку amount_rub на логичность записи ---
    amount_rub_check = result_df.filter(F.col("amount_rub") <= 0)
    amount_rub_check_count = amount_rub_check.count()
    if amount_rub_check_count > 0:
        raise ValueError(f"Количество значений amount_rub, которые меньше или равны нулю: {amount_rub_check_count}")
    
    # --- проверяем колокнку quantity на логичность записи ---
    quantity_check = result_df.filter(F.col("quantity") != 1)
    quantity_check_count = quantity_check.count()
    if quantity_check_count > 0:
        raise ValueError(f"Количество значений quantity, которые не равны 1: {quantity_check_count}")
    
    # --- проверяем колокнку line_number на логичность записи ---
    line_number_check = result_df.filter(F.col("line_number") != 1)
    line_number_check_count = line_number_check.count()
    if line_number_check_count > 0:
        raise ValueError(f"Количество значений line_number, которые не равны 1: {line_number_check_count}")




# -----------------------------------------------------------------------------------------------------------------------------------------------------------------
# ----------------------           ФУНКЦИЯ ЗАПИСИ ТАБЛИЦЫ В БД           ---------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------
def write_postgres_table(
    result_df: DataFrame,
    table_name: str
) -> None:
    # --- подключение к БД ---
    jdbc_url = (
        f"jdbc:postgresql://"
        f"{os.environ['DWH_POSTGRES_HOST']}:"
        f"{os.environ['DWH_POSTGRES_PORT']}/"
        f"{os.environ['DWH_POSTGRES_DB']}"
    )
    (
        result_df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("user", os.environ["DWH_POSTGRES_USER"])
        .option("password", os.environ["DWH_POSTGRES_PASSWORD"])
        .option("driver", "org.postgresql.Driver")
        .option("numPartitions", 2)
        .mode("errorifexists")
        .save()
    )





# -----------------------------------------------------------------------------------------------------------------------------------------------------------------
# ----------------------           MAIN ФУНКЦИЯ           ---------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------
def main() -> None:
    # --- создаём Spark-сессию для чтения и обработки данных ---
    spark = create_spark_session()

    try:
        
        # --- читаем исходные продажи из посгри ---
        sales_df = read_postgres_table(
            spark,
            "raw.kaggle_coffee",
        )
        # --- читаем справочник городов из посгри ---
        cities_df = read_postgres_table(
            spark,
            "raw.cities",
        )
        # --- читаем CSV с часовыми поясами и коэффициентами цен городов ---
        city_profiles_df = read_csv_table(
            spark,
            "/opt/spark/jobs/config/city_sales_profiles.csv",
        )

        # --- передаём три таблицы в нашу функцию преобразования, она размножает продажи по городам, рассчитывает цены, создает идентификаторы и подготавливает время продажи ---
        result_df = transform_sales(
            sales_df,
            cities_df,
            city_profiles_df,
        )


        # --- читаем исходные продажи и города --
        sales_count = sales_df.count()
        cities_count = cities_df.count()

        # --- каждая продажа должна появиться в каждом городе --- 
        expected_count = sales_count * cities_count

        # --- вызываем функцию проверки --- 
        validate_sales(
            result_df,
            expected_count
        )
        print("Проверки пройдены")

        # --- Выполняем запись в БД ---
        write_postgres_table(
            result_df,
            "raw.synthetic_coffee_sales"
        )
        print("Запись завершена")

    # --- освобождаем ресурсы спарка --- 
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
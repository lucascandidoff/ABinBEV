from etl import bronze_layer, silver_layer, gold_layer

if __name__ == "__main__":
    print(">>> Starting Bronze Layer")
    bronze_layer.run()

    print(">>> Starting Silver Layer")
    silver_layer.run()

    print(">>> Starting Gold Layer")
    gold_layer.run()

    print(">>> ETL pipeline completed!")

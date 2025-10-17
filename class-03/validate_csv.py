# validate_csv.py
import sys
import great_expectations as gx

# 1) Kontekst
context = gx.get_context()  # FileDataContext jeśli w katalogu jest ./gx, inaczej Ephemeral

# 2) Datasource -> Asset -> Batch Definition (ścieżka do pojedynczego pliku CSV)
data_source = context.data_sources.add_pandas_filesystem(
    name="local_files", 
    base_directory="."            # katalog bazowy; możesz dać np. "./data"
)

# Uwaga: add_csv_asset tworzy "file data asset" na pliki CSV
csv_asset = data_source.add_csv_asset(name="orders_csv")

# Wskaż konkretny plik (ścieżka względna względem base_directory z datasource)
batch_def = csv_asset.add_batch_definition_path(
    name="orders_file",
    path="data/orders.csv"            # <- podmień na swoją ścieżkę, np. "data/orders.csv"
)

# 3) Expectation Suite (definicja reguł jakości)
suite_name = "orders_suite"
suite = gx.ExpectationSuite(name=suite_name)

# Dodaj kilka przykładowych oczekiwań (używa się klas z gx.expectations)
suite.add_expectation(
    gx.expectations.ExpectTableRowCountToBeBetween(min_value=1, max_value=None)
)
suite.add_expectation(
    gx.expectations.ExpectColumnToExist(column="order_id")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeUnique(column="order_id")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(column="amount", min_value=0)
)

# Zapisz suite do kontekstu (jeśli chcesz, żeby było trwałe w FileDataContext)
suite = context.suites.add(suite)

# 4) Validation Definition = (Batch Definition + Suite)
validation_definition = gx.ValidationDefinition(
    data=batch_def,
    suite=suite,
    name="orders_validation"
)

# (opcjonalnie można dodać do kontekstu)
# validation_definition = context.validation_definitions.add(validation_definition)

# 5) Uruchom walidację
results = validation_definition.run()

# 6) Wyświetl skrót wyników i ustaw kod wyjścia (0/1)
print(results)                       # JSON-owy skrót (pass/fail dla każdej expectation)
success = results.success            # True/False dla całej walidacji
sys.exit(0 if success else 1)


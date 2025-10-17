# validate_csv_doc.py
# Great Expectations 1.7.x — walidacja CSV + raport HTML (Data Docs)

from pathlib import Path
from datetime import datetime
import csv
import great_expectations as gx
from great_expectations.data_context import EphemeralDataContext

# --- 0) Ścieżki i dane przykładowe ---
ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
CSV_PATH = DATA_DIR / "orders.csv"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Utwórz przykładowy CSV, jeśli nie istnieje
if not CSV_PATH.exists():
    with CSV_PATH.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "country", "amount", "order_date"])
        w.writerow([1, "PL", 120.5, "2025-10-01"])
        w.writerow([2, "PL", 89.9, "2025-10-02"])
        w.writerow([3, "DE", 150.0, "2025-10-03"])
        w.writerow([4, "PL", "", "2025-10-04"])  # brak amount -> pokaże błąd
        w.writerow([5, "DE", 40.0, "2025-10-05"])

TS = datetime.now().strftime("%Y%m%d_%H%M%S")  # unikamy kolizji nazw

# --- 1) Kontekst GX (plikowy, żeby mieć ./gx i Data Docs) ---
context = gx.get_context()
if isinstance(context, EphemeralDataContext):
    context = context.convert_to_file_context()  # utworzy katalog ./gx

# --- 2) Datasource (pandas + filesystem) ---
# Spróbuj dodać; jeśli istnieje, pobierz istniejący o tej samej nazwie.
try:
    ds = context.data_sources.add_pandas_filesystem(
        name="local_fs",
        base_directory=str(ROOT),  # bazą jest katalog projektu
    )
except Exception:
    ds = context.data_sources.get(name="local_fs")

# --- 3) CSV Asset + Batch Definition (konkretny plik) ---
asset_name = f"orders_asset_{TS}"
csv_asset = ds.add_csv_asset(name=asset_name)

batch_def = csv_asset.add_batch_definition_path(
    name=f"orders_file_{TS}",
    path=str(CSV_PATH.relative_to(ROOT))  # np. "data/orders.csv"
)

# --- 4) Expectation Suite (reguły jakości) ---
suite_name = f"orders_suite_{TS}"
suite = gx.ExpectationSuite(name=suite_name)

suite.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(min_value=1))
suite.add_expectation(gx.expectations.ExpectColumnToExist(column="order_id"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="order_id"))

suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="country"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
    column="country",
    value_set=["PL", "DE", "US", "FR"],
))

suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="amount"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
    column="amount",
    min_value=0,
    max_value=100000,
    strict_min=False,
    strict_max=False,
))

suite.add_expectation(gx.expectations.ExpectColumnValuesToMatchStrftimeFormat(
    column="order_date",
    strftime_format="%Y-%m-%d",
))

# Zapisz suite do kontekstu
context.suites.add(suite)

# --- 5) Walidacja (ValidationDefinition) ---
validation = gx.ValidationDefinition(
    data=batch_def,
    suite=suite,
    name=f"orders_validation_{TS}",
)
results = validation.run()

print("\n=== WYNIK WALIDACJI ===")
print("Sukces:", results.success)
print("Podsumowanie:",
      {k: results.statistics.get(k) for k in ("evaluated_expectations",
                                               "successful_expectations",
                                               "unsuccessful_expectations",
                                               "success_percent")})

# --- 6) Raport HTML (Data Docs) ---
index_urls = context.build_data_docs()  # zwraca np. {'local_site': 'file:///.../gx/data_docs/local_site/index.html'}
print("\n📊 Data Docs index URLs:", index_urls)

# (opcjonalnie) automatycznie otwórz raport w przeglądarce:
# context.open_data_docs()


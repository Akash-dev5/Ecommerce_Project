from pathlib import Path
import pandas as pd


# ---------------------------------------------------------
# 1. PROJECT DIRECTORIES
# ---------------------------------------------------------

SOURCE_DIR = Path("Data/Source")
BATCH_DIR = Path("Data/Batches")


# ---------------------------------------------------------
# 2. SOURCE FILE PATHS
# ---------------------------------------------------------

orders_file = SOURCE_DIR / "orders_dataset.csv"
customers_file = SOURCE_DIR / "customers_dataset.csv"
order_items_file = SOURCE_DIR / "order_items_dataset.csv"
payments_file = SOURCE_DIR / "order_payments_dataset.csv"
reviews_file = SOURCE_DIR / "order_reviews_dataset.csv"


# ---------------------------------------------------------
# 3. READ SOURCE DATA
# ---------------------------------------------------------

print("Reading source files...")

orders = pd.read_csv(orders_file)
customers = pd.read_csv(customers_file)
order_items = pd.read_csv(order_items_file)
payments = pd.read_csv(payments_file)
reviews = pd.read_csv(reviews_file)

print("Source files loaded successfully.")


# ---------------------------------------------------------
# 4. CONVERT ORDER TIMESTAMP
# ---------------------------------------------------------

orders["order_purchase_timestamp"] = pd.to_datetime(
    orders["order_purchase_timestamp"]
)


# ---------------------------------------------------------
# 5. CREATE MONTH USED FOR BATCHING
# ---------------------------------------------------------

orders["batch_month"] = (
    orders["order_purchase_timestamp"]
    .dt.to_period("M")
)


# ---------------------------------------------------------
# 6. CREATE THE MAIN BATCH DIRECTORY
# ---------------------------------------------------------

BATCH_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------
# 7. GET ALL AVAILABLE MONTHS
# ---------------------------------------------------------

batch_months = (
    orders["batch_month"]
    .dropna()
    .sort_values()
    .unique()
)

print()
print(f"Number of monthly batches to create: {len(batch_months)}")


# ---------------------------------------------------------
# 8. CREATE EACH MONTHLY BATCH
# ---------------------------------------------------------

for batch_month in batch_months:

    print()
    print("=" * 60)
    print(f"Creating batch: {batch_month}")
    print("=" * 60)

    # -----------------------------------------------------
    # 8.1 FILTER ORDERS FOR THIS MONTH
    # -----------------------------------------------------

    batch_orders = orders.loc[
        orders["batch_month"] == batch_month
    ].copy()

    # Keep the original source structure.
    # batch_month was only used for processing.
    batch_orders = batch_orders.drop(
        columns=["batch_month"]
    )


    # -----------------------------------------------------
    # 8.2 GET ORDER IDs FROM THIS BATCH
    # -----------------------------------------------------

    batch_order_ids = batch_orders["order_id"]


    # -----------------------------------------------------
    # 8.3 GET CUSTOMERS FOR THIS BATCH
    # -----------------------------------------------------

    batch_customer_ids = batch_orders["customer_id"]

    batch_customers = customers.loc[
        customers["customer_id"].isin(batch_customer_ids)
    ].copy()


    # -----------------------------------------------------
    # 8.4 GET ORDER ITEMS FOR THIS BATCH
    # -----------------------------------------------------

    batch_order_items = order_items.loc[
        order_items["order_id"].isin(batch_order_ids)
    ].copy()


    # -----------------------------------------------------
    # 8.5 GET PAYMENTS FOR THIS BATCH
    # -----------------------------------------------------

    batch_payments = payments.loc[
        payments["order_id"].isin(batch_order_ids)
    ].copy()


    # -----------------------------------------------------
    # 8.6 GET REVIEWS FOR THIS BATCH
    # -----------------------------------------------------

    batch_reviews = reviews.loc[
        reviews["order_id"].isin(batch_order_ids)
    ].copy()


    # -----------------------------------------------------
    # 8.7 CREATE BATCH FOLDER
    # -----------------------------------------------------

    batch_folder = BATCH_DIR / f"batch_{batch_month}"

    batch_folder.mkdir(
        parents=True,
        exist_ok=True
    )


    # -----------------------------------------------------
    # 8.8 WRITE CSV FILES
    # -----------------------------------------------------

    batch_orders.to_csv(
        batch_folder / "orders.csv",
        index=False
    )

    batch_customers.to_csv(
        batch_folder / "customers.csv",
        index=False
    )

    batch_order_items.to_csv(
        batch_folder / "order_items.csv",
        index=False
    )

    batch_payments.to_csv(
        batch_folder / "payments.csv",
        index=False
    )

    batch_reviews.to_csv(
        batch_folder / "reviews.csv",
        index=False
    )


    # -----------------------------------------------------
    # 8.9 PRINT BATCH SUMMARY
    # -----------------------------------------------------

    print(
        f"Orders:       {len(batch_orders):,}"
    )

    print(
        f"Customers:    {len(batch_customers):,}"
    )

    print(
        f"Order items:  {len(batch_order_items):,}"
    )

    print(
        f"Payments:     {len(batch_payments):,}"
    )

    print(
        f"Reviews:      {len(batch_reviews):,}"
    )

    print(
        f"Created at:   {batch_folder}"
    )


# ---------------------------------------------------------
# 9. FINAL MESSAGE
# ---------------------------------------------------------

print()
print("=" * 60)
print("ALL MONTHLY BATCHES CREATED SUCCESSFULLY.")
print("=" * 60)
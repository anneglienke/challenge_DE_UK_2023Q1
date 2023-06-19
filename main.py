import json
import pandas as pd
import logging
import os
from cerberus import Validator

INPUT_PATH = "./input/orders.jsonl"
OUTPUT_PATH = "./output/"

OUTPUT_CONFIG = {
    "customers": ["customer_id", "city", "country"],
    "products": ["product_id", "product_name"],
    "order_items": ["order_id", "customer_id", "product_id", "quantity", "price_gbp"],
}

ORDER_SCHEMA = {
    "order_id": {
        "type": "integer",
        "required": True,
        "nullable": False,
        "empty": False,
    },
    "customer_id": {
        "type": "integer",
        "required": True,
        "nullable": False,
        "empty": False,
    },
    "customer_city": {
        "type": "string",
        "required": True,
        "nullable": False,
        "empty": False,
    },
    "customer_country": {
        "type": "string",
        "required": True,
        "nullable": False,
        "empty": False,
    },
    "order_items": {
        "type": "list",
        "minlength": 1,
        "empty": False,
        "schema": {
            "type": "dict",
            "schema": {
                "product_id": {
                    "type": "integer",
                    "required": True,
                    "nullable": False,
                    "empty": False,
                },
                "product_name": {
                    "type": "string",
                    "required": True,
                    "nullable": False,
                    "empty": False,
                },
                "quantity": {
                    "type": "integer",
                    "required": True,
                    "nullable": False,
                    "empty": False,
                    "min": 1,
                },
                "price_gbp": {
                    "type": "number",
                    "required": True,
                    "nullable": False,
                    "empty": False,
                },
            },
        },
    },
}

# Set up logs
logging.basicConfig(format="[%(asctime)s] %(message)s", level=logging.INFO)


# Use a Cerberus to validate dicts according to ORDER_SCHEMA
def validateObjects(order: dict):
    v = Validator(ORDER_SCHEMA)
    return v.validate(order), v.errors


# Open a .jsonl file and transform into a list of Python dicts
def extractData(dataset: str):
    # Open JSON file
    input_json = open(dataset, "r")
    list_obj = []
    # Iterate through each JSON object, convert JSON string into Python dict
    for obj in input_json:
        py_dict = json.loads(obj)
        list_obj.append(py_dict)
    logging.info(f"Loaded {len(list_obj)} rows.")

    # Validate each obj in the list according to ORDER_SCHEMA
    valid_obj = []
    for obj in list_obj:
        valid, errors = validateObjects(obj)
        if valid:
            valid_obj.append(obj)
        else:
            logging.info(f'Discarded invalid object(s): "{obj}", errors: {errors}')
    valid_rows = len(valid_obj)
    return valid_obj, valid_rows


# Transform order list by initializing a DataFrame with flattened nested values and renamed columns
def transformOrdersList(valid_orders: list, valid_rows: int):
    # Flatten order_items
    flat_df = pd.json_normalize(
        valid_orders,
        meta=["order_id", "customer_id", "customer_city", "customer_country"],
        record_path="order_items",
    )
    # Rename columns
    renamed_df = flat_df.rename(
        columns={"customer_city": "city", "customer_country": "country"}
    )
    logging.info(f"Created a dataframe with {valid_rows} valid rows.")
    return renamed_df


# Deletes files on ./output/ directory
def deleteOutputFiles(output_path: str):
    # Get a list of all the files in the directory
    files = os.listdir(output_path)
    logging.info(f"Found the following file(s) in the output directory: {files}")
    for file in files:
        # Create the full path to the file
        file_path = os.path.join(output_path, file)
        # Delete the file
        os.remove(file_path)
    logging.info(f"Deleted all the files in the output directory")


# Assemble output data as configured in OUTPUT_CONFIG, remove duplicates and create CSV files
def loadData(df: pd.DataFrame):
    # Delete existing files in the ./output/ directory
    deleteOutputFiles(OUTPUT_PATH)

    # Create new files in the ./output/ directory
    for file_name, cols in OUTPUT_CONFIG.items():
        # Drop duplicates
        deduplicated_df = df.drop_duplicates(
            subset=cols, keep="first", ignore_index=True
        )
        # Create CSV file
        deduplicated_df.to_csv(
            f"{OUTPUT_PATH}{file_name}.csv",
            sep=",",
            columns=cols,
            header=True,
            index_label="index",
        )
        logging.info(f'Created CSV file "{file_name}.csv"')


def execute():
    valid_orders, valid_rows = extractData(INPUT_PATH)
    df = transformOrdersList(valid_orders, valid_rows)
    loadData(df)


execute()

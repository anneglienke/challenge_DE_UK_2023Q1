## 1. The Task

The task is to write a Python program to read in an input file [orders.jsonl](input/orders.jsonl) and output three CSV files:

- File 1: `customers.csv`

**Fields**: customer_id, city (renamed from customer_city), country (renamed from customer_country)

- File 2: `products.csv`

**Fields**: product_id, product_name

- File 3: `order_items.csv`

**Fields**: order_id, customer_id, product_id, quantity, price_gbp

<br />

## 2. Table of contents of this repository

| Content                                  | Description |
| ------                                   | ------ |
| [input/](input/)                          | Folder containing the input dataset [orders.jsonl](input/orders.jsonl) and 5 aditional test files used to test the validation and deduplication functions |
| [output/](output/)                        | Folder where the output result will be stored (CSV files) |
| [main.py](main.py)                       | Script that takes the input dataset and generates the required output|
| [requirements.txt](requirements.txt)     | Python requirements to run [main.py](main.py)|
| [Dockerfile](Dockerfile)                 | Dockerfile for building a Docker image |
| [docker-compose.yml](docker-compose.yml) | Docker Compose file to run a container which executes [main.py](main.py) |

<br />

## 3. The Solution

The solution was implemented as a Python script that contains three main functions executing different steps:

1. Loading the input data (`extractData`), validating it and discarding invalid objects
2. Performing transformations (flattening and renaming) required to generate the desired output (`transformOrdersList`)
3. Deleting existing files in the output directory, creating deduplicated dataframes and generating CSV output files (`loadData`)

The solution was designed to fulfill the requirements of this specific task scope. 

### 3.1 How to run

The project can be executed either locally or in a Docker container.

Once executed, the `main.py` script will create the following three files in the [output](output/) directory: `customers.csv`, `products.csv`, `order_items.csv`. 

### Running locally
To run locally, you will need Python 3.11 or newer installed in your local environment. 
<br />
Then, install the dependencies: 
```
pip install -r requirements.txt
```
<br />
And run the script:
```
python3 ./main.py
```
Make sure to have these two folders available in the same directory of the `main.py` file: `input` (containing orders.jsonl) and `output`.

### Running with Docker
To run using Docker, [install Docker](https://docs.docker.com/get-docker/) and run the following command from the project's root directory:
```
docker-compose up
```

This will build a Docker image according to instructions in `Dockerfile` and create a Docker container that will run in any environment. It will then execute once and exit.

<br />

## 4. Future improvements
### To the code base
- Separate validation schema (`ORDER_SCHEMA`) and validation function (`validateObjects`) from the main script
- Add unit tests
- Automate unit tests execution
### For analytical purpose
- Add a total_price column with the total price of each order 
- Create a proper order_item_id for order_items.csv (unique values are currently being identified by the automatic index generated while creating the CSV files)





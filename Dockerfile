FROM python:3.11-slim

# Create a folder to store our code
RUN mkdir /app

# Set the folder as our working directory
WORKDIR /app

# Copy the pip dependencies file
COPY requirements.txt ./

# Copy .py file
COPY main.py ./

# Copy the dataset
COPY input/ ./

# Define input and output paths as environment variables
# ENV INPUT_PATH='./input/orders.jsonl'
# ENV OUTPUT_PATH='./output/'

# Create output directory
RUN mkdir ./output/
# RUN mkdir $OUTPUT_PATH

# Install project dependencies
RUN pip install -r requirements.txt

CMD ["python", "main.py"]

# Install latest spark image
FROM apache/spark

# As root user
USER root

# Set the working directory
WORKDIR /app

# Copy only the requirements file to the working directory
COPY requirements.txt .

# Copy the rest of the application code to the working directory
COPY . /app

RUN apt-get update && apt-get install -y python3-pip

# Install the dependencies
RUN pip install -r requirements.txt

# Set env path
ENV PATH="/opt/spark/bin:${PATH}"

# Copy the entrypoint script to the container
COPY entrypoint.sh /app/entrypoint.sh

# Make the entrypoint script executable
RUN chmod +x /app/entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]

# Run etl process and save parquet output to, run test in the end so logs wont get lost
CMD ["sh", "-c", "spark-submit main.py && cp -r /app/output_data /host_output && python3 -m unittest test.py"]

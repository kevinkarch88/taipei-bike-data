# Use an official openjdk image that also includes Python
FROM openjdk:11-jdk-slim

# install python and remove unnecessary files
RUN apt-get update && apt-get install -y python3 python3-pip && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# install dependencies like pyspark, dagster, etc
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# run docker build from application directory
COPY . .

# dagster needs an exposed port
EXPOSE 3000

# start a shell, then run program inside
CMD ["bash"]

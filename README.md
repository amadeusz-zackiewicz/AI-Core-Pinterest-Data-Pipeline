# What is this project?
This is my final project on the AICore aiming to get me familiar with Data Engineering techniques and technology. In this project I will be setting up a full pipeline based on Pinterest Experimentation Data Pipeline.

## Milestone 1 - 3
I have read the brief of what my specification of the project is and I have set up my virtual environment with all the required libraries and dependencies and made it ready to start development. I have then downloaded all the required files and software to simulate users sending data.

---

## Milestone 4 - 5
I have set up a zookeeper server and a Apache Kafka broker in order to balance the load of incoming data. I have then created a kafka producer that would send the data received from FastAPI to kafka. And finally I would create kafka consumers that would consume the data and send it to an AWS S3 bucket.

![s3_bucket_img](/readme_res/s3_bucket.png)

---

## Milestone 6
I have build a simple Apache Spark application that downloads all the files from the AWS S3 bucket, cleans up data and produces new data that is ready to be visualised. 

Example showing a sum of downloads per category:

![spark_example](/readme_res/spark_process_example.png)

---
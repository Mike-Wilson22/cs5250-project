FROM python:3.9
ADD consumer.py .
RUN pip install boto3 botocore
CMD ["python", "consumer.py", "-dwt", "widgets", "-sqs", "https://sqs.us-east-1.amazonaws.com/785484577828/cs5250-requests"]
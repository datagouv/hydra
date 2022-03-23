import os
import boto3
from botocore.client import Config
import io
import requests
import logging
from dotenv import load_dotenv


load_dotenv()


def save_resource_to_minio(key, resource):
    logging.info('Saving to minio')
    s3 = boto3.client(
        's3',
        endpoint_url=os.getenv('MINIO_URL'),
        aws_access_key_id=os.getenv('MINIO_USER'),
        aws_secret_access_key=os.getenv('MINIO_PWD'),
        config=Config(signature_version='s3v4')
    )
    response = requests.get(resource['url'])
    s3.upload_fileobj(io.BytesIO(response.content), os.getenv('MINIO_BUCKET'), 'stan/'+key+'/'+resource['id'])
    logging.info('Resource saved into minio at {}'.format(os.getenv('MINIO_URL')+os.getenv('MINIO_BUCKET')+'/stan/'+key+'/'+resource['id']))


def manage_resource(key, resource):
    logging.getLogger().setLevel(logging.INFO)
    logging.info('Processing task for resource {} in dataset {}'.format(resource['id'], key))
    save_resource_to_minio(key, resource)
    return 'Resource processed {} - END'.format(resource['id'])

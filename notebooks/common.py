import iris
import xarray
import os
from numcodecs import Blosc
import s3fs
import zarr
import intake
from datetime import datetime, timezone, timedelta
import cf_units
import json
import  dask_kubernetes
import distributed
import boto3
from iris.experimental.equalise_cubes import equalise_attributes
import pandas as pd
import sys
import numpy as np

sys.path.append(os.path.normpath(os.getcwd()))
from offsetmap import OffSetS3Map

sqs = boto3.client('sqs')

AWS_EARTH_TIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
# SQS_QUEUE_URL = 'https://sqs.eu-west-2.amazonaws.com/536099501702/aws-earth-test'
SQS_QUEUE_URL = 'https://sqs.eu-west-2.amazonaws.com/536099501702/rolling_zarr_test_queue'

BUCKET = "metoffice-aws-earth-zarr"

def del_msg(msg):
    sqs.delete_message(
        QueueUrl=SQS_QUEUE_URL,
        ReceiptHandle=msg['receipt_handle']
    )
    
def get_messages(max_num=10):
    res = sqs.receive_message(QueueUrl=SQS_QUEUE_URL, MaxNumberOfMessages=max_num, VisibilityTimeout=60*10)
    messages  = []
    for message in res['Messages']:
        msg = json.loads(message['Body'])
        msg['receipt_handle'] = message['ReceiptHandle']
        messages.append(msg)
    return messages

def get_zar_path(meta):
    base = f"{BUCKET}/{meta['model']}-{meta['name']}"
    if meta.get('cell_methods', False):
        base += f"-{meta['cell_methods']}"
    if  meta.get('height', False) and (len(meta['height'].strip().split(' ')) > 1):
        base += '-at_heights'
    if meta.get('pressure', False) and (len(meta['pressure'].strip().split(' ')) > 1):
        base += '-at_pressures'
    return base + '.zarr'
    

def zarr_store(meta):
    return OffSetS3Map(root=get_zar_path(meta), temp_chunk_path=meta['name'], check=False)
    
def msg_to_path(msg):
    return f'/s3/{msg["bucket"]}/{msg["key"]}'

def reshape_to_dest_cube(cube):
    return iris.util.new_axis(iris.util.new_axis(cube, 'forecast_period'), 'forecast_reference_time')


def get_proto_zarr_array(meta):
    OffSetS3Map(root=get_zar_path(meta), temp_chunk_path=meta['name'], check=False)
    array_store = OffSetS3Map(root=get_zar_path(meta) +'/' + meta['name'], temp_chunk_path='', check=False)
    return zarr.open(array_store)

def meta_from_zarr_path(path):
    decompose = path
    decompose = decompose.rsplit('.',1)[0]
    meta = {}
    
    models = ['mo-atmospheric-global-prd', 'mo-atmospheric-mogreps-g-prd', 'mo-atmospheric-ukv-prd', 'mo-atmospheric-mogreps-uk-prd']
    for possiable_model in models:
        if path.startswith(possiable_model):
            model = possiable_model
        
    meta['model'] = model
    
    decompose = decompose.replace(model+'-', '')
    
    if path.find('-at_heights'):
        height = "0 1 2 3"
        meta['height'] = height
        decompose = decompose.replace('-at_heights', '')
        
    if path.find('-at_pressures') > 0:
        pressure = "1000 2000 3000" 
        meta['pressure'] = pressure
        decompose = decompose.replace('-at_pressures', '')
        
    meta['name'] = decompose 
    
    return meta
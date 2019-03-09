import json
import urllib.parse
import boto3
import pandas as pd
from io import StringIO
import io

s3 = boto3.client('s3')
s3_resource = boto3.resource("s3")

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        # initialize
        idx = 0
        csize = 50000

        # Temporary columns name
        col_names = [ 'c{0:03d}'.format(i) for i in range(300) ]

        s_string = pd.read_csv(io.BytesIO(response['Body'].read()),header=None,dtype='object',chunksize=csize,names=col_names,encoding="SHIFT-JIS",skiprows=1)
        # chk iput file name
        if key.split('.')[1] == 'TXT':
            for df in s_string:
                #print(df)
                #for c006_uniq_value in ['00','01','02','03','04','05','06']:
                for c006_uniq_value in df.c006.unique():
                    csv_buffer = StringIO()
                    df[df.c006 == c006_uniq_value].to_csv(csv_buffer,index=False,sep=",",encoding='shift_jis')
                    out_subdir = 'div0/'
                    out_filename = key.split('.')[0] + '_' + c006_uniq_value + '_' + str(idx) + '.csv'
                    s3_resource.Object('input2td',out_subdir + out_filename).put(Body=csv_buffer.getvalue().encode('shift_jis'))
                idx+=1
        else:
            skip
        return response['ContentType']
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

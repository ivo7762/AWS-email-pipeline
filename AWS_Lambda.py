import boto3
import base64
import email as my_email
import csv

s3 = boto3.client('s3')


def lambda_handler(event, context):
    for record in event['Records']:
        bucket = 'ivo-project-bucket'
        key = record['s3']['object']['key']
        data = s3.get_object(Bucket=bucket, Key=key)
        decode_and_store(data, key)

    return 1


def decode_and_store(my_data, my_key):
    dest_bucket = 'project-target-bucket'

    contents = my_data['Body'].read()
    my_data = my_email.message_from_string(contents)

    for part in my_data.walk():
        filename = part.get_filename()
        payload = str(part.get_payload())

        if filename:
            print("\n--- filename: %s" % (filename))

            clean_data = base64.b64decode(payload)

            print(clean_data)

            data_as_rows = clean_data.split('\n')
            csv_rows = []
            for rc, row in enumerate(data_as_rows):
                clean_row = row.replace('\r', '')
                items_in_row = clean_row.split(',')
                if items_in_row:
                    csv_rows.append(items_in_row)

            out_file_name = '/tmp/x.csv'
            with open(out_file_name, mode='wb') as csv_data:
                data_writer = csv.writer(csv_data, delimiter=',')
                data_writer.writerows(csv_rows)

            s3_resource = boto3.resource('s3')

            s3_resource.meta.client.upload_file(out_file_name, dest_bucket, my_key + 'test.csv')


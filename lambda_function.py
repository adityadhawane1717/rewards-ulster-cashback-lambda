import os
import csv
import psycopg2
import requests
import uuid
from datetime import datetime
import logging
import configparser
import json
import boto3
from urllib.parse import urlencode

# Generate a timestamp in the format YYYY-MM-DD_HH-MM-SS
timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

# Configure logging to write logs to a file
logging.basicConfig(level=logging.INFO)

def lambda_handler(event, context):
    try:
        # Extract S3 bucket and key from the event
        s3_bucket = event['Records'][0]['s3']['bucket']['name']
        s3_key = event['Records'][0]['s3']['object']['key']

        # Download CSV file from S3
        s3_file_content = download_file_from_s3(s3_bucket, s3_key)

        # Process CSV data
        process_csv_data(s3_file_content)

        return {
            'statusCode': 200,
            'body': 'Successfully processed CSV data.'
        }
    except Exception as e:
        print(f"Error processing CSV data: {str(e)}")
        return {
            'statusCode': 500,
            'body': 'Error processing CSV data.'
        }
        
def get_db_password():
    ssm_client = boto3.client('ssm')
    # Retrieve the parameter value
    parameter_response = ssm_client.get_parameter(Name='data_migration-db-password', WithDecryption=True)
    db_password = parameter_response['Parameter']['Value']
    return db_password

def download_file_from_s3(bucket, key):
    print("Inside download_file_from_s3")
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket, Key=key)
    return response['Body'].read().decode('utf-8')

def process_csv_data(csv_content):
    print("Inside process_csv_data")
    # Parse CSV data
    csv_reader = csv.DictReader(csv_content.splitlines())
    connection = get_postgres_connection()
    for row in csv_reader:
        customer_identifier = row['CUSTOMER_IDENTIFIER']
        reminder = row['First/Reminder']
        cashback_refs = [row['Cashback Ref1'], row['Cashback Ref2'], row['Cashback Ref3'], row['Cashback Ref4']]

        #Get non-empty cashback_refs
        processed_cashback_refs = process_cashback_refs(cashback_refs)
        number_of_cashback = len(processed_cashback_refs)

        # Fetch data from PostgreSQL database
        result = fetch_data_from_postgres(customer_identifier, connection)
        
        # Call communication API
        if len(result)==4:
            fname, pkg_name,primary_email_address,ng_member_id = result
            merge_fields = create_merge_fields(fname, pkg_name, number_of_cashback, processed_cashback_refs)
            req_body = create_request_body(merge_fields, primary_email_address, ng_member_id, customer_identifier)
            call_communication_api(req_body)
        else:
            print("Error: The function did not return the expected number of values for the customer_identifier: ", customer_identifier)
    close_postgres_connection(connection)

def close_postgres_connection(connection):
    print("Inside close_postgres_connection")
    # Close the connection
    connection.close()

def get_postgres_connection():
    logging.info("Inside get_postgres_connection")
    # PostgreSQL connection parameters
    db_host = os.environ.get('db_host')
    db_port = os.environ.get('db_port')
    db_user = os.environ.get('db_user')
    db_password = get_db_password()
    db_name = os.environ.get('db_name')

    # Establish a connection to PostgreSQL
    connection = psycopg2.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        dbname=db_name,
        sslmode='prefer', 
        sslrootcert='rds-ca-2019-root.pem'
    )
    return connection

def process_cashback_refs(cashback_refs):
    print("Inside process_cashback_refs")
    # Use a list comprehension to filter out 'X' and empty values
    processed_refs = [ref for ref in cashback_refs if ref is not None and ref != 'X']
    return processed_refs

def fetch_data_from_postgres(customer_identifier, connection):
    print("Inside fetch_data_from_postgres")
    cursor = connection.cursor()
    # Execute a SELECT query based on CUSTOMER_IDENTIFIER
    #query = f"select p.*,smm.* from ng_intermediate_data_store.stage_membership_member smm inner join ng_intermediate_data_store.stage_membership smp on smp.membership_id =smm.membership_id inner join ng_intermediate_data_store.solicitation s on s.sol_id=smp.sol_id inner join ng_intermediate_data_store.package p on p.pkg_id =s.ben_pkg_id where smm.ext_member_ref = '{customer_identifier}' and p.client_id in (1016498) ;"
    query = f"SELECT sm.fname,p.pkg_name,sm.primary_email_address,sm.ng_member_id FROM ng_intermediate_data_store.stage_membership_member smm INNER JOIN ng_intermediate_data_store.stage_membership smp ON smp.membership_id = smm.membership_id INNER JOIN ng_intermediate_data_store.solicitation s ON s.sol_id = smp.sol_id INNER JOIN ng_intermediate_data_store.package p ON p.pkg_id = s.ben_pkg_id INNER JOIN ng_intermediate_data_store.stage_member sm ON sm.member_id = smm.member_id WHERE smm.ext_member_ref = '{customer_identifier}' AND p.client_id IN (1016498, 1016568, 1024226);"
    cursor.execute(query, customer_identifier)
    result = cursor.fetchone()
    cursor.close()
    if result:
        fname, pkg_name,primary_email_address,ng_member_id = result
        return fname, pkg_name,primary_email_address,ng_member_id
    else:
        return None, None

def get_refined_pkgName(pkg_name):
    assigned_name = ""
    if "Private" in pkg_name:
        assigned_name = "Ufirst Private"
    elif "Gold" in pkg_name:
        assigned_name = "Ufirst Gold"
    else:
        assigned_name = pkg_name
    
    return assigned_name

def create_merge_fields(fname, pkg_name, number_of_cashback, cashback_refs):
    print("Inside create_merge_fields")
    # Define dynamic values for merge fields
    refined_pkg_name = get_refined_pkgName(pkg_name)
    reffnumber_list = [{"reffNumbers": ref} for ref in cashback_refs]
    merge_fields = {
        "params.firstName": fname,
        "params.packageName": refined_pkg_name,
        "params.cashbackCount": number_of_cashback,
        "params.reffnumberList": reffnumber_list
    }
    return merge_fields

def create_request_body(merge_fields, receiverEmailAddress, memberId, customer_identifier):
    print("Inside create_request_body")
    # Generate a unique correlationId and timestamp
    correlation_id = str(uuid.uuid4())
    timestamp = datetime.utcnow().isoformat() + "Z"

    # Fetch values from the configuration file
    application_id = os.environ.get('application_id')
    sender_identity = os.environ.get('sender_identity')
    sender_name = os.environ.get('sender_name')
    templateName = os.environ.get('templateName')

    # Construct the request body
    request_body = {
        "correlationId": correlation_id,
        "tenantId": "1024226",
        "applicationId": application_id,
        "timeStamp": timestamp,
        "userType": "member",
        "memberId": memberId,
        "attributes": {
            "externalRefId": customer_identifier
        },
        "messageConfiguration": {
            "commType": "email",
            "senderIdentity": sender_identity,
            "receiverIdentity": receiverEmailAddress,
            "templateName": templateName,
            "mergeFields": merge_fields,
            "emailMessage": {
                "senderName": sender_name
            }
        }
    }
    return request_body

def call_communication_api(req_body):
    print("Inside call_communication_api")
    # Communication API endpoint
    api_url = os.environ.get('api_url')
    api_key = os.environ.get('api_key')  # Include any required authentication headers
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': f'{api_key}'
    }
    # Send data to the API
    response = requests.post(api_url, json=req_body, headers=headers)
    if response.status_code == 200:
        print(f"Successfully sent data to the communication API, Status Code: {response.status_code}, Response: {response.text}")
    else:
        print(f"Failed to send data to the communication API. Status Code: {response.status_code}, Response: {response.text}")

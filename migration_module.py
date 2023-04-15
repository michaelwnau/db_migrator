import pymysql
import psycopg2
import boto3
import os

# Step 1: If the size of the database allows, xport the source database to a file (e.g., a .sql or .csv file)
# 


def export_source_db_to_file(source_connection_params, export_file_path):
    # Connect to the source database
    source_conn = pymysql.connect(**source_connection_params)

    # Export the database to a .sql file
    export_cmd = f"mysqldump -u {source_connection_params['user']} -p{source_connection_params['password']} -h {source_connection_params['host']} {source_connection_params['db']} > {export_file_path}"
    os.system(export_cmd)

    # Close the connection
    source_conn.close()

# Step 2: Transfer the exported file to the target cloud platform


def transfer_file_to_target_cloud(aws_access_key, aws_secret_key, export_file_path, s3_bucket, s3_key):
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key,
                      aws_secret_access_key=aws_secret_key)
    s3.upload_file(export_file_path, s3_bucket, s3_key)

# Step 3: Create a new database in the target cloud platform


def create_target_database(target_connection_params, target_db_name):
    target_conn = psycopg2.connect(**target_connection_params)
    target_conn.autocommit = True

    with target_conn.cursor() as cursor:
        cursor.execute(f"CREATE DATABASE {target_db_name};")

    target_conn.close()

# Step 4: Import the data from the exported file into the target database


def import_data_to_target_db(target_connection_params, s3_bucket, s3_key, aws_access_key, aws_secret_key):
    target_conn = psycopg2.connect(**target_connection_params)
    target_conn.autocommit = True

    with target_conn.cursor() as cursor:
        cursor.execute(f"CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE;")
        cursor.execute(
            f"SELECT aws_s3.import_from_s3('{s3_bucket}', '{s3_key}', '', 'aws_access_key_id={aws_access_key};aws_secret_access_key={aws_secret_key}');")

    target_conn.close()


# Execute the workflow
if __name__ == "__main__":
    source_connection_params = {
        'host': 'source_db_host',
        'user': 'source_db_user',
        'password': 'source_db_password',
        'db': 'source_db_name',
        'port': 3306,  # For MySQL, update the port if required
    }
    target_connection_params = {
        'host': 'target_db_host',
        'user': 'target_db_user',
        'password': 'target_db_password',
        'dbname': 'target_db_name',
        'port': 5432,  # For PostgreSQL, update the port if required
    }
    export_file_path = "source_db_export.sql"
    s3_bucket = "your-target-bucket"
    s3_key = "source_db_export.sql"
    aws_access_key = "your_aws_access_key"
    aws_secret_key = "your_aws_secret_key"

    export_source_db_to_file(source_connection_params, export_file_path)
    transfer_file_to_target_cloud(
        aws_access_key, aws_secret_key, export_file_path, s3_bucket, s3_key)
    create_target_database(target_connection_params, "new_target_db")
    import_data_to_target_db(target_connection_params,
                             s3_bucket, s3_key, aws_access_key, aws_secret_key)

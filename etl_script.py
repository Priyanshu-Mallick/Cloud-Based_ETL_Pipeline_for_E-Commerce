import boto3

s3 = boto3.client('s3')
bucket_name = 'ecommerce-data-storage-1'

def download_data():
    # Download data from S3
    s3.download_file(bucket_name, 'data/raw/ecommerce_data.csv', '/tmp/ecommerce_data.csv')

def upload_transformed_data():
    # Upload transformed data to S3
    s3.upload_file('/tmp/transformed_data.csv', bucket_name, 'data/transformed/ecommerce_data.csv')

def transform_data():
    # Example transformation
    with open('/tmp/ecommerce_data.csv', 'r') as f:
        data = f.readlines()
    
    transformed_data = [line.upper() for line in data]
    
    with open('/tmp/transformed_data.csv', 'w') as f:
        f.writelines(transformed_data)
    
def main():
    download_data()
    transform_data()
    upload_transformed_data()

if __name__ == "__main__":
    main()

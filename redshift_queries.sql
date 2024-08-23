CREATE TABLE transformed_data (
    id VARCHAR(256),
    value DOUBLE PRECISION
);

COPY transformed_data
FROM 's3://ecommerce-data-bucket-12345/data/transformed/ecommerce_sales_data.csv'
CREDENTIALS 'aws_iam_role=arn:aws:iam::123456789012:role/RedshiftS3ReadRole'
DELIMITER ','
IGNOREHEADER 1;

# Snowflake Storage Integration (AWS S3 External Stage)

This avoids storing AWS keys in Airflow or `.env` by using an IAM role that Snowflake assumes.

## 1) Create the Snowflake storage integration
Run as a Snowflake admin:
```sql
CREATE STORAGE INTEGRATION IF NOT EXISTS s3_int_bronze
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<acct-id>:role/<snowflake-role-name>'
  STORAGE_ALLOWED_LOCATIONS = ('s3://<bucket>/');
```

Find the AWS IAM user/role Snowflake uses:
```sql
DESC STORAGE INTEGRATION s3_int_bronze;
```
Copy the `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID`.

## 2) Add IAM trust on the AWS role
In AWS IAM, update the role trust policy to allow Snowflake:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "AWS": "<STORAGE_AWS_IAM_USER_ARN>" },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": { "sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID>" }
      }
    }
  ]
}
```

## 3) Create the external stage
```sql
CREATE STAGE IF NOT EXISTS bronze_stock_quotes_stage
  URL = 's3://<bucket>/topic=stock-quotes/'
  STORAGE_INTEGRATION = s3_int_bronze
  FILE_FORMAT = (TYPE = PARQUET);
```

## 4) Configure the DAG
In `ifra/.env.local` (or `.env.docker`):
```env
SNOWFLAKE_STAGE_URL=s3://<bucket>/topic=stock-quotes/
SNOWFLAKE_STORAGE_INTEGRATION=s3_int_bronze
```

Now Airflow can load directly from S3 without AWS keys in `.env`.

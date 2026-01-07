# Local AWS S3 Access (for Future Use)

This documents how to access AWS S3 from your laptop using an IAM role.

## Prereqs
- AWS CLI installed
- An IAM role you can assume (trust policy allows your IAM user or SSO principal)
- A base profile or SSO profile that can assume the role

## Create a role-assuming profile
```bash
cd real-time-data-mds
ROLE_ARN=arn:aws:iam::<acct-id>:role/<role-name> REGION=us-east-1 ./scripts/aws_role_profile_setup.sh
```

Verify the role works:
```bash
aws sts get-caller-identity --profile my-role
```

## Configure local env
In `real-time-data-mds/ifra/.env` or `real-time-data-mds/ifra/producer/.env`:
```env
AWS_REGION=us-east-1
S3_BUCKET=your-bucket-name
AWS_PROFILE=my-role
USE_MINIO=false
```

## Test bucket access
```bash
AWS_PROFILE=my-role aws s3api head-bucket --bucket your-bucket-name
```

## Test upload
```bash
echo "ok" > /tmp/s3_test.txt
AWS_PROFILE=my-role aws s3 cp /tmp/s3_test.txt s3://your-bucket-name/smoke/s3_test.txt
```

## Troubleshooting
- If you get `ProfileNotFound`, create the profile with the script above.
- If you get `AccessDenied`, update the role policy or bucket policy to allow `s3:PutObject` and `s3:ListBucket`.

import boto3
import csv
import io

# Initialize S3 client
s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Get the bucket name and file key from the event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']

    try:
        # Read the CSV file from S3
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        csv_content = response['Body'].read().decode('utf-8')

        # Process the CSV content
        processed_rows = []
        reader = csv.reader(io.StringIO(csv_content))
        header = next(reader)  # Extract the header row

        for row in reader:
            # Example preprocessing: filter out rows with missing values
            if all(row):
                processed_rows.append(row)

        # Write processed data back to a new CSV in memory
        output_csv = io.StringIO()
        writer = csv.writer(output_csv)
        writer.writerow(header)  # Write the header row
        writer.writerows(processed_rows)

        # Upload the processed file to the processed data bucket
        processed_file_key = file_key.replace('raw/', 'processed/')
        s3.put_object(
            Bucket='<YOUR_PROCESSED_BUCKET_NAME>',
            Key=processed_file_key,
            Body=output_csv.getvalue()
        )

        print(f"Processed file uploaded to: {processed_file_key}")

    except Exception as e:
        print(f"Error processing file: {str(e)}")
        raise
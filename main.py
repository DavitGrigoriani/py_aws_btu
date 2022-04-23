import time
import boto3


AWS_REGION = "us-east-1"
s3 = boto3.client("s3")
lambda_client = boto3.client('lambda', region_name=AWS_REGION)
ZIPNAME = "lambda.zip"


def create_bucket(davaleba5bucket):
    try:
        s3.create_bucket(Bucket=davaleba5bucket)
    except Exception as ex:
        print(ex)


def aws_file():
    with open(ZIPNAME, 'rb') as file_data:
        bytes_content = file_data.read()
    return bytes_content


def create_lambda(davaleba5lambda):
    response = lambda_client.create_function(
        Code={
            'ZipFile': aws_file()
        },
        Description='Recognize object from photos',
        FunctionName=davaleba5lambda,
        Handler='lambda_function.lambda_handler',
        Publish=True,
        Role='arn:aws:iam::114232093311:role/LabRole',
        Runtime='python3.8',
    )
    return response


def add_permission(davaleba5bucket, davaleba5lambda):
    lambda_client.add_permission(
        FunctionName=davaleba5lambda,
        StatementId='1',
        Action='lambda:InvokeFunction',
        Principal='s3.amazonaws.com',
        SourceArn=f'arn:aws:s3:::{davaleba5bucket}',
    )


def s3_trigger(davaleba5bucket, davaleba5lambda):
    add_permission(davaleba5bucket, davaleba5lambda)
    response = s3.put_bucket_notification_configuration(
        Bucket=davaleba5bucket,
        NotificationConfiguration={'LambdaFunctionConfigurations': [
            {
                'LambdaFunctionArn': f'arn:aws:lambda:{AWS_REGION}:114232093311:function:{davaleba5lambda}',
                'Events': [
                    's3:ObjectCreated:*'
                ],
                'Filter': {
                    'Key': {
                        'FilterRules': [
                            {
                                'Name':  'suffix',
                                'Value': '.jpg'
                            },
                        ]
                    }
                }
            },
        ],
          },
        SkipDestinationValidation=True
    )
    return response


def upload_file(puppy, davaleba5bucket, file):
    try:
        s3.upload_file(puppy, davaleba5bucket, file)
        time.sleep(150)
        data = s3.get_object(Bucket=davaleba5bucket, Key=file.replace('.jpg', '.json'))
        # data_exists = data.get_waiter('json exists')
        # data_exists.wait(data)
        contents = data['Body'].read()
        print(contents)
    except Exception as ex:
        print(f"Something went wrong :( {ex}")


def main(davaleba5bucket, davaleba5lambda, puppy):
    file = puppy
    create_bucket(davaleba5bucket)
    create_lambda(davaleba5lambda)
    s3_trigger(davaleba5bucket, davaleba5lambda)
    upload_file(puppy, davaleba5bucket, file)


if __name__ == '__main__':
    main("davaleba5bucket", "davaleba5lambda", "puppy.jpg")
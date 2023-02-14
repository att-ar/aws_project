#function to add random suffix to bucket names
import boto3
from uuid import uuid4
def gen_bucket_name(bucket_name: str) -> str:
    '''
    Precondition: `bucket_name` contains S3 valid characters
    str -> str

    This function returns a `len(bucket_name) + 36` character long string that is likely a unique bucket name
    Parameters:
    `bucket_name` is the user given bucket name that the random suffix will be added to
    '''
    hyphen = {True: "", False: "-"}
    return "".join(
            [bucket_name, hyphen[bucket_name[-1] == "-"], str(uuid4())]
        )[:min((len(bucket_name) + 36), 63)]

def gen_bucket(bucket_name: str, s3_boto_connection, suffix = True):
    '''
    Creates an S3 bucket and returns the suffixed bucket name as well as the S3 response

    Precondition:
        `s3_boto_connection` is of type botocore.client.S3 or boto3.resources.factory.s3.ServiceResource
        `bucket_name` must contain characters valid in S3

    This function uses the user that connected through the boto SDK to find its default region.

    Parameters:
    `suffix` whether to assign a random suffix to `bucket_name` using gen_bucket_name()
        if suffix == True:
            The `gen_bucket_name` function is used to generate a suffixed version of the input name.
        if suffix == False:
            The `bucket_name` argument is used as the S3 bucket name
            (this may throw an error since buckets need to be globally unique).
    '''
    user_region = boto3.Session().region_name
    if suffix: bucket_name = gen_bucket_name(bucket_name)
    bucket_response = s3_boto_connection.create_bucket(
        Bucket = bucket_name,
        CreateBucketConfiguration={
            "LocationConstraint": user_region
        }
    )
    print(f"Bucket: {bucket_name}\tRegion: {user_region}")
    return bucket_name, bucket_response

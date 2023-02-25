from uuid import uuid4
from urllib import parse

def gen_tagging_list_from_python_dict(tags:dict):
    '''Converts a dict of key-value pairs to a list of s3 tagging dicts'''
    return [{'Key': key, 'Value': val} for key,val in tags.items()]

def gen_python_dict_from_tagging_list(tags:list[dict]):
    '''Converts list of s3 tagging dicts to dict of key-value pairs'''
    return dict([(tag["Key"],tag["Value"]) for tag in tags])

def gen_bucket_name(bucket_name: str) -> str:
    '''
    Returns a `len(bucket_name) + 36` character long string that is likely a unique bucket name

    Precondition: `bucket_name` contains S3 valid characters

    Parameters:
    `bucket_name` is the user given bucket name that the random suffix will be added to
    '''
    hyphen = {True: "", False: "-"}
    return "".join(
            [bucket_name, hyphen[bucket_name[-1] == "-"], str(uuid4())]
        )[:min((len(bucket_name) + 36), 63)]

def gen_bucket(session, bucket_name: str, tags: list[dict] = None, region = None, suffix = True):
    '''
    Creates an S3 bucket and returns the suffixed bucket name as well as the S3 response

    Precondition:
        `bucket_name` must contain characters valid in S3
        Each tag in `tags` must be in s3 tag format: {'Key':key_argument, 'Value':value_argument}

    This function uses `session` to find its default region.

    Parameters:
    `session` boto3.session.Session()
    `bucket_name` str
        The S3 bucket's name
    `tags` list[dict[str]]
        List of dictionaries containing the key-value pairs to be assigned as tags to the bucket.
        Each tag must be formatted in the s3 tag format: {'Key':key_argument, 'Value': value_argument}.
        Use the gen_tagging_list_from_python_dict() with a regular python dictionary to format your input
        Example for two tags:
            [{'Key': 'creator', 'Value': "john-doe"},
             {'Key': 'content', 'Value': 'simulated-data'}]
    `region` str
        The region that the bucket should be deployed in, defaults to the user's default region.
        Valid regions: af-south-1, ap-east-1, ap-northeast-1, ap-northeast-2, ap-northeast-3, ap-south-1,
        ap-south-2, ap-southeast-1, ap-southeast-2, ap-southeast-3, ca-central-1, cn-north-1,
        cn-northwest-1, EU, eu-central-1, eu-north-1, eu-south-1, eu-south-2, eu-west-1, eu-west-2,
        eu-west-3, me-south-1, sa-east-1, us-east-2, us-gov-east-1, us-gov-west-1, us-west-1, us-west-2
    `suffix` str
        whether to assign a random suffix to `bucket_name` using gen_bucket_name()
        if suffix == True:
            The `gen_bucket_name` function is used to generate a suffixed version of the input name.
        if suffix == False:
            The `bucket_name` argument is used as the S3 bucket name
            (this may throw an error since buckets need to be globally unique).
    '''
    s3_boto_connection = session.resource("s3")
    if not region:
        region = session.region_name
    if suffix: bucket_name = gen_bucket_name(bucket_name)
    bucket_response = s3_boto_connection.create_bucket(
        Bucket = bucket_name,
        CreateBucketConfiguration={
            "LocationConstraint": region
        }
    )
    print(f"Bucket: {bucket_name}\tRegion: {region}")

    if isinstance(tags, list):
        bucket_tagger = s3_boto_connection.BucketTagging(bucket_name)
        set_tag = bucket_tagger.put(Tagging={"TagSet":tags})
        # bucket_tagger.reload() #useless here since it isn't called again
        print(f"Bucket tags: {tags}")
        return bucket_name, bucket_response, set_tag
    return bucket_name, bucket_response

    

def gen_object(session, bucket_name:str, object_path:str,
               tags: dict = None, storage_class = "STANDARD"):
    '''
    Creates an S3 object in a bucket
    
    Returns the object path inside its bucket, the S3 response, and the tags that were set.

    Precondition:
        `bucket_name` and `object_name` must contain characters valid in S3
        Each tag in `tags` must be in s3 tag format: {'Key':key_argument, 'Value':value_argument}

    This function uses `session` to find its default region.
    Note: the boto3 implementation for uploading a file to an object is very straightforward
    so no additional abstraction via functions is needed.

    Parameters:
    `session` boto3.session.Session()
    `bucket_name` str
        The S3 bucket's name
    `object_path` str
        The path inside the bucket to be used as the object key.
        Forward slashes are used as 1 dimensional "folder-like" separations.
    `tags` dict
        Must be a regular python dictionary such as {key_arg1:val_arg1, key_arg2:val_arg2}.
        This is converted to url encoding inside the function. So S3 formatted list[dict] will not work.
    `storage_class` str
        One of 'STANDARD'|'REDUCED_REDUNDANCY'|'STANDARD_IA'|'ONEZONE_IA'|
        'INTELLIGENT_TIERING'|'GLACIER'|'DEEP_ARCHIVE'|'OUTPOSTS'|'GLACIER_IR'
    '''
    valid_storage = ['STANDARD', 'REDUCED_REDUNDANCY', 'STANDARD_IA', 'ONEZONE_IA', 
        'INTELLIGENT_TIERING', 'GLACIER', 'DEEP_ARCHIVE', 'OUTPOSTS', 'GLACIER_IR']
    assert storage_class in valid_storage, f"incorrect storage_class input = {storage_class}"

    s3_obj = session.resource("s3").Object(bucket_name, object_path)
    if tags:
        url_tags = parse.urlencode(tags)
        response = s3_obj.put(
            ACL = "private",
            StorageClass = storage_class,
            Tagging = url_tags)
        return object_path, response, gen_tagging_list_from_python_dict(tags)
    else:
        response = s3_obj.put(
            ACL = "private",
            StorageClass = storage_class)
        
    return object_path, response
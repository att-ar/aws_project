#function to add random suffix to bucket names
import boto3
import datetime
import json
from uuid import uuid4

# generating things --------------------------
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

# getting things based on filters -------------------------
def helper_date_comparison(bucket_creationdate: datetime.datetime, *args) -> bool:
    '''helper for get_buckets_with_name_date, returns the date comparison result'''
    timezone = bucket_creationdate.tzinfo
    args = list(args)
    for i in range(len(args)):
        if not args[i].tzinfo:
            args[i] = args[i].replace(tzinfo = timezone)
    if len(args) == 2:
        assert args[0] < args[1], "start date must be before end date in `use_date`."
        return bucket_creationdate <= args[1] and bucket_creationdate >= args[0]
    else:
        if not args[0]: #default value is None in get_bucket_with_name_date
            return True #so all dates are retrieved
        else:
            return bucket_creationdate.date() == args[0].date()
            #will use the date portion for single comparison
def get_buckets_with_name_date(prefix: str,
    use_date: list[str|datetime.datetime|datetime.date]
                | str|datetime.datetime|datetime.date
                | None = None) -> list[str]:
    '''
    Returns list of bucket names who start with `prefix` and made on or in (list) `use_date` using boto3.

    Parameters:
    `prefix` str
        the first len(prefix) characters in bucket name must be `prefix` to work.
        If you want all buckets, to only filter with date, use `prefix` = ""
    `use_date` list[str|datetime.datetime|datetime.date] or str|datetime.datetime|datetime.date
        defaults to None if only the name is needed as a search.
        Used to retrieve the buckets made on the date given.
        if str, must be in the format "year-month-day-hour-minute-second".
            only year, month, and day are required.
        If you want to check an interval of dates, use a for loop with an f-string for `use_date`.

    Doesn't throw an indexing error if the prefix is longer than the bucket name.
    '''
    assert isinstance(use_date, (str, list, datetime.date, datetime.datetime)), f"Invalid type(use_date) = {type(use_date)}"
    # turning use_date into datetime.datetime
    if isinstance(use_date, str):
        try:
            use_date = [int(i) for i in use_date.split("-")]
            use_date = [datetime.datetime(*use_date)]
        except Exception:
            return f"Incorrect string input for `use_date` = {use_date}"
    elif isinstance(use_date, datetime.date):
        use_date = [datetime.datetime.combine(use_date, datetime.time(0))]

    elif isinstance(use_date, list):
        assert len(use_date) == 2, "Date interval can only have two dates."
        for i,d in enumerate(use_date):
            if isinstance(d,str):
                try:
                    d = [int(num) for num in d.split("-")]
                    use_date[i] = datetime.datetime(*d)
                except Exception:
                    return f"Incorrect string input for `use_date[{i}]` = {d}"
            elif isinstance(d, datetime.date):
                use_date[i] = datetime.datetime.combine(d, datetime.time(0))
    #use_date is all datetime.datetime now

    return [bucket for bucket
        in boto3.client("s3").list_buckets()["Buckets"]
        if (bucket["Name"][: min(len(bucket["Name"]), len(prefix))] == prefix)
            & (helper_date_comparison(bucket["CreationDate"], *use_date))]

#deleting objects from a bucket based on a prefix(es) filter: ------------
# can use this to delete all objects if you pass "" as the prefix

def delete_objects_in_bucket(bucket_name: str, prefixes: str|list[str]):
    '''
    Delete objects from a bucket using `prefixes` as the filter for object names

    Can call `for object in bucket.object_versions.all(): print(object.key)`
    to get the object that remain after running this delete function

    Parameters:
    `bucket_name` str
        the bucket name
    `prefixes` str or list[str]
        one or more prefixes to use to delete objects with these prefixes.
    '''
    assert isinstance(bucket_name, str)
    assert isinstance(prefixes, (str, list))

    if isinstance(prefixes, str):
        prefixes = [prefixes]
    else:
        for prefix in prefixes:
            assert isinstance(prefix, str), f"must give strings as prefixes, instead gave {type(prefix)}"

    max_len_prefix = max([len(prefix) for prefix in prefixes])
    response = []
    bucket = boto3.resource("s3").Bucket(bucket_name)

    for obj in bucket.object_versions.all():
        try:
            if obj.object_key[:max_len_prefix] in prefixes:
                response.append({
                    "Key": obj.object_key,
                    "VersionID": obj.version_id
                })
        except IndexError:
            continue

    bucket.delete_objects(Delete={"Objects": response})
    print("Objects Deleted:", *response, sep="\n\t")
    return response


#setting/granting things like bucket server access logging ---------------
def grant_logging_permissions_bucket_policy(logging_bucket_name: str,
                                            source_accounts: str|list[str]):
    '''
    Gives buckets a bucket policy that will allow it to be used for server access logging.

    Grants s3:PutObject permissions to the logging service principal (logging.s3.amazonaws.com)
    https://docs.aws.amazon.com/AmazonS3/latest/userguide/enable-server-access-logging.html#grant-log-delivery-permissions-general

    Parameters:
    `logging_bucket_name` str
        the name of the bucket to receive the server access logging permissions
    `source_accounts` str|list[str]
        the source accounts of the buckets' that are being logged (I think, I couldn't confirm this anywhere).
        IF this is the case, it prevents random accounts from sending logs to your logging bucket.
        A list works by checking if the bucket source account is any of the values in `source_accounts`
        (case sensitive).
        It might also be that the source account of the logging bucket must be one of the passed values;
        either way the implementation is the same.

    The * wildcard will give logging permissions to all the paths in the logging_bucket_name.
    Can add this snippet inside Condition to specify allowed source bucket prefixes for the logging:
    (* wildcard if you want more than one source bucket)
        "ArnLike": {
        "aws:SourceArn": "arn:aws:s3:::SOURCE-BUCKET-PREFIX*"
        },
    Can do a lot with Condition, just google "IAM JSON policy elements: Condition" and it'll pop up.

    Warning:
    As a security precaution, the root user of the Amazon Web Services account that owns a bucket
    can always use this operation, even if the policy explicitly denies the root user the ability
    to perform this action.
    '''
    assert isinstance(source_accounts, (str,list)), f"Wrong type: {type(source_accounts)}"
    if isinstance(source_accounts, str):
        source_accounts = [source_accounts]
    else:
        for account_id in source_accounts:
            assert isinstance(account_id, str), f"Wrong type: {type(account_id)}"

    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "S3ServerAccessLogsPolicy",
                "Effect": "Allow",
                "Principal": {
                    "Service": "logging.s3.amazonaws.com"
                },
                "Action": [
                    "s3:PutObject"
                ],
                "Resource": f"arn:aws:s3:::{logging_bucket_name}/*",
                "Condition": {
                    "StringEquals": {"aws:SourceAccount": source_accounts}
                }
            }
        ]
    }
    policy = json.dumps(policy) #JSON dict to a string

    s3_policy_resource = boto3.resource("s3").BucketPolicy(logging_bucket_name)
    response = s3_policy_resource.put(
        Policy = policy
    )
    s3_policy_resource.reload() #same as .load()
    print(f"Update Policy:\n{s3_policy_resource.policy}")
    return response



def set_bucket_server_access_logging_on(source_bucket_name: str,
                                     logging_bucket_name: str,
                                     logging_path_prefix: str|None = None):
    '''
    Activates logging of `source_bucket_name` in `logging_bucket_name` at path = `logging_path_prefix/`

    Parameters:
    self explanatory, read the names.
    `logging_path_prefix` str
        use this to separate different source buckets inside the logging bucket
        if `logging_path_prefix` = None, it will default to the `source_bucket_name` argument

    Example Use:
        source_bucket_name is "melon"
        logging_bucket_name is "bread"
        logging_path_prefix is "melon_bucket"
        Then logging will be activate for 'arn:aws:s3:::melon/'
        and the logs will be put in 'arn:aws:s3:::bread/melon_bucket/'
    '''
    assert isinstance(source_bucket_name, str)
    assert isinstance(logging_bucket_name, str)
    if logging_path_prefix != None: assert isinstance(logging_path_prefix, str)
    else: logging_path_prefix = source_bucket_name[:] #makes a copy to avoid referencing each other

    bucket_logging_settings = boto3.resource("s3").BucketLogging(source_bucket_name)
    response = bucket_logging_settings.put(
        BucketLoggingStatus = {
            "LoggingEnabled": {
                "TargetBucket": logging_bucket_name,
                "TargetPrefix": logging_path_prefix
            }
        }
    )
    bucket_logging_settings.reload() #same as .load()
    print("Logging settings:", bucket_logging_settings.logging_enabled, sep="\n")
    return response

# def set_bucket_server_access_logging_off(source_bucket_name: str, delete_logs: bool = False):
#     '''
#     Turns off logging for `source_bucket_name`, can also delete all of its logs.

#     Parameters:
#     self-explanatory, read the names.
#     '''
#     resource = boto3.resource("s3").BucketLogging(source_bucket_name)
#     response = resource.put(BucketLoggingStatus = {}) #empty BLS turns off logging
#     if delete_logs:
    #need to check the format of bucket.logging_enabled return value for the target stuff

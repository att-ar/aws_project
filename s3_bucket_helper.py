#function to add random suffix to bucket names
import boto3
from botocore.exceptions import ClientError
import datetime
import json
from uuid import uuid4
from inspect import isclass, isfunction

def display_callables(name):
    '''Displays the functions and classes defined in the module `name`'''
    functions = []
    for name in dir(name):
        obj = eval(name)
        if callable(obj): #hasattr(obj,'__call__'):
            if obj.__module__ == __name__:
                functions.append(name)
                if isclass(obj):
                    methods = []
                    for key,val in vars(obj).items():
                        if isfunction(val) and key[:2] != "__":
                            methods.append(key)
                    functions.append(f'{name} methods: {methods}')
    return functions

# generating things --------------------------
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

def gen_bucket(bucket_name: str, tags: dict = None, region = None, suffix = True):
    '''
    Creates an S3 bucket and returns the suffixed bucket name as well as the S3 response

    Precondition:
        `bucket_name` must contain characters valid in S3
        Each tag in `tags` must be in s3 tag format: {'Key':key_argument, 'Value':value_argument}

    This function uses the user that connected through the boto SDK to find its default region.

    Parameters:
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
    s3_boto_connection = boto3.resource("s3")
    if not region:
        region = boto3.Session().region_name
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

# getting things based on filters -------------------------
# at some point these will likely be changed to use metadata stored by AWS either in S3 or RDS

## tag filter
def helper_tag_filter(tag_list:dict, source_tags:dict):
    '''helper for get_buckets_tag_filter() and get_object_tag_filter()'''
    result = len(tag_list)
    for tkey, tval in tag_list.items():
        if not result: return True
        if source_tags.get(tkey) == tval:
            result -= 1
    return not result

def get_buckets_with_tags(tags: list[dict]|dict, s3_format = True) -> list[tuple[str]]:
    '''
    Returns list of tuples containing buckets and their tags based on filtering by `tags`

    Parameters:
    `tags` list[dict]|dict
        See `s3_format` for more information
    `s3_format` bool
        if True, `tags` is a list S3 tag formatted dicts {"Key":key_arg, "Value":value_arg}
        if False, `tags` is a dict of regular key-value pairs {key_arg1: value_arg1, key_arg2: value_arg2}
    '''
    assert isinstance(tags,(list,dict))
    if s3_format:
        tags = gen_python_dict_from_tagging_list(tags)
    s3_client = boto3.client("s3")
    result = []
    for bucket in s3_client.list_buckets()["Buckets"]:
        try:
            bucket_tags = s3_client.get_bucket_tagging(Bucket=bucket["Name"])["TagSet"]
            if helper_tag_filter(tags, gen_python_dict_from_tagging_list(bucket_tags)):
                result.append((bucket, bucket_tags))
        except ClientError as err:
            if err.response["Error"]["Code"] == "NoSuchTagSet": continue
            else: print("Error: ", err.response)
    return result

def get_objects_with_tags_from_bucket(bucket_name:str, tags:list[dict]|dict,
                                      object_prefix:str = None, s3_format = True) -> list[tuple[str]]:
    '''
    Returns list of tuples containing `bucket_name`'s objects and their tags based on `tags` 

    Parameters:
    `bucket_name` str
        the name of the bucket to check
    `tags` list[dict]|dict
        See `s3_format` for more information
    `object_prefix` str
        Prefix of objects that should be returned, makes the request from AWS smaller I think, accelerates the function.
        defaults to None, all objects in the bucket are checked for the tags
    `s3_format` bool
        if True, `tags` is a list S3 tag formatted dicts {"Key":key_arg, "Value":value_arg}
        if False, `tags` is a dict of regular key-value pairs {key_arg1: value_arg1, key_arg2: value_arg2}
    '''
    s3_client = boto3.client("s3")
    result = []
    if s3_format:
        tags = gen_python_dict_from_tagging_list(tags)
    if object_prefix: #!= None
        assert isinstance(object_prefix,str)
        object_list =  s3_client.list_objects(Bucket = bucket_name, Prefix = object_prefix)["Contents"]
    else:
        object_list = s3_client.list_objects(Bucket = bucket_name)["Contents"]
    for object in object_list:
        tag_dict = gen_python_dict_from_tagging_list(
            s3_client.get_object_tagging(Bucket = bucket_name, Key = object["Key"])["TagSet"]
        )
        if helper_tag_filter(tags,tag_dict):
            result.append((object,tag_dict))
    return result


## name date filter
def helper_date_comparison(bucket_creationdate: datetime.datetime, *args) -> bool:
    '''helper for get_buckets_with_name_date, returns the date comparison result'''
    timezone = bucket_creationdate.tzinfo
    args = list(args) #args is a tuple but a mutable object is needed
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

#deleting objects from a bucket based on a prefix(es) filter: --------------
# can use this to delete all objects if you pass "" as the prefix

def delete_objects__with_prefix(bucket_name: str, prefixes: str|list[str]):
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


#adding things to buckets/objects
def add_tags_to_bucket(bucket_name:str, tags:list[dict]|dict, s3_format=True):
    '''
    Adds tags to an s3 bucket. Does not remove existing tags.

    Will add an option to overwrite duplicate tags at some point.

    Parameters:
    `bucket_name` str
        the name of the s3 bucket
    `tags` list[dict]|dict
        the tags to be added
        See `s3_format` for more information
    `s3_format` bool
        if True, `tags` is a list S3 tag formatted dicts {"Key":key_arg, "Value":value_arg}
        if False, `tags` is a dict of regular key-value pairs {key_arg1: value_arg1, key_arg2: value_arg2}
    '''
    assert isinstance(tags, (list,dict))
    if not s3_format:
        tags = gen_tagging_list_from_python_dict(tags)
    bucket_tagger = boto3.resource("s3").BucketTagging(bucket_name)
    
    try:
        existing_tags = bucket_tagger.tag_set
        tags.extend(existing_tags)
    except ClientError as err:
        if err.response["Error"]["Code"] == "NoSuchTagSet":
            pass
    try:
        set_tag = bucket_tagger.put(Tagging={"TagSet":tags})
        # bucket_tagger.reload() #useless here
        print("Bucket tags:", *tags, sep="\n\t")
        return set_tag
    except ClientError as err:
        if err.response["Error"]["Code"] == "InvalidTag":
            print("There may be a duplicate tag")
            return None
    
def add_tags_to_object(bucket_name:str, object_name:str, tags:list[dict]|dict,
                       s3_format=True, s3_client=None):
    '''
    Adds tags to an s3 bucket. Does not remove existing tags.

    Will add an option to overwrite duplicate tags at some point.

    Parameters:
    `bucket_name` str
        the name of the s3 bucket containing the object
    `object_name` str
        the name of the s3 object
    `tags` list[dict]|dict
        the tags to be added
        See `s3_format` for more information
    `s3_format` bool
        if True, `tags` is a list S3 tag formatted dicts {"Key":key_arg, "Value":value_arg}
        if False, `tags` is a dict of regular key-value pairs {key_arg1: value_arg1, key_arg2: value_arg2}
    `s3_client` boto3.client("s3")
        Gives the option to pass an s3 client if this function is used for many objects in a loop.
        Saves time and space by not reinitializing a client every function call
    '''
    if s3_client == None: s3_client = boto3.client("s3")
    if not s3_format:
        tags = gen_tagging_list_from_python_dict(tags)

    existing_tags = s3_client.get_object_tagging(Bucket = bucket_name, Key = object_name)["TagSet"]
    tags.extend(existing_tags)
    try:
        set_tag = s3_client.put_object_tagging(
            Bucket = bucket_name,
            Key = object_name,
            Tagging = {"TagSet" : tags}
        )
        print("New tag set:", *tags, sep="\n\t")
        return set_tag
    except ClientError as err:
        if err.response["Error"]["Code"] == "InvalidTag":
            print("There may be a duplicate tag")
            return None

#setting/granting things like bucket server access logging ---------------
def helper_lifecycle(**kwargs):
    '''Makes the JSON formatted policy for the set_bucket_lifecycle()'''
    config_json = {
        "Rules": [{
            "ID": kwargs.get("lifecycle_name"),
            "Status": "Enabled"
        }]}
    
    if kwargs.get("transition") and kwargs.get("transition_days"):
        config_json["Rules"][0]["Transitions"] = [{
            "Days": kwargs.get("transition_days"),
            "StorageClass": kwargs.get("transition")
        }]
    
    if kwargs.get("expiration") and kwargs.get("expiration_days"):
        config_json["Rules"][0]["Expiration"] = {"Days": kwargs.get("expiration_days")}
    
    if kwargs.get("noncurrent_transition") and kwargs.get("noncurrent_transition_days"):
        config_json["Rules"][0]["NoncurrentVersionTransitions"] = [{
            "NoncurrentDays": kwargs.get("noncurrent_transition_days"),
            "StorageClass": kwargs.get("noncurrent_transition")
        }]

    if kwargs.get("noncurrent_expiration") and kwargs.get("noncurrent_expiration_days"):
        use_noncurrent = {"NoncurrentDays": kwargs.get("noncurrent_expiration_days")}
        if kwargs.get("newer_noncurrent_versions"):
            use_noncurrent["NewerNoncurrentVersions"] = kwargs.get("newer_noncurrent_versions")
        config_json["Rules"][0]["NoncurrentVersionExpiration"] = use_noncurrent

    if kwargs.get("filter"):
        use_filter = {
            "p" : {"Prefix": kwargs.get("prefix_filter")},
            "t" : {"Tag": kwargs.get("tag_filter")},
            "tt" : {"And": {"Tags": kwargs.get("tag_filter")}}, 
            "pt" : {"And": {"Prefix": kwargs.get("prefix_filter"),
                        "Tags": kwargs.get("tag_filter")}}}
        config_json["Rules"][0]["Filter"] = use_filter[kwargs.get("filter")]
    
    config_json["Rules"][0]["AbortIncompleteMultipartUpload"] = {"DaysAfterInitiation": kwargs.get("abort_incomplete_days")}
    
    return config_json

def set_bucket_lifecycle(
    lifecycle_name: str, 
    bucket_name: str,
    transition: str = "Standard_IA",
    transition_days: int = 30,
    expiration: bool = True,
    expiration_days: int = 90,
    noncurrent_transition: str = None,
    noncurrent_transition_days: int = None,
    noncurrent_expiration: bool = False,
    noncurrent_expiration_days: int = None,
    newer_noncurrent_versions: int = None,
    prefix_filter: str = None,
    tag_filter: list[dict]|dict = None,
    s3_format: bool = True,
    abort_incomplete_days: int = 1,
    expected_owner: str = None
    ):
    '''
    Sets a data lifecycle management configuration on a bucket in S3.
    Good practice to have one rule per lifecycle configuration.

    https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html
    Valid storage options to transition:
    'STANDARD_IA', 'INTELLIGENT_TIERING', 'ONEZONE_IA', 'GLACIER', 'GLACIER_IR', 'DEEP_ARCHIVE'
    
    Parameters:
    `lifecycle_name` str
        The name that will be assigned to the lifecyclee policy.
    `bucket_name` str
        The name of the bucket to assign the policy unto.
    `transition` str
        The storage class to transition bucket objects to.
    `transition_days` int
        The days between object creation and object transition. 
    `expiration` bool
        True: adds a delete objects rule
        False: does not add a delete objects rule
    `expiration_days` int
        The days between object creation and object expiration.
    `noncurrent_transition` str
        The storage class to transition bucket noncurrent objects to.
        Only applicable to buckets with versionin enabled, will have no effect otherwise.
    `noncurrent_transition_days` int
        The days between object becoming noncurrent and object transition. 
    `noncurrent_expiration` bool
        True: adds a delete noncurrent objects rule
        False: does not add a delete noncurrent objects rule
    `noncurrent_expiration_days` int
        The days between object becoming noncurrent and object expiration.
    `prefix_filter` str
        Policy will only affect objects with this prefix.
    `tag_filter` list[dict]|dict
        Policy will only affect objects with these tags.
    `s3_format` bool
        if True, `tag_filter` is a list S3 tag formatted dicts {"Key":key_arg, "Value":value_arg}
        if False, `tag_filter` is a dict of regular key-value pairs {key_arg1: value_arg1, key_arg2: value_arg2}
    `abort_incomplete_days` int
        Specifies the days since the initiation of an incomplete multipart upload that Amazon S3
        will wait before permanently removing all parts of the upload.
    `expected_owner` str
        The account ID of the S3 bucket's expected owner. Unnecessary if you are the bucket owner.

    Request syntax based on boto3 documentation:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#bucketlifecycleconfiguration
    '''
    assert isinstance(lifecycle_name, (str,None))
    assert isinstance(bucket_name, (str,None))
    assert isinstance(transition, (str,None))
    assert isinstance(transition_days, (int,None))
    assert isinstance(expiration, (bool,None))
    assert isinstance(expiration_days, (int,None))
    assert isinstance(noncurrent_transition, (str,None))
    assert isinstance(noncurrent_transition_days, (int,None))
    assert isinstance(noncurrent_expiration, (bool,None))
    assert isinstance(noncurrent_expiration_days, (int,None))
    assert isinstance(newer_noncurrent_versions, (int,None))
    if isinstance(newer_noncurrent_versions, int):
        assert newer_noncurrent_versions <= 100 and newer_noncurrent_versions >= 0
    assert isinstance(prefix_filter, (str,None))
    assert isinstance(tag_filter, (list,dict,None))
    assert isinstance(s3_format, (bool,None))
    assert isinstance(abort_incomplete_days, int)
    assert isinstance(expected_owner, (str,None))
    
    #setup for the helper function helper_lifecycle()
    filter = "" #evaluates as False in a conditional.
    if prefix_filter:
        filter += "p"
    if tag_filter:
        filter += "t" * min( 2, len(tag_filter) )
    filter = filter[: min( len(filter), 2 )]

    #setting the lifecycle
    config_json = helper_lifecycle(
        lifecycle_name = lifecycle_name,
        transition = transition,
        transition_days = transition_days,
        expiration = expiration,
        expiration_days = expiration_days,
        noncurrent_transition = noncurrent_transition,
        noncurrent_transition_days = noncurrent_transition_days,
        noncurrent_expiration = noncurrent_expiration,
        noncurrent_expiration_days = noncurrent_expiration_days,
        newer_noncurrent_versions = newer_noncurrent_versions,
        prefix_filter = prefix_filter,
        tag_filter = tag_filter,
        filter = filter,
        abort_incomplete_days = abort_incomplete_days
    ) # this is the lifecycle configuration policy that will be added

    bucket_lifecycle_tool = boto3.resource("s3").BucketLifecycleConfiguration(bucket_name)
    if expected_owner:
        response = bucket_lifecycle_tool.put(
            LifecycleConfiguration = config_json,
            ExpectedBucketOwner = expected_owner
        )
    else:
        response = bucket_lifecycle_tool.put(
            LifecycleConfiguration = config_json
        )
    print("Lifecycle Configuration:", config_json)
    return response

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
        The values of the strings will be the 12-digit AWS account ID on whose behalf the service is publishing data.
        Can pass multiple source accounts as a list of 12-digit AWS ID strings.
        A list works by checking if the bucket source account is any of the values in `source_accounts`
        (case sensitive).

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
    policy = json.dumps(policy) #JSON dict to a string because BucketPolicy needs a string argument

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
from botocore.exceptions import ClientError
import json

from s3_generate import gen_tagging_list_from_python_dict#, gen_python_dict_from_tagging_list
# from s3_delete import delete_objects__with_prefix

#adding things to buckets/objects
def add_tags_to_bucket(session, bucket_name:str, tags:list[dict]|dict, s3_format=True, overwrite=False):
    '''
    Adds tags to an s3 bucket. Does not remove existing tags.

    Returns the tagging response.
    Will add an option to overwrite duplicate tags at some point.

    Parameters:
    `session` boto3.session.Session()
    `bucket_name` str
        the name of the s3 bucket
    `tags` list[dict]|dict
        the tags to be added
        See `s3_format` for more information
    `s3_format` bool
        if True, `tags` is a list S3 tag formatted dicts {"Key":key_arg, "Value":value_arg}
        if False, `tags` is a dict of regular key-value pairs {key_arg1: value_arg1, key_arg2: value_arg2}
    `overwrite` bool
        if True, existing tags are overwritten.
        if False, existing tags are added to.
    '''
    assert isinstance(tags, (list,dict))
    if not s3_format:
        tags = gen_tagging_list_from_python_dict(tags)
    bucket_tagger = session.resource("s3").BucketTagging(bucket_name)
    
    if not overwrite:
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
    
def add_tags_to_object(session, bucket_name:str, object_name:str, tags:list[dict]|dict,
                       s3_format=True, s3_client=None, overwrite=False):
    '''
    Adds tags to an s3 bucket. Does not remove existing tags.

    Returns the tagging response.
    Will add an option to overwrite duplicate tags at some point.

    Parameters:
    `session` boto3.session.Session()
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
    `overwrite` bool
        if True, existing tags are overwritten.
        if False, existing tags are added to.
    '''
    if s3_client == None: s3_client = session.client("s3")
    if not s3_format:
        tags = gen_tagging_list_from_python_dict(tags)
    if not overwrite:
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

def add_bucket_lifecycle(
    session,
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

    Returns the configuration response.
    Good practice to have one rule per lifecycle configuration.

    https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html
    Valid storage options to transition:
    'STANDARD_IA', 'INTELLIGENT_TIERING', 'ONEZONE_IA', 'GLACIER', 'GLACIER_IR', 'DEEP_ARCHIVE'
    
    Parameters:
    `session` boto3.session.Session()
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
    assert isinstance(lifecycle_name, (str,type(None)))
    assert isinstance(bucket_name, (str,type(None)))
    assert isinstance(transition, (str,type(None)))
    assert isinstance(transition_days, (int,type(None)))
    assert isinstance(expiration, (bool,type(None)))
    assert isinstance(expiration_days, (int,type(None)))
    assert isinstance(noncurrent_transition, (str,type(None)))
    assert isinstance(noncurrent_transition_days, (int,type(None)))
    assert isinstance(noncurrent_expiration, (bool,type(None)))
    assert isinstance(noncurrent_expiration_days, (int,type(None)))
    assert isinstance(newer_noncurrent_versions, (int,type(None)))
    if isinstance(newer_noncurrent_versions, int):
        assert newer_noncurrent_versions <= 100 and newer_noncurrent_versions >= 0
    assert isinstance(prefix_filter, (str,type(None)))
    assert isinstance(tag_filter, (list,dict,type(None)))
    assert isinstance(s3_format, (bool,type(None)))
    assert isinstance(abort_incomplete_days, int)
    assert isinstance(expected_owner, (str,type(None)))
    
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

    bucket_lifecycle_tool = session.resource("s3").BucketLifecycleConfiguration(bucket_name)
    #check for existing lifecycle policies
    try:
        existing_policies = bucket_lifecycle_tool.rules
        config_json["Rules"].extend(existing_policies)
    except ClientError as err:
        if err.response["Error"]["Code"] == "NoSuchLifecycleConfiguration":
            pass
    if expected_owner:
        response = bucket_lifecycle_tool.put(
            LifecycleConfiguration = config_json,
            ExpectedBucketOwner = expected_owner
        )
    else:
        response = bucket_lifecycle_tool.put(
            LifecycleConfiguration = config_json
        )
    print("Lifecycle Configuration:\n\t", config_json)
    return response

def grant_logging_permissions_bucket_policy(session, logging_bucket_name: str, source_accounts: str|list[str]):
    '''
    Gives buckets a bucket policy that will allow it to be used for server access logging.

    Returns the policy response

    Grants s3:PutObject permissions to the logging service principal (logging.s3.amazonaws.com)
    https://docs.aws.amazon.com/AmazonS3/latest/userguide/enable-server-access-logging.html#grant-log-delivery-permissions-general

    Parameters:
    `session` boto3.session.Session()
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

    s3_policy_resource = session.resource("s3").BucketPolicy(logging_bucket_name)
    response = s3_policy_resource.put(
        Policy = policy
    )
    s3_policy_resource.reload() #same as .load()
    print(f"Update Policy:\n{s3_policy_resource.policy}")
    return response

def set_bucket_server_access_logging_on(session,
                                        source_bucket_name: str,
                                        logging_bucket_name: str,
                                        logging_path_prefix: str|None = None):
    '''
    Activates logging of `source_bucket_name` in `logging_bucket_name` at path = `logging_path_prefix/`

    Returns the logging response.

    Parameters:
    `session` boto3.session.Session()
    `source_bucket_name` str
    `logging_bucket_name` str
    `logging_path_prefix` str
        use this to separate different source buckets inside the logging bucket
        if `logging_path_prefix` = None, it will default to the `source_bucket_name` argument
        Note that the function adds a forward slash to the path prefix if there is not already one.

    Example Use:
        source_bucket_name is "melon"
        logging_bucket_name is "bread"
        logging_path_prefix is "melon_bucket"
        Then logging will be activate for 'arn:aws:s3:::melon/'
        and the logs will be put in 'arn:aws:s3:::bread/melon_bucket/'
    '''
    assert isinstance(source_bucket_name, str)
    assert isinstance(logging_bucket_name, str)
    if logging_path_prefix != None:
        assert isinstance(logging_path_prefix, str)
        if logging_path_prefix[-1] != "/": logging_path_prefix += "/"
    else: logging_path_prefix = source_bucket_name[:] + "/" #makes a copy to avoid referencing each other

    bucket_logging_settings = session.resource("s3").BucketLogging(source_bucket_name)
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

def set_bucket_server_access_logging_off(session, source_bucket_name: str):#, delete_logs: bool = False):
    '''
    Turns off logging for `source_bucket_name`

    Returns the logging response.
    The deletion option was removed due to security reasons.
    Copy the function and uncomment the code to use it in another environment.

    Parameters:
    `session` boto3.session.Session()
    `source_bucket_name` str
    '''
    resource = session.resource("s3").BucketLogging(source_bucket_name)
    response = resource.put(BucketLoggingStatus = {}) #empty BLS turns off logging
    # if delete_logs:
    #     try:
    #         #the following should work because the resource was not reloaded
    #         logging_status = resource.logging_enabled
    #         #format of resource.logging_enabled is {'TargetBucket': logging_bucket, 'TargetPrefix': logging_prefix}
    #         del_response = delete_objects__with_prefix(session,
    #                                                    logging_status["TargetBucket"],
    #                                                    logging_status["TargetPrefix"])
    #         return response, del_response
    #     except TypeError:
    #         print(f"No logs found for source bucket {source_bucket_name}.")
    return response
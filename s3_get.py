import boto3
from botocore.exceptions import ClientError
import datetime

from s3_generate import gen_python_dict_from_tagging_list#, gen_tagging_list_from_python_dict
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

def get_buckets_with_tags(session, tags: list[dict]|dict, s3_format = True) -> list[tuple[str]]:
    '''
    Returns list of tuples containing buckets and their tags based on filtering by `tags`

    Parameters:
    `session` boto3.session.Session()
    `tags` list[dict]|dict
        See `s3_format` for more information
    `s3_format` bool
        if True, `tags` is a list S3 tag formatted dicts {"Key":key_arg, "Value":value_arg}
        if False, `tags` is a dict of regular key-value pairs {key_arg1: value_arg1, key_arg2: value_arg2}
    '''
    assert isinstance(tags,(list,dict))
    if s3_format:
        tags = gen_python_dict_from_tagging_list(tags)
    s3_client = session.client("s3")
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

def get_objects_with_tags_from_bucket(session, bucket_name:str, tags:list[dict]|dict,
                                      object_prefix:str = None, s3_format = True) -> list[tuple[str]]:
    '''
    Returns list of tuples containing `bucket_name`'s objects and their tags based on `tags` 

    Parameters:
    `session` boto3.session.Session()
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
    s3_client = session.client("s3")
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

def get_buckets_with_name_date(session,
                               prefix: str,
                               use_date: list[str|datetime.datetime|datetime.date]
                               | str|datetime.datetime|datetime.date
                               | None = None) -> list[str]:
    '''
    Returns list of bucket names who start with `prefix` and made on or in (list) `use_date` using boto3.

    Parameters:
    `session` boto3.session.Session()
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
        in session.client("s3").list_buckets()["Buckets"]
        if (bucket["Name"][: min(len(bucket["Name"]), len(prefix))] == prefix)
            & (helper_date_comparison(bucket["CreationDate"], *use_date))]

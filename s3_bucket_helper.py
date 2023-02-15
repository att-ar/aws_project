#function to add random suffix to bucket names
import boto3
from uuid import uuid4
import datetime
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
                | str|datetime.datetime|datetime.date = None) -> list[str]:
    '''
    Returns list of buckets who start with `prefix` and made on or in (list) `use_date` using boto3.

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

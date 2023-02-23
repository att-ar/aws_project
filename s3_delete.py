import boto3

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

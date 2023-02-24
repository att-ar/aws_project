import boto3

# can use this to delete all objects if you pass "" as the prefix
def delete_objects__with_prefix(bucket_name: str, prefix: str):
    '''
    Delete objects from a bucket using `prefixes` as the filter for object names

    Can call `for object in bucket.object_versions.all(): print(object.key)`
    to get the object that remain after running this delete function

    Parameters:
    `bucket_name` str
        the bucket name
    `prefix` str
        The path prefix to use to delete objects that start with `prefix`
    '''
    assert isinstance(bucket_name, str)
    assert isinstance(prefix, str)
    
    response = []
    bucket = boto3.resource("s3").Bucket(bucket_name)

    for obj in bucket.object_versions.all():
        try:
            if obj.object_key[:len(prefix)] == prefix:
                response.append({
                    "Key": obj.object_key,
                    "VersionID": obj.version_id
                })
        except IndexError:
            continue

    bucket.delete_objects(Delete={"Objects": response})
    print("Objects Deleted:", *response, sep="\n\t")
    return response

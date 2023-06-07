import click
from chunk_replicator import DataProxyBucket, User
from fladec import S2SToken
from pathlib import Path

@click.command()
@click.option("--src", help="Source file")
@click.option("--dst-bucket", help="Destination Bucket")
@click.option("--dst-filename", help="Destination Filename")
def main(src: str, dst_bucket: str, dst_filename: str):
    if not src:
        raise Exception(f"--src must be defined!")
    if not dst_bucket:
        raise Exception(f"--dst-bucket must be defined!")
    if not dst_filename:
        raise Exception(f"--dst-filename must be defined!")
    
    user = User(S2SToken.get_token())
    bucket = DataProxyBucket(user=user, bucketname=dst_bucket)

    src_file = Path(src)
    with open(src_file, "rb") as fp:
        bucket.put_object(dst_filename, fp.read())
    

if __name__ == "__main__":
    main()

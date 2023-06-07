import click
from chunk_replicator import LocalSrcAccessor, EbrainsDataproxyHttpReplicatorAccessor, DataProxyBucket, User
from fladec import get_all, S2SToken


@click.command()
@click.option("--src", help="Source directory")
@click.option("--dst-bucket", help="Destination Bucket")
@click.option("--recursive", default=False, help="Recursively search for info")
def main(src: str, dst_bucket: str, recursive: bool):
    if not src:
        raise Exception(f"--src must be defined!")
    if not dst_bucket:
        raise Exception(f"--dst-bucket must be defined!")
    
    for v in get_all(src, recursive=recursive):
        src_acc = LocalSrcAccessor(v.base_dir, v.flat, v.gzip)
        
        user = User(S2SToken.get_token())
        bucket = DataProxyBucket(user=user, bucketname=dst_bucket)
        dst_bucket_acc = EbrainsDataproxyHttpReplicatorAccessor(prefix=str(v.relative_path), dataproxybucket=bucket, smart_gzip=True)
            
        src_acc.mirror_to(dst_bucket_acc)
    
    

if __name__ == "__main__":
    main()

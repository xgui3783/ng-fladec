import click
from chunk_replicator import LocalSrcAccessor
from neuroglancer_scripts.file_accessor import FileAccessor
from fladec import get_all
from pathlib import Path

@click.command()
@click.option("--src", help="Source directory")
@click.option("--dst", help="Destination directory")
@click.option("--recursive", default=False, help="Recursively search for info")
def main(src: str, dst: str, recursive: bool):
    if not src:
        raise Exception(f"src must be defined!")
    if not dst:
        raise Exception(f"src must be defined!")
    
    _dst = Path(dst)
    for v in get_all(src, recursive=recursive):
        src_acc = LocalSrcAccessor(v.base_dir, v.flat, v.gzip)
        
        _dst_path = _dst / v.relative_path
        _dst_path.mkdir(exist_ok=True)
            
        dst_acc = FileAccessor(_dst_path, True, False)
        src_acc.mirror_to(dst_acc)
    
    

if __name__ == "__main__":
    main()

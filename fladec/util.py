from pathlib import Path
from dataclasses import dataclass
import json
import os
import typing
import re
class PrecompSrcVerificationException(Exception): pass

@dataclass
class PrecompSrc:
    base_dir: str
    gzip: bool=None
    flat: bool=None

    def verify(self):
        base_dir = Path(self.base_dir)
        if not (base_dir / 'info').exists():
            raise PrecompSrcVerificationException(f"{base_dir / 'info'} does not exist!")
        for dirpath, dirnames, filenames in os.walk(base_dir):
            for filename in filenames:
                block = r'[0-9]+-[0-9]+'
                if re.search(block, filename):
                    if Path(dirpath, filename).suffix == ".gz":
                        self.gzip = True
                    
                    if re.match('_'.join([block]*3), filename):
                        self.flat = True
                    elif re.search('/'.join([block]*2), dirpath):
                        self.flat = False
                    else:
                        raise Exception(f"Neither flat nor not flat: {dirpath}, {filename}")
                    
                    return
                
def get_all(src: Path, recursive: bool=False):
    for dirpath, dirnames, filenames in os.walk(src, topdown=True):
        for filename in dirnames:
            precomp_src = PrecompSrc(base_dir=str(Path(dirpath) / filename))
            try:
                precomp_src.verify()
                yield precomp_src
            except PrecompSrcVerificationException:
                pass
        if not recursive:
            break

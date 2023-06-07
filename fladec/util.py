from pathlib import Path
from dataclasses import dataclass
import json
import os
import re
from base64 import b64decode
from authlib.integrations.requests_client.oauth2_session import OAuth2Session
import time

class PrecompSrcVerificationException(Exception): pass

@dataclass
class PrecompSrc:
    base_dir: str
    relative_path: str=None
    gzip: bool=None
    flat: bool=None

    def verify(self):
        base_dir = Path(self.base_dir)
        if not (base_dir / 'info').exists():
            raise PrecompSrcVerificationException(f"{base_dir / 'info'} does not exist!")
        with open(base_dir / 'info', 'r') as fp:
            loaded_info = json.load(fp=fp)
            if loaded_info.get("@type") == "neuroglancer_legacy_mesh":
                raise PrecompSrcVerificationException(f"{base_dir / 'info'} appears to be legacy mesh according to @type attribute")
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
    root = PrecompSrc(base_dir=src)
    try:
        root.verify()
        yield root
    except PrecompSrcVerificationException:
        pass

    if not recursive:
        return

    for dirpath, dirnames, filenames in os.walk(src, topdown=True, followlinks=True):
        for filename in dirnames:
            precomp_src = PrecompSrc(base_dir=str(Path(dirpath) / filename))
            try:
                precomp_src.verify()
                precomp_src.relative_path = (Path(dirpath) / filename).relative_to(Path(src))
                yield precomp_src
            except PrecompSrcVerificationException:
                pass

"""
Modified from https://github.com/FZJ-INM1-BDA/voluba/blob/79aae70/backend/voluba_backend/voluba_auth.py#L121-L147
"""
class NoAuthException(Exception): pass

IAM_DISCOVERY_URL = os.getenv("IAM_DISCOVERY_URL")
IAM_CLIENT_ID = os.getenv("IAM_CLIENT_ID")
IAM_CLIENT_SECRET = os.getenv("IAM_CLIENT_SECRET")

class S2SToken:
    s2s_token: str=None
    exp=None

    @staticmethod
    def refresh():
        if not IAM_CLIENT_SECRET or not IAM_CLIENT_ID:
            raise NoAuthException(f"sa client id or sa client secret not set. cannot get s2s token")
        
        token_endpoint = f"{IAM_DISCOVERY_URL}/protocol/openid-connect/token"

        client = OAuth2Session(IAM_CLIENT_ID, IAM_CLIENT_SECRET, scope="openid team roles group")
        token = client.fetch_token(token_endpoint, grant_type='client_credentials')
        auth_token = token.get("access_token")
        S2SToken.s2s_token = auth_token
        _header, body, _sig, *_rest = auth_token.split('.')
        S2SToken.exp = json.loads(b64decode(body.encode("utf-8") + b"====").decode("utf-8")).get("exp")

    @staticmethod
    def get_token():
        if S2SToken.s2s_token is None:
            S2SToken.refresh()
        diff = S2SToken.exp - time.time()
        # if the token is about to expire (30 seconds)
        if diff < 30:
            S2SToken.refresh()
        return S2SToken.s2s_token
        
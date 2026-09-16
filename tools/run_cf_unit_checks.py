"""Offline cf tests: first argument is the copied function directory, rest are pytest paths.
Set CF_PRNSRV_SOURCE to the pinned prnsrv checkout when testing prnsrv-generator.
"""
import os,sys,socket
from pathlib import Path
root=Path(sys.argv[1]);os.chdir(root)
sys.path.insert(0,str(root));
if os.environ.get('CF_PRNSRV_SOURCE'):
    sys.path.insert(0, os.environ['CF_PRNSRV_SOURCE'])
os.environ['PYTEST_DISABLE_PLUGIN_AUTOLOAD']='1';os.environ['PYTHONDONTWRITEBYTECODE']='1';os.environ['AWS_EC2_METADATA_DISABLED']='true'
for k in list(os.environ):
 if k.startswith(('YMQ_','AWS_ACCESS','AWS_SECRET','AWS_SESSION')) or k in ['MAX_BOT_TOKEN','MAX_CHAT_ID','TOKENS_CONFIG','suz_worker_config','token_config']:os.environ.pop(k,None)
def blocked(*a,**kw):raise AssertionError('Real network prohibited')
socket.socket.connect=socket.socket.connect_ex=blocked
import pytest
raise SystemExit(pytest.main(['-q','-p','no:cacheprovider',*sys.argv[2:]]))

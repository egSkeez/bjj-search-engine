import os
for v in ('ALL_PROXY','all_proxy','HTTP_PROXY','http_proxy',
          'HTTPS_PROXY','https_proxy','SOCKS_PROXY','SOCKS5_PROXY'):
    os.environ.pop(v, None)

from sqlalchemy import create_engine, text
from app.config import settings

e = create_engine(settings.database_url.replace('+asyncpg', ''))
with e.connect() as c:
    dvds = c.execute(text("SELECT COUNT(*) FROM dvds")).scalar()
    vols = c.execute(text("SELECT COUNT(*) FROM volumes")).scalar()
    total = c.execute(text("SELECT COUNT(*) FROM chunks")).scalar()
    gran = c.execute(text("SELECT COUNT(*) FROM chunks WHERE chunk_type='granular'")).scalar()
    sem = c.execute(text("SELECT COUNT(*) FROM chunks WHERE chunk_type='semantic'")).scalar()
    unid = c.execute(text("SELECT COUNT(*) FROM chunks WHERE technique='unidentified' OR technique IS NULL")).scalar()
    tagged = total - unid

print(f"DVDs:           {dvds}")
print(f"Volumes:        {vols}")
print(f"Total chunks:   {total}")
print(f"  Granular:     {gran}")
print(f"  Semantic:     {sem}")
print(f"  Tagged:       {tagged}")
print(f"  Unidentified: {unid}")
e.dispose()

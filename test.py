import glob
import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(ROOT_DIR, 'data/*.json')
print(DATA_DIR)
files = []
for filepath in glob.iglob(r'/home/dej/mes-projets/etl-log/data/*.json', recursive=True):
    print('ok')
    files.append(filepath)

print(files)
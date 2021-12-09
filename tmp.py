from PIL import Image, ImageDraw
from PIL import ImagePath
import os
from tqdm import tqdm
import concurrent.futures

def ThreadWorker(x,idx):
    print(idx)
    image = Image.open(path+x)
    image.save(path2+x)
    image.close()
path = '/data/collection/PRJ3507/'
path2 = '/data/collection/PRJ3507_2/'
futures = []
executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
idx=1
for item in tqdm(os.listdir(path)):
    
    futures.append(executor.submit(
                    ThreadWorker, item,idx)
                )
    idx+=1
        
import re
import os
from PIL import Image, ImageDraw,ImageFont, ExifTags, ImagePath

def name_without_extension(full_name):
    p = re.compile('(.*)[.][a-zA-Z]{2,4}')
    try:
        return re.search(p,full_name).group(1)
    except:
        return full_name
def remove_dir(full_dir):
    return os.path.basename(full_dir)
    
def append_val(path,val):
    name = name_without_extension(path)
    ext = os.path.splitext(path)
    return name+'_'+val+ext[1]
def rotate(img,x):
    if x == '3':
        img=img.rotate(180, expand=True)
    elif x == '1':
        img=img.rotate(270, expand=True)
    elif x == '2':
        img=img.rotate(90, expand=True)
    return img

def rotate_img(img):
    try:
        for orientation in ExifTags.TAGS.keys():
            if ExifTags.TAGS[orientation]=='Orientation':
                break
            exif=dict(img._getexif().items())
            print(exif[orientation])
            if exif[orientation] == 3:
                img=img.rotate(180, expand=True)
            elif exif[orientation] == 6:
                img=img.rotate(270, expand=True)
            elif exif[orientation] == 8:
            
                img=img.rotate(90, expand=True)

    except (AttributeError, KeyError, IndexError):
        # cases: image don't have getexif
        pass
    return img

def get_header(key):
    p = re.compile('name_(.*)')
    if(re.search(p,key)):
        return 'result'
    else:
        return key


if __name__ == '__main__':
    print(remove_dir('asdasd/fdsadfasdfsdfsd.csv'))
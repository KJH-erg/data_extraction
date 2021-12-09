def change_space(input):
    '''
    change space in URL that LINUX cannot read
    '''
    input = input.replace(' ', '\ ')
    return input

def change_doubleParanthesis(input):
    '''
    change space in URL that LINUX cannot read
    '''
    input = input.replace('(', '"(')
    input = input.replace(')', ')"')
    return input

def change_linux_URL(input):
    input = change_space(input)
    input = change_doubleParanthesis(input)
    return input

    
if __name__ == '__main__':
    print(change_linux_URL('gs://cw_pm_upload_files/Aihub_1차_53번/특수차량 사람과숲(큐보이드,일반바운딩)/bbox_cuboid_12795_1014.zip'))
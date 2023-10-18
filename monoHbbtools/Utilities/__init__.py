import time

def get_timestamp():
    now = time.localtime()
    formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", now)
    return formatted_time
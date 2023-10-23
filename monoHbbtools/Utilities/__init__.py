import time

def get_timestamp():
    """
    Returns the current time according to the local machine.
    """
    now = time.localtime()
    formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", now)
    return formatted_time

def normalize(Histogram):
    """
    Takes in a 1D hist object and normalizes it.
    """
    integral = Histogram.sum()
    

    pass


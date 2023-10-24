import time
import hist

def get_timestamp():
    """
    Returns the current time according to the local machine.
    """
    now = time.localtime()
    formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", now)
    return formatted_time

def normalize(input_hist):
    """
    Takes in a 1D hist object and normalizes it.
    """
    output_hist_axis , = input_hist.axes
    output_hist = hist.Hist(output_hist_axis)
    integral = input_hist.sum()
    for bin_index in range(input_hist.size - 2) :
        output_hist[bin_index] = input_hist[bin_index] / integral
    return output_hist


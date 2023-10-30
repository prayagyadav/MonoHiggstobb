"""
This file generates a list of luminosity using brilcalc for different runs.
"""

import os , sys , subprocess
from coffea import util

def Generate_Luminosity_file(data_dict):
    runs = data_dict["MET_Run2018"]["RunSet"] #This is a set of runs
    luminosities = {}
    for run in runs :
        output = subprocess.check_output(["brilcalc","lumi","-c","web","-r", str(run)]).decode("ASCII")
        out_list = output.split("\n")
        with open("test.txt","a+") as outfile :    
            outfile.writelines([line+"\n" for line in out_list])
    return luminosities

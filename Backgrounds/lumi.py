"""
This file generates a list of luminosity using brilcalc for different runs.
python 2.7 compatible
"""

import subprocess
import json

def Generate_Luminosity_file(runs):
    runkeys = runs.keys()
    for run in runkeys :
        output = subprocess.check_output(["brilcalc","lumi","-c","web","-r", str(run)]).decode("ASCII")
        out_list = output.split("\n")
        with open("currentRunLumi.txt","a+") as outfile :    
            outfile.writelines([line+"\n" for line in out_list])

with open("../CurrentRunFileset.json") as file :
    runs = json.load(file)
    Generate_Luminosity_file(runs)

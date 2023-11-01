import os, sys, subprocess , json , argparse
from monoHbbtools import Load
import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument(
    "-k",
    "--keymap",
    choices=["MET_Run2018","ZJets_NuNu"],
    help="Enter which dataset to run: example MET_Run2018 , ZJets_Nu_Nu etc.",
    type=str
)
parser.add_argument(
     "-n",
     "--nset",
     help="Number of files to be run at a time",
     type=int
)
parser.add_argument(
    "-m",
    "--max_chunks",
    help="Enter the number of chunks(filechunks) to be processed; by default None i.e.  full dataset",
    type=int
    )
inputs = parser.parse_args()

keymap = inputs.keymap
nset = inputs.nset
maxchunks = inputs.maxchunks

with open("../monoHbbtools/Load/newfileset.json") as f:
        fileset_dict = json.load(f)

MCmaps = ["ZJets_NuNu"]

if keymap == "MET_Run2018" :
    runnerfileset = Load.buildFileset(fileset_dict["Data"][keymap],"fnal")
elif keymap in MCmaps :
    runnerfileset = Load.buildFileset(fileset_dict["MC"][keymap],"fnal")
flat_list={}
flat_list[keymap] = []

for key in runnerfileset.keys() :
    flat_list[keymap] += runnerfileset[key]
fullfileset = {keymap : flat_list[keymap]}

Total_files = len(fullfileset[keymap])
full_list = fullfileset[keymap]
begin = 0
end = nset

print("Current working directory : ", os.getcwd())
files = os.listdir()
for filename in files :
    if filename.startswith(f"Zjetsnunu_{keymap}_from") :
        print(filename, " already exists")
        pass

#generate chunks
nchunks = 20
split_list = [full_list[i:i+nchunks] for i in range(0, len(full_list), nchunks)]

fileindex = 1

for chunk in split_list[:maxchunks] :
    command = "python runner_Zjetsnunu.py -k "+keymap+" -e futures -c 1000000 -w 12 --begin "+str(fileindex)+" --end "+str(fileindex + len(chunk))
    subprocess.run(command, shell=True ,executable="/bin/bash")
    subprocess.run("sleep 1m",shell=True ,executable="/bin/bash")
    fileindex += len(chunk)+1
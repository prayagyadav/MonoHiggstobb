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
parser.add_argument(
    "--skipchunks",
    help="skip the already ran chunks; 1 for yes",
    type=int
        )
parser.add_argument(
    "--wait",
    help="wait time in seconds after each run",
    type = int
        )
inputs = parser.parse_args()

keymap = inputs.keymap
nset = inputs.nset - 1
maxchunks = inputs.max_chunks

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

skiplist=[]
if inputs.skipchunks == 1:
    print("Current working directory : ", os.getcwd())
    files = os.listdir()
    for filename in files :
        if filename.startswith(f"Zjetsnunu_{keymap}_from") :
            temp = filename.split("_")
            from_number = temp[4]
            to_number = temp[6].strip(".coffea")
            skiplist.append((from_number,to_number))

#generate chunks
nchunks = nset
split_list = [full_list[i:i+nchunks] for i in range(0, len(full_list), nchunks)]

with open("log_futuresBatch_oldrun.txt","r+") as oldlogfile:
    with open("log_futuresBatch_newrun.txt","w+") as newlogfile:
        run_signature = oldlogfile.readlines()
        fileindex = 1
        for chunk in split_list[:maxchunks] :
            if str(chunk)+"\n" in run_signature:
                print(chunk, "\n already processed.\nDelete the old run logfile if that isn't the case.") 
                continue
            else:
                flag = True
                for index_tuple in skiplist :
                    b , e = index_tuple
                    if (fileindex == int(b)) & (fileindex+len(chunk) == int(e)) :
                        print("Skipped file: ", f"Zjetsnunu_{keymap}_from_{b}_to_{e}.coffea" )
                        flag = False
                if flag :
                    print("processing-->",chunk)
                    command = "python runner_Zjetsnunu.py -k "+keymap+" -e futures -c 1000000 -w 12 --begin "+str(fileindex)+" --end "+str(fileindex + len(chunk))
                    subprocess.run(command, shell=True ,executable="/bin/bash")
                    newlogfile.write(str(chunk)+"\n")
                    print("waiting...")
                    subprocess.run(f"sleep {inputs.wait}",shell=True ,executable="/bin/bash")
                fileindex += len(chunk)+1

print("Execution completed")

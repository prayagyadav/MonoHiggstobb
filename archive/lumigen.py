from coffea import util
import json
data_dict = util.load("Backgrounds/Zjetsnunu_MET_Run2018.coffea")

runset = list(data_dict["MET_Run2018"]["RunSet"])

outdict={}
for run in runset :
    outdict[run] = 0 

with open("CurrentRunFileset.json","w") as outfile :
    outfile.write(json.dumps(outdict, indent= 4 ))
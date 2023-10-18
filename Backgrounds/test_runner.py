import argparse
from coffea.nanoevents import NanoAODSchema #,NanoEventsFactory 
from coffea import processor
from coffea import util
from monoHbbtools import Load
from monoHbbtools.Utilities import condor
from test_runner import Zjetsnunu
import json
keylist = ["MET","Zjetsnunu"]
fileset = Load.Loadfileset("../monoHbbtools/Load/newfileset.json")
fileset_dict = fileset.getraw()
runnerfileset = processor.accumulate([
    Load.buildFileset(fileset_dict["Data"]["MET"],"fnal") ,
    Load.buildFileset(fileset_dict["MC"]["Zjetsnunu"],"fnal")
    ])
try :
    runnerfileset = {"MET": fileset_dict["Data"]["MET"]["MET_Run2018A"][0:]}
except :
    print("Numbers of files requested is greater than the numbers of files in first dictionary of the fileset.")
    raise ValueError
futures_run = processor.Runner(
    executor = processor.FuturesExecutor(workers=4),
    schema=NanoAODSchema,
    chunksize= 100000,
    maxchunks= 2,
    xrootdtimeout=120
)
Output = futures_run(
    runnerfileset,
    "Events",
    processor_instance=Zjetsnunu(keylist=keylist)
)

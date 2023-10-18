import argparse
from coffea.nanoevents import NanoAODSchema #,NanoEventsFactory 
from coffea import processor
from coffea import util
from monoHbbtools import Load
from monoHbbtools.Utilities import condor
from test_processor import Zjetsnunu
import json

parser = argparse.ArgumentParser()
parser.add_argument(
    "-e",
    "--executor",
    choices=["futures","condor", "dask"],
    help="Enter where to run the file : futures(local) or dask(local) or condor",
    default="futures",
    type=str
)
parser.add_argument(
    "-c",
    "--chunk_size",
    help="Enter the chunksize; by default 100k",
    type=int ,
    default=100000
    )
parser.add_argument(
    "-m",
    "--max_chunks",
    help="Enter the number of chunks to be processed; by default None ie full dataset",
    type=int
    )
parser.add_argument(
    "-w",
    "--workers",
    help="Enter the number of workers to be employed for processing in local; by default 4",
    type=int ,
    default=4 
    )
parser.add_argument(
    "-f",
    "--files",
    help="Enter the number of files to be processed",
    type=int 
    )
inputs = parser.parse_args()

keylist = ["MET","Zjetsnunu"]
fileset = Load.Loadfileset("../monoHbbtools/Load/newfileset.json")
fileset_dict = fileset.getraw()
runnerfileset = processor.accumulate([
    Load.buildFileset(fileset_dict["Data"]["MET"],"fnal") ,
    Load.buildFileset(fileset_dict["MC"]["Zjetsnunu"],"fnal")
    ])
try :
    #runnerfileset = {"MET": fileset_dict["Data"]["MET"]["MET_Run2018A"][0:inputs.files]}
    runnerfileset = {"Zjetsnunu": fileset_dict["MC"]["Zjetsnunu"]["Z1Jets_NuNu_ZpT_50To150_18"][0:inputs.files]}
except :
    print("Numbers of files requested is greater than the numbers of files in first dictionary of the fileset.")
    raise ValueError
futures_run = processor.Runner(
    executor = processor.FuturesExecutor(workers=inputs.workers),
    schema=NanoAODSchema,
    chunksize= inputs.chunk_size,
    maxchunks= inputs.max_chunks,
    xrootdtimeout=120
)
Output = futures_run(
    runnerfileset,
    "Events",
    processor_instance=Zjetsnunu(keylist=keylist)
)

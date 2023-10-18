""" 
~~~~~~~~~~~~~~~~~~~~~~RUNNER SCRIPT~~~~~~~~~~~~~~~~~~~~~~~~
This script studies the Z--> \nu + \nu + jets background .

/Author/: Prayag Yadav
/Created/: 11 Oct 2023
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

#################################
# Import the necessary packages #
#################################

import argparse
from coffea.nanoevents import NanoAODSchema #,NanoEventsFactory 
from coffea import processor
from coffea import util
from monoHbbtools import Load
from monoHbbtools.Utilities import condor
from processor_Zjetsnunu import Zjetsnunu
import json

##############################
# Define the terminal inputs #
##############################

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

#################################
# Run the processor #
#################################

keylist = ["MET","Zjetsnunu"]

#For futures execution
if inputs.executor == "futures" :

    fileset = Load.Loadfileset("../monoHbbtools/Load/newfileset.json")
    fileset_dict = fileset.getraw()
    runnerfileset = processor.accumulate([
        Load.buildFileset(fileset_dict["Data"]["MET"],"fnal") ,
        Load.buildFileset(fileset_dict["MC"]["Zjetsnunu"],"fnal")
        ])
    try :
        runnerfileset = {
            "MET": fileset_dict["Data"]["MET"]["MET_Run2018A"][:inputs.files] ,
            "Z1Jets_Nu_Nu": fileset_dict["MC"]["Zjetsnunu"]["Z1Jets_NuNu_ZpT_50To150_18"][:inputs.files]
            }
    except :
        print("Numbers of files requested is greater than the numbers of files in first dictionary of the fileset.")
        raise ValueError
    futures_run = processor.Runner(
        executor = processor.FuturesExecutor(workers=inputs.workers),
        schema=NanoAODSchema,
        chunksize= inputs.chunk_size ,
        maxchunks= inputs.max_chunks,
        xrootdtimeout=120
    )
    Output = futures_run(
        runnerfileset,
        "Events",
        processor_instance=Zjetsnunu(keylist=keylist)
    )

#For dask execution
elif inputs.executor == "dask" :
    print("WARNING: This feature is still in development!\nAttemping to run nevertheless ...")
    from dask.distributed import Client , LocalCluster
    cluster = LocalCluster()
    client = Client(cluster)
    cluster.scale(inputs.workers)
    client.upload_file("../monoHbbtools/Load/newfileset.json")
    with open("newfileset.json") as f: #load the fileset
        files = json.load(f)
    files = {"MET": files["Data"]["MET"]["MET_Run2018A"][:inputs.files]}
    dask_run = processor.Runner(
        executor = processor.DaskExecutor(client=client),
        schema=NanoAODSchema,
        chunksize= inputs.chunk_size ,
        maxchunks= inputs.max_chunks,
        xrootdtimeout=120
    )
    Output = dask_run(
        files,
        "Events",
        processor_instance=Zjetsnunu(keylist=keylist)
    )

#For condor execution
elif inputs.executor == "condor" :
    print("Preparing to run at condor...\n")
    executor , client = condor.runCondor()
    client.upload_file("../monoHbbtools/Load/newfileset.json")
    with open("../monoHbbtools/Load/newfileset.json") as f:
        files = json.load(f)
    files = {"MET": files["Data"]["MET"]["MET_Run2018A"][:inputs.files]}

    runner = processor.Runner(
        executor=executor,
        schema=NanoAODSchema,
        chunksize=inputs.chunk_size,
        maxchunks=inputs.max_chunks,
        xrootdtimeout=300,
    )
    print("Running...\n")
    Output = runner(
        files,
        treename="Events",
        processor_instance=Zjetsnunu(keylist=keylist),
    )

#################################
# Create the output file #
#################################
output_file = f"Zjetsnunu.coffea"
print("Saving the output to : " , output_file)
util.save(output= Output, filename=output_file)
print(f"File {output_file} saved.")

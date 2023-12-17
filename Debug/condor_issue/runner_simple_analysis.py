
#################################
# Import the necessary packages #
#################################

import argparse
from coffea import util
from coffea.nanoevents import NanoAODSchema #,NanoEventsFactory 
from coffea import processor
import condor
import json
import logging
import matplotlib.pyplot as plt
import mplhep as hep
plt.style.use(hep.style.CMS)
from processor_simple_analysis import JetKinem


##############################
# Define the terminal inputs #
##############################

parser = argparse.ArgumentParser()
parser.add_argument(
    "Mode",
    help="Enter MC to run Monte Carlo Samples or enter Data to run Data samples"
    )
parser.add_argument(
    "-e",
    "--executor",
    choices=["local","condor"],
    help="Enter where to run the file : local or condor",
    default="local",
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
    type=int
    )
inputs = parser.parse_args()

#################################
# Run the processor #
#################################

#Create a console log in case of a warning 
logging.basicConfig(
        format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
        level=logging.WARNING,
    )

Mode = inputs.Mode

#For local execution
if inputs.executor == "local" :
    with open("fileset.json") as f: #load the fileset
        files = json.load(f)
    futures_run = processor.Runner(
        executor = processor.FuturesExecutor(workers=inputs.workers),
        schema=NanoAODSchema,
        chunksize= inputs.chunk_size ,
        maxchunks= inputs.max_chunks,
    )
    Output = futures_run(
        files[Mode],
        "Events",
        processor_instance=JetKinem()
    )

#For condor execution
elif inputs.executor == "condor" :
    print("Preparing to run at condor...\n")
    executor , client = condor.runCondor()
    client.upload_file("fileset.json")
    with open("fileset.json") as f:
        files = json.load(f)

    runner = processor.Runner(
        executor=executor,
        schema=NanoAODSchema,
        chunksize=inputs.chunk_size,
        maxchunks=inputs.max_chunks,
        xrootdtimeout=300,
    )
    print("Running...\n")
    Output = runner(
        files[Mode],
        treename="Events",
        processor_instance=JetKinem(),
    )

#################################
# Create the output file #
#################################

output_file = f"Debug.coffea"
print("Saving the output to : " , output_file)
util.save(output= Output, filename=output_file)
print(f"File {output_file} saved.")
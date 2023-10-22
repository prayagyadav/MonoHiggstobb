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
import logging
from monoHbbtools import Load
from monoHbbtools.Utilities import condor
from processor_Zjetsnunu import SignalSignature
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
    "-k",
    "--keymap",
    choices=["MET","Z1Jets_NuNu"],
    help="Enter which dataset to run: example MET , Z1Jets_Nu_Nu etc.",
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

def getDataset(keymap, files=None):
    fileset = Load.Loadfileset("../monoHbbtools/Load/newfileset.json")
    fileset_dict = fileset.getraw()
    MCmaps = ["Z1Jets_NuNu"]

    if keymap == "MET" :
        runnerfileset = Load.buildFileset(fileset_dict["Data"][keymap],"fnal")
    elif keymap in MCmaps :
        runnerfileset = Load.buildFileset(fileset_dict["MC"][keymap],"fnal")
    flat_list={}
    flat_list[keymap] = []

    if files == None :
        print("No max files provided.\nFalling back to full dataset...")
        outputfileset = runnerfileset
    else :
        match keymap :
            case "MET": #Simply chain up all the files
                for key in runnerfileset.keys() :
                    flat_list[keymap] += runnerfileset[key]
                outputfileset = {keymap : flat_list[keymap][:files]}
            case "Z1Jets_NuNu": # Divide the share of files from all the 8 categories of Z1Jets_NuNu
                file_number = 0
                while file_number < files :
                    for key in runnerfileset.keys():
                        if file_number >= files :
                            break
                        flat_list[keymap] += [runnerfileset[key][0]]
                        runnerfileset[key] = runnerfileset[key][1:]
                        file_number += 1
                outputfileset = {keymap : flat_list[keymap]}
        print("Running ", len(outputfileset[keymap]), " files...")
    
    return outputfileset


#For futures execution
if inputs.executor == "futures" :
    files = getDataset(inputs.keymap, inputs.files)
    futures_run = processor.Runner(
        executor = processor.FuturesExecutor(workers=inputs.workers),
        schema=NanoAODSchema,
        chunksize= inputs.chunk_size ,
        maxchunks= inputs.max_chunks,
        xrootdtimeout=120
    )
    Output = futures_run(
        files,
        "Events",
        processor_instance=SignalSignature()
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
        processor_instance=SignalSignature()
    )

#For condor execution
elif inputs.executor == "condor" :
    #Create a console log in case of a warning 
    logging.basicConfig(
        format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
        level=logging.WARNING,
    )
    print("Preparing to run at condor...\n")
    executor , client = condor.runCondor()
    client.upload_file("../monoHbbtools/Load/newfileset.json")
    # with open("../monoHbbtools/Load/newfileset.json") as f:
    #     files = json.load(f)
    # files = {"MET": files["Data"]["MET"]["MET_Run2018A"][:inputs.files]}
    files = getDataset(inputs.keymap, inputs.files)

    runner = processor.Runner(
        executor=executor,
        schema=NanoAODSchema,
        chunksize=inputs.chunk_size,
        maxchunks=inputs.max_chunks,
        xrootdtimeout=300,
    )
    print("Starting the workers...\n")
    Output = runner(
        files,
        treename="Events",
        processor_instance=SignalSignature()
    )

#################################
# Create the output file #
#################################
output_file = f"Zjetsnunu_{inputs.keymap}.coffea"
print("Saving the output to : " , output_file)
util.save(output= Output, filename=output_file)
print(f"File {output_file} saved.")

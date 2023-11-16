""" 
~~~~~~~~~~~~~~~~~~~~~~RUNNER SCRIPT~~~~~~~~~~~~~~~~~~~~~~~~
This script studies the background .

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
import numpy as np
from processor_SR_Resolved_Backgrounds import SignalSignature
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
    choices=[
        "MET_Run2018",
        "ZJets_NuNu",
        "TTToSemiLeptonic",
        "TTTo2L2Nu",
        "WJets_LNu",
        "DYJets_LL",
        "VV",
        "QCD",
        "ST"
        ],
    help="Enter which dataset to run: example MET_Run2018 , ZJets_Nu_Nu etc.",
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
parser.add_argument(
    "--begin",
    help="Begin Sequential execution from file number (inclusive)",
    type=int
)
parser.add_argument(
    "--end",
    help="End Sequential execution from file number 'int'",
    type=int
)
parser.add_argument(
    "--short",
    help="Use the short fileset",
    type=int
)
inputs = parser.parse_args()

#################################
# Run the processor #
#################################

def getDataset(keymap,load=True, dict = None, files=None, begin=0, end=0, mode = "sequential"):
    #Warning : Never use 'files' with 'begin' and 'end'
    if load :
        if inputs.short == 1 :
            fileset = Load.Loadfileset("../monoHbbtools/Load/shortfileset.json")
        else :
            fileset = Load.Loadfileset("../monoHbbtools/Load/newfileset.json")
        fileset_dict = fileset.getraw()
    else :
        fileset_dict = dict
    MCmaps = [
        "MET_Run2018",
        "ZJets_NuNu",
        "TTToSemiLeptonic",
        "TTTo2L2Nu",
        "WJets_LNu",
        "DYJets_LL",
        "VV",
        "QCD",
        "ST"
        ]

    
    runnerfileset = Load.buildFileset(fileset_dict[keymap],"fnal")
    flat_list={}
    flat_list[keymap] = []

    if mode == "sequential":
        if end - begin <= 0:
            print("Invalid begin and end values.\nFalling back to full dataset...")
            outputfileset = runnerfileset
        else:
            # for key in runnerfileset.keys() :
            #     flat_list[keymap] += runnerfileset[key]
            #indexer
            index={}
            i = 1
            for key in runnerfileset.keys() :
                index[key] = []
                for file in runnerfileset[key] :
                    index[key].append(i)
                    i += 1

            accept = np.arange(begin,end+1,1)
            print(accept)
            temp = {}
            for key in runnerfileset.keys() :
                temp[key] = []
                for i in range(len(runnerfileset[key])) :
                    if index[key][i] in accept :
                        temp[key].append(runnerfileset[key][i])
            #outputfileset = {keymap : flat_list[keymap][(begin - 1) :end]}
            #outputfileset = {keymap : temp}
            outputfileset = temp
    elif mode == "divide" :
        if files == None:
            print("Invalid number of files.\nFalling back to full dataset...")
            outputfileset = runnerfileset
        else:
            # Divide the share of files from all the 8 categories of ZJets_NuNu
            file_number = 0
            while file_number < files :
                for key in runnerfileset.keys():
                    if file_number >= files :
                        break
                    flat_list[keymap] += [runnerfileset[key][0]]
                    runnerfileset[key] = runnerfileset[key][1:]
                    file_number += 1
            outputfileset = {keymap : flat_list[keymap]}
    else:
        print("Invalid mode of operation", mode)
        raise KeyError
    
    print("Running ", np.array([len(value) for value in outputfileset.values()]).sum(), " files...")
    return outputfileset


#For futures execution
if inputs.executor == "futures" :
    files = getDataset(keymap=inputs.keymap,load=True, mode="sequential", begin=inputs.begin, end=inputs.end)
    #files = getDataset(keymap=inputs.keymap, mode="divide", files=inputs.files)
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
    if input.short == 1 :
        client.upload_file("../monoHbbtools/Load/shortfileset.json")
        with open("shortfileset.json") as f: #load the fileset
            files = json.load(f)
    else:
        client.upload_file("../monoHbbtools/Load/newfileset.json")
        with open("newfileset.json") as f: #load the fileset
            files = json.load(f)
    files = {"MET": files["Data"]["MET_Run2018"]["MET_Run2018A"][:inputs.files]}
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
    if inputs.short == 1 :
        client.upload_file("../monoHbbtools/Load/shortfileset.json")
        with open("../monoHbbtools/Load/shortfileset.json") as f: #load the fileset
            filedict = json.load(f)
    else:
        client.upload_file("../monoHbbtools/Load/newfileset.json")
        with open("../monoHbbtools/Load/newfileset.json") as f: #load the fileset
            filedict = json.load(f)

    files = getDataset(
        keymap=inputs.keymap,
        load=False ,
        dict=filedict,
        mode="sequential",
        begin=inputs.begin,
        end=inputs.end
        )
    #files = getDataset(keymap=inputs.keymap, mode="divide", files=inputs.files)

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

try :
    output_file = f"SR_Resolved_Backgrounds_{inputs.keymap}_from_{inputs.begin}_to_{inputs.end}.coffea"
    pass
except :
    output_file = f"SR_Resolved_Backgrounds_{inputs.keymap}.coffea"
print("Saving the output to : " , output_file)
util.save(output= Output, filename=output_file)
print(f"File {output_file} saved.")

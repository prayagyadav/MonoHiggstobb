if __name__=="__main__":
    from coffea import processor
    import argparse
    from coffea import util
    from coffea.nanoevents import NanoAODSchema , NanoEventsFactory
    from coffea.lumi_tools import LumiMask
    import awkward as ak
    import condor
    import numba
    from Debug.control.Snip_debug import *
    import json
    import rich
    import numpy as np
    import os
    import shutil
    import logging
    from processor_Top_mu_debug import Top_mu
    
    class Loadfileset():
        def __init__(self, jsonfilename) :
            with open(jsonfilename) as f :
                self.handler = json.load(f)
    
        
        def Show(self , verbosity=1):
            if verbosity==1 :
                for key, value in self.handler.items() :
                        rich.print(key+" : ", list(value.keys()))
            elif verbosity==2 :
                for key, value in self.handler.items() :
                        rich.print(key+" : ", list(value.keys()), "\n")
                        for subkey , subvalue in value.items() :
                            rich.print("\t"+subkey+" : ")
                            for file in subvalue :
                                rich.print("\t", file)
            elif verbosity==3 :
                for key, value in self.handler.items() :
                        rich.print(key+" : ", list(value.keys()), "\n")
                        for subkey , subvalue in value.items() :
                            rich.print("\t"+subkey+" : ")
                            for subsubkey, subsubvalue in subvalue.items() :
                                try :
                                    for file in subvalue :
                                        rich.print("\t", file)
                                except:
                                    rich.print("\t"+subsubkey+" : ")
        
        @numba.jit(forceobj=True)
        def getFileset(self, mode ,superkey, key, redirector ) :
            if redirector=="fnal":
                redirector_string = "root://cmsxrootd.fnal.gov//"
            elif redirector=="infn":
                redirector_string = "root://xrootd-cms.infn.it//"
            elif redirector=="wisc":
                redirector_string = "root://pubxrootd.hep.wisc.edu//"
            elif redirector=="unl":
                redirector_string = "root://xrootd-local.unl.edu:1094//"
            elif redirector=="kisti":
                redirector_string = "root://cms-xrdr.sdfarm.kr:1094//xrd//"
            raw_fileset = self.handler[mode][superkey][key] 
            requested_fileset = {superkey : [redirector_string+filename for filename in raw_fileset]}
            return requested_fileset
        
        def getraw(self):
            #load the raw dictionary
            full_fileset = self.handler
            return full_fileset
        
    def buildFileset(dict , redirector):
        '''
        To return a run-able dict with the appropriate redirector.
        Please input a dictionary which is only singly-nested
        '''
        redirectors = {
            "fnal": "root://cmsxrootd.fnal.gov//",
            "infn": "root://xrootd-cms.infn.it//",
            "wisc": "root://pubxrootd.hep.wisc.edu//",
            "unl":  "root://xrootd-local.unl.edu:1094/",
            "kisti": "root://cms-xrdr.sdfarm.kr:1094//xrd/",
            "hdfs": "/hdfs",
            "commonfs": "/commonfs"
    
        }
    
        if (redirector=="fnal") | (redirector==1) :
            redirector_string = redirectors["fnal"]
        elif (redirector=="infn") | (redirector==2) :
            redirector_string = redirectors["infn"]
        elif (redirector=="wisc") | (redirector==3):
            redirector_string = redirectors["wisc"]
        elif (redirector=="unl") | (redirector==4):
            redirector_string = redirectors["unl"]
        elif (redirector=="kisti") | (redirector==5):
            redirector_string = redirectors["kisti"]
        elif (redirector=="hdfs") | (redirector==6):
            redirector_string = redirectors["hdfs"]
        elif (redirector=="commonfs") | (redirector==7):
            redirector_string = redirectors["commonfs"]
    
        temp = dict 
        output = {}
        for key in temp.keys() :
            try :
                g = temp[key]
                if isinstance(g,list):
                    templist = []
                    for filename in g :
                        filename = filename[filename.find("/store/") :]
                        templist.append(redirector_string+filename)
                    output[key] = templist
            except :
                raise KeyError
        return output
    
    def getDataset(keymap, load=True, dict = None, files=None, begin=0, end=0, mode = "sequential"):
        #Warning : Never use 'files' with 'begin' and 'end'
        fileset = Loadfileset("newfileset.json")
        fileset_dict = fileset.getraw()
        MCmaps = [
            "MET_Run2018",
            "ZJets_NuNu",
            "TTToSemiLeptonic",
            "TTTo2L2Nu",
    	"TTToHadronic",
            "WJets_LNu",
            "DYJets_LL",
            "VV",
            "QCD",
            "ST"
            ]
    
        
        runnerfileset = buildFileset(fileset_dict[keymap],"fnal")
        flat_list={}
        flat_list[keymap] = []
    
        if mode == "sequential":
            if end - begin < 0:
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
    
    print("Stage 1")
    
    def get_lumiobject():
        path = "Cert_314472-325175_13TeV_Legacy2018_Collisions18_JSON.txt" 
        return LumiMask(path)
    
    lumimaskobject = get_lumiobject()
    
    def lumi(events,cutflow,path="",lumiobject=None):
        #Selecting use-able events
        if lumiobject==None :
            #path_of_file = path+"Cert_314472-325175_13TeV_Legacy2018_Collisions18_JSON.json"
            path_of_file = path+"golden.json"
            lumimask = LumiMask(path_of_file)
        else :
            lumimask = lumiobject
        events = events[lumimask(events.run, events.luminosityBlock)]
        cutflow["lumimask"] = len(events)
        return events , cutflow
    
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
    	"TTToHadronic",
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
    parser.add_argument(
        "-cat",
        help="category : resolved or boosted",
        choices=["resolved","boosted"],
        type=str
    )
    inputs = parser.parse_args()
    

    print("Stage 2")

    def zip_files(list_of_files):
        os.makedirs("temp_folder")
        for file in list_of_files :
            shutil.copy(file,"temp_folder")
        archive_name = "helper_files"
        shutil.make_archive(archive_name,"zip","temp_folder")
        shutil.rmtree("temp_folder")
        return archive_name+".zip"


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
            processor_instance=Top_mu(category=inputs.cat,helper_objects=[lumimaskobject])
        )
    
    #For dask execution
    elif inputs.executor == "dask" :
        print("WARNING: This feature is still in development!\nAttemping to run nevertheless ...")
        from dask.distributed import Client , LocalCluster
        cluster = LocalCluster()
        client = Client(cluster)
        cluster.scale(inputs.workers)
        #client.upload_file("../monoHbbtools/Load/newfileset.json")

        client.upload_file(
                    zip_files(
                        [
                            "Cert_314472-325175_13TeV_Legacy2018_Collisions18_JSON.txt",
                            "Snip_debug.py",
                            "processor_Top_mu_debug.py"
                            ]
                        )
                    )
        #client.upload_file("Snip.py")
        with open("newfileset.json") as f: #load the fileset
            filedict = json.load(f)
        #files = {"MET": files["Data"]["MET_Run2018"]["MET_Run2018A"][:inputs.files]}
        files = getDataset(
            keymap=inputs.keymap,
            load=False ,
            dict=filedict,
            mode="sequential",
            begin=inputs.begin,
            end=inputs.end
            )
        print(files)
        #files 
        dask_run = processor.Runner(
            executor = processor.DaskExecutor(client=client),
            schema=NanoAODSchema,
            chunksize= inputs.chunk_size ,
            maxchunks= inputs.max_chunks
        )
        Output = dask_run(
            files,
            "Events",
            processor_instance=Top_mu(category=inputs.cat,helper_objects=[lumimaskobject])
        )
    
    #For condor execution
    elif inputs.executor == "condor" :
        #Create a console log for easy debugging 
        logging.basicConfig(
            format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
            level=logging.WARNING,
        )
        print("Preparing to run at condor...\n")
        executor , client = condor.runCondor(workers=inputs.workers)
        print("Executor and Client Obtained")
        if inputs.short == 1 :
            # import shutil
            # shutil.make_archive("monoHbbtools", "zip", base_dir="monoHbbtools")
            # client.upload_file("monoHbbtools.zip")
    
            client.upload_file(
                    zip_files(
                        [
                            "Cert_314472-325175_13TeV_Legacy2018_Collisions18_JSON.txt",
                            "Snip_debug.py",
                            "processor_Top_mu_debug.py"
                            ]
                        )
                    )
            with open("shortfileset.json") as f: #load the fileset
                filedict = json.load(f)
        else:
            # import shutil
            # shutil.make_archive("monoHbbtools", "zip", base_dir="monoHbbtools")
            # client.upload_file("monoHbbtools.zip")
        
            #client.wait_for_workers(1)
            #client.upload_file("Cert_314472-325175_13TeV_Legacy2018_Collisions18_JSON.txt")
            #client.upload_file("Snip.py")
            client.upload_file(
                    zip_files(
                        [
                            "Cert_314472-325175_13TeV_Legacy2018_Collisions18_JSON.txt",
                            "Snip_debug.py",
                            "processor_Top_mu_debug.py"
                            ]
                        )
                    )
            with open("newfileset.json") as f: #load the fileset
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
            processor_instance=Top_mu(category=inputs.cat,helper_objects=[lumimaskobject])
        )

    #################################
    # Create the output file #
    #################################
    print("stage 3")
    try :
        output_file = f"CR_{inputs.cat}_Top_{inputs.keymap}_from_{inputs.begin}_to_{inputs.end}.coffea"
        pass
    except :
        output_file = f"CR_{inputs.cat}_Top_{inputs.keymap}.coffea"
    print("Saving the output to : " , output_file)
    util.save(output= Output, filename="coffea_files/"+output_file)
    print(f"File {output_file} saved.")
    print("Stage 4")

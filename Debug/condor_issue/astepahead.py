from coffea import processor
import argparse
from coffea import util
from coffea.nanoevents import NanoAODSchema , NanoEventsFactory
import awkward as ak
import condor 
import numba
import hist
from Snip import *
import json
import rich
import numpy as np
import os

def move_X509():
    try:
        _x509_localpath = (
            [
                line
                for line in os.popen("voms-proxy-info").read().split("\n")
                if line.startswith("path")
            ][0]
            .split(":")[-1]
            .strip()
        )
    except Exception as err:
        raise RuntimeError(
            "x509 proxy could not be parsed, try creating it with 'voms-proxy-init'"
        ) from err
    _x509_path = f'/scratch/{os.environ["USER"]}/{_x509_localpath.split("/")[-1]}'
    os.system(f"cp {_x509_localpath} {_x509_path}")
    return os.path.basename(_x509_localpath)

def runCondor(cores=1, memory="2 GB", disk="1 GB", death_timeout = '60', workers=4):
    from distributed import Client
    from dask_jobqueue import HTCondorCluster

    os.environ["CONDOR_CONFIG"] = "/etc/condor/condor_config"
    _x509_path = move_X509()

    cluster = HTCondorCluster(
        cores=1,
        memory="2 GB",
        disk="1 GB",
        job_extra_directives={
            "+JobFlavour": '"longlunch"',
            "log": "dask_job_output.$(PROCESS).$(CLUSTER).log",
            "output": "dask_job_output.$(PROCESS).$(CLUSTER).out",
            "error": "dask_job_output.$(PROCESS).$(CLUSTER).err",
            "should_transfer_files": "yes",
            "when_to_transfer_output": "ON_EXIT_OR_EVICT",
            "+SingularityImage": '"/cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-dask-cc7:latest"',
            "Requirements": "HasSingularityJobStart",
            "request_GPUs" : "1",
            "InitialDir": f'/scratch/{os.environ["USER"]}',
            "transfer_input_files": f'{_x509_path},{os.environ["EXTERNAL_BIND"]}'
        },
        job_script_prologue=[
            "export XRD_RUNFORKHANDLER=1",
            f"export X509_USER_PROXY={_x509_path}",
        ]
    )
    cluster.adapt(minimum=1, maximum=workers)
    executor = processor.DaskExecutor(client=Client(cluster))
    return executor, Client(cluster)


class Loadfileset():
    def __init__(self, jsonfilename) :
        with open(jsonfilename) as f :
            self.handler = json.load(f)

    
    def Show(self , verbosity=1):
        match verbosity :
            case 1 :
                for key, value in self.handler.items() :
                    rich.print(key+" : ", list(value.keys()))
            case 2 :
                for key, value in self.handler.items() :
                    rich.print(key+" : ", list(value.keys()), "\n")
                    for subkey , subvalue in value.items() :
                        rich.print("\t"+subkey+" : ")
                        for file in subvalue :
                            rich.print("\t", file)
            case 3 :
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
        # Construct with desired redirector
        match redirector :
            case "fnal" | 1 :
                redirector_string = "root://cmsxrootd.fnal.gov//"
            case "infn" | 2 :
                redirector_string = "root://xrootd-cms.infn.it//"
            case "wisc" | 3 :
                redirector_string = "root://pubxrootd.hep.wisc.edu//"

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
        "wisc": "root://pubxrootd.hep.wisc.edu//"

    }
    match redirector :
        case "fnal" | 1 :
            redirector_string = redirectors["fnal"]
        case "infn" | 2 :
            redirector_string = redirectors["infn"]
        case "wisc" | 3 :
            redirector_string = redirectors["wisc"]

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

def getDataset(keymap, dict = None, files=None, begin=0, end=0, mode = "sequential"):
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

print("Stage 1")

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
inputs = parser.parse_args()


class barebones(processor.ProcessorABC):
    def __init__(self):
        pass
    def process(self, events):
        njets = ak.num(events.Jet.pt , axis=0)
        out = {"nJets": njets }
        return out
    def postprocess(self, accumulator):
        pass

print("Stage 2")

filename = "root://cmsxrootd.fnal.gov///store/data/Run2018A/MET/NANOAOD/UL2018_MiniAODv2_NanoAODv9-v2/110000/0F8C0C8C-63E4-1D4E-A8DF-506BDB55BD43.root"
#exec = "futures"
exec = "condor"
Mode = "MET_Run2018"

print("Stage 3")

#For local execution
if exec == "futures" :
    futures_run = processor.Runner(
        executor = processor.FuturesExecutor(workers=2),
        schema=NanoAODSchema,
        chunksize= 100000,
        maxchunks= 2,
    )

    Files = {"MET_Run2018":{"MET_Run2018A":[filename,]}}

    print("Stage 4")

    Output = futures_run(
        Files[Mode],
        "Events",
        processor_instance=barebones()
    )

#For condor execution
elif exec == "condor" :
    print("Preparing to run at condor...\n")
    executor , client = condor.runCondor()
    runner = processor.Runner(
        executor=executor,
        schema=NanoAODSchema,
        chunksize=100000,
        maxchunks=2,
        xrootdtimeout=300,
    )

    Files = {"MET_Run2018":{"MET_Run2018A":[filename,]}}

    print("Stage 4")

    print("Running...\n")
    Out = runner(
        Files[Mode],
        treename="Events",
        processor_instance=barebones(),
    )

#Begin the processor definition
class SignalSignature(processor.ProcessorABC):
    """
    Flow of Data:

    INPUT EVENTS
        |
        |-------------------------------------------
        |                                           |
        |                                           |
        v                                           v
      if MET_Run2018                        else if MC
        |                                           |
        |                                           |
        |                                           |
        v                                           |
    MET TRIGGER                                     |
        |                                           |
        |                                           |
        |                                           |
        v                                           |
    MET FILTERS                                     |
        |                                           |
        |<------------------------------------------
        |                                           
        v
    MET CUT
        |
        |
        |
        v
    OBJECT SELECTION
        |
        |
        |
        v
    Make MET pt eta phi, DIJET mass etc plots - all of them plots
    """


    def __init__(self):
        # Initialize the cutflow dictionary
        self.cutflow = {}
        self.run_set = set({})

    def process(self, events):
        dataset = events.metadata["dataset"]
        self.mode = dataset
        cutflow = {}
        cutflow["Total events"] = len(events) #Total Number of events

        #Preparing histogram objects
        #MET
        met_pt_min = 0
        met_pt_max = 1000
        met_pt_nbins = 50
        met_pt_hist = (
            hist.
            Hist.
            new.
            Reg(met_pt_nbins,met_pt_min,met_pt_max).
            Double()
        )
        met_phi_min = -3.14
        met_phi_max = 3.14
        met_phi_nbins = 6
        met_phi_hist = (
            hist.
            Hist.
            new.
            Reg(met_phi_nbins,met_phi_min,met_phi_max).
            Double()
        )
        #Leading ak4bjets
        leadingjets_pt_min = 0
        leadingjets_pt_max = 1000
        leadingjets_pt_nbins = 50
        leadingjets_pt_hist = (
            hist.
            Hist.
            new.
            Reg(leadingjets_pt_nbins,leadingjets_pt_min,leadingjets_pt_max).
            Double()
        )
        leadingjets_eta_min = -2.5
        leadingjets_eta_max = 2.5
        leadingjets_eta_nbins = 5
        leadingjets_eta_hist = (
            hist.
            Hist.
            new.
            Reg(leadingjets_eta_nbins,leadingjets_eta_min,leadingjets_eta_max).
            Double()
        )
        leadingjets_phi_min = -3.14
        leadingjets_phi_max = 3.14
        leadingjets_phi_nbins = 6
        leadingjets_phi_hist = (
            hist.
            Hist.
            new.
            Reg(leadingjets_phi_nbins,leadingjets_phi_min,leadingjets_phi_max).
            Double()
        )
        leadingjets_mass_min = 0
        leadingjets_mass_max = 1000
        leadingjets_mass_nbins = 50
        leadingjets_mass_hist = (
            hist.
            Hist.
            new.
            Reg(leadingjets_mass_nbins,leadingjets_mass_min,leadingjets_mass_max).
            Double()
        )
        #Subleading ak4 bjets
        subleadingjets_pt_min = 0
        subleadingjets_pt_max = 1000
        subleadingjets_pt_nbins = 50
        subleadingjets_pt_hist = (
            hist.
            Hist.
            new.
            Reg(subleadingjets_pt_nbins,subleadingjets_pt_min,subleadingjets_pt_max).
            Double()
        )
        subleadingjets_eta_min = -2.5
        subleadingjets_eta_max = 2.5
        subleadingjets_eta_nbins = 5
        subleadingjets_eta_hist = (
            hist.
            Hist.
            new.
            Reg(subleadingjets_eta_nbins,subleadingjets_eta_min,subleadingjets_eta_max).
            Double()
        )
        subleadingjets_phi_min = -3.14
        subleadingjets_phi_max = 3.14
        subleadingjets_phi_nbins = 6
        subleadingjets_phi_hist = (
            hist.
            Hist.
            new.
            Reg(subleadingjets_phi_nbins,subleadingjets_phi_min,subleadingjets_phi_max).
            Double()
        )
        subleadingjets_mass_min = 0
        subleadingjets_mass_max = 1000
        subleadingjets_mass_nbins = 50
        subleadingjets_mass_hist = (
            hist.
            Hist.
            new.
            Reg(subleadingjets_mass_nbins,subleadingjets_mass_min,subleadingjets_mass_max).
            Double()
        )
        #ak4bjet-ak4bjet dijets 
        dijets_pt_min = 0
        dijets_pt_max = 1000
        dijets_pt_nbins = 50
        dijets_pt_hist = (
            hist.
            Hist.
            new.
            Reg(dijets_pt_nbins,dijets_pt_min,dijets_pt_max).
            Double()
            )
        dijets_eta_min = -2.5
        dijets_eta_max = 2.5
        dijets_eta_nbins = 5
        dijets_eta_hist = (
            hist.
            Hist.
            new.
            Reg(dijets_eta_nbins,dijets_eta_min,dijets_eta_max).
            Double()
            )
        dijets_phi_min = -3.14
        dijets_phi_max = 3.14
        dijets_phi_nbins = 6
        dijets_phi_hist = (
            hist.
            Hist.
            new.
            Reg(dijets_phi_nbins,dijets_phi_min,dijets_phi_max).
            Double()
            )
        dijets_mass_min = 100
        dijets_mass_max = 150
        dijets_mass_nbins = 5
        dijets_mass_hist = (
            hist.
            Hist.
            new.
            Reg(dijets_mass_nbins,dijets_mass_min,dijets_mass_max).
            Double()
            )

        if (self.mode).startswith("MET") :

            #choosing certified good events
            #figure out how to implement condor folder transfers and fix this, in the meantime ignore for condor runs
            should_lumi = False
            if should_lumi :
                events, cutflow = lumi(events,cutflow)

            #Saving the event run
            for run in set(events.run):
                self.run_set.add(run)

            #MET_Trigger
            events, cutflow = met_trigger(events,cutflow)

            #MET_Filters
            events, cutflow = met_filter(events,cutflow)
            

        #MET_selection
        events, cutflow = met_selection(events,cutflow)
        
        #vetoes
        events, cutflow = no_electrons(events,cutflow)
        events, cutflow = no_muons(events,cutflow)
        events, cutflow = no_photons(events,cutflow)
        if (self.mode).startswith("MonoHTobb_ZpBaryonic"):
            events, cutflow = no_taus(events,cutflow, version=7)
        else :
            events, cutflow = no_taus(events,cutflow, version=9)

        #Object selections
        #ak4Jets

        #Apply pt and eta cut
        events, cutflow = jet_pt(events,cutflow)
        events, cutflow = jet_eta(events,cutflow)

        #Anti-QCD DeltaPhi selection
        events, cutflow = anti_QCD(events,cutflow)

        #Apply the btag 
        bjets = get_bjets(events)
        
        #At least two bjets
        events, cutflow = at_least_two_bjets(events,cutflow)

        #Create Dijets
        # leading bjet pt
        events, cutflow = leading_jet_pt(events,cutflow)
        #subleading bjet pt
        events, cutflow = subleading_jet_pt(events,cutflow)

        #At most 2 additional jets
        events, cutflow = additional_jets(events, cutflow)

        leading_jets = events.Jet[:,0]
        subleadingjets = events.Jet[:,1]
        dijets = events.Jet[:,0] + events.Jet[:,1] #Leading jet + Subleading jet

        #Dijet mass window
        dijets , cutflow = dijet_mass(dijets,cutflow)

        #Dijet pt
        dijets, cutflow = dijet_pt(dijets,cutflow)

        #Fill the histogram
        #MET
        met_pt_hist.fill(events.MET.pt)
        met_phi_hist.fill(events.MET.phi)
        #Leading jets
        leadingjets_pt_hist.fill(leading_jets.pt)
        leadingjets_eta_hist.fill(leading_jets.eta)
        leadingjets_phi_hist.fill(leading_jets.phi)
        leadingjets_mass_hist.fill(leading_jets.mass)
        #Subleading jets
        subleadingjets_pt_hist.fill(subleadingjets.pt)
        subleadingjets_eta_hist.fill(subleadingjets.eta)
        subleadingjets_phi_hist.fill(subleadingjets.phi)
        subleadingjets_mass_hist.fill(subleadingjets.mass)
        #ak4bjet-ak4bjet dijets
        dijets_pt_hist.fill(dijets.pt)
        dijets_eta_hist.fill(dijets.eta)
        dijets_phi_hist.fill(dijets.phi)
        dijets_mass_hist.fill(dijets.mass)
        

        #Prepare the output
        key_list = [
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
        if self.mode.startswith("MET_Run2018") :
            self.key = key_list[0]
        elif "Jets_NuNu" in self.mode :
            self.key = key_list[1]
        elif self.mode.startswith("TTToSemiLeptonic"):
            self.key = key_list[2]
        elif self.mode.startswith("TTTo2L2Nu"):
            self.key = key_list[3]
        elif self.mode.startswith("TTToHadronic"):
            self.key = key_list[4]
        elif self.mode.startswith("WJets_LNu"):
            self.key = key_list[5]
        elif self.mode.startswith("DYJets_LL"):
            self.key = key_list[6]
        elif ( self.mode.startswith("WW") | self.mode.startswith("WZ") | self.mode.startswith("ZZ") ) :
            self.key = key_list[7]
        elif self.mode.startswith("QCD"):
            self.key = key_list[8]
        elif self.mode.startswith("ST"):
            self.key = key_list[9]
        else :
            print("Unidentified dataset ", self.mode)
            raise KeyError
        

        output = {
            self.key : {
                self.mode : {
                    "Cutflow": cutflow ,
                    "Histograms": {
                        "met_pt_hist" : met_pt_hist ,
                        "met_phi_hist" : met_phi_hist ,
                        "leadingjets_pt_hist" : leadingjets_pt_hist ,
                        "leadingjets_eta_hist" : leadingjets_eta_hist ,
                        "leadingjets_phi_hist" : leadingjets_phi_hist ,
                        "leadingjets_mass_hist" : leadingjets_mass_hist ,
                        "subleadingjets_pt_hist" : subleadingjets_pt_hist ,
                        "subleadingjets_eta_hist" : subleadingjets_eta_hist ,
                        "subleadingjets_phi_hist" : subleadingjets_phi_hist ,
                        "subleadingjets_mass_hist" : subleadingjets_mass_hist ,
                        "dijets_pt" : dijets_pt_hist ,
                        "dijets_eta" : dijets_eta_hist ,
                        "dijets_phi" : dijets_phi_hist ,
                        "dijets_mass" : dijets_mass_hist ,
                        },
                    "RunSet":self.run_set
                    }
                }
            }
        return output
    
    def postprocess(self, accumulator):
        pass


print("stage 5")

file_name= "outfile_astepahead.coffea"
util.save(output=Out , filename= file_name)
print(f"File {file_name} saved.")

print("Stage 6")

""" 
~~~~~~~~~~~~~~~~~~~~~~~PLOTTER SCRIPT~~~~~~~~~~~~~~~~~~~~~~
This script studies the background .

/Author/: Prayag Yadav
/Created/: 11 Oct 2023
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""
#################################
# Import the necessary packages #
#################################

import argparse
import awkward as ak 
from coffea import util, processor
import hist
import json
import matplotlib.pyplot as plt
import mplhep as hep
from monoHbbtools.Load import crossSections
from monoHbbtools.Utilities import get_timestamp , normalize
import numpy as np
import os
#plt.style.use(hep.style.CMS)
hep.style.use(["CMS","fira","firamath"])
#hep.style.use("CMSTex")
import rich

parser = argparse.ArgumentParser()

parser.add_argument(
    "-f",
    "--fulldataset",
    help="1 if running on full dataset",
    type = int ,
    default= 0
)
inputs = parser.parse_args()

#Load the output
def showinfo(Output):
    for key in Output.keys():
        rich.print(key, " :")
        for subkey in Output[key] :
            rich.print(subkey, " :")
            cutflow = Output[key][subkey]["Cutflow"]
            rich.print(cutflow)

def get_plotting_essentials(Output, histogram_key, Sorted=True, Reverse=True) :
    color_list = ["#4E3636","#116D6E","#321E1E"]
    hist_tuple = [(key, Output[key]["Histograms"][histogram_key] ) for key in Output.keys()]
    if Sorted :
        output_hist_tuple = sorted(hist_tuple , key = lambda x : x[1].sum() , reverse=Reverse)
    else :
        output_hist_tuple = hist_tuple
    output_hists = [i[1] for i in output_hist_tuple]
    output_labels = [i[0] for i in output_hist_tuple]
    # for index in range(len(output_labels)) :

    #     if output_labels[index] == "MET_Run2018" :
    return output_hists, output_labels, color_list 

def plot(Output, type = "dijets_mass"):

    #Dijet mass plot
    for key in Output.keys() : #keys are MET_Run2018 , ZJets_NuNu etc
        hlist = []
        for subkey in Output[key].keys() : #subkeys are MET_Run2018A ,MET_Run2018B ...etc
            hlist.append(Output[key][subkey]["Histograms"][type])
        h = hlist[0]
        for histo in hlist[1:] :
            h += histo
        fig, ax = plt.subplots()
        hep.histplot(
            h,
            histtype="fill",
            color="#525FE1",
            label=type,
            edgecolor="black",
            lw=1,
            ax=ax
        )
        hep.cms.label("Preliminary", data= key.startswith("MET"))
        ax.set_ylabel("Events")
        ax.set_xlabel("Mass (GeV)")
        ax.set_title(f"{key} : {type}",pad=35,  fontsize= "20")
        fig.text(0.01,0.01,"Generated : "+get_timestamp(), fontsize = "10")
        fig.legend(loc= (0.57,0.64))
        plotname = f"Znunu_{key}_{type}.png"
        fig.savefig(plotname, dpi=300)
        fig.clear()
        print(plotname , f" created at {os.getcwd()}")

def plotall(Output):
    type_list = Output["MET_Run2018"]["MET_Run2018A"]["Histograms"].keys()
    for type in type_list :
        plot(Output,type)
    pass

def combined_plot(Output):


    Integrated_luminosity = crossSections.crossSections.lumis[2018]

    #Dijet mass plot
   
    Hist_List , label_List , Color_List = get_plotting_essentials(Output, "DiJet", Sorted=True)
    norm_Hist_List = []
    for histogram in Hist_List :
        histogram = normalize(histogram)
        norm_Hist_List.append(histogram)
    fig, ax = plt.subplots()
    hep.histplot(
        norm_Hist_List,
        histtype="fill",
        color=[ Color_List[1] , Color_List[0] ],
        label=label_List,
        edgecolor="black",
        lw=1,
        ax=ax
        )
    hep.cms.label("Preliminary", data= False)
    ax.set_ylabel("Normalized with integral")
    ax.set_xlabel("Mass (GeV)")
    ax.set_title(r"ak4 $b \bar{b}$ mass",pad=40, color="#192655")
    fig.text(0.01,0.01,"Generated : "+get_timestamp(), fontsize = "10")
    fig.text(0.87,0.01," Mode: Overlayed", fontsize = "10")
    fig.legend(loc= (0.57,0.64))
    plotname = f"SR_Resolved_Backgrounds_dijet_mass_combined.png"
    fig.savefig(plotname, dpi=300)
    print(plotname , f" created at {os.getcwd()}")

def combined_plot_manual(Output,norm = False , xsec = False):
    
    #Access keys and make histogram dictionaries
    MET_Run2018_hists = {}
    ZJets_NuNu_hists = {}
    TTToSemiLeptonic_hists = {}
    TTTo2L2Nu_hists = {}
    WJets_LNu_hists = {}
    DYJets_LL_hists = {}
    VV_hists = {}
    QCD_hists = {}
    ST_hists = {}
    for key in Output.keys() :
        #Dijet Histogram
        if key.startswith("MET_Run2018") :
            for subkey in Output[key].keys():
                MET_Run2018_hists[subkey] = Output[key][subkey]["Histograms"]["dijets_mass"]
        elif key.startswith("ZJets_NuNu") :
            for subkey in Output[key].keys():
                ZJets_NuNu_hists[subkey] = Output[key][subkey]["Histograms"]["dijets_mass"]
        elif key.startswith("TTToSemiLeptonic"):
            for subkey in Output[key].keys():
                TTToSemiLeptonic_hists[subkey] = Output[key][subkey]["Histograms"]["dijets_mass"]
        elif key.startswith("TTTo2L2Nu"):
            for subkey in Output[key].keys():
                TTTo2L2Nu_hists[subkey] = Output[key][subkey]["Histograms"]["dijets_mass"]
        elif key.startswith("WJets_LNu"):
            for subkey in Output[key].keys():
                WJets_LNu_hists[subkey] = Output[key][subkey]["Histograms"]["dijets_mass"]
        elif key.startswith("DYJets_LL"):
            for subkey in Output[key].keys():
                DYJets_LL_hists[subkey] = Output[key][subkey]["Histograms"]["dijets_mass"]
        elif key.startswith("VV"):
            for subkey in Output[key].keys():
                VV_hists[subkey] = Output[key][subkey]["Histograms"]["dijets_mass"]
        elif key.startswith("QCD"):
            for subkey in Output[key].keys():
                QCD_hists[subkey] = Output[key][subkey]["Histograms"]["dijets_mass"]
        elif key.startswith("ST"):
            for subkey in Output[key].keys():
                ST_hists[subkey] = Output[key][subkey]["Histograms"]["dijets_mass"]
    #cross sections
    if xsec :
        match inputs.fulldataset :
            case 1 :
                lumi = crossSections.lumis[2018]
            case 0 :
                with open("lumi_lookup.json") as f :
                    lumijson = json.load(f)
                lumi = lumijson["Sum"]["Recorded"]
        #print("Integrated Luminosity(pb): ", lumi)

        #load cross sections

        ZJets_NuNu_xsec = {
            "Z1Jets_NuNu_ZpT_50To150_18": crossSections.crossSections["Z1Jets_NuNu_ZpT_50To150_18"],
            "Z1Jets_NuNu_ZpT_150To250_18": crossSections.crossSections["Z1Jets_NuNu_ZpT_150To250_18"],
            "Z1Jets_NuNu_ZpT_250To400_18": crossSections.crossSections["Z1Jets_NuNu_ZpT_250To400_18"],
            "Z1Jets_NuNu_ZpT_400Toinf_18": crossSections.crossSections["Z1Jets_NuNu_ZpT_400Toinf_18"],
            "Z2Jets_NuNu_ZpT_50To150_18": crossSections.crossSections["Z2Jets_NuNu_ZpT_50To150_18"],
            "Z2Jets_NuNu_ZpT_150To250_18": crossSections.crossSections["Z2Jets_NuNu_ZpT_150To250_18"],
            "Z2Jets_NuNu_ZpT_250To400_18": crossSections.crossSections["Z2Jets_NuNu_ZpT_250To400_18"],
            "Z2Jets_NuNu_ZpT_400Toinf_18": crossSections.crossSections["Z2Jets_NuNu_ZpT_400Toinf_18"]
        }

        TTToSemiLeptonic_xsec = {
            "TTToSemiLeptonic_18":crossSections.crossSections["TTToSemiLeptonic_18"]
        }
        

        #compute weight_factor for ZJets_NuNu 
        N_i = {}
        ZJets_NuNu_weight_factor = {}
        for subkey in ZJets_NuNu_xsec.keys() :
            N_i[subkey] = Output["ZJets_NuNu"][subkey]["Cutflow"]["Total events"]
            ZJets_NuNu_weight_factor[subkey] = ( lumi * ZJets_NuNu_xsec[subkey] )/N_i[subkey]

        #compute weight_factor for TTToSemiLeptonic
        N_i = {}
        TTToSemiLeptonic_weight_factor = {}
        for subkey in TTToSemiLeptonic_xsec.keys() :
            N_i[subkey] = Output["TTToSemiLeptonic"][subkey]["Cutflow"]["Total events"]
            TTToSemiLeptonic_weight_factor[subkey] = ( lumi * TTToSemiLeptonic_xsec[subkey] )/N_i[subkey]
        
        #Simply add up the data histograms
        MET_Run2018_hists_list=[]
        for key in MET_Run2018_hists.keys() :
            MET_Run2018_hists_list.append(MET_Run2018_hists[key])

        MET_Run2018_hist = MET_Run2018_hists_list[0]
        for histogram in MET_Run2018_hists_list[1:] :
            MET_Run2018_hist += MET_Run2018_hists[key]

        #individually apply cross section and then add up the MC histograms by key
        def hist_xsec(key_hists, key_weight_factor):
            key_hists_list = []
            #print(key_hists.keys())
            for key in key_hists.keys() :
                key_hists[key] *= key_weight_factor[key]
                key_hists_list.append(key_hists[key])
            key_hist = key_hists_list[0]
            for histogram in key_hists_list[1:] :
                key_hist += key_hists[key]
            return key_hist
        
        ZJets_NuNu_hist = hist_xsec(ZJets_NuNu_hists , ZJets_NuNu_weight_factor)
        TTToSemiLeptonic_hist = hist_xsec(TTToSemiLeptonic_hists, TTToSemiLeptonic_weight_factor)

        # Zjets_hists_list = []
        # for key in ZJets_NuNu_hists.keys() :
        #     ZJets_NuNu_hists[key] *= ZJets_NuNu_weight_factor[key]
        #     Zjets_hists_list.append(ZJets_NuNu_hists[key])
        # Zjets_hist = Zjets_hists_list[0]
        # for histogram in Zjets_hists_list[1:] :
        #     Zjets_hist += ZJets_NuNu_hists[key]



    #normalize
    norm_factor = 1.0
    if norm :
        norm_factor= 1.0 / (
            MET_Run2018_hist.sum()+
            ZJets_NuNu_hist.sum()+
            TTToSemiLeptonic_hist.sum()
            )

    fig, ax = plt.subplots()
    hep.histplot(
        norm_factor*MET_Run2018_hist ,
        histtype='errorbar',
        color="black",
        #marker=[],
        label="MET_Run2018",
        xerr = 1,
        yerr= 0,
        lw=1,
        ax=ax
        )
    hep.histplot(
        [norm_factor*ZJets_NuNu_hist, norm_factor*TTToSemiLeptonic_hist],
        histtype="fill",
        color=["#525FE1","red"],
        #marker=[],
        label=["ZJets_NuNu","TTToSemiLeptonic"],
        edgecolor="black",
        stack=True,
        lw=1,
        ax=ax
        )
    # hep.histplot(
    #     norm_factor*TTToSemiLeptonic_hist,
    #     histtype="fill",
    #     color="red",
    #     #marker=[],
    #     label="TTToSemiLeptonic",
    #     edgecolor="black",
    #     lw=1,
    #     ax=ax
    #     )

    hep.cms.label("Preliminary",data = False)
    ax.set_ylabel("Normalized")
    plt.xlim([100.0,150.0])
    ax.set_xlabel("Mass (GeV)")
    ax.set_title(r"ak4 $b \bar{b}$ mass",pad=40, color="#192655")
    #plt.yscale("log")
    fig.text(0.01,0.01,"Generated : "+get_timestamp(), fontsize = "10")
    fig.text(0.87,0.01," Mode: Overlayed", fontsize = "10")
    fig.legend(loc= (0.63,.71))
    #fig.legend(loc=1)
    plotname = f"SR_Resolved_Backgrounds_dijet_mass_Combined.png"
    fig.savefig(plotname, dpi=300)
    fig.clear()
    print(plotname , f" created at {os.getcwd()}")

def combinecutflow(parent):
    out = {}
    for key in parent:
        out[key] = {}
        for subkey in parent[key]:
            for subsubkey in parent[key][subkey]["Cutflow"]:
                out[key][subsubkey] = 0
    for key in parent:
        for subkey in parent[key]:
            for subsubkey in parent[key][subkey]["Cutflow"]:
                out[key][subsubkey] += parent[key][subkey]["Cutflow"][subsubkey]
    #print(out)
    return out

def plotcutflow(parent):
    combined = combinecutflow(parent)
    for key in combined.keys() :
        nbins = len(combined[key].keys())

        histogram = hist.Hist.new.Reg(nbins,1,nbins+1).Double()

        weights = []
        n = 1 
        for item in combined[key].keys():
            weights.append(combined[key][item])
            print(n, " : ", item , " : ", combined[key][item])
            n += 1
        #print(f"{key} : ", weights)

        for i in range(nbins):
            histogram.fill(i+1, weight=weights[i])
        #print(histogram)

        fig, ax = plt.subplots()
        hep.histplot(
            histogram,
            histtype='fill',
            color="#192655",
            #marker=[],
            label=key,
            lw=1,
            ax=ax
            )

        hep.cms.label("Preliminary", data= key.startswith("MET_Run"))
        ax.set_ylabel("log(Events)")
        ax.set_xlabel("Cutflow order")
        plt.xlim([1,nbins+1])
        plt.xticks(np.arange(1,nbins+1,1))
        ax.set_title(f"{key} cutflow",pad=35,  fontsize= "20", color="#192655")
        fig.legend(loc= (0.57,0.64))
        plt.yscale("log")
        fig.text(0.01,0.01,"Generated : "+get_timestamp(), fontsize = "10")
        #fig.text(0.87,0.01," Mode: Overlayed", fontsize = "10")
        #fig.legend(loc= (0.70,.91))
        #fig.legend(loc=1)
        plotname = f"Znunu_{key}_cutflow.png"
        fig.savefig(plotname, dpi=300)
        fig.clear()
        print(plotname , f" created at {os.getcwd()}")

def accum(key):
    list_files = os.listdir()
    valid_list = []
    for file in list_files :
        if file.startswith(f"SR_Resolved_Backgrounds_{key}_from") :
            valid_list.append(file)
    full = processor.accumulate([util.load(name) for name in valid_list])
    return full

match inputs.fulldataset :
    case 1 :
        MET_Run2018 = accum("MET_Run2018")
        #showinfo(MET_Run2018)
        ZJets_NuNu = accum("ZJets_NuNu")
        #showinfo(ZJets_NuNu)
        TTToSemiLeptonic = accum("TTToSemiLeptonic")
        #showinfo(TTToSemiLeptonic)
        
    case 0 :
        MET_Run2018 = util.load("SR_Resolved_Backgrounds_MET_Run2018.coffea")
        ZJets_NuNu = util.load("SR_Resolved_Backgrounds_ZJets_NuNu.coffea")
        TTToSemiLeptonic = util.load("SR_Resolved_Backgrounds_TTToSemiLeptonic.coffea")
master_dict = processor.accumulate([MET_Run2018,ZJets_NuNu,TTToSemiLeptonic])
util.save(master_dict, "BackgroundDijets.coffea")
#showinfo(master_dict)
#plotall(master_dict)
#combined_plot(master_dict)
combined_plot_manual(master_dict,norm=False, xsec=True)
plotcutflow(master_dict)

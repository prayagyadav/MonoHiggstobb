""" 
~~~~~~~~~~~~~~~~~~~~~~~PLOTTER SCRIPT~~~~~~~~~~~~~~~~~~~~~~
This script studies the Z--> \nu + \nu + jets background .

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
    type_list = Output["MonoHTobb_ZpBaryonic"]["MonoHTobb_ZpBaryonic"]["Histograms"].keys()
    for type in type_list :
        plot(Output,type)
    

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
    plotname = f"ZnunuCombined.png"
    fig.savefig(plotname, dpi=300)
    print(plotname , f" created at {os.getcwd()}")

    Hist_List , label_List , Color_List = get_plotting_essentials(Output, "DiJetMETcut")
    norm_Hist_List = []
    for histogram in Hist_List :
        histogram = normalize(histogram)
        norm_Hist_List.append(histogram)
    fig, ax = plt.subplots()
    hep.histplot(
        norm_Hist_List,
        histtype="fill",
        color=[ Color_List[0] , Color_List[1] ],
        label=label_List,
        edgecolor="black",
        lw=1,
        ax=ax
        )
    hep.cms.label("Preliminary", data= False)
    ax.set_ylabel("Normalized with integral")
    ax.set_xlabel("Mass (GeV)")
    ax.set_title(r"ak4 $b \bar{b}$ mass with MET cut",pad=40, color="#192655")
    fig.text(0.01,0.01,"Generated : "+get_timestamp(), fontsize = "10")
    fig.text(0.87,0.01," Mode: Overlayed", fontsize = "10")
    fig.text(0.6,0.5," MET > 200 GeV", fontsize = "20")
    fig.legend(loc= (0.57,0.64))
    plotname = f"ZnunuCombinedwithMETcut.png"
    fig.savefig(plotname, dpi=300)
    print(plotname , f" created at {os.getcwd()}")

def combined_plot_manual(Output,norm = False , xsec = False):
    
    #Access keys and make histogram dictionaries
    Data_hists = {}
    MonoHTobb_ZpBaryonic_hists = {}
    for key in Output.keys() :
        #Dijet Histogram
        if key.startswith("MET_Run2018") :
            for subkey in Output[key].keys():
                Data_hists[subkey] = Output[key][subkey]["Histograms"]["dijets_mass"]
                #print(Data_hists[subkey])
        elif key.startswith("MonoHTobb_ZpBaryonic") :
            for subkey in Output[key].keys():
                MonoHTobb_ZpBaryonic_hists[subkey] = Output[key][subkey]["Histograms"]["dijets_mass"]

    Data_hist = Data_hists["MET_Run2018A"]
    MonoHTobb_ZpBaryonic_hist = MonoHTobb_ZpBaryonic_hists["MonoHTobb_ZpBaryonic"]
    #print("data hists: " , Data_hists["MET_Run2018A"])
    #print("MonoHTobb_ZpBaryonic hists", MonoHTobb_ZpBaryonic_hists)
    #Apply cross sections
    # if xsec :
    #     match inputs.fulldataset :
    #         case 1 :
    #             lumi = crossSections.lumis[2018]
    #         case 0 :
    #             with open("lumi_lookup.json") as f :
    #                 lumijson = json.load(f)
    #             lumi = lumijson["Sum"]["Recorded"]
    #     #print("Integrated Luminosity(pb): ", lumi)
    #     xsec = {
    #         "Z1Jets_NuNu_ZpT_50To150_18": crossSections.crossSections["Z1Jets_NuNu_ZpT_50To150_18"],
    #         "Z1Jets_NuNu_ZpT_150To250_18": crossSections.crossSections["Z1Jets_NuNu_ZpT_150To250_18"],
    #         "Z1Jets_NuNu_ZpT_250To400_18": crossSections.crossSections["Z1Jets_NuNu_ZpT_250To400_18"],
    #         "Z1Jets_NuNu_ZpT_400Toinf_18": crossSections.crossSections["Z1Jets_NuNu_ZpT_400Toinf_18"],
    #         "Z2Jets_NuNu_ZpT_50To150_18": crossSections.crossSections["Z2Jets_NuNu_ZpT_50To150_18"],
    #         "Z2Jets_NuNu_ZpT_150To250_18": crossSections.crossSections["Z2Jets_NuNu_ZpT_150To250_18"],
    #         "Z2Jets_NuNu_ZpT_250To400_18": crossSections.crossSections["Z2Jets_NuNu_ZpT_250To400_18"],
    #         "Z2Jets_NuNu_ZpT_400Toinf_18": crossSections.crossSections["Z2Jets_NuNu_ZpT_400Toinf_18"]
    #     }
    #     #print("Zjets_NuNu cross sections :", xsec)
    #     N_i = {}
    #     weight_factor = {}
    #     for subkey in xsec.keys() :
    #         N_i[subkey] = Output["ZJets_NuNu"][subkey]["Cutflow"]["Total events"]
    #         weight_factor[subkey] = ( lumi * xsec[subkey] )/N_i[subkey]
    #     #print("N_i: ", N_i)
    #     #print("weight_factor: ", weight_factor)

    #     #Simply add up the data histograms
    #     Data_hists_list=[]
    #     for key in Data_hists.keys() :
    #         Data_hists_list.append(Data_hists[key])

    #     Data_hist = Data_hists_list[0]
    #     for histogram in Data_hists_list[1:] :
    #         Data_hist += Data_hists[key]

    #     #individually apply cross section and then add up the MC histograms by key
    #     Zjets_hists_list = []
    #     for key in Zjets_hists.keys() :
    #         Zjets_hists[key] *= weight_factor[key]
    #         Zjets_hists_list.append(Zjets_hists[key])


    #     Zjets_hist = Zjets_hists_list[0]
    #     for histogram in Zjets_hists_list[1:] :
    #         Zjets_hist += Zjets_hists[key]



    #normalize
    norm_factor= 1.0
    if norm :
        norm_factor= 1.0 / ( Data_hist.sum() + MonoHTobb_ZpBaryonic_hist.sum())


    fig, ax = plt.subplots()
    hep.histplot(
        norm_factor*Data_hist ,
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
        norm_factor*MonoHTobb_ZpBaryonic,
        histtype="fill",
        color="#525FE1",
        #marker=[],
        label="MonoHTobb_ZpBaryonic",
        edgecolor="black",
        lw=1,
        ax=ax
        )

    hep.cms.label("Preliminary", data= False)
    ax.set_ylabel("Normalized")
    plt.xlim([100.0,150.0])
    ax.set_xlabel("Mass (GeV)")
    ax.set_title(r"ak4 $b \bar{b}$ mass",pad=40, color="#192655")
    #plt.yscale("log")
    fig.text(0.01,0.01,"Generated : "+get_timestamp(), fontsize = "10")
    fig.text(0.87,0.01," Mode: Overlayed", fontsize = "10")
    fig.legend(loc= (0.70,.91))
    #fig.legend(loc=1)
    plotname = f"Combined.png"
    fig.savefig(plotname, dpi=300)
    fig.clear()
    print(plotname , f" created at {os.getcwd()}")

def accum(key):
    list_files = os.listdir()
    valid_list = []
    for file in list_files :
        if file.startswith(f"Zjetsnunu_{key}_from") :
            valid_list.append(file)
    full = processor.accumulate([util.load(name) for name in valid_list])
    print(full)
    return full

match inputs.fulldataset :
    case 1 :
        MET_Run2018 = accum("MET_Run2018")
        MonoHTobb_ZpBaryonic = accum("MonoHTobb_ZpBaryonic")
    case 0 :
        MET_Run2018 = util.load("Zjetsnunu_MET_Run2018.coffea")
        MonoHTobb_ZpBaryonic = util.load("Zjetsnunu_MonoHTobb_ZpBaryonic.coffea")
master_dict = processor.accumulate([MET_Run2018,MonoHTobb_ZpBaryonic])
util.save(master_dict, "BackgroundDijets.coffea")
showinfo(master_dict)
plotall(master_dict)
#combined_plot(master_dict)
combined_plot_manual(master_dict,norm=False, xsec=False)

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

import awkward as ak 
from coffea import util, processor
import matplotlib.pyplot as plt
import mplhep as hep
from monoHbbtools.Utilities import get_timestamp , normalize
import numpy as np
import os
#plt.style.use(hep.style.CMS)
hep.style.use(["CMS","fira","firamath"])
#hep.style.use("CMSTex")
import rich

#Load the output
def showinfo(Output):
    for key in Output.keys():
        rich.print(key, " :")
        cutflow = Output[key]["Cutflow"]
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

def plot(Output):

    #Dijet mass plot
    for key in Output.keys() :
        h = Output[key]["Histograms"]["DiJet"]
        fig, ax = plt.subplots()
        hep.histplot(
            h,
            histtype="fill",
            color="#525FE1",
            label="Dijet mass",
            edgecolor="black",
            lw=1,
            ax=ax
        )
        hep.cms.label("Preliminary", data= key.startswith("MET"))
        ax.set_ylabel("Events")
        ax.set_xlabel("Mass (GeV)")
        ax.set_title(f"{key} : Dijet mass",pad=35,  fontsize= "20")
        fig.text(0.01,0.01,"Generated : "+get_timestamp(), fontsize = "10")
        fig.legend(loc= (0.57,0.64))
        plotname = f"Znunu{key}.png"
        fig.savefig(plotname, dpi=300)
        fig.clear()
        print(plotname , f" created at {os.getcwd()}")


def combined_plot(Output):

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

def combined_plot_manual(Output):
    #Dijet Histogram
    Data_hist = Output["MET_Run2018"]["Histograms"]["DiJet"]
    Zjets_hist = Output["ZJets_NuNu"]["Histograms"]["DiJet"]
    
    fig, ax = plt.subplots()
    hep.histplot(
        [Data_hist,Zjets_hist],
        histtype=["step","fill"],
        color=["red","blue"],
        #marker=[],
        label=["MET_Run2018", "ZJets_NuNu"],
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
    fig.clear()
    print(plotname , f" created at {os.getcwd()}")

    Data_hist = Output["MET_Run2018"]["Histograms"]["DiJetMETcut"]
    Zjets_hist = Output["Zjets_NuNu"]["Histograms"]["DiJetMETcut"]
    fig, ax = plt.subplots()
    hep.histplot(
        [Data_hist,Zjets_hist],
        histtype=["step","fill"],
        color=["red","blue"],
        #marker=[],
        label=["MET_Run2018", "ZJets_NuNu"],
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

MET_Run2018 = util.load("Zjetsnunu_MET_Run2018.coffea")
Znunu = util.load("Zjetsnunu_ZJets_NuNu.coffea")
master_dict = processor.accumulate([MET_Run2018,Znunu])
util.save(master_dict, "BackgroundDijets.coffea")
showinfo(master_dict)
plot(master_dict)
#combined_plot(master_dict)
combined_plot_manual(master_dict)

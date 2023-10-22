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
from monoHbbtools.Utilities import get_timestamp
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

def get_plotting_essentials(Output, histogram_key) :
    color_list = ["#4E3636","#116D6E","#321E1E"]
    hist_tuple = [(key, Output[key]["Histograms"][histogram_key] ) for key in Output.keys()]
    sorted_hist_tuple = sorted(hist_tuple , key = lambda x : x[1].sum() , reverse=True)
    sorted_hists = [i[1] for i in sorted_hist_tuple]
    sorted_labels = [i[0] for i in sorted_hist_tuple]
    color_list = color_list[:len(sorted_hists)]
    return sorted_hists, sorted_labels, color_list 

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
   
    Hist_List , label_List , Color_List = get_plotting_essentials(Output, "DiJet")
    fig, ax = plt.subplots()
    hep.histplot(
        Hist_List,
        histtype="fill",
        color=Color_List,
        label=label_List,
        edgecolor="black",
        lw=1,
        ax=ax
        )
    hep.cms.label("Preliminary", data= False)
    ax.set_ylabel("Events")
    ax.set_xlabel("Mass (GeV)")
    ax.set_title(r"ak4 $b \bar{b}$ mass",pad=40, color="#192655")
    fig.text(0.01,0.01,"Generated : "+get_timestamp(), fontsize = "10")
    fig.text(0.87,0.01," Mode: Overlayed", fontsize = "10")
    fig.legend(loc= (0.57,0.64))
    plotname = f"ZnunuCombined.png"
    fig.savefig(plotname, dpi=300)
    print(plotname , f" created at {os.getcwd()}")

    Hist_List , label_List , Color_List = get_plotting_essentials(Output, "DiJetMETcut")
    fig, ax = plt.subplots()
    hep.histplot(
        Hist_List,
        histtype="fill",
        color=Color_List,
        label=label_List,
        edgecolor="black",
        lw=1,
        ax=ax
        )
    hep.cms.label("Preliminary", data= False)
    ax.set_ylabel("Events")
    ax.set_xlabel("Mass (GeV)")
    ax.set_title(r"ak4 $b \bar{b}$ mass with MET cut",pad=40, color="#192655")
    fig.text(0.01,0.01,"Generated : "+get_timestamp(), fontsize = "10")
    fig.text(0.87,0.01," Mode: Overlayed", fontsize = "10")
    fig.text(0.6,0.5," MET $p_t > 200 $ (GeV)", fontsize = "20")
    fig.legend(loc= (0.57,0.64))
    plotname = f"ZnunuCombinedwithMETcut.png"
    fig.savefig(plotname, dpi=300)
    print(plotname , f" created at {os.getcwd()}")

MET = util.load("Zjetsnunu_MET.coffea")
Znunu = util.load("Zjetsnunu_Z1Jets_NuNu.coffea")
master_dict = processor.accumulate([MET,Znunu])
util.save(master_dict, "BackgroundDijets.coffea")
showinfo(master_dict)
plot(master_dict)
combined_plot(master_dict)

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
from coffea import util
import matplotlib.pyplot as plt
import mplhep as hep
from monoHbbtools.Utilities import get_timestamp
import numpy as np
import os
plt.style.use(hep.style.CMS)
import rich

#Load the output
def showinfo(filename):
    Output = util.load(filename)
    for key in Output.keys():
        cutflow = Output[key]["Cutflow"]
        rich.print(cutflow)

def plot(filename):
    Output = util.load(filename)

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
        hep.cms.label("Preliminary", data= "Data" in filename)
        ax.set_ylabel("Events")
        ax.set_xlabel("Mass (GeV)")
        ax.set_title(f"{key} : Dijet mass",pad=35,  fontsize= "20")
        fig.text(0.01,0.01,"Generated : "+get_timestamp(), fontsize = "10")
        fig.legend(loc= (0.57,0.64))
        plotname = f"Znunu{key}.png"
        fig.savefig(plotname, dpi=300)
        fig.clear()
        print(plotname , f" created at {os.getcwd()}")


def combined_plot(filename):
    Output = util.load(filename)

    #Dijet mass plot
    Hist_List = []
    Color_List = ["#4E3636","#116D6E","#321E1E",]
    label_List = []
    for key in Output.keys() :
        Hist_List.append(Output[key]["Histograms"]["DiJet"])
        label_List.append(key)
    nHists = len(Hist_List)
    fig, ax = plt.subplots()
    hep.histplot(
        Hist_List,
        histtype="fill",
        color=Color_List[:nHists],
        label=label_List,
        edgecolor="black",
        stack=True,
        lw=1,
        ax=ax
        )
    hep.cms.label("Preliminary", data= False)
    ax.set_ylabel("Events")
    ax.set_xlabel("Mass (GeV)")
    ax.set_title("Dijet mass",pad=35)
    fig.text(0.01,0.01,"Generated : "+get_timestamp(), fontsize = "10")
    fig.legend(loc= (0.57,0.64))
    plotname = f"ZnunuCombined.png"
    fig.savefig(plotname, dpi=300)
    print(plotname , f" created at {os.getcwd()}")

filenames = ["Zjetsnunu.coffea"]
for file in filenames:
    if file in os.listdir():
        showinfo(file)
        plot(file)
        combined_plot(file)

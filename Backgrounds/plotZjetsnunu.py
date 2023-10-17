"""
This is a template of the plotter for data obtained from a processor.
Author : Prayag Yadav
Created : 11 October 2023
"""
#################################
# Import the necessary packages #
#################################

import awkward as ak 
from coffea import util
import matplotlib.pyplot as plt
import mplhep as hep
import numpy as np
import os
plt.style.use(hep.style.CMS)
import rich

#Load the output
def showinfo(filename):
    Output = util.load(filename)
    cutflow = Output["Cutflow"]
    rich.print(cutflow)

def plot(filename):
    Output = util.load(filename)

    #pt plot
    h = Output["Histograms"]["Dijet"]
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
    hep.cms.label("Preliminary", data= True)
    ax.set_ylabel("Events")
    ax.set_xlabel("Mass (GeV)")
    ax.set_title("Dijet mass",pad=35)
    fig.legend()
    plotname = f"ZnunuDijets.png"
    fig.savefig(plotname, dpi=300)
    print(plotname , f" created at {os.getcwd()}")

filenames = ["Zjetsnunu.coffea"]
for file in filenames:
    if file in os.listdir():
        showinfo(file)
        plot(file)
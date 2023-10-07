"""
This file loads the data obtained from kinematics.py and plots a few kinematics
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

############################
# Common plotting function #
############################

def PlotSeparately(filename):
    #Load the output
    Output = util.load(filename)
    if filename.startswith("Data"):
        Mode = "Data"
    elif filename.startswith("MC"):
        Mode = "MC"
    else :
        raise Exception("File not found or incorrect name of file.")
    print("The flow of events :\n ", Output["Cutflow"])
    Tag_Hist = Output["Histograms"]["Tag"]
    Jetpt_Hist = Output["Histograms"]["Jetpt"]
    DiJetMass_Hist = Output["Histograms"]["DiJetMass"]
    MET_Hist = Output["Histograms"]["MET"]

    #1. bTag score histogram
    x_min = 0.
    x_max = 1.
    bin_size = 0.05
    n_bins=int((x_max - x_min)/bin_size)
    fig , ax = plt.subplots()
    hep.histplot(
        Tag_Hist ,
        histtype="fill",
        color='#8A307F',#purple-ish
        edgecolor="black",
        label=r"Tag" ,
        lw=1,
        ax=ax
    )
    ax.set_title("BTag Score", y=1.0, pad = -35 , fontsize=25, color="#053B50") #Teal
    ax.set_xlabel("Score", fontsize=20)
    ax.set_ylabel(f"Events / {bin_size}", fontsize=20)
    ax.set_xticks(np.arange(x_min,x_max+bin_size,bin_size*10))
    hep.cms.label("Preliminary",data = Mode == "Data", rlabel="")
    fig.savefig(f"Tag{Mode}.png", dpi= 320)
    plt.clf()

    #2. Jets pt : Untagged and Tagged 
    x_min = 0.
    x_max = 500.
    bin_size = 10
    n_bins=int((x_max - x_min)/bin_size)
    #fig , ax= plt.subplots(figsize=(10,10))
    fig , ax = plt.subplots()
    hep.histplot(Jetpt_Hist["Untagged",:], 
                 #bins=bins ,
                 histtype="fill",
                 color="#79A7D3",#pale sky blue
                 edgecolor="black",
                 label=r"Untagged",
                 lw=1,
                 ax=ax
                )
    hep.histplot(Jetpt_Hist["btagDeepFlavB",:], 
                 #bins=bins ,
                 histtype="fill",
                 color="#8A307F",#purple-ish
                 edgecolor="black",
                 label=r"btagDeepFlavB",
                 lw=1,
                 ax=ax
                )
    ax.set_title("Jet $p_t$", y=1.0, pad = -35 , fontsize=25, color="#053B50") #Teal
    ax.set_xlabel("$p_t$ (GeV)", fontsize=20)
    ax.set_ylabel(f"Events / {bin_size} GeV", fontsize=20)
    ax.set_xticks(np.arange(x_min,x_max+bin_size,bin_size*10))
    hep.cms.label("Preliminary",data = Mode == "Data" , rlabel="")
    ax.legend()
    fig.savefig(f"Jets{Mode}.png", dpi= 300)
    plt.clf()

    #3. DiJets Mass : Untagged and Tagged
    x_min = 0.
    x_max = 500.
    bin_size = 10
    n_bins=int((x_max - x_min)/bin_size)
    fig , ax= plt.subplots()
    hep.histplot(DiJetMass_Hist["Untagged",:], 
                 #bins=bins ,
                 histtype="fill",
                 color="#79A7D3",#pale sky blue
                 edgecolor="black",
                 label=r"Untagged",
                 lw=1,
                 ax=ax
                )
    hep.histplot(DiJetMass_Hist["btagDeepFlavB",:], 
                 #bins=bins ,
                 histtype="fill",
                 color="#8A307F",#purple-ish
                 edgecolor="black",
                 label=r"btagDeepFlavB",
                 lw=1,
                 ax=ax
                )
    ax.set_title("DiJet Invariant Mass", y=1.0, pad = 35 , fontsize=25, color="#053B50" ) #Teal
    ax.set_xlabel("Mass (GeV)", fontsize=20)
    ax.set_ylabel(f"Events / {bin_size} GeV", fontsize=20)
    ax.set_xticks(np.arange(x_min,x_max+bin_size,bin_size*10))
    hep.cms.label("Preliminary",data = Mode == "Data" , rlabel="")
    ax.legend()
    fig.savefig(f"DiJets{Mode}.png", dpi= 300)
    plt.clf()

    #4. MET histogram
    x_min = 0.
    x_max = 500.
    bin_size = 10
    n_bins=int((x_max - x_min)/bin_size)
    fig , ax = plt.subplots()
    hep.histplot(
        MET_Hist ,
        histtype="fill",
        color='#8A307F',#purple-ish
        edgecolor="black",
        label=r"MET $p_t$" ,
        lw=1,
        ax=ax
    )
    ax.set_title("MET $p_t$", y=1.0, pad = -35 , fontsize=25, color="#053B50") #Teal
    ax.set_xlabel("$p_t$ (GeV)", fontsize=20)
    ax.set_ylabel(f"Events / {bin_size} GeV", fontsize=20)
    ax.set_xticks(np.arange(x_min,x_max+bin_size,bin_size*10))
    hep.cms.label("Preliminary",data = Mode == "Data", rlabel="")
    fig.savefig(f"MET{Mode}.png", dpi= 320)
    plt.clf()


######################
# Generate the plots #
######################
files = ["Datakinematics.coffea","MCkinematics.coffea"]
print("Plotting ...")
for  file in files :
    if file in os.listdir() :
        PlotSeparately(file)
print("Done")
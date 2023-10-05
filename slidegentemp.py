Titles=["BTag Scores : MC","BTag Scores : Data", "Jet $p_t$ : MC", "Jet $p_t$ : Data", "DiJet mass : MC","DiJet mass : Data","MET $p_t$ : MC","MET $p_t$ : Data"]
labels=["TagMC","TagData","JetMC","JetData","DiJetMC","DiJetData","METMC","METMC"]
filenames=["TagMC.png","TagData.png","JetsMC.png","JetsData.png","DiJetsMC.png","DiJetsData.png","METMC.png","METData.png"]
captions=["BTag score for signal MC sample","BTag score for Data samples","Jet $p_t$ of signal MC samples","Jet $p_t$ of Data samples","DiJet mass of signal MC samples","DiJet mass of Data samples","MET $p_t$ for signal MC samples","MET $p_t$ for Data samples"]

for i in range(len(Titles)):
   a= f"\\begin{{frame}}[fragile]{{{Titles[i]}}} \n \\begin{{columns}}\n \column{{0.58\\textwidth}} \n \\begin{{figure}} \n \\centering \n  \\includegraphics[width=1\\textwidth]{{../Archive/KinemPlots/{filenames[i]} }}"
   b= f"\n \\label{{{labels[i]}}} \n \\caption{{{captions[i]}}}\n \\end{{figure}} \n \\column{{0.38\\textwidth}} \n \\begin{{itemize}} \n \\raggedright \n \\small"
   c= f"\n \\item {{Btagger used : \\texttt{{btagDeepFlavB}}}} \n \\item {{Sample used: \\texttt{{MonoHTobb\_ZpBaryonic}}}} \n \\item Lots of bjets in Signal MC \n \\end{{itemize}}"
   d= f"\n \\end{{columns}} \n \\end{{frame}} \n \n "
   print(a+b+c+d)
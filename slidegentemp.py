Titles=[
   "BTag Scores : MC",
   "BTag Scores : Data",
   "Jet $p_t$ : MC",
   "Jet $p_t$ : Data",
   "DiJet mass : MC",
   "DiJet mass : Data",
   "MET $p_t$ : MC",
   "MET $p_t$ : Data"
   ]
labels=[
   "TagMC",
   "TagData",
   "JetMC",
   "JetData",
   "DiJetMC",
   "DiJetData",
   "METMC",
   "METMC"
   ]
filenames=[
   "TagMC.png",
   "TagData.png",
   "JetsMC.png",
   "JetsData.png",
   "DiJetsMC.png",
   "DiJetsData.png",
   "METMC.png",
   "METData.png"
   ]
captions=[
   "BTag score for signal MC sample",
   "BTag score for Data samples",
   "Jet $p_t$ of signal MC samples",
   "Jet $p_t$ of Data samples",
   "DiJet mass of signal MC samples",
   "DiJet mass of Data samples",
   "MET $p_t$ for signal MC samples",
   "MET $p_t$ for Data samples"
   ]

def gen1():
   for i in range(len(Titles)):
      a= f"\\begin{{frame}}[fragile]{{{Titles[i]}}} \n \\begin{{columns}}\n \column{{0.58\\textwidth}} \n \\begin{{figure}} \n \\centering \n  \\includegraphics[width=1\\textwidth]{{../Archive/KinemPlots/{filenames[i]} }}"
      b= f"\n \\label{{{labels[i]}}} \n \\caption{{{captions[i]}}}\n \\end{{figure}} \n \\column{{0.38\\textwidth}} \n \\begin{{itemize}} \n \\raggedright \n \\small"
      c= f"\n \\item {{Btagger used : \\texttt{{btagDeepFlavB}}}} \n \\item {{Sample used: \\texttt{{MonoHTobb\_ZpBaryonic}}}} \n \\item Lots of bjets in Signal MC \n \\end{{itemize}}"
      d= f"\n \\end{{columns}} \n \\end{{frame}} \n \n "
      print(a+b+c+d)

def section(title=r"Section title" , label=r"Section label", date_string= r"\today"):
   section = r"  \section["+label+r"]{\small{"+date_string+r"} \\ "+title+r" }"
   print(section)

def frame(title=r"Frame title",plot=r"uoh_logo.png", label=r"plot label",caption=r"Your caption",points_list=[r"point 1", r"point 2"]) :
   line={}
   line["frame_up"] = r"     \begin{frame}[fragile]{"+title+r"}" 
   line["column_up"] = r"      \begin{columns}"
   line["col1"] = r"        \column{0.58\textwidth}" 
   line["figure_up"] = r"        \begin{figure}"
   line["align_centre"] = r"          \centering" 
   line["plot"] = r"          \includegraphics[width=1\textwidth]{"+plot+r"}"
   line["label"] = r"          \label{"+label+r"}"
   line["caption"] = r"          \caption{"+caption+r"}"
   line["figure_down"] = r"        \end{figure}"
   line["col2"]= r"        \column{0.38\textwidth}" 
   line["points_up"]= r"        \begin{itemize}"
   line["align_right"]= r"          \raggedright "
   line["text_size"]= r"          \small"
   for item_index in range(len(points_list)) :
      line["item"+str(item_index)]= r"          \item {"+points_list[item_index]+r"}"
   line["points_down"]=r"        \end{itemize}"
   line["column_down"]=r"      \end{columns}"
   line["frame_down"]=r"    \end{frame}"

   for key in line.keys():
      print(line[key])

def comment_break():
   print("\n%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")


comment_break()
section(r"Thu, $5^{th}$ October 2023 ")
comment_break()
frame()
comment_break()

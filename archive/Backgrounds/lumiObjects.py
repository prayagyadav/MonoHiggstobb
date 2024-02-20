import  json

with open("currentRunLumi.txt","r") as file :
    #read all the lines
    lines = file.readlines()
    nlines = len(lines)
    print("Number of lines read: ", nlines)

    #create blocks
    blocks=[]
    for line_index in range(len(lines)) :
        block = []
        line = lines[line_index]
        if line.startswith("#Data"):
            for i in range(0,12):
                block.append(lines[line_index+i])
            blocks.append(block)
    print("Number of blocks: ",len(blocks))
    
    #create luminosity json lookup
    lumi_lookup = {"Delivered":{}, "Recorded":{}, "Sum":{}}
    integrated_delivered = 0
    integrated_recorded = 0
    for block in blocks :
        lumiline = block[4]
        lumiline = lumiline.strip("|")
        lumiline = lumiline.strip("|\n")
        varlist = lumiline.split(" | ")
        run_fill = varlist[0].strip()
        delivered  = float(varlist[-2]) #in per microbarn
        delivered *= 1e-9 #in per picobarn
        recorded = float(varlist[-1]) #in per microbarn
        recorded *= 1e-9 #in per picobarn
        lumi_lookup["Delivered"][run_fill] = delivered
        lumi_lookup["Recorded"][run_fill] = recorded 
        integrated_delivered += delivered
        integrated_recorded += recorded
    lumi_lookup["Sum"]["Delivered"] = integrated_delivered
    lumi_lookup["Sum"]["Recorded"] = integrated_recorded

    #save the json file
    with open("lumi_lookup.json","w") as output :
        output.write(json.dumps(lumi_lookup, indent=4))


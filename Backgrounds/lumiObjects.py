with open("currentRunLumi.txt","r") as file :
    blocks = file.read().split("#Data tag : 23v1 , Norm tag: onlineresult")
    for block in blocks :
        print(blocks)
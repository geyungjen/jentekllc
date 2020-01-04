'''
Developped by George Jen, Jen Tek LLC

Implment the following:

1. Create a binary tree buildBinTree(arr) and add array element to the tree nodes.
if parent node is at index i in the array, 
    then 
        left child of that node is at index (2*i + 1) in the array 
        right child is at index (2*i + 2) in the array.

2. Tree Depth First Traversals:

In Order left to right-->leftRootRight(node)
In Order right to left-->rightRootLeft(node)
Pre Order left to right-->rootLeftRight(node)
Pre Order right to left-->def rootRightLeft(node)
Post Order left to right-->leftRightRoot(node)
Post Order right to left-->rightLeftRoot(node)

3. Tree Breadth first traversal:
Breadth first traversal from left to right--> breadthFirstTraversalLeftRight(node)
Breadth first traversal from right to left--> breadthFirstTraversalRightLeft(node)

4  Discover tree depth --> getBinTreeDepth(node,depth=[0])
It will fill in the depth value in one element depth array

5. Binary tree search

Binary tree search, traverse the tree branch return original index of arr if found, -1 if not found
return_index is an array that that one element.  It is default to [-1], once found, it will be set to [matched node.index]

binTreeSearch(node,element,return_index)

6. Plotting the tree-->plotTree(node)
It will basically perform breadth first traversal and print each node along the way



'''

#Implement DFS Tree search, traverse the tree from root to leave via left and right branches

#Create Tree Structure:

class binTree(object):
    def __init__(self,node,index):
        self.node=node
        self.index=index
        self.right=None
        self.left=None

#Build Tree from sorted arr (arr needs to be presorted, to avoid branch rearrange)        
        
def buildBinTree(arr): #arr is not sorted, Tree is not sorted
    #Create tree starting with root node

#if parent node is at index i in the array 
#then the left child of that node is at index (2*i + 1) and 
#right child is at index (2*i + 2) in the array.

    if len(arr)==0:
        return None
    
    nodes=[0]*len(arr)
    
    level=0
    count=-1
#create nodes for each element of arr, not link them yet to form data structure such as tree

    for i in range(len(arr)):
        nodes[i]=binTree(arr[i],i)
        
#Now link the nodes together to form a binary tree

    i=0
    while 2*i+2<len(nodes):
        nodes[i].left=nodes[2*i+1]
        nodes[i].right=nodes[2*i+2] 
        i+=1
    
    return nodes[0]

def plotTree(node):
    def printGivenLevel(node, level,line_counter,deep):
        if not node:
            return
        if level==1:
            for j in range(line_counter-2):
                print(" ",end="")
            print(node.node, end=" ")
        elif level>1:
            printGivenLevel(node.left, level-1,line_counter-1,deep)
            printGivenLevel(node.right, level-1,line_counter-1,deep)

            
    depth=[0]
    getBinTreeDepth(node,depth)
    for d in range(1,depth[0]):
        line_counter=depth[0]
        deep=[depth[0]]
        printGivenLevel(node, d, line_counter,deep)
        print(" ")

def rootLeftRight(node):
    if node:
        print(node.node)
        rootLeftRight(node.left)
        rootLeftRight(node.right)

    

def rootRightLeft(node):
    if node:
        print(node.node)
        rootRightLeft(node.right)
        rootRightLeft(node.left)

def leftRightRoot(node):
    if node:
        leftRightRoot(node.left)
        leftRightRoot(node.right)
        print(node.node)
        
def rightLeftRoot(node):
    if node:
        rightLeftRoot(node.right)
        rightLeftRoot(node.left)
        print(node.node)

def leftRootRight(node):
    if node:
        leftRootRight(node.left)
        print(node.node)
        leftRootRight(node.right)

#Print the values stored in tree in desc order        
        
def rightRootLeft(node):
    if node:
        #desc
        rightRootLeft(node.right)
        print(node.node)
        rightRootLeft(node.left)

def getBinTreeDepth(node,depth=[0]):
    #maximum depth of a balanced binary tree is usually on its left most path from root to left most leaf
    #it is stored in one element array depth[0]
    if node:
        depth[0]+=1
        getBinTreeDepth(node.left,depth)
    else:
        depth[0]+=1
    
        
        
def breadthFirstTraversalLeftRight(node):
    def printGivenLevel(node, level):
        if not node:
            return
        if level==1:
            print(node.node, end=" ")
        elif level>1:
            printGivenLevel(node.left, level-1)
            printGivenLevel(node.right, level-1)

            
    depth=[0]
    getBinTreeDepth(node,depth)
    for d in range(1,depth[0]):
        printGivenLevel(node, d)
        print(" ")

        
def breadthFirstTraversalRightLeft(node):
    def printGivenLevel(node, level):
        if not node:
            return
        if level==1:
            print(node.node,end=" ")
        elif level>1:
            printGivenLevel(node.right, level-1)
            printGivenLevel(node.left, level-1)
            
    depth=[0]
    getBinTreeDepth(node,depth)
    for d in range(1,depth[0]):
        printGivenLevel(node, d)
        print(" ")
    

#Binary tree search, traverse the tree branch return original index of arr if found, -1 if not found
#return_index is an array that that one element.  It is default to [-1], once found, it will be set to [matched node.index]
def binTreeSearch(node,element,return_index):

    if return_index[0]!=-1:
        print(" ")
        return
    if node:
        if node.node==element:
            return_index[0]=node.index
            return
        else:
            binTreeSearch(node.left,element,return_index)
            binTreeSearch(node.right,element,return_index)

    
#Driver code:

if __name__=='__main__':
    #build the tree from arr
    print("Now build the tree from arr, return root node")
    root=buildBinTree([1,2,3,4,5,6,7])
#Traverse the tree in pre-order, in-order and post-order:
    print("Pre order tree traversal, left to right")
    rootLeftRight(root)
    print("pre order tree traversal, right to left")
    rootRightLeft(root)
    print("In order tree traversal, left to right")
    leftRootRight(root)
    print("In Order tree traversal, right to left")
    rightRootLeft(root)
    print("Post order tree traversal, left to right")
    leftRightRoot(root)
    print("Post order tree traversal, right to left")
    rightLeftRoot(root)
    print("Breadth first tree traversal, left to right")
    breadthFirstTraversalLeftRight(root)
    print("Breadth first tree traversal, right to left")
    breadthFirstTraversalRightLeft(root)
    print("Search value 5, if found, return index in original arr")
    search_index=[-1]
    binTreeSearch(root,5,search_index)
    if search_index[0]!=-1:
        print("Found, return index is {}".format(search_index[0]))
    else:
        print("Not found, return {}".format(search_index[0]))
    print("Search value 10, if not found, return -1")
    search_index=[-1]
    binTreeSearch(root,10,search_index)
    if search_index[0]!=-1:
        print("Found, return index is {}".format(search_index[0]))
    else:
        print("Not found, return {}".format(search_index[0]))
    
    print("Now plotting the tree")
    plotTree(root)
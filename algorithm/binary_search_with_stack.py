#binary search algorithm, without recursion, but using stack
#George Jen,  Jen Tek LLC

class stack(object):
    def __init__(self):
        self.container=[]

    def push(self, content):
        self.container.append(content)
    
    def pop(self,index):
        return self.container.pop(index)

def binSearchStack(arr,element): #return the 1st index found, arr is sorted
    if len(arr)==0:
        return -1
    start_org=0
    start=0
    end=len(arr)-1
   
    mystack=stack()
    mystack.push(arr)
    while start<end:
#        print(mystack.container)
#        input()
        a=mystack.pop(-1)
        
        if len(a)==0:
            return -1
        start_org+=start
        start=0
        end=len(a)-1
        mid=(start+end)//2
        if a[mid]==element:
#        print(mid)
            return start_org+mid
        elif element>a[mid]:
            #search right subarray
            start=mid+1
        else:
            #search left subarray
            end=mid-1
    
        mystack.push(a[start:end+1])
    return -1

#Driving code
if __name__=='__main__':
    print(binSearchStack([1,2,3,4,5,6,7,8,9,10],6))
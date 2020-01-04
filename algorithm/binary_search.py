#binary search algorithm, with iteration without recursion
#George Jen,  Jen Tek LLC

def binSearch(arr,element): #return the 1st index found, arr is sorted
    if len(arr)==0:
        return -1
    start=0
    end=len(arr)-1
    
    while start<end:
        mid=(start+end)//2
        if arr[mid]==element:
            return mid
        elif element>arr[mid]:
            #search right subarray
            start=mid+1
        else:
            #search left subarray
            end=mid-1
    return -1    
            
#Driving code
if __name__=='__main__':
    print(binSearch([1,2,3,4,5,6,7,8,9,10],6))

    
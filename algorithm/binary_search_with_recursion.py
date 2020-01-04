
#binary search algorithm, with recursion
#George Jen,  Jen Tek LLC


def binSearchR(arr,element): #return the 1st index found, arr is sorted
    if len(arr)==0:
        return -1
    start=0
    end=len(arr)-1
        
    mid=(start+end)//2
    if arr[mid]==element:
#        print(mid)
        return mid
    elif element>arr[mid]:
        #search right subarray
        start=mid+1
    else:
        #search left subarray
        end=mid-1
    
    return start+binSearchR(arr[start:end+1],element) #important to add start value

#Driving code
if __name__=='__main__':
    print(binSearchR([1,2,3,4,5,6,7,8,9,10],6))
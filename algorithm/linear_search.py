#Lenear Search
#George Jen, Jen Tek LLC

def linSearch(arr,element):
    if len(arr)==0:
        return -1
    for i in range(len(arr)):
        if arr[i]==element:
            return i
    return -1

#Driving code
if __name__=='__main__':
    print(linSearch([1,2,3,4,5,6,7,8,9,10],6))
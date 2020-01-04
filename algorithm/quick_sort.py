#Quick Sort Implementation, in place sort
#George Jen, Jen Tek LLC

def partition(arr,low,high):
    #pivot (Element to be placed at right position)
    pivot = arr[high]
    i=low-1
    
    for j in range(low,high): 
        # If current element is smaller than the pivot
        if arr[j] < pivot:
            i+=1;    # increment index of smaller element
            #swap arr[i] and arr[j]
            arr[i],arr[j]=arr[j],arr[i]
    arr[i+1],arr[high]=arr[high],arr[i+1]
    return i+1

def quickSort(arr,low,high):
    if low<high:
        pi = partition(arr, low, high)
        quickSort(arr,low,pi-1)
        quickSort(arr,pi+1,high)

#Driver code:
if __name__=='__main__':
    arr=[10,2,5,6,4]
    print("Before quick sort {}".format(arr))
    quickSort(arr,0,len(arr)-1)
    print("After quick sort {}".format(arr))
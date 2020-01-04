#Insersion Sort
#Create the sorted output array the same length of unsorted input array, please each element of input array into the right
#index of output array with list.insert() and list.append() methods
#George Jen,  Jen Tek LLC

def insersionSort(arr):
    output_list=[]
    p=0
    while p<len(arr):
        if p==0:
            output_list.append(arr[0])
            p+=1
            continue
        if arr[p]<output_list[0]:
            output_list.insert(0,arr[p])
            p+=1
            continue
        else:
            #find the right position to insert
            insert=False
            for i in range(1,len(output_list)):
                if arr[p]<output_list[i]:
                    output_list.insert(i,arr[p])
                    p+=1
                    insert=True
                    break
            if insert==False:
                output_list.append(arr[p])
                p+=1
                continue
    return output_list     
            
#Driving code
if __name__=='__main__':
    A=[10,50,2,3,60]
    print(A)
    print(insersionSort(A))
    
    

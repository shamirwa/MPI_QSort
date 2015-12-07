#include "header.h"
#include <string.h>

#define DPRINTF 0

// Compare function used in qsort
int compare (const void * a, const void * b)
{
        int key_a = *(int*)a;
        int key_b = *(int*)b;
        return (key_a - key_b);
}

// Binary Search Api to find the pivot position in the array.
int searchPivotIndex(int *inputData, int lowValue, int highValue, int keyValue) 
{ 
    int midIndex, midValue;
    while (lowValue <= highValue) {
        midIndex = lowValue + (highValue - lowValue) / 2;
        midValue = inputData[midIndex];
        if (midValue < keyValue)
            lowValue = midIndex + 1;
        else if (midValue > keyValue)
            highValue = midIndex - 1;
        else
            return midIndex; 
    }
    return -1*(lowValue + 1);
}

// Merge Api to merge two arrays in sorted order
int* mergeOrigRecvd(int *origArray, int lengthOrig, int *rcvdArray, int lengthRcvd){

    int i = 0, j = 0, k =0;
    int totalLength = lengthOrig + lengthRcvd;
    int* finalMergedArray = (int*)malloc(totalLength * sizeof(int));
    
    while(i < lengthOrig && j < lengthRcvd){
        if(origArray[i] < rcvdArray[j]){
            finalMergedArray[k++] = origArray[i++];
        }else{
            finalMergedArray[k++] = rcvdArray[j++];
        }
    }
    // If element is left in origArray then copy them directly into final array
    memcpy(finalMergedArray + k, origArray + i, (lengthOrig - i) * sizeof(int));
    
    // If elements are left in rcvdArray then copy them directly into final array
    memcpy(finalMergedArray + k, rcvdArray + j, (lengthRcvd - j)* sizeof(int));

    return finalMergedArray;
}


int* mpiqsort(int* input, int globalNumElements, int* dataLengthPtr, MPI_Comm comm, int commRank, int commSize) {

    // Local sort
    qsort(input, *dataLengthPtr, sizeof(int), compare);
    
    // Find the dimension
    int i = 0;
    int dimension = 0;
    int temp = commSize>>1;
    for(; temp>0; temp = temp >> 1){
        dimension++;
    }
    int pByNodesNum = 0;
    int errorCode = 0;

    for(i=0; i<dimension; i++){
        int median = 0, receiverID = 0, senderID = 0; 
        pByNodesNum = commSize / (1<<i);

        if((commRank % pByNodesNum) == 0){
            median = input[(*dataLengthPtr) / 2];
            receiverID = commRank + 1;

            // Send messages to 
            for(;receiverID < (commRank + pByNodesNum); receiverID++){

                errorCode = MPI_Send(&median, 1, MPI_INT, receiverID, 0, comm);
                if (errorCode != MPI_SUCCESS && DPRINTF){
                    printf("Error: %d Failed to send the median value to %d\n", commRank, receiverID);
                    return;
                }
            }
        }
        else{
            senderID = commRank - (commRank % pByNodesNum);
            errorCode = MPI_Recv(&median, 1,  MPI_INT, senderID, MPI_ANY_TAG, comm, NULL);

            if(errorCode != MPI_SUCCESS && DPRINTF){
                printf("Error: %d Failed to receive the median value from %d\n", commRank, senderID);
                return;
            }
        }

        // Partition the local Array on the basis of received pivot value.
        int pivotIndex = searchPivotIndex(input, 0, *dataLengthPtr, median);
        int numOfLargeElements = 0;
        int numOfSmallerElements = 0;


        if (pivotIndex < 0){
            pivotIndex = -1 * (pivotIndex + 1);
        }else{
            pivotIndex += 1;
        }

        if (pivotIndex >= *dataLengthPtr){
            numOfLargeElements = 0;
            numOfSmallerElements = *dataLengthPtr;
            pivotIndex = *dataLengthPtr;
        }else{
            numOfLargeElements = *dataLengthPtr - pivotIndex;
            numOfSmallerElements = pivotIndex;
        }

        // Variable to store the num of elements received and elements that are received
        int numOfElementsRcvd = 0;
        int *receivedElements = NULL;
        int *mergedArray = NULL;

        if ((commRank % pByNodesNum) < (pByNodesNum / 2)){
            // Transmit the portion greater than m to adjacent processor having msb as 1
            receiverID = commRank + (pByNodesNum / 2);

            // Send the length of elements first
            errorCode = MPI_Send(&numOfLargeElements, 1, MPI_INT, receiverID, 0, comm);
            if(errorCode != MPI_SUCCESS && DPRINTF){
                printf("Error: %d Failed to send the number of elements greater than median to %d\n", commRank, receiverID);
                return;
            }

            errorCode = MPI_Send(input + pivotIndex, numOfLargeElements, MPI_INT, receiverID, 0, comm);
            if(errorCode != MPI_SUCCESS && DPRINTF){
                printf("Error: %d Failed to send the elements greater than median value to %d\n", commRank, receiverID);
                return;
            }
            // Update the dataLength Pointer
            *dataLengthPtr = numOfSmallerElements;

            // Receive the elements from adjacent processor having msb 0
            senderID = receiverID;

            errorCode = MPI_Recv(&numOfElementsRcvd, 1, MPI_INT, senderID, MPI_ANY_TAG, comm, NULL);
            if(errorCode != MPI_SUCCESS && DPRINTF){
                printf("Error: %d Failed to receive the num of elements less than median from sender %d\n", commRank, senderID);
                return;
            }

            receivedElements = (int*)malloc(numOfElementsRcvd * sizeof(int));

            errorCode = MPI_Recv(receivedElements, numOfElementsRcvd, MPI_INT, senderID, MPI_ANY_TAG, comm, NULL);
            if(errorCode != MPI_SUCCESS && DPRINTF){
                printf("Error: %d Failed to receive the elements less than median value from sender %d\n", commRank, senderID);
                return;
            }
            
            if (DPRINTF){
                printf("In if before merge OrigArrayLength %d and numElementsRcvd %d\n", *dataLengthPtr, numOfElementsRcvd);
            }
            mergedArray = mergeOrigRecvd(input, *dataLengthPtr, receivedElements, numOfElementsRcvd);

            // Update the data pointer after merging the array
            *dataLengthPtr += numOfElementsRcvd;
            input = mergedArray;
        }else{

            senderID = commRank - (pByNodesNum / 2);
            errorCode = MPI_Recv(&numOfElementsRcvd, 1, MPI_INT, senderID, MPI_ANY_TAG, comm, NULL);
            if(errorCode != MPI_SUCCESS && DPRINTF){
                printf("Error: %d Failed to receive the num of elements greater than pivot from sender %d\n",commRank, senderID);
                return;
            }

            receivedElements = (int*)malloc(numOfElementsRcvd * sizeof(int));
            errorCode = MPI_Recv(receivedElements, numOfElementsRcvd, MPI_INT, senderID, MPI_ANY_TAG, comm, NULL);
            if(errorCode != MPI_SUCCESS && DPRINTF){
                printf("Error: %d Failed to receive the elements greater than pivot value from sender %d\n",commRank, senderID);
                return;
            }

            // Transmit the portion of x less than median to adjacent processor whose msb is 0
            receiverID = senderID;
            errorCode = MPI_Send(&numOfSmallerElements, 1, MPI_INT, receiverID, 0, comm);
            if(errorCode != MPI_SUCCESS && DPRINTF){
                printf("Error: %d Failed to send the num of elements less than pivot to receiver %d\n",commRank,receiverID);
                return;
            }

            errorCode = MPI_Send(input, numOfSmallerElements, MPI_INT, receiverID, 0, comm);
            if(errorCode != MPI_SUCCESS && DPRINTF){
                printf("Error: %d Failed to send the elements less than pivot to the receiver %d\n", commRank, receiverID);
                return 0;
            }

            // Update the input array
            input += pivotIndex;
            *dataLengthPtr = numOfLargeElements;

            if (DPRINTF){
                    printf("In if before merge OrigArrayLength %d and numElementsRcvd %d\n", *dataLengthPtr, numOfElementsRcvd);
            }
                                
            mergedArray = mergeOrigRecvd(input, *dataLengthPtr, receivedElements, numOfElementsRcvd);

            // Update the dataPointer with the number of elements in the merged array
            *dataLengthPtr += numOfElementsRcvd; 
            input = mergedArray;

        }

    }

    return input;
}


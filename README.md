# Project: DFS With Probabilistic Routing Version SFDFS

## Controller
Controller recieves the heartbeat from the storage nodes and keeps track of the storage nodes that are alive 

message HeartBeat{
    string ipAddress = 1;
    string port = 2;
    int64 spaceRemaining = 3;
    int64 requestProcessed = 4;
    int64 retrievalProcessed = 5;
}

The heartbeat contains the identification of the storage node i.e. the IPAddress and Port, and the statistics for spaceRemaining, requestProcessed and retrievalProcessed.

Controller recieves requests from client to retrieve or save a file. 
Save : In case of saving the file is broken into chunks by the client and sent to the controller with the file Metadata.

message ChunkMeta {
    string fileName = 1;
    int32 chunkId = 2;
    int32 chunkSize = 3;
    int32 totalChunks = 4;
    repeated string storageNodeIds = 5;
}

The Controller replies with list of storage nodes containing the primary and 2 replicas for each chunk using the bloomfilters.

Retrieval: The Controller recieves a request from the client to retrieve a file 

message RetrieveFile {
    string fileName = 1;
}

The controller prepares a check meta message for the first chunk and asks the storage node (probabilistically) containing the chunk for the metadata of the file.

message RetrieveChunkMeta{
    string fileChunkId = 1;
}

When the controller receives the metadata, the controller gets the mapping for chunkId to StorageNodes and sends to the client for retrieval

message MappingChunkIdToStorageNodes{
    map<string,StorageNodesHavingChunk> mapping = 1;
}



## Storage Nodes 
Storage Node stores the file chunks and chunk meta data in disk and these chunks can be retrieved through chunkName.

a. Store chunk message is used to store data and meta of chunk on disk.

message StoreChunk {
     string fileName = 1;
     int32 chunkId = 2;
     int32 chunkSize = 3;
     int32 totalChunks = 4;
     repeated string storageNodeIds = 5;
     bytes data = 6;
     string toStorageNodeId = 7;
 }
 
b. Storage node receive 

message RetrieveChunk{
    string fileChunkId = 1;
    repeated string storageNodeIds = 2;
} and responds back with message Chunk {
    bool found = 1;
    string fileChunkId = 2;
    bytes data = 3;
    repeated string storageNodeIds = 4;
}

c. Storage node receive 

message RetrieveChunkForBadChunk{
    string fileChunkId = 1;
    repeated string storageNodeIds = 2;
    string primaryNode = 3;
} and responds back with message ChunkForBadChunk {
    bool found = 1;
    string fileChunkId = 2;
    bytes data = 3;
    repeated string storageNodeIds = 4;
    string primaryIdForChunk = 5;
}

d. Storage node receives 

message BecomePrimary{
    string forApAddress = 1;
    string forPort = 2;
    repeated string askIds = 3;
} and becomes primary for forApAddress-forPort chunks.

e. Storage node receieves 

message CreateNewReplica {
    string lostReplicaId = 1;
    string newReplicaId = 2;
} and sends new filechunks to new replica.

f. Whenever a bad chunk is found Storage node sends 

message BadChunkFound{
    string selfId = 1;
    string fileChunkId = 2;
    string primaryIdForChunk = 3;
}, 

Controller responds back with 

message HealBadChunk{
    string selfId = 1;
    string badFileChunkId = 2;
    repeated string storageNodes = 3;
    string primaryIdForChunk = 4;
}, 

Storage node uses the storage node list to ask for the particular chunk.
 
 
 

## Client
Saving : The client breaks a file to be stored into multiple chunks and sends a chunkMetaMsg for each chunk, the controller replies with a list of storage nodes for each chunk, primary and 2 replicas. 

message ChunkMeta {
    string fileName = 1;
    int32 chunkId = 2;
    int32 chunkSize = 3;
    int32 totalChunks = 4;
    repeated string storageNodeIds = 5;
}

Retrieval : On requesting for a retrieval of a file, the client recieves a mapping of all the chunkIds to storage Nodes.

message MappingChunkIdToStorageNodes{
    map<string,StorageNodesHavingChunk> mapping = 1;
}

The Storage Nodes reply with the bytes for a requested chunk

message Chunk {
    bool found = 1;
    string fileChunkId = 2;
    bytes data = 3;
    repeated string storageNodeIds = 4;
}

## Bloom Filter Configuration 
{
  "filterSize" : "5000000",
  "hashes" : "3"
}

## Storing Chunks
To store a file in sfdfs:
First the Client gets the file size, to know the number of chunks the file will have. Then sends the ChunkMeta for each chunk to controller. Controller responds by sending a list of storage nodes (with primary at index 0) to client for every chunkmeta received.
Secondly, the client sends each StoreChunk message to first storage node in the list.

Storage node store the storeChunk data in chunkFiles folder and metadata in metaFiles inside the folder of primary storage node. Then the node forwards the storechunk to the next storage node in the list.

## Retrieval 
To retrieve a file:
Client sends the name of the file to the Controller.
Controller receives the request, creates a 1st chunkId for filename and finds the primary storage nodes from bloomfilter and replicas from StorageNodeGroupRegister. And sends a request to receive chunkmeta data for the first chunk. 
From the detail in chunk meta that storage node sends back, Controller understands total number of chunks for the file.

It then prepares a map of chunkid to storage nodes with help of bloomfilter and stroageNodeGroupRegister and sends it back to client.

Client takes the mapping and begin asking for each chunk from stroage node async.

## Fault Tolerance
Storage node goes down :
When a controller does not receive heartbeat periodicaly in 5 seconds, it considers that storage node has stopped working.
To handle stopped storage node,Controller sends become primary to one of failed storage node replica. And the chunks are sent to replicas of the new primary. A new replica is chosen (which as max free space) if the new primary does not have enough replicas.
Controller also sends create new replica msg to all the storage nodes that has the failed node as one of their replica. Storage node then sends Chunks in primary folder to the new replica.

Corrupted chunk :
If the checksum of the filechunk does not match with the metadata held in the memory. Then storage node sends chunk not found message to client. So client moves on to the next storage node in list and ask for the chunk. 
In the mean while, storage node that found a bad chunk sends a badchunk found message to controller. Controller sends back a heal bad chunk message which contains a list of storage nodes that also have the chunk. Storage node uses this list and ask for the chunk from the fellow storage nodes in the list.



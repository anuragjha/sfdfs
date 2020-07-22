# Project: DFS With Probabilistic Routing Version - SFDFS

In this project, we will build your own distributed file system (DFS) based on the technologies we’ve studied from Amazon, Google, and others. Our DFS will support multiple storage nodes responsible for managing data. Key features include:

- Probabilistic Routing: to enable lookups without requiring excessive RAM, client requests will be routed probabilistically to relevant storage nodes via a collection of bloom filters.
- Entropy-Driven Compression: file entropy is analyzed before storage; low-entropy files will be compressed before storage to save disk space.
- Parallel retrievals: large files will be split into multiple chunks. Client applications retrieve these chunks in parallel using threads.
- Interoperability: the DFS will use Google Protocol Buffers to serialize messages. Do not use Java serialization. This allows other applications to easily implement your wire format.
- Fault tolerance: your system must be able to detect and withstand two concurrent storage node failures and continue operating normally. It will also be able to recover corrupted files.

### Controller
The Controller is responsible for managing resources in the system, somewhat like an HDFS NameNode. When a new storage node joins your DFS, the first thing it does is contact the Controller. At a minimum, the Controller contains the following data structures:

A list of active storage nodes
A routing table (set of bloom filters for probabilistic lookups)
When clients wish to store a new file, they will send a storage request to the controller, and it will reply with a list of destination storage nodes (plus replica locations) to send the chunks to. The Controller itself should never see any of the actual files, only their metadata.

To maintain the routing table, you will implement a bloom filter of file names for each storage node. When the controller receives a file retrieval request from a client, it will query the bloom filter of each storage node with the file name and return a list of matching nodes (due to the nature of bloom filters, this may include false positives).

The Controller is also responsible for detecting storage node failures and ensuring the system replication level is maintained. In your DFS, every chunk will be replicated twice for a total of 3 duplicate chunks. This means if a system goes down, you can re-route retrievals to a backup copy. You’ll also maintain the replication level by creating more copies in the event of a failure. You will need to design an algorithm for determining replica placement.

### Storage Node
Storage nodes are responsible for storing and retrieving file chunks. When a chunk is stored, it will be checksummed so on-disk corruption can be detected. When a corrupted file is retrieved, it should be repaired by requesting a replica before fulfilling the client request.

Some messages that your storage node could accept (although you are certainly free to design your own):

Store chunk [File name, Chunk Number, Chunk Data]
Get number of chunks [File name]
Get chunk location [File name, Chunk Number]
Retrieve chunk [File name, Chunk Number]
List chunks and file names [No input]
Metadata (checksums, chunk numbers, etc.) should be stored alongside the files on disk.

After receiving a storage request, storage nodes should calculate the Shannon Entropy of the files. If their maximum compression is greater than 0.6 (1 - (entropy bits / 8)), then the chunk should be compressed before it is written to disk. You are free to choose the compression algorithm, but be prepared to justify your choice.

The storage nodes will send a heartbeat to the controller periodically to let it know that they are still alive. Every 5 seconds is a good interval for sending these. The heartbeat contains the free space available at the node and the total number of requests processed (storage, retrievals, etc.).

On startup: provide a storage directory path and the hostname/IP of the controller. Any old files present in the storage directory should be removed.

### Client
The client’s main functions include:

Breaking files into chunks, asking the controller where to store them, and then sending them to the appropriate storage node(s).
Note: Once the first chunk has been transferred to its destination storage node, that node will pass replicas along in a pipeline fashion. The client should not send each chunk 3 times.
If a file already exists, replace it with the new file. If the new file is smaller than the old, you are not required to remove old chunks (but file retrieval should provide the correct data).
Retrieving files in parallel. Each chunk in the file being retrieved will be requested and transferred on a separate thread. Once the chunks are retrieved, the file is reconstructed on the client machine.
The client will also be able to print out a list of active nodes (retrieved from the controller), the total disk space available in the cluster (in GB), and number of requests handled by each node.

NOTE: Your client must either accept command line arguments or provide its own text-based command entry interface.


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



package edu.usfca.cs.dfs.controllerNode;

import edu.usfca.cs.dfs.ClientNode;
import edu.usfca.cs.dfs.controllerNode.data.FileMetaData;
import edu.usfca.cs.dfs.controllerNode.data.StorageNodeDetail;
import edu.usfca.cs.dfs.data.FileChunkId;
import edu.usfca.cs.dfs.data.NodeId;
import edu.usfca.cs.dfs.filter.BloomFilter;
import edu.usfca.cs.dfs.init.ConfigSystemParam;


import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ControllerDS {

    private static ControllerDS controllerDS = null;

    private ControllerDS(){
    }

    public static ControllerDS getInstance(){
        if(controllerDS == null){
            controllerDS =  new ControllerDS();
        }
        return controllerDS;
    }

    // here key = ip+port
    private Map<String, StorageNodeDetail> storageNodeRegister = new ConcurrentHashMap<>();
    private Map<String, List<String>> storageNodeGroupRegister = new ConcurrentHashMap<>(); //exp
    //private Map<String,List<String>> requestToClientIdMap = new ConcurrentHashMap<>();
   // HashMap<String,ArrayList<String>> mapping = new HashMap<>();

    private FileMetaData fileMetaData = null;

    public void setFileMetaData(FileMetaData storageNodeFileMetaData){
        fileMetaData = storageNodeFileMetaData;
    }

    public FileMetaData getFileMetaData(){
        return fileMetaData;
    }

    public Map<String, StorageNodeDetail> getStorageNodeRegister() {
        return storageNodeRegister;
    }
    public Logger logger = Logger.getLogger(ControllerDS.class.getName());


    public String getStorageNodeKey(String ipAddress, String port) {
       return NodeId.getId(ipAddress,port);
       // return ipAddress + port;
    }

    public void updateStorageNodeRegister(StorageNodeDetail snd) {
        String key = getStorageNodeKey(snd.getIpAddress(), snd.getPort());

        if(storageNodeRegister.containsKey(key)){
            existInStorageNodeRegister(key, snd);

        } else {
            newInStorageNodeRegister(key,snd);
        }
    }

    private void newInStorageNodeRegister(String key, StorageNodeDetail snd) {
        storageNodeRegister.put(key,snd);
    }

    private void existInStorageNodeRegister(String key, StorageNodeDetail snd) {
        storageNodeRegister.get(key).setSpaceRemaining(snd.getSpaceRemaining());
        storageNodeRegister.get(key).setRequestProcessed(snd.getRequestProcessed());
        storageNodeRegister.get(key).setRetrievalProcessed(snd.getRetrievalProcessed());
        storageNodeRegister.get(key).setTimeStamp(snd.getTimeStamp());
    }

    public void deleteFromStorageNodeRegister(String key) {
        if (storageNodeRegister.containsKey(key)) {
            storageNodeRegister.remove(key);
        }
    }

    public String findTheStorageNodeToSaveChunk(int size){
        String storageNodeKey = new String();
        storageNodeKey = getStorageNodesMinProcessing(size);
        return storageNodeKey;
    }

    public String getSNWithMaxSpace(int requiredChunkSize){
        String node = "";
        long size = 0;

        Iterator storageNodeIterator = storageNodeRegister.entrySet().iterator();
        while (storageNodeIterator.hasNext()){
            Map.Entry storageNode = (Map.Entry) storageNodeIterator.next();

            StorageNodeDetail details = (StorageNodeDetail) storageNode.getValue();

            if(size < details.getSpaceRemaining()){
                size =  details.getSpaceRemaining();
                node = (String) storageNode.getKey();
            }
        }
        if(size > requiredChunkSize) {
            return node;
        }else{
            logger.log(Level.INFO,"Give config json as param");

            //System.out.println("Storage Node size is less than the required Chunk size!!");
            return "";
        }
    }

    public String getStorageNodesMinProcessing(int requiredChunkSize){
        String node = "";
        long min = 0;
        Iterator storageNodeIterator = storageNodeRegister.entrySet().iterator();
        while (storageNodeIterator.hasNext()){

            Map.Entry storageNode = (Map.Entry) storageNodeIterator.next();

            StorageNodeDetail details = (StorageNodeDetail) storageNode.getValue();

            if(min >= (details.getRequestProcessed()+details.getRetrievalProcessed())){
                min = (details.getRequestProcessed()+details.getRetrievalProcessed());
                node = (String) storageNode.getKey();
            }
        }
        if(storageNodeRegister.containsKey(node)){
            if(storageNodeRegister.get(node).getSpaceRemaining() > requiredChunkSize){
                return node;
            }
        }
//        else{
            //System.out.println("Storage Node size is less than the required Chunk size!!");
            return (String)storageNodeRegister.keySet().toArray()[0];
//        }
    }

    public String getSNWithMaxSpaceExcludingTheSNs(ArrayList<String> storageNodes){
        String node = "";
        long size = 0;

        Iterator storageNodeIterator = storageNodeRegister.entrySet().iterator();
        while (storageNodeIterator.hasNext()){
            Map.Entry storageNode = (Map.Entry) storageNodeIterator.next();

            String key = (String) storageNode.getKey();

            if(storageNodes.size() > 0){
                if(!storageNodes.contains(key)){
                    StorageNodeDetail details = (StorageNodeDetail) storageNode.getValue();

                    if (size < details.getSpaceRemaining()) {
                        size = details.getSpaceRemaining();
                        node = (String) storageNode.getKey();
                    }
                }
            }

//            if(storageNodes.length == 2) {
//                if(key != storageNodes[1] && key != storageNodes[2]) {
//                    StorageNodeDetail details = (StorageNodeDetail) storageNode.getValue();
//
//                    if (size < details.getSpaceRemaining()) {
//                        size = details.getSpaceRemaining();
//                        node = (String) storageNode.getKey();
//                    }
//                }
//            }else if(storageNodes.length ==1){
//                if(key != storageNodes[1] ) {
//                    StorageNodeDetail details = (StorageNodeDetail) storageNode.getValue();
//
//                    if (size < details.getSpaceRemaining()) {
//                        size = details.getSpaceRemaining();
//                        node = (String) storageNode.getKey();
//                    }
//                }
//
//            }
        }
        if(size > 0) {
            return node;
        }else{
            logger.log(Level.INFO,"Storage Node size is less than the required Chunk size!!");
            //System.out.println("Storage Node size is less than the required Chunk size!!");
            return "";
        }
    }


    public ArrayList<String> getNewReplicas(int requiredChunkSize, String primaryNodeKey){
        ArrayList<String> replicas = new ArrayList<>();
        String replica1 = "";
        String replica2 = "";
        long size1 = 0;
        long size2 = 0;
        if(!storageNodeRegister.isEmpty()){
            Iterator storageNodeIterator = storageNodeRegister.entrySet().iterator();
            while (storageNodeIterator.hasNext()){
                Map.Entry storageNode = (Map.Entry) storageNodeIterator.next();
                if(storageNode.getKey() != primaryNodeKey){
                    StorageNodeDetail details = (StorageNodeDetail) storageNode.getValue();
                    if(size1 <  details.getSpaceRemaining()){
                        size2 = size1;
                        size1 =  details.getSpaceRemaining();

                        replica2 = replica1;
                        replica1 = (String) storageNode.getKey();
                    }else if(size2 <  details.getSpaceRemaining()){
                        size2 =  details.getSpaceRemaining();
                        replica2 = (String) storageNode.getKey();
                    }
                }
            }
        }
       if(size1 > requiredChunkSize){
            replicas.add( replica1);
           replicas.add( replica2);
       }
        return replicas;
    }

    public Map<String, List<String>> getStorageNodeGroupRegister(){
        return this.storageNodeGroupRegister;
    }


    /// storageNodeGroupRegister
    public void addAPrimaryNode(String primaryKey) {
        storageNodeGroupRegister.put(primaryKey, new ArrayList<>());
    }

    public boolean addAReplica(String primaryKey, String replicaKey) {
        int replication = ConfigSystemParam.getReplication();
        if((storageNodeGroupRegister.containsKey(primaryKey)) && storageNodeGroupRegister.get(primaryKey).size() < replication) {
            storageNodeGroupRegister.get(primaryKey).add(replicaKey);
            return true;
        }
        return false;
    }

    public ArrayList<String> checkStorageNodeGroupRegister(String node, int chunkSize){

        logger.log(Level.INFO,"Check Storage Node Group Register !!!! \n");
        //System.out.println("Check Storage Node Group Register !!!! \n");
        ArrayList<String> storageNodePrimaryReplicaDetails = new ArrayList<>();
        if(checkIfPrimaryExists(node)){
            storageNodePrimaryReplicaDetails.add(node);
            storageNodePrimaryReplicaDetails.addAll(getReplicaList(node));
           //System.out.println("Already exists : "+Arrays.toString(storageNodePrimaryReplicaDetails.toArray()));
        }else{
            storageNodePrimaryReplicaDetails.add(node);
            ArrayList<String> replicas = ControllerDS.getInstance().getNewReplicas(chunkSize,node);
            storageNodePrimaryReplicaDetails.addAll(replicas);
            storageNodeGroupRegister.put(node,replicas);
            //System.out.println("New : "+Arrays.toString(storageNodePrimaryReplicaDetails.toArray()));
        }
       // System.out.println("Size of the arraylist being returned : "+storageNodePrimaryReplicaDetails.size());
        return storageNodePrimaryReplicaDetails;
    }

    public boolean checkIfPrimaryExists(String node){
        return this.storageNodeGroupRegister.containsKey(node);
    }

    public List<String> getReplicaList(String node){
        return this.storageNodeGroupRegister.get(node);
    }


    public ArrayList<String> checkBloomFiltersForChunk(String chunkName){
        ArrayList<String> storageNodes = new ArrayList<>();

        if(!storageNodeRegister.isEmpty()){
            Iterator storageNodeIterator = storageNodeRegister.entrySet().iterator();
            while (storageNodeIterator.hasNext()) {
                Map.Entry storageNode = (Map.Entry) storageNodeIterator.next();

                StorageNodeDetail storageNodeDetail1 = (StorageNodeDetail) storageNode.getValue();
                BloomFilter bloomFilter = storageNodeDetail1.getBloomFilter();
                boolean inStorageNode = bloomFilter.getFromBloom(chunkName);
                if(inStorageNode) {
                   // System.out.println("present in the bloomfilter!!!");
                    String storageNodeKey = (String) storageNode.getKey();
                    storageNodes.add(storageNodeKey);
                    storageNodes.addAll(storageNodeGroupRegister.get(storageNodeKey));
                }else{
                    //System.out.println("Not in this bloomfilter!!!");
                }
            }
        }else {
            logger.log(Level.INFO,"The StorageNode register is empty! No storage Node Details are stored! ");
            // System.out.println("The StorageNode register is empty! No storage Node Details are stored! ");
        }
        return storageNodes;
    }

    public boolean storeChunkInBloomFilter(String storageNodeKey,String fileName,int chunkNumber){
        boolean stored = false;
       String chunkName =  FileChunkId.getFileChunkId(fileName,chunkNumber);

        if(!storageNodeRegister.isEmpty()){
           if(storageNodeRegister.containsKey(storageNodeKey)){
                StorageNodeDetail storageNodeDetail = storageNodeRegister.get(storageNodeKey);
                storageNodeDetail.storeInBloomFilter(chunkName);
                stored = true;
           }
        }else {
            logger.log(Level.INFO,"StorageNodeRegister is empty!");
            //System.out.println("StorageNodeRegister is empty!");
        }
        return stored;
    }

    public HashMap<String,ArrayList<String>> getMappingOfChunkIdToStorageNodes(String fileName ,int totalchunks){
        HashMap<String,ArrayList<String>> chunkIdToStorageNodeIds = new HashMap<String, ArrayList<String>>();

        for(int i = 1; i <= totalchunks;i++){

            String chunkId = FileChunkId.getFileChunkId(fileName,i);

            ArrayList<String> storageNodeIdsForAChunk = checkBloomFiltersForChunk(chunkId);

            if(storageNodeIdsForAChunk.size() == 0){
                logger.log(Level.INFO,"No Storage Nodes found !!! File cannot be found because one chunk is missing from the bloom filters of the storage nodes!!");
                //System.out.println("No Storage Nodes found !!! File cannot be found because one chunk is missing from the bloom filters of the storage nodes!!");
                return chunkIdToStorageNodeIds;
            } else if (storageNodeIdsForAChunk.size() > 0){
                logger.log(Level.INFO,"Chunk present in "+storageNodeIdsForAChunk.size()+" Storage Node!!");
                //System.out.println("Chunk present in "+storageNodeIdsForAChunk.size()+" Storage Node!!");
            }
            chunkIdToStorageNodeIds.put(chunkId,storageNodeIdsForAChunk);
        }
        return chunkIdToStorageNodeIds;
    }

    //Recovery

    //Getting
    public List<String> getReplicasForTheStorageNode (String nodeId){
        List<String> replicas = null;
        if(storageNodeGroupRegister.containsKey(nodeId)) {
            replicas = storageNodeGroupRegister.get(nodeId);

            logger.log(Level.INFO,"Getting the size of the replicas being returned : "
                    +replicas.size()+" replicas are "+Arrays.toString(replicas.toArray()));
            //System.out.println("Getting the size of the replicas being returned : "
            //        +relicas.size()+" replicas are "+Arrays.toString(relicas.toArray()));
        }
        return replicas;
    }

    //Delete the storage node from the StorageNode register and from the storageNodeGroupRegister
    public boolean deleteTheStorageNode(String nodeIdToBeDeleted){
        boolean deleted = false;
        if(!storageNodeGroupRegister.isEmpty()){
            if(storageNodeGroupRegister.containsKey(nodeIdToBeDeleted)){
                storageNodeGroupRegister.remove(nodeIdToBeDeleted);
                if(storageNodeRegister.containsKey(nodeIdToBeDeleted)) {
                    storageNodeRegister.remove(nodeIdToBeDeleted);
                }
            }else{
                logger.log(Level.INFO,"Storage Node Group register does not contain the node Id to be deleted.");
                //System.out.println("Storage Node Group register does not contain the node Id to be deleted.");
            }
        }else {
            logger.log(Level.INFO,"The storage node group register is empty.");
            //  System.out.println("The storage node group register is empty.");
        }
        return deleted;
    }

    //getting the storageNodes replicas stored in a Storage node to be deleted and delete the Storage node from the replica list of these storageNodes
    public ArrayList<String> getStorageNodesWithReplicaInNodeToBeDeleted (String nodeId){
        ArrayList<String> storageNodesWithReplicasInThisNode = new ArrayList<>();
        if(!storageNodeGroupRegister.isEmpty()){
            Iterator storageNodeGroupIterator = storageNodeGroupRegister.entrySet().iterator();
            while (storageNodeGroupIterator.hasNext()){
                Map.Entry storageNodeGroup = (Map.Entry)storageNodeGroupIterator.next();
                String key = (String) storageNodeGroup.getKey();
                if (key != nodeId){
                    List<String> listOfReplicas = (List<String>)storageNodeGroup.getValue();
                    if(listOfReplicas.contains(nodeId)){
                        storageNodesWithReplicasInThisNode.add(key);
                        //Delete the entry that has the storage node to be deleted as replica
                        listOfReplicas.remove(nodeId);
                        storageNodeGroup.setValue(listOfReplicas);
                    }
                }
            }
        }else{
            logger.log(Level.INFO,"Storage Node Group Register is empty");
           // System.out.println("Storage Node Group Register is empty");
        }
        return storageNodesWithReplicasInThisNode;
    }

    public String getReplicaWithMaxSpace(List<String> replicas){

        String node = "";
        long space = 0;

        for(String replica : replicas) {
            StorageNodeDetail storageNodeDetail = storageNodeRegister.get(replica);
            if(storageNodeDetail!=null) {
                if (space < storageNodeDetail.getSpaceRemaining()) {
                    node = replica;
                    space = storageNodeDetail.getSpaceRemaining();
                }
            }else{
                logger.log(Level.INFO,"The Storage Node Details for \"+replica+\" is null.");
                //System.out.println("The Storage Node Details for "+replica+" is null.");
            }
        }

        return node;
    }

    public  void faultToleranceWhenAStorageNodeIsDown(String nodeId,StorageNodeDetail oldStorageNodeDetail){

        //Get the replicas of the storage node to be deleted
        logger.log(Level.INFO,"Found the list of replicas for the node that is down");
        //System.out.println("Found the list of replicas for the node that is down");
        List<String> replicas = getReplicasForTheStorageNode(nodeId);

        logger.log(Level.INFO,"get the list of replicas that the Storage node to be deleted stores");
      //  System.out.println("get the list of replicas that the Storage node to be deleted stores");
        //get the list of replicas that the Storage node to be deleted stores
        List<String> storageNodesToReplicate = getStorageNodesWithReplicaInNodeToBeDeleted(nodeId);

        logger.log(Level.INFO,"Delete the storage node");
        //System.out.println("Delete the storage node");
        //Delete the storage node
        deleteTheStorageNode(nodeId);

        String replicaPresentInStorageNodesToReplicate = "";
        String newPrimaryNode = "";

//        for (String replica : replicas){
//            if(storageNodesToReplicate.contains(replica)){
//                replicaPresentInStorageNodesToReplicate  = replica;
//            }else if(replicaPresentInStorageNodesToReplicate != ""){
//                newPrimaryNode = replica;
//                //as there will be only two replicas if the replicaPresentInStorageNodesToReplicate is not empty make the other replica the primary
//            }
//        }
//        if(replicaPresentInStorageNodesToReplicate.length() == 0) {
            //Get the new primary node by comparing the replicas

        if(replicas != null && replicas.size() > 0) {
            newPrimaryNode = getReplicaWithMaxSpace(replicas);
            logger.log(Level.INFO,"New Primary : " + newPrimaryNode);
           // System.out.println("New Primary : " + newPrimaryNode);
        }else {
            newPrimaryNode = getStorageNodesMinProcessing(0);
        }
//        }
        List<String> newReplicas = getReplicasForTheStorageNode(newPrimaryNode);

        if(newReplicas != null && newReplicas.size() == 1){
            //Get one more replica and add in this list
            String storageNodesToExclude = newReplicas.get(0);

            ArrayList<String> nodesToExclude = new ArrayList<>();
            nodesToExclude.add(newPrimaryNode);
            nodesToExclude.add(storageNodesToExclude);

            String replica = getSNWithMaxSpaceExcludingTheSNs(nodesToExclude);
            List<String> oldReplicas = storageNodeGroupRegister.get(newPrimaryNode);
            oldReplicas.add(replica);
            storageNodeGroupRegister.put(newPrimaryNode,oldReplicas);

            newReplicas.add(replica);
        }else if(newReplicas == null){
            newReplicas = new ArrayList<>();
            newReplicas.addAll(getNewReplicas(0,newPrimaryNode));
            storageNodeGroupRegister.put(newPrimaryNode,newReplicas);
        }
        logger.log(Level.INFO,"Replicas : "+newReplicas.toString());
        //System.out.println("Replicas : "+newReplicas.toString());
        //Contact the new primary and complete the data transfer also complete the replication
        ControllerNodeHelper.becomeNewPrimary(newPrimaryNode,newReplicas,nodeId);

        //update the bloomfilters
        boolean result = updateBloomFilter(newPrimaryNode,oldStorageNodeDetail);

        logger.log(Level.INFO,"Bloomfilter updated successfully : "+result);
        //System.out.println("Bloomfilter updated successfully : "+result);
        //choose a new node for replicas and

        ArrayList<String> storageNodeToExclude = new ArrayList<> ();
        storageNodeToExclude.add(newPrimaryNode);
        storageNodeToExclude.addAll(storageNodesToReplicate);
        storageNodeToExclude.addAll(getListOfReplicasForTheNodes(storageNodesToReplicate));

        String storageNodeToReplicate = getSNWithMaxSpaceExcludingTheSNs(storageNodeToExclude);

    }

    public List<String> getListOfReplicasForTheNodes(List<String> nodes){
        List<String> listOfReplicas = new ArrayList<>();
        if (nodes.size() >0) {
            for (String node : nodes){
                if(storageNodeGroupRegister.containsKey(node)){
                    logger.log(Level.INFO,"Checking for node : "+ node);
                    //System.out.println("Checking for node : "+ node);
                    listOfReplicas.addAll(storageNodeGroupRegister.get(node));
                }
            }
        }
        return listOfReplicas;
    }

    public boolean updateBloomFilter(String newPrimary,StorageNodeDetail storageNodeDetail){
        StorageNodeDetail storageNodeDetailNew = storageNodeRegister.get(newPrimary);
        BloomFilter filteNew = storageNodeDetailNew.getBloomFilter();
        BloomFilter filterOld = storageNodeDetail.getBloomFilter();
        return filteNew.mergeBloomFilters(filterOld);
    }

    public String[] convertArrayListOfStringToArrayOfString(List<String> nodes){
        Object[] objects = nodes.toArray();
        String[] arrayOfString = Arrays
                .copyOf(objects, objects
                                .length,
                        String[].class);

        return arrayOfString;
    }
}

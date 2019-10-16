package edu.usfca.cs.dfs.storageNode;

import edu.usfca.cs.dfs.clientNode.ClientStorageMessagesHelper;
import edu.usfca.cs.dfs.init.ConfigSystemParam;
import edu.usfca.cs.dfs.net.MessageSender;
import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.data.FileChunkId;
import edu.usfca.cs.dfs.data.NodeId;
import edu.usfca.cs.dfs.fileUtil.Entropy;
import edu.usfca.cs.dfs.fileUtil.Fileify;
import edu.usfca.cs.dfs.fileUtil.Zipper;
import edu.usfca.cs.dfs.net.InboundHandler;
import edu.usfca.cs.dfs.storageNode.data.ChunkFileMeta;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class StorageInboundHandler extends InboundHandler {

    @Override
    public void channelRead0(ChannelHandlerContext ctx, StorageMessages.StorageMessageWrapper msg) {

        if(msg.hasStoreChunkMsg()) {
            System.out.println("\n**************************storage receieved store chunk *******************************\n");
            System.out.println("Size of storage node list : "+ msg.getStoreChunkMsg().getStorageNodeIdsList().size());
            // 1. create a directory, where directory name is 1st storage node in storageNodeIds field
            String nodeDir = System.getProperty("user.home")+"/sfdfs_"+ msg.getStoreChunkMsg().getToStorageNodeId()+"/"+msg.getStoreChunkMsg().getStorageNodeIds(0);
            Fileify.createDirectory(nodeDir);
            // 2. read the bytes in data field - getData()
            byte[] dataArr = msg.getStoreChunkMsg().getData().toByteArray();
            // 3. check for shanon entropy
            double entropy = Entropy.entropy(dataArr);
            // 4. if entropy is low - compress using gzip
            boolean isCompressed = false;
            if(entropy < 0.6) {
                dataArr = Zipper.compress(dataArr.clone());
                isCompressed = true;
            }
            // 5. calculate checksum of compressed or uncompressed data in byte[]
            int checksum = Arrays.hashCode(dataArr);
            // 6. prepare chunkFileMeta top be stored for particular chunkData
            ChunkFileMeta forMetaFile = new ChunkFileMeta(
                    msg.getStoreChunkMsg().getFileName(),
                    msg.getStoreChunkMsg().getChunkId(),
                    msg.getStoreChunkMsg().getChunkSize(),
                    msg.getStoreChunkMsg().getTotalChunks(),
                    msg.getStoreChunkMsg().getStorageNodeIdsList(),
                    entropy,
                    isCompressed,
                    checksum
            );
            String forMetaFileJson = forMetaFile.toJson();
            // 7. check and create 1st storageNodeId meta and chunk dir
            String nodeMetaDir = nodeDir+"/metaFiles/";
            Fileify.createDirectory(nodeMetaDir);
            String nodeChunkDir = nodeDir+"/chunkFiles/";
            Fileify.createDirectory(nodeChunkDir);
            String fileChunkId = FileChunkId.getFileChunkId(msg.getStoreChunkMsg().getFileName(),msg.getStoreChunkMsg().getChunkId());
            // 8. store chunkFileMeta in a file inside metaFiles dir
            String metaFilePath = nodeMetaDir+"/"+fileChunkId;
            //String metaFilePath = nodeMetaDir+"/"+msg.getStoreChunkMsg().getFileName()+"_chunk_"+msg.getStoreChunkMsg().getChunkId();
            boolean isMetaWritten = Fileify.writeToAFile(metaFilePath, forMetaFileJson);
            // 9. store fileChunk in a file inside chunkFiles dir
            String chunkFilePath = nodeChunkDir+"/"+fileChunkId;
            //String chunkFilePath = nodeChunkDir+"/"+msg.getStoreChunkMsg().getFileName()+"_chunk_"+msg.getStoreChunkMsg().getChunkId();
            boolean isChunkWritten = Fileify.writeToAFile(chunkFilePath, msg.getStoreChunkMsg().getData().toByteArray());
            // 10. check if everything done
            if(isMetaWritten && isChunkWritten) {
                StorageNodeDS.getInstance().getChunksMetaInfo().put(fileChunkId, forMetaFile);
                System.out.println("Meta and Chunk saved on Storage node :->");
            } else {
                System.out.println("Something went wrong in Meta and Chunk saved on Storage node :-<");
            }

            ctx.close();
            //
            // forwarding storeChunk to other replica
            //
            if ( msg.getStoreChunkMsg().getStorageNodeIdsList().size() > 1 &&
                    msg.getStoreChunkMsg().getToStorageNodeId().equals(msg.getStoreChunkMsg().getStorageNodeIds(0))) { // in primary node
                // change toaddress in strorechunk message to 2nd replica and send to 2nd replica
                String[] sendingInfo = NodeId.getIPAndPort(msg.getStoreChunkMsg().getStorageNodeIds(1));
                try {
                    new MessageSender().send(
                            false, "storage", sendingInfo[0], Integer.parseInt(sendingInfo[1]),
                            StorageStorageMessagesHelper.prepareStoreChunkMsgForReplica(msg, 1));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            } else if(msg.getStoreChunkMsg().getStorageNodeIdsList().size() > 2 &&
                    msg.getStoreChunkMsg().getToStorageNodeId().equals(msg.getStoreChunkMsg().getStorageNodeIds(1))) { //in 1st replica
                // change toaddress in strorechunk message to 3rd replica and send to 3rd replica
                String[] sendingInfo = NodeId.getIPAndPort(msg.getStoreChunkMsg().getStorageNodeIds(2));
                try {
                    new MessageSender().send(
                            false, "storage", sendingInfo[0], Integer.parseInt(sendingInfo[1]),
                            StorageStorageMessagesHelper.prepareStoreChunkMsgForReplica(msg, 2));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            } else if(msg.getStoreChunkMsg().getToStorageNodeId().equals(msg.getStoreChunkMsg().getStorageNodeIds(2))) { // in 2nd replica

            }
        }  // store chunk and send to replica if needed
        else if(msg.hasRetrieveChunkMetaMsg()) {
            System.out.println("RetrieveChunkMeta received from controller");

            String fileChunkId = msg.getRetrieveChunkMetaMsg().getFileChunkId();
            ChunkFileMeta chunkFileMeta = StorageNodeDS.getInstance().getChunkMetaInfo(fileChunkId);

            StorageMessages.StorageMessageWrapper msgWrapper = StorageStorageMessagesHelper.prepareChunkMetaInfo(chunkFileMeta);

            Channel chan = ctx.channel();
            ChannelFuture future = chan.write(msgWrapper);
            chan.flush();

            System.out.println("Sent chunkMetaInfo Back to the controller");
           // ctx.close();
        }
        else if(msg.hasRetrieveChunkMsg()) {  //storage node should send chunkMsg

            System.out.println("Client asking for a file chunk");
            boolean isChunkFound = false;
            ByteBuffer buff;

            String fileChunkId = msg.getRetrieveChunkMsg().getFileChunkId();
            String[] dirs = Fileify.getListOfDirs(StorageNodeDS.getInstance().getBasePath());
            for(String dir : dirs) {
                String pathForFileChunkId = StorageNodeDS.getInstance().getBasePath()+ dir + "/chunkFiles/"+ fileChunkId;
                if(Fileify.doesFileExist(pathForFileChunkId)) {
                    try {
                        buff = Fileify.readToBuffer(pathForFileChunkId);
                        //create checksum of the buff
                        byte[] arr = new byte[buff.remaining()];
                        buff.get(arr);
                        long checksumNew = Arrays.hashCode(arr);
                        long checksumExisting = StorageNodeDS.getInstance().getChunkMetaInfo(fileChunkId).getChecksum();
                        // match it with checksum in the mata data
                        if(checksumNew == checksumExisting) {
                            System.out.println("Checksum matches....... :) :) ");
                            isChunkFound = true;
                            StorageMessages.StorageMessageWrapper msgWrapper = StorageStorageMessagesHelper.prepareChunkMsg(fileChunkId, ByteBuffer.wrap(arr));

                            System.out.println("Sending fileChunk to client : "+ msgWrapper.getChunkMsg().getFileChunkId());
                            Channel chan = ctx.channel();
                            ChannelFuture future = chan.write(msgWrapper);
                            chan.flush();  // sending data back to client

                            break;
                        }else{
                            System.out.println("Checksum does not match :( :(");
                            // handle corrupt chunkFile
//                            System.out.println("Not found : "+fileChunkId+" in any directory, sending not found message");
//
//                            String selfId = NodeId.getId(ConfigSystemParam.getAddress(), ConfigSystemParam.getPort());
//                            for(int i = 0; i< msg.getRetrieveChunkMsg().getStorageNodeIdsCount(); i++) {
//                                if(msg.getRetrieveChunkMsg().getStorageNodeIds(i).equalsIgnoreCase(selfId)) {
//                                    msg.getRetrieveChunkMsg().getStorageNodeIdsList().remove(i);
//                                }
//                            }
//
//                            StorageMessages.StorageMessageWrapper msgWrapper =
//                                    StorageStorageMessagesHelper.prepareChunkNotFoundMsg(
//                                            msg.getRetrieveChunkMsg().getFileChunkId(),
//                                            msg.getRetrieveChunkMsg().getStorageNodeIdsList()
//                                    );
//                            Channel chan = ctx.channel();
//                            ChannelFuture future = chan.write(msgWrapper);
//                            chan.flush();  // sending data back to client
//

                            StorageMessages.StorageMessageWrapper msgWrapper = this.handleChunkNotFound(fileChunkId, msg);
                            Channel chan = ctx.channel();
                            ChannelFuture future = chan.write(msgWrapper);
                            chan.flush();  // sending data back to client


                            System.out.println("Preparing  and sending BadChunkFoundMsg  to controller");
                            StorageMessages.StorageMessageWrapper badChunkFoundMsgWrapper =
                                    StorageStorageMessagesHelper.prepareBadChunkFoundMsg(StorageNodeDS.getInstance().getNodeId(), fileChunkId);
                            try {
                                new MessageSender().send(false,
                                        ConfigSystemParam.getNodeType(),
                                        ConfigSystemParam.getControllerAddress(),
                                        ConfigSystemParam.getControllerPort(),
                                        badChunkFoundMsgWrapper);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

            }

            if(isChunkFound == false) {
                //fileChunk not found in any directory
//                System.out.println("Not found : "+fileChunkId+" in any directory, sending not found message");
//                String selfId = NodeId.getId(ConfigSystemParam.getAddress(), ConfigSystemParam.getPort());
//
//                List<String> updatedStorageNodeList = new ArrayList<>();
//                for(int i = 0; i< msg.getRetrieveChunkMsg().getStorageNodeIdsCount(); i++) {
//                    System.out.println("checking from list : "+ msg.getRetrieveChunkMsg().getStorageNodeIds(i));
//                    if(msg.getRetrieveChunkMsg().getStorageNodeIds(i).equalsIgnoreCase(selfId)) {
//                        System.out.println(" - - Removing self from list : "+ selfId);
//                    } else {
//                        updatedStorageNodeList.add(msg.getRetrieveChunkMsg().getStorageNodeIds(i));
//                    }
//                }
//
//                StorageMessages.StorageMessageWrapper msgWrapper =
//                        StorageStorageMessagesHelper.prepareChunkNotFoundMsg(
//                                msg.getRetrieveChunkMsg().getFileChunkId(),
//                                updatedStorageNodeList
//                        );

                StorageMessages.StorageMessageWrapper msgWrapper = this.handleChunkNotFound(fileChunkId, msg);
                Channel chan = ctx.channel();
                ChannelFuture future = chan.write(msgWrapper);
                chan.flush();  // sending data back to client
            }

        } // closing hasRetrieveChunkMsg
        else if(msg.hasBecomePrimaryMsg()){

            System.out.println("Become Primary!!!");
            String fromIP = msg.getBecomePrimaryMsg().getForApAddress();
            String fromPort = msg.getBecomePrimaryMsg().getForPort();
            List<String> selfReplicas = msg.getBecomePrimaryMsg().getAskIdsList();

            String fromNodeId = NodeId.getId(fromIP,fromPort);

            String source = StorageNodeDS.getInstance().getBasePath()+fromNodeId;

            String destination = StorageNodeDS.getInstance().getBasePath() + StorageNodeDS.getInstance().getNodeId();

            if(checkIfSourceExists(source)){ Fileify.copyDirectory(new File(source),new File(destination));

                //replicate the changes to the replicas
             List<String> storageNodes = new ArrayList<>();
             storageNodes.add(StorageNodeDS.getInstance().getNodeId());
             storageNodes.addAll(selfReplicas);
            String toReplica = selfReplicas.get(0) ;
            sendChunksToReplica(toReplica,source,storageNodes);

            StorageMessages.NewPrimaryReply reply = StorageMessages.NewPrimaryReply.newBuilder()
                                                    .setReplicated(true)
                                                    .build();

            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
                    .setReplyMsg(reply)
                    .build();

                Channel chan = ctx.channel();
                ChannelFuture future = chan.write(msgWrapper);
                chan.flush();  // sending data back to client
                //delete the source folder
            }else{
                //todo: if the storage node is not the replica of the node to be deleted, Should send the list of replicas of the node to be deleted to contact
            }
            //respond to the controller
        }
        else if(msg.hasCreateNewReplicaMsg()) {
            String prevReplicaID = msg.getCreateNewReplicaMsg().getLostReplicaId();
            String newReplicaID = msg.getCreateNewReplicaMsg().getNewReplicaId();
            System.out.println("Creating new replica at : "+newReplicaID);

            // go to self folder and send storeChunkMessage for all fileChunks to new id
            String selfFolder = StorageNodeDS.getInstance().getBasePath()+StorageNodeDS.getInstance().getNodeId();
            String[] files = Fileify.getListOfFiles(selfFolder);

            for(String file : files ) {
                String filePath = selfFolder+"/"+file;


                try {
                    ByteBuffer buff = Fileify.readToBuffer(filePath);

                    ChunkFileMeta chunkFileMeta = StorageNodeDS.getInstance().getChunkMetaInfo(file);
                    StorageMessages.StorageMessageWrapper msgWrapper = StorageStorageMessagesHelper.prepareStoreChunkMsg(chunkFileMeta, buff, newReplicaID);

                    String[] connectingInfo = NodeId.getIPAndPort(newReplicaID);
                    new MessageSender().send(false, "storage", connectingInfo[0], Integer.parseInt(connectingInfo[1]), msgWrapper );

                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            } // end of for

        } // end of msg.hasCreateNewReplicaMsg()
        else if(msg.hasHealBadChunkMsg()) {
            // 1. creates retrieve chunkmessage
            StorageMessages.StorageMessageWrapper retrieveChunkMsgWrapper =
                    ClientStorageMessagesHelper.prepareRetrieveChunk(msg.getHealBadChunkMsg().getBadFileChunkId(), msg.getHealBadChunkMsg().getStorageNodesList());
            // and sends chunkmessage Wrapper to nodes in the list
            for(int i =0; i<msg.getHealBadChunkMsg().getStorageNodesCount(); i++) {
                String[] connectInfo = NodeId.getIPAndPort(msg.getHealBadChunkMsg().getStorageNodes(i));
                try {
                    ChannelFuture f = new MessageSender().send(
                            false,
                            ConfigSystemParam.getNodeType(),
                            connectInfo[0],
                            Integer.parseInt(connectInfo[1]),
                            retrieveChunkMsgWrapper);
                    f.get(200, TimeUnit.MILLISECONDS);
                    if(f.isSuccess()) {
                        break;
                    }
                } catch (TimeoutException e) {
                    System.out.println("TIMEOUT, continuing to next if any");
                    continue;
                }catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }


            } // end of for
        } // end of msg.hasHealBadChunkMsg()
    }

    private boolean checkIfSourceExists(String sourcePath){
        Path path = Paths.get(sourcePath);
        return Files.exists(path);
    }

    private void sendChunksToReplica(String nodeId,String sourcePath,List<String> storageNodes){
        File source = new File(sourcePath+"/chunkFiles/");
        if(source.isDirectory()){
            File[] files = source.listFiles();
            for(File file : files) {
                String filename = file.toString();
                String[] filepaths = file.toString().split("/");
                System.out.println("Filename : "+filepaths[filepaths.length-1]);
                ByteBuffer buff = null;
                try {
                    buff = Fileify.readToBuffer(filename);
                    ChunkFileMeta chunkFileMeta = StorageNodeDS.getInstance().getChunksMetaInfo().get(filepaths[filepaths.length-1]);

                    StorageMessages.StorageMessageWrapper msgWrapper = StorageStorageMessagesHelper.prepareStoreChunkMsg(nodeId,storageNodes,chunkFileMeta, buff);

                    String[] connectingIPAddressAndPort = NodeId.getIPAndPort(nodeId);

                   new MessageSender().send(false,"storage",connectingIPAddressAndPort[0],Integer.parseInt(connectingIPAddressAndPort[1]),msgWrapper);
                } catch (IOException | InterruptedException e) {
                    System.out.println("Exception while sending chunks to replicas of the new primary!!");
                    e.printStackTrace();
                }
            }
        }
//        try {
//            Fileify.deleteDirectory(StorageNodeDS.getInstance().getBasePath(),sourcePath);
//        } catch (IOException e) {
//            System.out.println("Error while deleting a directory!!");
//            e.printStackTrace();
//        }
    }


    private StorageMessages.StorageMessageWrapper handleChunkNotFound(String fileChunkId, StorageMessages.StorageMessageWrapper msg){
        System.out.println("Not found : "+fileChunkId+" in any directory, sending not found message");
        String selfId = NodeId.getId(ConfigSystemParam.getAddress(), ConfigSystemParam.getPort());

        List<String> updatedStorageNodeList = new ArrayList<>();
        for(int i = 0; i< msg.getRetrieveChunkMsg().getStorageNodeIdsCount(); i++) {
            System.out.println("checking from list : "+ msg.getRetrieveChunkMsg().getStorageNodeIds(i));
            if(msg.getRetrieveChunkMsg().getStorageNodeIds(i).equalsIgnoreCase(selfId)) {
                System.out.println(" - - Removing self from list : "+ selfId);
            } else {
                updatedStorageNodeList.add(msg.getRetrieveChunkMsg().getStorageNodeIds(i));
            }
        }

        StorageMessages.StorageMessageWrapper msgWrapper =
                StorageStorageMessagesHelper.prepareChunkNotFoundMsg(
                        msg.getRetrieveChunkMsg().getFileChunkId(),
                        updatedStorageNodeList
                );
        return msgWrapper;
    }



}

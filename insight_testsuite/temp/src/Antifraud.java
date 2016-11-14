/**
 * Created by yuanzhi on 11/13/16.
 */
import java.io.*;
import java.util.*;

class Antifraud {

    private static String SPLITTER =  ",";
    private static String TRUSTED = "trusted\n";
    private static String UNVERIFIED = "unverified\n";

    public static void main(String[] args) throws IOException {
        if(args.length != 5) {
            System.out.println("Wrong arguments"); return;
        }
        String BATCH_FILE = args[0];
        String STREAM_FILE = args[1];
        String OUTPUT_1 = args[2];
        String OUTPUT_2 = args[3];
        String OUTPUT_3 = args[4];

        /* step-1 Build Hash Table based on batch data */
        HashMap<String, HashSet<String>> map = InitializeGraph(BATCH_FILE);

        /*step-2 check the streaming data with figure 1,2 and 3 in one read. */
        System.out.println("Start checking the streaming data");
        long startTime2 = System.nanoTime();
        String line = "";
        BufferedReader stream = new BufferedReader(new FileReader(STREAM_FILE));
        BufferedWriter bw1 = new BufferedWriter(new FileWriter(OUTPUT_1));
        BufferedWriter bw2 = new BufferedWriter(new FileWriter(OUTPUT_2));
        BufferedWriter bw3 = new BufferedWriter(new FileWriter(OUTPUT_3));
        try {
            stream.readLine();
            while((line = stream.readLine()) != null) {
                String[] streamingData = line.split(SPLITTER);
                if(!Character.isDigit(streamingData[0].charAt(0))) continue;

                String senderId = streamingData[1];
                String receiverId = streamingData[2];

                // Need to consider sendId or receiveId is not in hashmap
                if(!map.containsKey(senderId) || !map.containsKey(receiverId)) {
                    if(!map.containsKey((senderId))) map.put(senderId, new HashSet<String>());
                    if(!map.containsKey(receiverId)) map.put(receiverId, new HashSet<String>());
                    bw1.write(UNVERIFIED); bw2.write(UNVERIFIED); bw3.write(UNVERIFIED);

                } else {
                    // Figure 1
                    if(map.get(senderId).contains(receiverId)) {
                        bw1.write(TRUSTED); bw2.write(TRUSTED); bw3.write(TRUSTED);
                    }
                    else {
                        // Figure 2
                        boolean flag = false;
                        for(String user : map.get(senderId)) {
                            if(map.get(user).contains(receiverId)) {
                                bw1.write(UNVERIFIED); bw2.write(TRUSTED); bw3.write(TRUSTED);
                                flag = true; break;
                            }
                        }
                        // Figure 3
                        if( !flag && checkFriend(map, senderId, receiverId, 4)) {
                            bw1.write(UNVERIFIED); bw2.write(UNVERIFIED); bw3.write(TRUSTED);
                        }
                    }
                }
                map.get(senderId).add(receiverId);
                map.get(receiverId).add(senderId);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (stream != null) stream.close();
                bw1.close();
                bw2.close();
                bw3.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        long stopTime2 = System.nanoTime();
        System.out.println("Time for testing the streaming data is " + (stopTime2 - startTime2)/1000000000.0 + " seconds");
    }

    /**
     * This function initialize the graph from batch_payment.txt.
     * @param File
     * @return
     */
    private static HashMap<String, HashSet<String>> InitializeGraph(String File) throws FileNotFoundException {
        System.out.println("Start building the graph");
        long startTime1 = System.nanoTime();
        String line = "";
        HashMap<String, HashSet<String>> map = new HashMap<>();
        BufferedReader batch = new BufferedReader((new FileReader(File)));
        try {
            batch.readLine();
            while ((line = batch.readLine()) != null) {

                String[] batchData = line.split(SPLITTER);
                if(!Character.isDigit(batchData[0].charAt(0))) continue;

                if(!map.containsKey(batchData[1])) {
                    map.put(batchData[1], new HashSet<String>());
                }
                if(!map.containsKey(batchData[2])) {
                    map.put(batchData[2], new HashSet<String>());
                }
                map.get(batchData[1]).add(batchData[2]);
                map.get(batchData[2]).add(batchData[1]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (batch != null) batch.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        long stopTime1 = System.nanoTime();
        System.out.println("Time for building the graph is " + (stopTime1 - startTime1)/1000000000.0 + " seconds");
        return map;
    }

    /**
     * This function checks the distance between two users in the graph.
     * @param map
     * @param sender
     * @param receiver
     * @param depth
     * @return
     */
    private static boolean checkFriend(HashMap<String, HashSet<String>> map, String sender, String receiver, int depth) {
        HashSet<String> start = new HashSet<>();
        HashSet<String> stop = new HashSet<>();
        HashSet<String> used = new HashSet<>();
        int level = 0;
        used.add(sender); used.add(receiver);
        start.add(sender); stop.add(receiver);

        while(level < depth && !start.isEmpty() && !stop.isEmpty()) {
            if(start.size() > stop.size()) {
                // bi-direction bfs, normal bfs is too slow here
                HashSet<String> tmp = start;
                start = stop;
                stop = tmp;
            }
            HashSet<String> temp = new HashSet<>();
            for(String user : start) {
                for(String descend : map.get(user)) {
                    if(stop.contains(descend)) return true;
                    if(!used.contains(descend)) {
                        used.add(descend);
                        temp.add(descend);
                    }
                }
            }
            start = temp; level++;
        }
        return false;
    }
}

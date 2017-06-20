package cs276.assignments;

import cs276.util.Pair;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;



public class Index {
    
    // Term id -> (position in index file, doc frequency) dictionary
    private static Map<Integer, Pair<Long, Integer>> postingDict
    = new TreeMap<Integer, Pair<Long, Integer>>();
    // Doc name -> doc id dictionary
    private static Map<String, Integer> docDict
    = new TreeMap<String, Integer>();
    // Term -> term id dictionary
    private static Map<String, Integer> termDict
    = new TreeMap<String, Integer>();
    // Block queue
    private static LinkedList<File> blockQueue
    = new LinkedList<File>();
    
    // Total file counter
    private static int totalFileCount = 0;
    // Document counter
    private static int docIdCounter = 0;
    // Term counter
    private static int wordIdCounter = 0;
    // Index
    private static BaseIndex index = null;
    
    /*
     * Write a posting list to the file
     * You should record the file position of this posting list
     * so that you can read it back during retrieval
     *
     * */
    private static void writePosting(FileChannel fc, PostingList posting)
    throws IOException {
        /*
         * TODO: Your code here
         *
         */
        if (blockQueue.isEmpty()) {
            postingDict.put(posting.getTermId(), new Pair<Long,Integer>(fc.position(), posting.getList().size()));
        }
        index.writePosting(fc, posting);
        
    }
    private static Map<Integer, PostingList> postingMap = new TreeMap<Integer, PostingList>();
    
    private static Map<Integer, PostingList> retrievePostingMap(FileChannel fc) {
        Map<Integer, PostingList> result = new TreeMap<Integer, PostingList>();
        
        ByteBuffer buf = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE);
        buf.clear();
        
        // In the block file, info is stored like this:
        // termId1/docFrequency1/<list of docId>/termId2/docFrequency2/<list of docId>/...
        try {
            while (fc.read(buf) != -1) {
                buf.flip();
                PostingList p = new PostingList(buf.getInt());
                buf.clear();
                
                // read doc frequency.
                fc.read(buf);
                buf.flip();
                int docFrequency = buf.getInt();
                buf.clear();
                
                // Read posting list.
                ByteBuffer postingBuffer = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE * docFrequency);
                postingBuffer.clear();
                fc.read(postingBuffer);
                postingBuffer.flip();
                for (int i = 0; i < docFrequency; i++) {
                    p.getList().add(postingBuffer.getInt());
                }
                postingBuffer.clear();
                
                // Add posting list to posting list map.
                result.put(p.getTermId(), p);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return result;
    }
    
    public static void main(String[] args) throws IOException {
        /* Parse command line */
        if (args.length != 3) {
            System.err
            .println("Usage: java Index [Basic|VB|Gamma] data_dir output_dir");
            return;
        }
        
        /* Get index */
        String className = "cs276.assignments." + args[0] + "Index";
        try {
            Class<?> indexClass = Class.forName(className);
            index = (BaseIndex) indexClass.newInstance();
        } catch (Exception e) {
            System.err
            .println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
            throw new RuntimeException(e);
        }
        
        /* Get root directory */
        String root = args[1];
        File rootdir = new File(root);
        if (!rootdir.exists() || !rootdir.isDirectory()) {
            System.err.println("Invalid data directory: " + root);
            return;
        }
        
        /* Get output directory */
        String output = args[2];
        File outdir = new File(output);
        if (outdir.exists() && !outdir.isDirectory()) {
            System.err.println("Invalid output directory: " + output);
            return;
        }
        
        if (!outdir.exists()) {
            if (!outdir.mkdirs()) {
                System.err.println("Create output directory failure");
                return;
            }
        }
        
        /* BSBI indexing algorithm */
        File[] dirlist = rootdir.listFiles();
        
        /* For each block */
        for (File block : dirlist) {
            File blockFile = new File(output, block.getName());
            
            blockQueue.add(blockFile);
            
            File blockDir = new File(root, block.getName());
            File[] filelist = blockDir.listFiles();
            
            // termId -> PostingList map.
            //Map<Integer, PostingList> postingMap = new TreeMap<Integer, PostingList>();
            
            /* For each file */
            for (File file : filelist) {
                ++totalFileCount;
                String fileName = block.getName() + "/" + file.getName();
                docDict.put(fileName, docIdCounter++);
                
                BufferedReader reader = new BufferedReader(new FileReader(file));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] tokens = line.trim().split("\\s+");
                    for (String token : tokens) {
                        /*
                         * TODO: Your code here
                         *       For each term, build up a list of
                         *       documents in which the term occurs
                         */
                        
                        if (!termDict.containsKey(token)) {
                            int termId = wordIdCounter++;
                            termDict.put(token, termId);
                        }
                        
                        int termId = termDict.get(token);
                        
                        if (!postingMap.containsKey(termId)) {
                            postingMap.put(termId, new PostingList(termId));
                            
                        }
                        
                        List<Integer> postingList = postingMap.get(termId).getList();
                        
                        int docId = docDict.get(fileName);
                        
                        if (!postingList.contains(docId)) {
                            postingList.add(docId);
                        }
                        System.out.println("Processing token " + token + " for file " + fileName);
                        System.out.println(token + "   " + fileName);
                    }
                    
                    
                    
                }
                reader.close();
            }
            
            /* Sort and output */
            if (!blockFile.createNewFile()) {
                System.err.println("Create new block failure.");
                return;
            }
            
            RandomAccessFile bfc = new RandomAccessFile(blockFile, "rw");
            
            /*
             * TODO: Your code here
             *       Write all posting lists for all terms to file (bfc)
             */
            FileChannel fc = bfc.getChannel();
            
            
            for (PostingList posting : postingMap.values()) {
                index.writePosting(fc, posting);
            }
            
            postingMap.clear();
            
            bfc.close();
        }
        
        System.out.println("Word index");
        for (Map.Entry<String, Integer> T : termDict.entrySet()){
            System.out.println("Key: " + T.getKey() + " " + "Value: " + T.getValue());
        }
        
        
        System.out.println("Total File Count: " + totalFileCount);
        
        /* Merge blocks */
        while (true) {
            if (blockQueue.size() <= 1)
                break;
            
            File b1 = blockQueue.removeFirst();
            File b2 = blockQueue.removeFirst();
            
            File combfile = new File(output, b1.getName() + "+" + b2.getName());
            if (!combfile.createNewFile()) {
                System.err.println("Create new block failure.");
                return;
            }
            
            RandomAccessFile bf1 = new RandomAccessFile(b1, "r");
            RandomAccessFile bf2 = new RandomAccessFile(b2, "r");
            RandomAccessFile mf = new RandomAccessFile(combfile, "rw");
            
            /*
             * TODO: Your code here
             *       Combine blocks bf1 and bf2 into our combined file, mf
             *       You will want to consider in what order to merge
             *       the two blocks (based on term ID, perhaps?).
             *
             */
            FileChannel fc1 = bf1.getChannel();
            FileChannel fc2 = bf2.getChannel();
            FileChannel fcm = mf.getChannel();
            
            PostingList p1 = index.readPosting(fc1);
            PostingList p2 = index.readPosting(fc2);
            
            while(p1 != null || p2 != null)
            {
                while (p1 != null && (p2 == null || p1.getTermId() < p2.getTermId()))
                {
                    writePosting(fcm, p1);
                    p1 = index.readPosting(fc1);
                }
                
                while (p2 != null && (p1 == null || p2.getTermId() < p1.getTermId()))
                {
                    writePosting(fcm, p2);
                    p2 = index.readPosting(fc2);
                }
                
                // when same termID is found
                if (p1 != null && p2 != null)
                {
                    PostingList pList;
                    
                    pList = new PostingList(p1.getTermId());
                    Iterator<Integer> i1 = p1.getList().iterator();
                    Integer doc1 = getNext(i1);
                    Iterator<Integer> i2 = p2.getList().iterator();
                    Integer doc2 = getNext(i2);
                    
                    while(doc1 != null && doc2 != null)
                    {
                        // if equal
                        if(doc1.equals(doc2))
                        {
                            pList.getList().add(doc1);
                            doc1 = getNext(i1);
                            doc2 = getNext(i2);
                        }
                        else if(doc1 > doc2)
                        {
                            pList.getList().add(doc2);
                            doc2 = getNext(i2);
                        }
                        else
                        {
                            pList.getList().add(doc1);
                            doc1 = getNext(i1);
                        }
                    }
                    while(doc1 != null)
                    {
                        pList.getList().add(doc1);
                        doc1 = getNext(i1);
                    }
                    while(doc2 != null)
                    {
                        pList.getList().add(doc2);
                        doc2 = getNext(i2);
                    }
                    
                    writePosting(fcm, pList);
                }
                p1 = index.readPosting(fc1);
                p2 = index.readPosting(fc2);
            }
            
            bf1.close();
            bf2.close();
            mf.close();
            b1.delete();
            b2.delete();
            blockQueue.add(combfile);
        }
        
        /* Dump constructed index back into file system */
        File indexFile = blockQueue.removeFirst();
        indexFile.renameTo(new File(output, "corpus.index"));
        
        
        
        
        BufferedWriter termWriter = new BufferedWriter(new FileWriter(new File(
                                                                               output, "term.dict")));
        for (String term : termDict.keySet()) {
            termWriter.write(term + "\t" + termDict.get(term) + "\n");
        }
        termWriter.close();
        
        BufferedWriter docWriter = new BufferedWriter(new FileWriter(new File(
                                                                              output, "doc.dict")));
        for (String doc : docDict.keySet()) {
            docWriter.write(doc + "\t" + docDict.get(doc) + "\n");
        }
        docWriter.close();
        
        BufferedWriter postWriter = new BufferedWriter(new FileWriter(new File(
                                                                               output, "posting.dict")));
        for (Integer termId : postingDict.keySet()) {
            postWriter.write(termId + "\t" + postingDict.get(termId).getFirst()
                             + "\t" + postingDict.get(termId).getSecond() + "\n");
        }
        postWriter.close();
    }
    
    static <X> X getNext(Iterator<X> item)
    {
        if(item.hasNext())
        {
            return item.next();
        }
        else
        {
            return null;
        }
    }
    
}
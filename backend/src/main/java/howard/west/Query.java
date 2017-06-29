// package cs276.assignments;
package howard.west;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Query {

	// Term id -> position in index file
	private Map<Integer, Long> posDict;
	// Term id -> document frequency
	private Map<Integer, Integer> freqDict;
	// Doc id -> doc name dictionary
	private Map<Integer, String> docDict;
	// Term -> term id dictionary
	private Map<String, Integer> termDict;
	// Index
	private BaseIndex index;

	private String line;
	private RandomAccessFile indexFile;


	public Query() throws IOException{
		posDict = new TreeMap<Integer, Long>();
		freqDict = new TreeMap<Integer, Integer>();
		docDict = new TreeMap<Integer, String>();
		termDict = new TreeMap<String, Integer>();

		index = new BasicIndex();
		//this.line = line;

		// String className = "cs276.assignments." + "Basic" + "Index";
		// try {
		// 	Class<?> indexClass = Class.forName(className);
		// 	index = new (BaseIndex) indexClass.newInstance();
		// } catch (Exception e) {
		// 	System.err
		// 			.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
		// 	throw new RuntimeException(e);
		// }

		/* Get index directory */
		//System.out.println(new File(".").getAbsoluteFile());

		// String className = "howard.west" + args[0] + "Index";
		// try {
		// 	Class<?> indexClass = Class.forName(className);
		// 	index = (BaseIndex) indexClass.newInstance();
		// } catch (Exception e) {
		// 	System.err
		// 			.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
		// 	throw new RuntimeException(e);
		// }

		String input = "/output/index/";
		File inputdir = new File(input);


		if (!inputdir.exists() || !inputdir.isDirectory()) {
			System.err.println("Invalid index directory: " + input);

			return;
		}

		/* Index file */
		indexFile = new RandomAccessFile(new File(input,
			"corpus.index"), "r");

		String line = null;
		/* Term dictionary */
		BufferedReader termReader = new BufferedReader(new FileReader(new File(
			input, "term.dict")));
		while ((line = termReader.readLine()) != null) {
			String[] tokens = line.split("\t");
			termDict.put(tokens[0], Integer.parseInt(tokens[1]));
		}
		termReader.close();

		/* Doc dictionary */
		BufferedReader docReader = new BufferedReader(new FileReader(new File(
			input, "doc.dict")));
		while ((line = docReader.readLine()) != null) {
			String[] tokens = line.split("\t");
			docDict.put(Integer.parseInt(tokens[1]), tokens[0]);
		}
		docReader.close();

		/* Posting dictionary */
		BufferedReader postReader = new BufferedReader(new FileReader(new File(
			input, "posting.dict")));
		while ((line = postReader.readLine()) != null) {
			String[] tokens = line.split("\t");
			posDict.put(Integer.parseInt(tokens[0]), Long.parseLong(tokens[1]));
			freqDict.put(Integer.parseInt(tokens[0]),
				Integer.parseInt(tokens[2]));
		}
		postReader.close();

	}
	
	/* 
	 * Write a posting list with a given termID from the file 
	 * You should seek to the file position of this specific
	 * posting list and read it back.
	 * */
	private PostingList readPosting(FileChannel fc, int termId)
	throws IOException {
		fc.position(posDict.get(termId));
		return index.readPosting(fc);
	}

	public List<String> getQuery(String line) throws IOException{
		FileChannel indexChannel = indexFile.getChannel();
	    // Split the query into individual tokens.
		String[] queryTokens = line.split(" ");

	    // Fetch all the posting lists from the index.
		List<PostingList> postingLists = new ArrayList<PostingList>();
		List<Integer> p1 = new ArrayList<Integer>();
		List<String> stringOfFilenames = new ArrayList<String>();
		boolean noResults = false;
		for (String queryToken : queryTokens) {
		// Get the term id for this token using the termDict map.
			Integer termId = termDict.get(queryToken);
			if (termId == null) {
				noResults = true;
				continue;

			}
			
			else {
				postingLists.add(readPosting(indexChannel, termId));
			}
		}
		indexFile.close();
		if (noResults!=true) {
			p1 = postingLists.get(0).getList();
			List<Integer> p2 = new ArrayList<Integer>();
			if (postingLists.size() >= 2){
				for (int i = 1; i < postingLists.size(); i++){
					p2 = postingLists.get(i).getList();
					p1 = findCommons(p1, p2);
				}
			}
	    	/*For returning the file names list
	    	*/
	    	if (p1.size()==0){
	    		return new ArrayList<String>();
	    	}
	    }
	    // return p1;
	    for (int i = 0; i < p1.size(); i++){
	    	stringOfFilenames.add(docDict.get(p1.get(i)));
	    }
	    return stringOfFilenames;
	}

	public List<Integer> findCommons(List<Integer> list1, List<Integer> list2){
		Integer list1_pointer = 0;
		Integer list2_pointer = 0;
		List<Integer> conjunctive_pages = new ArrayList<Integer>();

		while ((list1_pointer < list1.size()) && (list2_pointer < list2.size())){
			if (list1.get(list1_pointer).equals(list2.get(list2_pointer))){
				conjunctive_pages.add(list1.get(list1_pointer));
				list1_pointer++;
				list2_pointer++;
			} else if (list1.get(list1_pointer).compareTo(list2.get(list2_pointer)) < 0){
				list1_pointer++;
			} else {
				list2_pointer++;
			}
		}
		return conjunctive_pages;
	}


	// public static void main(String[] args) throws IOException {

	// }
}

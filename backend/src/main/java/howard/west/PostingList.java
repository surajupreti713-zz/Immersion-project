// package cs276.assignments;
package howard.west;

import java.util.ArrayList;
import java.util.List;

public class PostingList {

	private int termId;
	/* A list of docIDs (i.e. postings) */
	private List<Integer> postings;

	public PostingList(int termId, List<Integer> list) {
		this.termId = termId;
		this.postings = list;
	}

	public PostingList(int termId) {
		this.termId = termId;
		this.postings = new ArrayList<Integer>();
	}

	public int getTermId() {
		return this.termId;
	}

	public List<Integer> getList() {
		return this.postings;
	}

	public void setList(List<Integer> list){
		this.postings = list;
	}
}

package warcsearch;

import org.apache.lucene.document.Document;

/**
 * encaps class for storing search results
 * expandable
 */
public class Result {

	private Document _doc;
	private int _id;
	private double _score;
	
	/**
	 * empty constructor
	 */
	public Result () {
		
	}
	
	/**
	 * "quickfill" constructor
	 * @param doc LuceneDocument data
	 * @param id int original id of the document in WARC
	 * @param score searched score for query in LuceneDocument data
	 */
	public Result (Document doc, int id, double score) {
		_doc = doc;
		_id = id;
		_score = score;
	}

	/**
	 * returns nice line of the result for user
	 */
	public String toString() {
		return _id + "\t" + _doc.get("recordId") + "\t" + _score;
	}
	
	/**
	 * @return the _doc
	 */
	public Document getDoc() {
		return _doc;
	}

	/**
	 * @param _doc the _doc to set
	 */
	public void setDoc(Document _doc) {
		this._doc = _doc;
	}

	/**
	 * @return the _id
	 */
	public int getId() {
		return _id;
	}

	/**
	 * @param _id the _id to set
	 */
	public void setId(int _id) {
		this._id = _id;
	}

	/**
	 * @return the _score
	 */
	public double getScore() {
		return _score;
	}

	/**
	 * @param _score the _score to set
	 */
	public void setScore(double _score) {
		this._score = _score;
	}

}

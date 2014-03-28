package warcsearch;

import org.apache.lucene.document.Document;

public class Result {

	private Document _doc;
	private int _id;
	private double _score;
	
	public Result (Document doc, int id, double score) {
		_doc = doc;
		_id = id;
		_score = score;
	}

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

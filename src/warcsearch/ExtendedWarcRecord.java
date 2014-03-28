package warcsearch;

import org.jsoup.Jsoup;
import org.jsoup.select.Elements;
import org.jwat.warc.WarcRecord;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import java.io.InputStream;
import java.util.Scanner;

/**
 * extends the WarcRecord capability
 * WarcRecord misses parsing html payload,
 * retrieving page texts from html pages,
 * copy constructor for deep copies,
 * basically it's crap.. 
 * life is too short to write custom warc parsers though
 * TODO - store webpage titles
 */
public class ExtendedWarcRecord {
	
	/** original WarcRecord */
	private WarcRecord _record;
	
	/** needed warc data */
	private String _payloadContent;
	private String _recordId;
	private String _targetUri;
	private String _date;
	private String _trecId;
	private org.jsoup.nodes.Document _htmlDoc;
	private boolean _terminator;
	
	/**
	 * empty constructor
	 * used to created terminator record
	 */
	public ExtendedWarcRecord() {
		_terminator = true;
	}
	
	/**
	 * constructor
	 * prepares the object data
	 * @param record original WarcRecord
	 */
	public ExtendedWarcRecord(WarcRecord record){
		_record = record;
		_terminator = false;
		_payloadContent = convertStreamToString(record.getPayloadContent());
		_recordId = record.getHeader("WARC-Record-ID").value;
		_targetUri = record.getHeader("WARC-Target-URI").value;
		_date = record.getHeader("WARC-Date").value;
		_trecId = record.getHeader("WARC-TREC-ID").value;
		_htmlDoc = Jsoup.parse(_payloadContent);
	}
	
	/**
	 * returns true if the record is terminator
	 * indicates the last parsed record
	 * for consumer to end itself
	 * @return the _terminator
	 */
	public boolean isTerminator() {
		return _terminator;
	}

	/**
	 * returns raw html _payloadContent
	 * @return _payloadContent as a String
	 */
	public String getPayloadContent() {
		return _payloadContent;
	}
	
	/**
	 * returns the content of this warc record
	 * as indexable Lucene Document
	 * @return doc LuceneDocument
	 */
	public Document getLuceneDocument() {
		Document doc = new Document();
		doc.add(new TextField("text", getHtmlBodyText(), Field.Store.YES));
		doc.add(new StringField("date", _date, Field.Store.YES));
		doc.add(new StringField("recordId", _recordId, Field.Store.YES));
		doc.add(new StringField("targetUri", _targetUri, Field.Store.YES));
		doc.add(new StringField("trecId", _trecId, Field.Store.YES));
		return doc;
	}
	
	/**
	 * returns parsed text from page body
	 * @return bodytext String
	 */
	public String getHtmlBodyText() {
		Elements bodies = _htmlDoc.getElementsByTag("body");
		String bodyText = "";
		if (bodies.size() > 0) {
			bodyText = bodies.get(0).text();
		}
		return bodyText;
	}
	
	/**
	 * converts InputStream into String
	 * thx to http://stackoverflow.com/a/5445161/1893452
	 * it's a bit wtf though
	 * @param is original java.io InputStream
	 * @return same data as java String
	 */
	private static String convertStreamToString(InputStream is) {
        Scanner s = new Scanner(is).useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
    }
	
	/**
	 * returns the original WarcRecord
	 * @return the _record
	 */
	public WarcRecord getRecord() {
		return _record;
	}
	
}

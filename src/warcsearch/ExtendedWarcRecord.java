package warcsearch;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jwat.warc.WarcRecord;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

public class ExtendedWarcRecord {
	
	private WarcRecord _record;
	private String _payloadContent;
	private String _recordId;
	private String _targetUri;
	private String _warcInfoId;
	private String _date;
	private String _trecId;
	private org.jsoup.nodes.Document _htmlDoc;
	
	public ExtendedWarcRecord(WarcRecord record){
		_record = record;
		_payloadContent = convertStreamToString(record.getPayloadContent());
		_recordId = record.getHeader("WARC-Record-ID").value;
		_targetUri = record.getHeader("WARC-Target-URI").value;
		_warcInfoId = record.getHeader("WARC-Warcinfo-ID").value;
		_date = record.getHeader("WARC-Date").value;
		_trecId = record.getHeader("WARC-TREC-ID").value;
		_htmlDoc = Jsoup.parse(_payloadContent);
	}
	
	public String getPayloadContent() {
		return _payloadContent;
	}
	
	/**
	 * returns the content of this warc record
	 * as indexable Lucene Document
	 */
	public Document getLuceneDocument() {
		Document doc = new Document();
		doc.add(new TextField("text", getHtmlBodyText(), Field.Store.YES));
		doc.add(new StringField("date", _date, Field.Store.YES));
		doc.add(new StringField("recordId", _recordId, Field.Store.YES));
		doc.add(new StringField("targetUri", _targetUri, Field.Store.YES));
		doc.add(new StringField("warcInfoId", _warcInfoId, Field.Store.YES));
		doc.add(new StringField("trecId", _trecId, Field.Store.YES));
		return doc;
	}
	
	public String getHtmlBodyText() {
		Element body = _htmlDoc.getElementsByTag("body").get(0);
		return body.text();
	}
	
	private static String convertStreamToString(java.io.InputStream is) {
        java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
    }
	
	/**
	 * @return the _record
	 */
	public WarcRecord getRecord() {
		return _record;
	}

	/**
	 * @param _record the _record to set
	 */
	public void setRecord(WarcRecord _record) {
		this._record = _record;
	}

	
}

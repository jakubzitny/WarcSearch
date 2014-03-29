package warcsearch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Information Retrieval HW 1
 * WarcSearch searches Warc archives and based
 * on a user's query retrieves ranked results
 * 
 * @author Jakub Zitny <t102012001@ntut.edu.tw>
 * @author Thanduxolo Zwane <tTODO@ntut.edu.tw>
 * @since Fri Mar 28 18:41:59 HKT 2014
 * 
 * TODO
 * - increase thread count to mergescheduler
 * - write own WARC parser
 * - parallel better
 * - keep track of processed records
 * - handle input better?
 */
public class WarcSearch {

	/** CLI parser, options, help */
	private static CommandLineParser parser = new GnuParser();
	private static HelpFormatter formatter = new HelpFormatter();
	private static Options options = new Options();
	private static Option[] option_array = new Option[] {
		new Option("h", "help", false, "displays this help message"),
		new Option("i", "interactive", false, "runs the program in interactive mode"),
		new Option("a", "archive", true, "path to warc file"),
		new Option("q", "query", true, "query"),
		new Option("t", "threads", true, "number of threas"),
	};
	
	/**
	 * parses archive into ExtendedWarcRecords and to LuceneDocuments
	 * indexes prepared LuceneDocuments
	 * runs a search for given query
	 * displays results
	 * @param query
	 * @param archive
	 * @param threadNo determined number of threads for indexing
	 * @throws InterruptedException 
	 */
	public static void run(String query, String archive, int threadNo) throws InterruptedException {
		// prepare queue and threads
		System.out.println("Configuring parser and indexer.");
		LinkedBlockingQueue<ExtendedWarcRecord> sharedQueue = new LinkedBlockingQueue<ExtendedWarcRecord>(512);
		WebArchive wa = new WebArchive(archive, sharedQueue, threadNo);
		Indexer indexer = new Indexer(sharedQueue, threadNo);
		// run p-c
		Thread produce = new Thread(wa);
		Thread consume = new Thread(indexer);
		long startTime = System.nanoTime();
		System.out.println("Parsing and indexing..");
		produce.start();
		consume.start();
		produce.join();
		double delta = (System.nanoTime() - startTime)/1000000000.0;
		System.out.println("Parsing done in " + delta + "s.");
		consume.join();
		delta = (System.nanoTime() - startTime)/1000000000.0;
		System.out.println("Indexing done in " + delta + "s.");
		// search
		System.out.println("Searching for \"" + query + "\".");
		startTime = System.nanoTime();
		ArrayList<Result> results = indexer.search(query);
		delta = (System.nanoTime() - startTime)/1000000000.0;
		// display
		System.out.println("Found " + results.size() + " hits in " + delta + "s.");
		System.out.println();
		System.out.println("Rank\tScore\t\t\tDocId");
		System.out.println("-------------------------------------------------------------------------------");
		int i = 0;
		for (Result r: results) {
			System.out.println(++i + "\t"+ r.toString());
		}
	}
	
	/**
	 * determines the number of consument threads to use for indexing
	 * is based on cli argument, but makes adjustments
	 * if no or wrong option or "0" from -i is specified - set to number of logical cores
	 * limit to no more than 4 times number of logical cores
	 * @param threadsStr string option -t parsed from command line
	 * @return number of threads integer
	 */
	public static int determineNumberOfThreads(String threadsNoStr) {
		int threadNo;
		int cores = Runtime.getRuntime().availableProcessors();
		try {
			threadNo = Integer.parseInt(threadsNoStr);
			if (threadNo < 1) {
				throw new NumberFormatException();
			} else if (threadNo > cores * 4) {
				threadNo = cores * 4;
				System.out.println("Adjusting number of threads to " + threadNo + ".");
			}
		} catch (NumberFormatException e) {
			threadNo = cores;
		}
		return threadNo;
	}
	
	/**
	 * main
	 * parses the command line arguments
	 * if run in interctive mode then asks for query and archive and runs search
	 * if run with proper arguments then just runs search
	 * otherwise displays help message
	 * @param args command line arguments
	 */
	public static void main(String[] args) {
		// prepares options (stupid library)
		for (Option o: option_array) {
			options.addOption(o);
		}
		// parses the options
		try {
	        CommandLine cli = parser.parse(options, args);
			if (cli.hasOption("a") && cli.hasOption("q")) {
				String query = cli.getOptionValue("q");
				String archive = cli.getOptionValue("a");
				int threads = determineNumberOfThreads(cli.getOptionValue("t", "0"));
				run(query, archive, threads);
			} else if (cli.hasOption("i")) {
				BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
				System.out.println("Pleae enter the path to WARC archive:");
				String archive = br.readLine();
				System.out.println("Pleae enter the query:");
				String query = br.readLine();
				run(query, archive, determineNumberOfThreads("0"));
			} else {
				formatter.printHelp("warcsearch", options );
				System.exit(1);
			}
		} catch (ParseException e) {
	        System.err.println("There was a problem with parsing arguments failed.");
	        e.getStackTrace();
	    } catch (IOException e) {
	        System.err.println("There was a problem with user input.");
	        e.getStackTrace();
	    } catch (InterruptedException e) {
	        System.err.println("There was a problem with threads.");
	        e.getStackTrace();
	    }
		
	}

}

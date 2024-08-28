package com.ericsson.eniq.etl.RedbackParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.xml.sax.helpers.DefaultHandler;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.parser.Main;
import com.distocraft.dc5000.etl.parser.MeasurementFile;
import com.distocraft.dc5000.etl.parser.Parser;
import com.distocraft.dc5000.etl.parser.SourceFile;

/**
 * 
 * Adapter implementation that reads redback format ASCII xml measurement data.<br>
 * <br>
 * Redback xml data format:<br>
 * epochtime:value;slot:value;port:value;key 1:value 1;...;key n:value n<br>
 * (N/A) values are converted to null values<br>
 * <br>
 * <table border="1" width="100%" cellpadding="3" cellspacing="0">
 * <tr bgcolor="#CCCCFF" class="TableHeasingColor">
 * <td colspan="4"><font size="+2"><b>Parameter Summary</b></font></td>
 * </tr>
 * <tr>
 * <td><b>Name</b></td>
 * <td><b>Key</b></td>
 * <td><b>Description</b></td>
 * </tr>
 * <tr>
 * <td>TagID pattern</td>
 * <td>tag_id</td>
 * <td>Defines the predefined vendor ID for measurement type or defines regexp pattern<br>
 * that is used to parse vendor ID from the name of sourcefile or from the 1st header line of sourcefile.</td>
 * </tr>
 * <tr>
 * <td>TagID mode</td>
 * <td>tag_id_mode</td>
 * <td>Defines the discovery method of the vendor identification (tag_id).<br>
 * 0 = vendor ID is predefined in parameter named tag_id<br>
 * 1 = vendor ID is parsed from name of sourcefile using regexp pattern defined in parameter named tag_id<br>
 * 2 = vendor ID is parsed from 1st header line of sourcefile using regexp pattern defined in parameter named tag_id</td>
 * </tr>
 * <tr>
 * <td>File Name</td>
 * <td>filename</td>
 * <td>contains the filename of the inputdatafile.</td>
 * <tr>
 * <td>DC_SUSPECTFLAG</td>
 * <td>DC_SUSPECTFLAG</td>
 * <td>EMPTY</td>
 * </tr>
 * <tr>
 * <td>Directory</td>
 * <td>DIRNAME</td>
 * <td>Conatins full path to the input data file.</td>
 * </tr>
 * <tr>
 * <td>TimeZone</td>
 * <td>JVM_TIMEZONE</td>
 * <td>contains the JVM timezone (example. +0200)</td>
 * </tr>
 * <tr>
 * <td>DateTime</td>
 * <td>DATETIME_ID</td>
 * <td>contains the measurement datetime (yyyyMMddHHmmss)</td>
 * </tr>
 * </table>
 * 
 * @author lmfakos
 * 
 */

public class RedbackParser extends DefaultHandler implements Parser {

	private static final String JVM_TIMEZONE = new SimpleDateFormat("Z").format(new Date());
	private String block = "";
	private final int bufferSize = 10000;
	private String filename;
	private Logger log;
	private Main mainParserObject = null;
	private String setName;
	private String setType;
	private int status = 0;
	private String techPack;
	private String workerName = "";
	//private static final String UTC_PATTERN = ".*UTC(.+?)\\..*";


	/*CR-145 The group keys of data being parsed.Class group are introduced which will be combined with policy keys
	if class exist*/
	private String[] groupKeys ;
	private String[] tempGroupKeys = {};
	private String[] nullableKeys = {};
	private String policyName = null;
	private final String meteringClass = "metering_class_counters";
	private final String policingClass = "policing_class_counters";

	// The group keys of the different policies:
	private final String[] GLOBAL_POLICY_KEYS = { "epochtime" };
	private final String[] PORT_POLICY_KEYS = { "epochtime", "slot", "port" };
	private final String[] CHANNEL_POLICY_KEYS = { "epochtime", "slot", "port", "channel" };
	private final String[] DOT1Q_POLICY_KEYS = { "epochtime", "slot", "port", "vlan_id", "policing_policy_name" };
	private final String[] DOT1Q_POLICY_KEYS_NULLABLE = { "policing_policy_name" };
	private final String[] CLASS_POLICY_KEYS = { "metering_class_counters", "policing_class_counters" };
	private final String[] ATM_POLICY_KEYS = { "epochtime", "slot", "port", "vpi", "vci" };
	private final String[] FRAMERELAY_POLICY_KEYS = { "epochtime", "slot", "port", "channel" };
	private final String[] SUBSCRIBER_POLICY_KEYS = { "epochtime", "user_name" };
	private final String[] CONTEXT_POLICY_KEYS = { "epochtime", "context_name" };
	private final String[] LINKGROUP_POLICY_KEYS = { "epochtime", "description" };
	private final String[] EPS_APN_POLICY_KEYS = { "epochtime", "SgiApnIndex" };
	private final String[] EPS_CONTROL_POLICY_KEYS = { "epochtime" };
	private final String[] EPS_TRAFFIC_POLICY_KEYS = { "epochtime" };
	private final String[] ASP_CPU_POLICY_KEYS = { "Epochtime", "slot", "asp" };
	private final String[] ASP_LOAD_POLICY_KEYS = { "Epochtime", "slot", "asp" };
	private final String[] ASP_MEM_POLICY_KEYS = { "Epochtime", "slot", "asp" };
	private final String[] PPA_CPU_POLICY_KEYS = { "Epochtime", "slot", "ppa" };
	private final String[] ASP_EPS_CPU_POLICY_KEYS = { "Epochtime", "slot", "asp" };
	
	// This stores the PM data for one Redback PM file
	private Map<String, ArrayList<NameValuePair>> m_dataStoreMap = null;
	// edeamai: this stores the PM data of 1 ROP
	private Map<String, ArrayList<NameValuePair>> rop_dataStoreMap = null;

	private ArrayList<String> tableFormatPolicyList = null;
	private ArrayList<String> tableColList;

	private long parseStartTime;
	private long fileSize = 0L;
	private long totalParseTime = 0L;
	private int fileCount = 0;
  
	/**
	 * Check that the group key ids are valid
	 * 
	 * @param list
	 * @return
	 */
	private boolean checkGroupKeys(final ArrayList<NameValuePair> list) {
		if (list.size() < groupKeys.length - nullableKeys.length) {
			return false;
		}
		boolean match;
		boolean keyIsNullable;
		
			for (final String groupKey2 : groupKeys) {
				match = false;
				keyIsNullable = false;
				for(String nullableKey : nullableKeys) {
					if(nullableKey.equalsIgnoreCase(groupKey2)) {
						keyIsNullable = true;
						match = true;
						break;
					}
				}
				if(keyIsNullable) {
					continue;
				}
				for(final NameValuePair a : list){
					if(groupKey2.equalsIgnoreCase(a.m_name)){
						match = true;
						break;
						}
					}
				if(!match){
					return false;
					}
				}
			return true;
			}

	/**
	 * Initialize parser
	 */
	@Override
	public void init(final Main main, final String techPack, final String setType, final String setName,
			final String workerName) {

		this.mainParserObject = main;
		this.techPack = techPack;
		this.setType = setType;
		this.setName = setName;
		this.status = 1;
		this.workerName = workerName;

		String logWorkerName = "";
		if (workerName.length() > 0) {
			logWorkerName = "." + workerName;
		}

		log = Logger.getLogger("etl." + techPack + "." + setType + "." + setName + ".parser.Redback" + logWorkerName);
	}

	/**
	 * Parser
	 */
	@Override
	public void parse(final SourceFile sf, final String techPack, final String setType, final String setName)
			throws Exception {

		this.filename = sf.getName();
		MeasurementFile mFile = null;
		block = "";
		BufferedReader br = null;
		final int rowDelimLength = 1;
		final String colDelim = ";";
		final String rowDelim = "\n";
		final String fieldDelim = ":";

		String tag_id = sf.getProperty("tag_id", "(.+)");
		log.finest("vendorID: " + tag_id);

		final int tag_id_mode = Integer.parseInt(sf.getProperty("tag_id_mode", "0"));
		log.finest("VendorID from: " + tag_id_mode);
		
		String UTC_PATTERN = sf.getProperty("utc_pattern", ".*([+|-].{4})\\..*");

		Pattern vendorPattern = null;

		try {
			if (tag_id_mode == 1) {
				vendorPattern = Pattern.compile(tag_id);
				final Matcher m = vendorPattern.matcher(filename);
				if (m.find()) {
					tag_id = m.group(1);
				}
			}
		} catch (final Exception e) {
			log.log(Level.WARNING, "Error while matching pattern " + tag_id + " from filename " + filename
					+ " for vendorId", e);
		}

		try {
			mFile = Main.createMeasurementFile(sf, tag_id, techPack, setType, setName, this.workerName, log);
			if (!mFile.isOpen()) {
				String exitLog = "There was a problem preparing measurement file object - possibly due to invalid or unsupported policy name ("
						+ tag_id
						+ "). "
						+ "\nRegular expression used to extract policy name: "
						+ vendorPattern.toString();
				log.info(exitLog);
				log.info("Nothing to Parse as the MO is not supported by the TP. Hence exiting...");
				return;
			}
		} catch (final Exception e) {
			throw e;
		}

		setGroupKeys(tag_id);

		try {

			// Construct input reader for ascii file type
			final String charsetName = StaticProperties.getProperty("charsetName", null);

			InputStreamReader isr = null;
			if (charsetName == null) {
				isr = new InputStreamReader(sf.getFileInputStream());
			} else {
				log.log(Level.FINEST, "InputStreamReader charsetName: " + charsetName);
				isr = new InputStreamReader(sf.getFileInputStream(), charsetName);
			}
			log.log(Level.FINEST, "InputStreamReader Encoding: " + isr.getEncoding());
			br = new BufferedReader(isr);

			// Start parsing the input file
			log.fine("Parsing File: " + sf.getName());

			// Read header and check policy
			final String firstLine = readLine(rowDelim, br, rowDelimLength);
			if (firstLine == null) {
				log.log(Level.WARNING, " Error reading header line from: " + this.filename);
			} else if (firstLine.indexOf(tag_id) == -1) {
				log.log(Level.WARNING, "Header (1st line of source file " + this.filename
						+ ") does not contain expected policy name.");
			}

			try {
				if (tag_id_mode == 2) {
					vendorPattern = Pattern.compile(tag_id);
					final Matcher m = vendorPattern.matcher(firstLine);
					if (m.find()) {
						tag_id = m.group(1);
					}
				}
			} catch (final Exception e) {
				log.log(Level.WARNING, "Error while matching pattern " + tag_id + " from header " + firstLine
						+ " for vendorID", e);
			}

			if (null == tableFormatPolicyList) {
				// Get list of measurement types that are in table format
				tableFormatPolicyList = new ArrayList<String>();
				String tableFormatPolicies = sf.getProperty("redback.tableFormatPolicies", null);
				// String tableFormatPolicies = null;
				if (null == tableFormatPolicies) {
					tableFormatPolicies = "PM_policy_aspcpu,PM_policy_aspload,PM_policy_aspmem,PM_policy_ppa,PM_policy_aspepscpu";
					log.fine("Property redback.tableFormatPolicies not found. \n Default list of table format policies being to be used: "
							+ tableFormatPolicies);
				}
				final String[] tableFormatPoliciesSplit = tableFormatPolicies.split(",");

				for (final String element : tableFormatPoliciesSplit) {
					tableFormatPolicyList.add(element);
				}
			}

			// log.log(Level.FINEST, "Table Format Policy List is : " + tableFormatPolicyList.toString());
			if (tableFormatPolicyList.contains(tag_id)) {
				// The file is expected to be in table format
				parseTable(br, rowDelimLength, colDelim, rowDelim, fieldDelim, firstLine, tag_id, sf);
			} else {
				/* It is not a source file with table format - normal redback format expected
				Modified to include tag_id so that class counters can be handled*/
				parseRecords(br, rowDelimLength, colDelim, rowDelim, fieldDelim, firstLine,tag_id);
			}

			Date epochdatetime = null;

			if (mFile.isOpen()) {
				final Iterator<ArrayList<NameValuePair>> iterator = m_dataStoreMap.values().iterator();
				String key_new = "";
				String value_new = "";

				while (iterator.hasNext()) {

					final ArrayList<NameValuePair> row = iterator.next();
					final Iterator<NameValuePair> counters = row.iterator();
					while (counters.hasNext()) {
						final NameValuePair nvp = counters.next();
						key_new = nvp.m_name;
						value_new = nvp.m_value;

						
						if (value_new != null) {
							if (value_new.equalsIgnoreCase("(N/A)")) {
								value_new = null;
							}
						}
						
						mFile.addData(key_new, value_new);

						log.log(Level.FINEST, " data element: " + key_new + " = " + value_new
								+ " added to measurement file");
					}
					//For Jira EQEV-53502
					if(filename.contains(".")||filename.contains("-")||filename.contains("+"))
					{
						SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd.HHmmss");
						String pattern = "(\\d{8})\\.(\\d{6})";
						Pattern r = Pattern.compile(pattern);
						Matcher m = r.matcher(filename);
						while (m.find()) {
						    String datetime= m.group(0);
						    Date date = df.parse(datetime);
						    long epoch = date.getTime()/1000;
						    String datetime_1 = Long.toString(epoch);
							//epochdatetime = new Date(timeAdjust(datetime_1)*1000);
					final GregorianCalendar calTime = new GregorianCalendar();
					calTime.setTimeInMillis(timeAdjust(datetime_1)*1000);
						boolean  isAddition = false;
						String utctz = getMatch(filename, UTC_PATTERN);
						if (utctz!=null)
						{
							isAddition = (utctz.contains("+")) ? false : true;
							int hour = Integer.parseInt(utctz.substring(1,3));
							int min = Integer.parseInt(utctz.substring(3,utctz.length()));
							if(!isAddition) {
								calTime.add(Calendar.HOUR,-hour);
								calTime.add(Calendar.MINUTE,-min);
							} else {
								calTime.add(Calendar.HOUR,+hour);
								calTime.add(Calendar.MINUTE,+min);
							}
						}			
					final DecimalFormat f4 = new DecimalFormat("0000");
					final DecimalFormat f2 = new DecimalFormat("00");
					final String datetimeoutput = f4.format(calTime.get(Calendar.YEAR))
							+ f2.format(calTime.get(Calendar.MONTH) + 1)
							+ f2.format(calTime.get(Calendar.DAY_OF_MONTH))
									+ f2.format(calTime.get(Calendar.HOUR_OF_DAY)) + f2.format(calTime.get(Calendar.MINUTE))
							+ f2.format(calTime.get(Calendar.SECOND));

					mFile.addData("DATETIME_ID", datetimeoutput);
					mFile.addData("filename", sf.getName());
					mFile.addData("DC_SUSPECTFLAG", "");
					mFile.addData("DIRNAME", sf.getDir());
					mFile.addData("JVM_TIMEZONE", JVM_TIMEZONE);
					mFile.addData("vendorID", tag_id);
					mFile.saveData();
						}
						}
						else
						{
							log.warning("File does not match with the pattern "+UTC_PATTERN+".Not able to extract timezone");
						}
				}

				// write file and clear data..
				m_dataStoreMap = null;
				mFile.close();
			}

		} catch (final Exception e) {

			log.log(Level.WARNING, "General Failure", e);

		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (final Exception e) {
					log.log(Level.WARNING, "Error closing Reader", e);
				}
			}

			if (mFile != null) {
				try {
					mFile.close();
				} catch (final Exception e) {
					log.log(Level.WARNING, "Error closing MeasurementFile", e);
				}
			}
		}
	}
	private String getMatch(String filename, String pattern) {
		Pattern p = Pattern.compile(pattern);
		Matcher m = p.matcher(filename);
		if(m.matches()) {
			return m.group(1);
		}

		return null;
	}
	private void parseTable(final BufferedReader br, final int rowDelimLength, final String colDelim,
			final String rowDelim, final String fieldDelim, final String firstLine, final String tag_id,
			final SourceFile sf) throws Exception {

		log.log(Level.FINEST, "Parse Table type ");

		// Get list of column names of the table in source file
		String tableCols = sf.getProperty("redback.tableFormat." + tag_id, null);
		if (null == tableCols) {
			log.warning("Interface property redback.tableFormat." + tag_id + " not found while parsing file "
					+ sf.getName() + "" + "\n - can't get column names of table in file");
			// Default list to be used! This is not ideal - should be provided by interface property!
			if (tag_id.equalsIgnoreCase("PM_policy_aspcpu")) {
				tableCols = "Slot,Asp,CpuUsageFiveSecondAvg,CpuUsageOneMinuteAvg,CpuUsageFiveMinuteAvg";
			} else if (tag_id.equalsIgnoreCase("PM_policy_aspload")) {
				tableCols = "Slot,Asp,LoadAvgOneMinute,LoadAvgFiveMinute,LoadAvgFifteenMinute";
			} else if (tag_id.equalsIgnoreCase("PM_policy_aspmem")) {
				tableCols = "Slot,Asp,TotalMemoryKBytes,FreeMemoryKBytes";
			} else if (tag_id.equalsIgnoreCase("PM_policy_ppa")) {
				tableCols = "Slot,PPA,CpuUsageFiveSecondAvg,CpuUsageOneMinuteAvg,CpuUsageFiveMinuteAvg";
			} else if (tag_id.equalsIgnoreCase("PM_policy_aspepscpu")) {
				tableCols = "Slot,Asp,CpuUsageFiveSecondAvg,CpuUsageOneMinuteAvg,CpuUsageFiveMinuteAvg";
			} else {
				log.severe("Table column names not available. Cannot process file.");
				throw new Exception("Table column names not available.");
			}
			log.warning("Using default (hardcoded) table columns instead: " + tableCols);
		}
		tableColList = new ArrayList<String>();

		final String[] tableColsSplit = tableCols.split(",");
		for (final String element : tableColsSplit) {
			tableColList.add(element);
		}

		String line;
		long lineNum = 1;

		if (firstLine.contains("Epochtime")) {
			line = firstLine;
			log.warning("The 1st line of redback source file is not a header.");
		} else {
			line = readLine(rowDelim, br, rowDelimLength);
			lineNum++;
		}

		log.log(Level.FINE, "First dataline: " + line);

		final String[] measLine = line.split(";");

		final String value = measLine[0].split(" ")[2].trim();

		final NameValuePair epochtime = new NameValuePair("Epochtime", value);
		log.fine("Parsed epochtime " + value + "from table format for " + tag_id);

		ArrayList<NameValuePair> dataLine = null;

		line = readLine(rowDelim, br, rowDelimLength);
		lineNum++;

		// String formatRegExp = sf.getProperty("redback.tableFormatRegExp", null);
		String formatRegExp = null;
		if (null == formatRegExp) {
			if (tag_id.equalsIgnoreCase("PM_policy_aspload")) {
				formatRegExp = "([0-9]+)/([0-9]+): ([0-9]+\\.[0-9]+) ([0-9]+\\.[0-9]+) ([0-9]+\\.[0-9]+)";
				log.fine("Using default (hardcoded) setting: " + formatRegExp);
			} else if (tag_id.equalsIgnoreCase("PM_policy_aspmem")) {
				formatRegExp = "([0-9]+)/([0-9]+): ([0-9]+) ([0-9]+)";
				log.fine(" Using default (hardcoded) setting: " + formatRegExp);
			} else if (tag_id.equalsIgnoreCase("PM_policy_ppa")) {
				formatRegExp = "([0-9]+)/((?i)[EI]PPA): ([0-9]+\\.[0-9]+)% ([0-9]+\\.[0-9]+)% ([0-9]+\\.[0-9]+)%";
				log.fine(" Using default (hardcoded) setting: " + formatRegExp);
			} else {
				formatRegExp = "([0-9]+)/([0-9]+): ([0-9]+\\.[0-9]+)% ([0-9]+\\.[0-9]+)% ([0-9]+\\.[0-9]+)%";
				log.fine(" Using default (hardcoded) setting: " + formatRegExp);
			}
		}

		while ((null != line) && (line.length() > 0)) {
			dataLine = new ArrayList<NameValuePair>();
			dataLine.add(epochtime);

			final Pattern pattern = Pattern.compile(formatRegExp);
			final Matcher m = pattern.matcher(line);
			if (m.find()) {
				final int groupCount = m.groupCount();
				if (groupCount != tableColList.size()) {
					log.warning("Line being skipped! The number of columns found in table format file does not match what is expected. Line "
							+ lineNum + ": " + line);
				} else {
					for (int i = 0; i < groupCount; i++) {
						dataLine.add(new NameValuePair(tableColList.get(i), m.group(i + 1)));
					}
					storeRow(dataLine);
				}
			} else {
				log.warning("Skipping line " + lineNum + " as it did not match expected format: " + line);
			}
			line = readLine(rowDelim, br, rowDelimLength);
			lineNum++;
		}

		final Iterator<ArrayList<NameValuePair>> ropRecords = rop_dataStoreMap.values().iterator();
		while (ropRecords.hasNext()) {
			final ArrayList<NameValuePair> record = ropRecords.next();
			final StringBuffer keyBuff = new StringBuffer(groupKeys.length);
			for(String keyInGroupKeys : groupKeys){
				for (final NameValuePair a : record) {
					if(keyInGroupKeys.equalsIgnoreCase(a.m_name)){
						keyBuff.append(a.m_name+a.m_value);
					}				
				}
			}
			final String key = keyBuff.toString();
			m_dataStoreMap.put(key, record);
		}
		rop_dataStoreMap.clear();

	}
/*Added for CR 145 to handle class counters*/
	private ArrayList<NameValuePair> handleClassCounters(String field,String classValue,final String fieldDelim,final String colDelim){
		final String classCounterDelim = ",";
		final String[] result = classValue.split(classCounterDelim);
		final ArrayList<NameValuePair> list = new ArrayList<NameValuePair>();
		boolean policyClass = false;
		for (final String keyValue : result){
			String key = null ,value = null;
			if(keyValue.contains("class")){ //for class is seperated by space
				String[] keySplitted = keyValue.split("\\s+");
				key = field.substring(0,field.indexOf(fieldDelim)).trim();
				value = keySplitted[keySplitted.length-1].trim();
				if(key.equalsIgnoreCase(meteringClass)){
					policyClass =true;
				}
				else{
					log.log(Level.FINEST, meteringClass +" not present");
					list.add(new NameValuePair(meteringClass,("(N/A)")));
					//Always add metering before policing even if it does not exist so that key sequence is maintained while using in store row method
				}
			}
			else{ //for other counters
				String[] keySplitted = keyValue.split(fieldDelim);
				key = keySplitted[keySplitted.length-2].trim();
				value = keySplitted[keySplitted.length-1].trim();
				if(value.contains(colDelim)){//remove ; from last counter
					value = value.substring(0,value.indexOf(colDelim));
				}
			}
			list.add(new NameValuePair(key, value));
			if(policyClass){//Add policing after metering if it does not exist to maintain group keys
				log.log(Level.FINEST, policingClass +" not present");
				list.add(new NameValuePair(policingClass,("(N/A)")));
				policyClass = false;
			}	
		}
		return list;	
	}

	private void parseRecords(final BufferedReader br, final int rowDelimLength, final String colDelim,
			final String rowDelim, final String fieldDelim, final String firstLine,final String tag_id) throws Exception {
		String line;
		final String colAppender ="_agg";
		long lineNum = 1;

		log.log(Level.FINEST, "Parse Records type");

		if (firstLine.contains("epochtime")) {
			line = firstLine;
			log.warning("The 1st line of source file is not a header.");
		} else {
			line = readLine(rowDelim, br, rowDelimLength);
			lineNum++;
		}

		log.log(Level.FINE, "First dataline: " + line);
		// While read line from file is not null
		while ((line != null) && (line.length() > 0)) {
			final String[] result = line.split(colDelim);
			final ArrayList<NameValuePair> list = new ArrayList<NameValuePair>();
			ArrayList<NameValuePair> classList = new ArrayList<NameValuePair>(); //list of classes which is list of classcounters inside class
			ArrayList<ArrayList<NameValuePair>> classList_total = new ArrayList<ArrayList<NameValuePair>>(); //required to handle metering and policing class counters present in single row
			try {

				for ( String field : result) {
					log.log(Level.FINEST, "Field: " + field);
					boolean joinedValues = false;
					String counterValJoined = null;
					String keySecond = null;
					String valueSecond = null;


					// String[] items = field.split(fieldDelim);
					String key =null , value =null;
					if (field.contains("epochtime")) {
						final String[] items = field.split(fieldDelim);
						key = items[items.length - 2].trim();
						value = items[items.length - 1].trim();
					}else if((field.contains(meteringClass)) || field.contains(policingClass)){ //CR 145 changes
						if((tag_id.indexOf(policyName + "_c")>=0)){
							boolean classExist = true;
							String newField = null;
							while(classExist){
								String classValue = readLine(rowDelim, br, rowDelimLength);
								if (classValue.contains(colDelim)){
									classExist = false;
									String[] tempValues =  classValue.split(colDelim);
									if(tempValues.length == 2 ){
										classValue = tempValues[(tempValues.length)-2];
										newField =  tempValues[(tempValues.length)-1];
									}
								}
								classList = handleClassCounters(field,classValue,fieldDelim,colDelim);
								if(!(classList.isEmpty())){
									classList_total.add(classList); //This is required to handle metering policing class counters coming in a single row
								}
								if(((newField != null)) && ((newField.contains(meteringClass)) || (newField.contains(policingClass)))){
									log.log(Level.FINEST,"Next field is class:"+newField);
									field = newField;
									classExist = true;
									newField = null;
									continue;
								}
							}
						}
						else {
							final String[] items = field.split(fieldDelim);
							key = items[items.length - 2].trim();
							value = items[items.length - 1].trim();
							//If no class counters are present then value will be N/A
						}
					} 
					else {
						key = field.substring(0, field.indexOf(fieldDelim)).trim();
						counterValJoined = field.substring(field.indexOf(fieldDelim)+1).trim();
						String[] splitValues = counterValJoined.split("\\s+");
						log.log(Level.FINEST, "Counter has " + splitValues.length + " values");
						if (splitValues.length == 2){		
							keySecond = key+colAppender;
							value = splitValues[0];
							valueSecond = splitValues[1];
							joinedValues = true;
						}
						else{
							value = counterValJoined;
						}
					}
					if (field.indexOf(fieldDelim) < 1) {
						log.log(Level.WARNING, " Error while parsing data from: " + field);

					} else {
						if (key!=null || value != null){
						list.add(new NameValuePair(key, value));
						
						if(joinedValues){
							list.add(new NameValuePair(keySecond,valueSecond));
							log.log(Level.FINEST, "Key::" + keySecond+" Value:: " + valueSecond);
						}
					}
				}
				}
			} catch (final Exception e) {

				log.log(Level.WARNING, "Error while parsing dataline, skipping(" + lineNum + "): " + line, e);
			}
			
			//CR 145 changes
			if(!(classList_total.isEmpty())){ 
				//Add one row for each class in metering or policing class counter
				for(ArrayList<NameValuePair> listtmp: classList_total){//get one row of class counter
					ArrayList<NameValuePair> listdup = new ArrayList<NameValuePair>();
					listdup.addAll(list.subList(0,(groupKeys.length-2)));//get policy keys from common list
					listdup.addAll(listtmp);//add list for class counters after adding policy keys
					log.log(Level.FINEST,"Adding classlist for storing row");
					if(!(checkGroupKeys(listdup))){
						log.log(Level.WARNING, "Error in the class counter group keys, skipping(" + lineNum + "): " + line);
					}
					else{
						storeRow(listdup); //store row with keys + class counters present in metering or policing class counter
					}
				}
			} else{
			if (!checkGroupKeys(list)) {
				log.log(Level.WARNING, "Error in the group keys, skipping(" + lineNum + "): " + line);
				}
				else{
				storeRow(list);
			}
			}	
			line = readLine(rowDelim, br, rowDelimLength);
			lineNum++;
		}
		final Iterator<ArrayList<NameValuePair>> ropRecords = rop_dataStoreMap.values().iterator();
		while (ropRecords.hasNext()) {
			final ArrayList<NameValuePair> record = ropRecords.next();
			final StringBuffer keyBuff = new StringBuffer(groupKeys.length);
			for(String keyInGroupKeys : groupKeys){
				for (final NameValuePair a : record) {
					if(keyInGroupKeys.equalsIgnoreCase(a.m_name)){
						keyBuff.append(a.m_name+a.m_value);
					}				
				}
			}
			final String key = keyBuff.toString();
			
			m_dataStoreMap.put(key, record);
		}
		rop_dataStoreMap.clear();
	}

	/**
	 * This method checks the input parameter for a valid policy name, and sets the groupKeys class object to the value
	 * of the corresponding hardcoded list of group keys. If no valid policy name is found and exception is thrown, and
	 * parsing of file will not continue.
	 * 
	 * @param tag_id
	 *            String that should have a valid policy/schema name
	 * @throws Exception
	 */
	private void setGroupKeys(final String tag_id) throws Exception {
		final String partPolicyName = "PM_policy_";
		String[] nullableKeyInit = {};
		
		// Check the policy and set groupKeys accordingly
		if (tag_id.indexOf(partPolicyName + "global") >= 0) {
			tempGroupKeys = this.GLOBAL_POLICY_KEYS;
			policyName = partPolicyName + "global" ;
		} else if (tag_id.indexOf(partPolicyName + "port") >= 0) {
			tempGroupKeys = this.PORT_POLICY_KEYS;
			policyName = partPolicyName + "port";
		} else if (tag_id.indexOf(partPolicyName + "channel") >= 0) {
			tempGroupKeys = this.CHANNEL_POLICY_KEYS;
			policyName = partPolicyName + "channel";
		} else if (tag_id.indexOf(partPolicyName + "dot1q") >= 0) {
			tempGroupKeys = this.DOT1Q_POLICY_KEYS;
			policyName = partPolicyName + "dot1q";
			nullableKeyInit = this.DOT1Q_POLICY_KEYS_NULLABLE;
		} else if (tag_id.indexOf(partPolicyName + "atm") >= 0) {
			tempGroupKeys = this.ATM_POLICY_KEYS;
			policyName = partPolicyName + "atm";
		} else if (tag_id.indexOf(partPolicyName + "fr") >= 0) {
			tempGroupKeys = this.FRAMERELAY_POLICY_KEYS;
			policyName = partPolicyName + "fr";
		} else if (tag_id.indexOf(partPolicyName + "sub") >= 0) {
			tempGroupKeys = this.SUBSCRIBER_POLICY_KEYS;
			policyName = partPolicyName + "sub";
		} else if (tag_id.indexOf(partPolicyName + "context") >= 0) {
			tempGroupKeys = this.CONTEXT_POLICY_KEYS;
			policyName = partPolicyName + "context";
		} else if (tag_id.indexOf(partPolicyName + "linkgroup") >= 0) {
			tempGroupKeys = this.LINKGROUP_POLICY_KEYS;
			policyName = partPolicyName + "linkgroup";
		}else if (tag_id.indexOf(partPolicyName + "link") >= 0){
			tempGroupKeys = this.LINKGROUP_POLICY_KEYS;
			policyName = partPolicyName + "link";
		} else if (tag_id.indexOf(partPolicyName + "eps_apn") >= 0) {
			tempGroupKeys = this.EPS_APN_POLICY_KEYS;
			policyName = partPolicyName + "eps_apn";
		} else if (tag_id.indexOf(partPolicyName + "eps_ctrl") >= 0) {
			tempGroupKeys = this.EPS_CONTROL_POLICY_KEYS;
			policyName = partPolicyName + "eps_ctrl";
		} else if (tag_id.indexOf(partPolicyName + "eps_traf") >= 0) {
			tempGroupKeys = this.EPS_TRAFFIC_POLICY_KEYS;
			policyName = partPolicyName + "eps_traf";
		} else if (tag_id.indexOf(partPolicyName + "aspcpu") >= 0) {
			tempGroupKeys = this.ASP_CPU_POLICY_KEYS;
			policyName = partPolicyName + "aspcpu";
		} else if (tag_id.indexOf(partPolicyName + "aspload") >= 0) {
			tempGroupKeys = this.ASP_LOAD_POLICY_KEYS;
			policyName = partPolicyName + "aspload";
		} else if (tag_id.indexOf(partPolicyName + "aspmem") >= 0) {
			tempGroupKeys = this.ASP_MEM_POLICY_KEYS;
			policyName = partPolicyName + "aspmem";
		} else if (tag_id.indexOf(partPolicyName + "ppa") >= 0) {
			tempGroupKeys = this.PPA_CPU_POLICY_KEYS;
			policyName = partPolicyName + "ppa";
		} else if (tag_id.indexOf(partPolicyName + "aspepscpu") >= 0) {
			tempGroupKeys = this.ASP_EPS_CPU_POLICY_KEYS;
			policyName = partPolicyName + "aspepscpu";
		} else {
			// Parsing of this file cannot proceed without knowing what policy (schema) the file has. Exception thrown.
			log.severe("Invalid Policy name (" + tag_id + ") found. Source filename " + this.filename
					+ ". Parsing of this file will not proceed.");
			throw new Exception("Invalid Policy name found in filename.");
		}
		
		nullableKeys = nullableKeyInit;
//CR 145 changes
		if ((tag_id.indexOf(policyName + "_c") >= 0)){
			groupKeys = new String[tempGroupKeys.length+CLASS_POLICY_KEYS.length];
			System.arraycopy(tempGroupKeys, 0, groupKeys, 0, tempGroupKeys.length);
			System.arraycopy(CLASS_POLICY_KEYS, 0, groupKeys,tempGroupKeys.length , CLASS_POLICY_KEYS.length);
		}
		else{
			groupKeys = tempGroupKeys;
		}
	}

	/**
	 * read characters from reader until eof or delimiter is encountered.
	 * 
	 * @param delimiter
	 *            row delimiter
	 * @param br
	 *            reader
	 * @param rowDelimLength
	 *            row delim length
	 * @return read text line
	 * @throws Exception
	 */
	private String readLine(final String delimiter, final BufferedReader br, final int rowDelimLength) throws Exception {

		final char[] tmp = new char[bufferSize];

		while (true) {

			final String[] result = this.block.split(delimiter);

			// delimiter found
			if (result.length > 1) {

				// remove discovered token + deliminator from block
				block = block.substring(result[0].length() + rowDelimLength);

				log.log(Level.FINEST, "result: " + result[0]);

				// return found block
				return result[0];

			} else {
				// delimiter not found, read next block
				final int count = br.read(tmp, 0, bufferSize);

				// if end of file return whole block
				if (count == -1) {
					return null;
				}

				this.block += (new String(tmp));

			}
		}
	}

	/**
	 * Parser thread
	 */
	@Override
	public void run() {
		try {

			this.status = 2;
			SourceFile sf = null;
			parseStartTime = System.currentTimeMillis();

			while ((sf = mainParserObject.nextSourceFile()) != null) {

				try {
					fileCount++;
					fileSize += sf.fileSize();		  
					mainParserObject.preParse(sf);
					parse(sf, techPack, setType, setName);
					mainParserObject.postParse(sf);
				} catch (final Exception e) {
					mainParserObject.errorParse(e, sf);
				} finally {
					mainParserObject.finallyParse(sf);
				}
			}
			totalParseTime = System.currentTimeMillis() - parseStartTime;
			if (totalParseTime != 0) {
				log.info("Parsing Performance :: " + fileCount
						+ " files parsed in " + totalParseTime 
						+ " ms, filesize is " + fileSize 
						+ " bytes and throughput : " + (fileSize / totalParseTime)
						+ " bytes/ms.");
			}
		} catch (final Exception e) {
			// Exception at top level
			log.log(Level.WARNING, "Worker parser failed to exception", e);
		} finally {
			this.status = 3;
		}
	}

	/**
	 * @return status of the parser
	 */
	@Override
	public int status() {

		return status;
	}

	public long timeAdjust(final String value) {
		final long epochTime = Long.decode(value); // get epochtime as a double
		final double divider = 900; // The num of sec in a ROP

		final double multi = epochTime / divider; // The total num of ROPs since 1970 (or whenever it was)

		final long multiRound = (long) Math.floor(multi); // Round down to nearest integer (in other words round down
															// the number of ROP).
		final long adjusted = multiRound * 900; // Convert back from num of ROPs to epochtime.
		return adjusted;
	}

	/**
	 * Repeated calls to this method collects parsed data into records in rop_dataStoreMap (rop_dataStoreMap is
	 * initialised if null), and ultimately copies the data to m_dataStoreMap. rop_dataStoreMap is for collecting data
	 * from current ROP being processed, and m_dataStoreMap is for collecting multiple ROPs of data. If data submitted
	 * to this method is found to belong to the next ROP (this is checked by examining epochtime value) this indicates
	 * that collection of current ROP has finished and next ROP has started, and so all records in rop_dataStoreMap get
	 * transfered to m_dataStoreMap, and rop_dataStoreMap is set to null.
	 * 
	 * Before adding data to rop_dataStoreMap, this method checks it to see if it belongs to a record already in it (it
	 * does this check by way of unique key). If yes, then it adds the data to the existing record (existing entry) in
	 * rop_dataStoreMap. If no, then it adds it to rop_dataStoreMap as a new record (new entry).
	 * 
	 * The unique key mentioned above consists of the values of the group keys of policy (excluding epochtime key).
	 * 
	 * @param list
	 *            This is an ArrayList of NameValuePair objects containing data parsed from a line read from a redback
	 *            PM file. It may consist of a partial record of data or full record (a record corresponds to 1 row in
	 *            the dwhdb table to which the data will ultimately be loaded).
	 */
	private void storeRow(final ArrayList<NameValuePair> list) {
		
		// Make a key made up of a concatenation of all group keys (excluding epochtime).
		final StringBuffer keyBuff = new StringBuffer(groupKeys.length-1);
		
		for(String keyInGroupKeys : groupKeys){
			for (final NameValuePair a : list.subList(1,list.size())) {
			//TR HS54247
				if(keyInGroupKeys.equalsIgnoreCase(a.m_name)){
					keyBuff.append(a.m_name+a.m_value);
				}				
			}
		}
		final String key = keyBuff.toString();
		

		if (m_dataStoreMap == null) {
			m_dataStoreMap = new TreeMap<String, ArrayList<NameValuePair>>();
		}

		if (rop_dataStoreMap == null) {
			rop_dataStoreMap = new TreeMap<String, ArrayList<NameValuePair>>();
			
			rop_dataStoreMap.put(key, list); // Add the first data of this rop to the rop store.
		} else {
			final long foundEpochtime = Integer.parseInt(list.get(0).m_value);

			if (rop_dataStoreMap.containsKey(key)) {
				// The group key values in this data (apart from epochtime) match an already found record in current ROP
				final long storedEpochtime = Integer.parseInt(rop_dataStoreMap.get(key).get(0).m_value);
				if ((foundEpochtime < (storedEpochtime + 900)) && (foundEpochtime > (storedEpochtime - 900))) {
					// This data belongs to current ROP. Adding it to existing entry in rop datastore.
					rop_dataStoreMap.get(key).addAll(list);					
				}
				else {
					// We have come to the 1st line of data of the next ROP!!
					log.info("Additional ROP found in file. Epochtime: " + foundEpochtime);
					// Copy all records in rop_dataStoreMap to m_dataStoreMap.
					final Iterator<ArrayList<NameValuePair>> ropRecords = rop_dataStoreMap.values().iterator();
					while (ropRecords.hasNext()) {
						final ArrayList<NameValuePair> record = ropRecords.next();
						final StringBuffer keyBuff2 = new StringBuffer(groupKeys.length);
						for(String keyInGroupKeys : groupKeys){
							for (final NameValuePair a : record) {
								if(keyInGroupKeys.equalsIgnoreCase(a.m_name)){
							keyBuff2.append(a.m_name+a.m_value);
						}
							}
						}
						
						final String key2 = keyBuff2.toString();
						m_dataStoreMap.put(key2, record);
					}
					// Clear the rop datastore (it will now be used for a new ROP) and add the new data to it.
					rop_dataStoreMap.clear();
					rop_dataStoreMap.put(key, list);
				}
			} else {
				// This data is a new found record in current ROP, add it as a new entry in rop datastore.
				rop_dataStoreMap.put(key, list);
			}
		}
	}
}

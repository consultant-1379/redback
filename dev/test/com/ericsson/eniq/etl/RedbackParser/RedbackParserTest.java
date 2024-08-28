
package com.ericsson.eniq.etl.RedbackParser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.TreeMap;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class RedbackParserTest {

  private static Method readLineMethod;
  private static Method storeRowMethod;
  private static Method checkGroupKeysMethod; 
  private static Method setGroupKeysMethod;
  
  private static Field block;  
  
  // init
  private static Field mainParserObject;

  private static Field techPack;

  private static Field m_dataStoreMap;
  private static Field rop_dataStoreMapField;
 
 // private static Field m_dataStorePolicy;

  private static Field setType;
  private static Field setName;
  private static Field status;
  private static Field workerName;
  
  private static Field groupKeysField;
  private static Field CHANNEL_POLICY_KEYSField;
  private static Field GLOBAL_POLICY_KEYSField;
  private static Field EPS_APN_POLICY_KEYSField;
    
 
  @BeforeClass
  public static void init() {
    try {
      readLineMethod = RedbackParser.class.getDeclaredMethod("readLine", String.class, BufferedReader.class, int.class);
      storeRowMethod = RedbackParser.class.getDeclaredMethod("storeRow", ArrayList.class);
//      getRowCount = RedbackParser.class.getDeclaredMethod("getRowCount", null);
      checkGroupKeysMethod = RedbackParser.class.getDeclaredMethod("checkGroupKeys", ArrayList.class);
      readLineMethod = RedbackParser.class.getDeclaredMethod("readLine", String.class, BufferedReader.class, int.class);
      setGroupKeysMethod = RedbackParser.class.getDeclaredMethod("setGroupKeys", String.class);
      
      m_dataStoreMap = RedbackParser.class.getDeclaredField("m_dataStoreMap");
      rop_dataStoreMapField = RedbackParser.class.getDeclaredField("rop_dataStoreMap");
 //     m_dataStorePolicy = RedbackParser.class.getDeclaredField("m_dataStorePolicy");
      block = RedbackParser.class.getDeclaredField("block");
      groupKeysField = RedbackParser.class.getDeclaredField("groupKeys");
      CHANNEL_POLICY_KEYSField = RedbackParser.class.getDeclaredField("CHANNEL_POLICY_KEYS");
      GLOBAL_POLICY_KEYSField = RedbackParser.class.getDeclaredField("GLOBAL_POLICY_KEYS");
      EPS_APN_POLICY_KEYSField = RedbackParser.class.getDeclaredField("EPS_APN_POLICY_KEYS");

      mainParserObject = RedbackParser.class.getDeclaredField("mainParserObject");
      techPack = RedbackParser.class.getDeclaredField("techPack");
      setType = RedbackParser.class.getDeclaredField("setType");
      setName = RedbackParser.class.getDeclaredField("setName");
      status = RedbackParser.class.getDeclaredField("status");
      workerName = RedbackParser.class.getDeclaredField("workerName");

      readLineMethod.setAccessible(true);
      block.setAccessible(true);
      storeRowMethod.setAccessible(true);
      checkGroupKeysMethod.setAccessible(true);
      setGroupKeysMethod.setAccessible(true);
   //   m_dataStorePolicy.setAccessible(true);
      m_dataStoreMap.setAccessible(true);
      rop_dataStoreMapField.setAccessible(true);
      
      groupKeysField.setAccessible(true);
      CHANNEL_POLICY_KEYSField.setAccessible(true);
      GLOBAL_POLICY_KEYSField.setAccessible(true);
      EPS_APN_POLICY_KEYSField.setAccessible(true);
      
      mainParserObject.setAccessible(true);
      techPack.setAccessible(true);
      setType.setAccessible(true);
      setName.setAccessible(true);
      status.setAccessible(true);
      workerName.setAccessible(true);
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("");
    }
  }

  @Test
  public void testInit() {
    RedbackParser ap = new RedbackParser();
    ap.init(null, "tp", "st", "sn", "wn");

    try {
      String expected = "null,tp,st,sn,wn,1";
      String actual = (String) mainParserObject.get(ap) + "," + techPack.get(ap) + "," + setType.get(ap) + "," + setName.get(ap)
          + "," + workerName.get(ap) + "," + status.get(ap);
      
      assertEquals(expected, actual);

    } catch (Exception e) {
      e.printStackTrace();
      fail("testInit() failed");
    }
  }
  
  @Test
  public void testInit2() {
    RedbackParser ap = new RedbackParser();

    try {
      ap.init(null, "tp", "st", "sn", null);
      fail("shouldn't execute this line, nullpointerException expected");
      
    } catch (Exception e) {
      // test passed
    }
  }

  @Test
  public void testStatus() {
    RedbackParser ap = new RedbackParser();

    assertEquals(0, ap.status());
  }
  
  /*
   * Test the timeAdjust method reduces the epochtime value to the nearest quarter hour.  
   */
  @Test
  public void testTimeAdjust() {
    RedbackParser ap = new RedbackParser();

    String value = "1250436302";  //16 Aug 2009 15:25:02 GMT
    long expectedResult = 1250435700;   //16 Aug 2009 15:15:00 GMT
    
    long result = ap.timeAdjust(value); 
    
    assertEquals(expectedResult, result);
  }
  
  @Test 
  public void testStorageOfFirstRow(){
	  RedbackParser ap = new RedbackParser();
	  try {
		  groupKeysField.set(ap, RedbackParserTest.EPS_APN_POLICY_KEYSField.get(ap));
		  ArrayList<NameValuePair> list = new ArrayList<NameValuePair>();
		  list.add(new NameValuePair("key1", "123456"));
		  list.add(new NameValuePair("key2", "value2"));
		  list.add(new NameValuePair("other1", "value3"));
		  list.add(new NameValuePair("other2", "value4"));
		  storeRowMethod.invoke(ap, list);
		  @SuppressWarnings("unchecked")
		TreeMap<String,ArrayList<NameValuePair>> map = (TreeMap<String,ArrayList<NameValuePair>>)rop_dataStoreMapField.get(ap);
		  assertEquals(1, map.size());
	  } catch (IllegalArgumentException e) {
		  fail(e.getMessage());
	  } catch (IllegalAccessException e) {
		  fail(e.getMessage());
	  } catch (InvocationTargetException e) {
		  fail(e.getMessage());
	}
  }
  
  @Test 
  public void testStorageOfSecondRowWithSameKey(){
	  RedbackParser ap = new RedbackParser();
	  try {
		  //Set the policy (it determines how may group keys there are)
		  groupKeysField.set(ap, RedbackParserTest.EPS_APN_POLICY_KEYSField.get(ap));
		  //Make some dummy data that represent 2 lines parsed from file: list1 and list 2. 
		  ArrayList<NameValuePair> list1 = new ArrayList<NameValuePair>();
		  list1.add(new NameValuePair("key1", "123456"));
		  list1.add(new NameValuePair("key2", "value2"));
		  list1.add(new NameValuePair("other1", "value3"));
		  list1.add(new NameValuePair("other2", "value4"));
		  ArrayList<NameValuePair> list2 = new ArrayList<NameValuePair>();
		  list2.add(new NameValuePair("key1", "123456"));
		  list2.add(new NameValuePair("key2", "value2"));
		  list2.add(new NameValuePair("other3", "value5"));
		  list2.add(new NameValuePair("other4", "value6"));
		  //Store both sets of data in rop_dataStoreMap
		  storeRowMethod.invoke(ap, list1);
		  storeRowMethod.invoke(ap, list2);
		  //Get rop_dataStoreMap
		  @SuppressWarnings("unchecked")
		TreeMap<String,ArrayList<NameValuePair>> map = (TreeMap<String,ArrayList<NameValuePair>>)rop_dataStoreMapField.get(ap);
		  //Check that the 2 sets of data have been added to the same entry (record) in rop_dataStoreMap.
		  assertEquals(1, map.size());
		  System.out.println(map);
		  assertEquals(6, map.get("key2value2").size());
	  } catch (IllegalArgumentException e) {
		  fail(e.getMessage());
	  } catch (IllegalAccessException e) {
		  fail(e.getMessage());
	  } catch (InvocationTargetException e) {
		  fail(e.getMessage());
	}
  }
  
  @SuppressWarnings("unchecked")
@Test 
  public void testStorageOfSecondRowWithDiffKey(){
	  RedbackParser ap = new RedbackParser();
	  try {
		//Set the policy (it determines how may group keys there are)
		  groupKeysField.set(ap, RedbackParserTest.EPS_APN_POLICY_KEYSField.get(ap));
		  //Make some dummy data that represent 2 lines parsed from file: list1 and list 2. 
		  ArrayList<NameValuePair> list1 = new ArrayList<NameValuePair>();
		  list1.add(new NameValuePair("key1", "123456"));
		  list1.add(new NameValuePair("key2", "value2"));
		  list1.add(new NameValuePair("other1", "value3"));
		  list1.add(new NameValuePair("other2", "value4"));
		  ArrayList<NameValuePair> list2 = new ArrayList<NameValuePair>();
		  list2.add(new NameValuePair("key1", "123456"));
		  list2.add(new NameValuePair("key2", "value7"));
		  list2.add(new NameValuePair("other3", "value5"));
		  list2.add(new NameValuePair("other4", "value6"));
		  //Run storeRow method to test that it correctly stores data in rop_dataStoreMap class object.
		  storeRowMethod.invoke(ap, list1);
		  storeRowMethod.invoke(ap, list2);
		  //Get rop_dataStoreMap
		  TreeMap<String,ArrayList<NameValuePair>> map = (TreeMap<String,ArrayList<NameValuePair>>)rop_dataStoreMapField.get(ap);
		  //Check that the 2 sets of data have been added as separate entries (records) in rop_dataStoreMap.
		  assertEquals(2, map.size());
		  assertEquals(4, map.get("key2value2").size());
		 // assertEquals(4, map.get("value7").size());
	  } catch (IllegalArgumentException e) {
		  fail(e.getMessage());
	  } catch (IllegalAccessException e) {
		  fail(e.getMessage());
	  } catch (InvocationTargetException e) {
		  fail(e.getMessage());
	}
  }
  
  @Test
  public void checkGroupKeys(){
	  RedbackParser ap = new RedbackParser();
	  try {
		  //setting the list to size 0.
		  ArrayList<NameValuePair> list1 = new ArrayList<NameValuePair>();
		  //Setting the required size to 4.
		  groupKeysField.set(ap, RedbackParserTest.CHANNEL_POLICY_KEYSField.get(ap));
		boolean result = (Boolean)checkGroupKeysMethod.invoke(ap, list1);
		assertFalse(result);
	} catch (IllegalArgumentException e) {
		  fail(e.getMessage());
	} catch (IllegalAccessException e) {
		  fail(e.getMessage());
	} catch (InvocationTargetException e) {
		System.out.println(e.getMessage());
		  fail(e.getMessage());
	}
  }
  
  /*
   * This tests that checkGroupKeys method is able to detect valid set of group keys.
   */
  @Test
  public void checkGroupKeys2(){
	  RedbackParser ap = new RedbackParser();
	  try {
		  //setting the list to size 4.
		  ArrayList<NameValuePair> list1 = new ArrayList<NameValuePair>();

		  list1.add(new NameValuePair("epochtime", "value1"));
		  list1.add(new NameValuePair("slot", "value2"));
		  list1.add(new NameValuePair("port", "value3"));
		  list1.add(new NameValuePair("channel", "value4"));
		  groupKeysField.set(ap, RedbackParserTest.CHANNEL_POLICY_KEYSField.get(ap)); //This policy has 4 group keys
		boolean result = (Boolean)checkGroupKeysMethod.invoke(ap, list1);
		assertTrue(result);
	} catch (IllegalArgumentException e) {
		  fail(e.getMessage());
	} catch (IllegalAccessException e) {
		  fail(e.getMessage());
	} catch (InvocationTargetException e) {
		System.out.println(e.getMessage());
		  fail(e.getMessage());
	}
  }

  /*
   * This tests that checkGroupKeys method is able to detect an invalid set of group keys.
   */
  @Test
  public void checkGroupKeys3(){
	  RedbackParser ap = new RedbackParser();
	  try {
		  //setting the list to size 3.
		  ArrayList<NameValuePair> list1 = new ArrayList<NameValuePair>();
		  list1.add(new NameValuePair("epochtime", "value1"));
		  list1.add(new NameValuePair("slot", "value2"));
		  list1.add(new NameValuePair("port", "value3"));
		  groupKeysField.set(ap, RedbackParserTest.CHANNEL_POLICY_KEYSField.get(ap));//This policy has 4 group keys, more then are in list1.
		boolean result = (Boolean)checkGroupKeysMethod.invoke(ap, list1);
		assertFalse(result);
	} catch (IllegalArgumentException e) {
		  fail(e.getMessage());
	} catch (IllegalAccessException e) {
		  fail(e.getMessage());
	} catch (InvocationTargetException e) {
		System.out.println(e.getMessage());
		  fail(e.getMessage());
	}
  }
  
  @Test
  public void testSetGroupKeys(){
	RedbackParser ap = new RedbackParser();
	try{
		String[] atmPolicyKeys = {"epochtime", "slot", "port", "vpi", "vci"};  
		
		setGroupKeysMethod.invoke(ap, "PM_policy_atm");
		
		String[] groupKeys = (String[])groupKeysField.get(ap);
		assertEquals(atmPolicyKeys.length, groupKeys.length);
		for(int i=0;i<atmPolicyKeys.length;i++){
			assertEquals(atmPolicyKeys[i], groupKeys[i]);
		}

  	} catch (Exception e) {
  		System.out.println(e.getMessage());
		fail(e.getMessage());
	}
  }
  
  @Test
  public void testSetGroupKeysInvalidPolicyName(){
	RedbackParser ap = new RedbackParser();
	try{
		setGroupKeysMethod.invoke(ap, "PM_policy_atZ");
		fail("An exception was expected but there was no exception thrown");
  	} catch (Exception e) {
		assertTrue(true);
	}
  }

  @AfterClass
  public static void clean() {
    File i = new File(System.getProperty("user.home"), "testFile");
    i.delete();
    i = new File(System.getProperty("user.home"), "asciiInputFile");
    i.delete();
    i = new File(System.getProperty("user.home"), "storageFile");
    i.deleteOnExit();
  }

}

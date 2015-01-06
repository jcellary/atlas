package com.aetna.hadoop.dgc.hive;


import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.ExplainTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.util.StringUtils;
//import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
//import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
//import org.apache.hadoop.yarn.client.api.TimelineClient;
//import org.apache.hadoop.yarn.conf.YarnConfiguration;


/**
 * DGC Hook sends query + plan info to DGCCollector Service. To enable (hadoop 2.4 and up) set
 * hive.exec.pre.hooks/hive.exec.post.hooks/hive.exec.failure.hooks to include this class.
 */

public class Hook implements ExecuteWithHookContext {

  private static final Log LOG = LogFactory.getLog(Hook.class.getName());
  private HiveLineageBean hlb;

  @Override
  public void run(HookContext hookContext) throws Exception {
    	 long currentTime = System.currentTimeMillis();
    	 String executionEngine = null;
          try {
            QueryPlan plan = hookContext.getQueryPlan();
            if (plan == null) {
              return;
            }
            ExplainTask explain = new ExplainTask();
            explain.initialize(hookContext.getConf(), plan, null);
            String queryId = plan.getQueryId();
            String queryStartTime = plan.getQueryStartTime().toString();
            String user = hookContext.getUgi().getUserName();
            String query = plan.getQueryStr();
            int numMrJobs = Utilities.getMRTasks(plan.getRootTasks()).size();
            int numTezJobs = Utilities.getTezTasks(plan.getRootTasks()).size();
            String hiveId = explain.getId();
            SessionState sess = SessionState.get();
            
            if (numTezJobs > 0) {
            	executionEngine="tez";
            }
            if (numMrJobs > 0) {
            	executionEngine="mr";

            }
            hiveId = sess.getSessionId();
           

            switch(hookContext.getHookType()) {
            case PRE_EXEC_HOOK:
              Set<ReadEntity> db = hookContext.getInputs();
              for (Object o : db) {
            	  LOG.debug("DB:Table="+o.toString());
            	  }
              
              currentTime = System.currentTimeMillis();
              HiveLineageInfo lep_pre = new HiveLineageInfo();
              lep_pre.getLineageInfo(query);
              hlb=lep_pre.getHLBean();
              hlb.setQueryEndTime(Long.toString(currentTime));
              hlb.setQueryId(queryId);
              hlb.setQuery(query);
              hlb.setUser(user);
              hlb.setHiveId(hiveId);
              hlb.setSuccess(false);
              if (executionEngine.equalsIgnoreCase("mr")) {
            	  hlb.setExecutionEngine("mapreduce");
              }
              if (executionEngine.equalsIgnoreCase("tez")) {
            	  hlb.setExecutionEngine("tez");
              }
              if (executionEngine.equalsIgnoreCase("spark")) {
            	  hlb.setExecutionEngine("spark");
              }
              hlb.setQueryStartTime(queryStartTime);
              fireAndForget(hookContext.getConf(), hlb, queryId);
          
              break;
            case POST_EXEC_HOOK: 
                currentTime = System.currentTimeMillis();
                HiveLineageInfo lep_post = new HiveLineageInfo();
                lep_post.getLineageInfo(query);
                hlb=lep_post.getHLBean();
                hlb.setQueryEndTime(Long.toString(currentTime));
                hlb.setQueryId(queryId);
                hlb.setQuery(query);
                hlb.setUser(user);
                hlb.setQueryStartTime(queryStartTime);
                hlb.setSuccess(true);
                hlb.setHiveId(hiveId);
                if (executionEngine.equalsIgnoreCase("mr")) {
              	  hlb.setExecutionEngine("mapreduce");
                }
                if (executionEngine.equalsIgnoreCase("tez")) {
              	  hlb.setExecutionEngine("tez");
                }
                if (executionEngine.equalsIgnoreCase("spark")) {
              	  hlb.setExecutionEngine("spark");
                }
                fireAndForget(hookContext.getConf(), hlb, queryId);

              break;
            case ON_FAILURE_HOOK:
                HiveLineageInfo lep_failed = new HiveLineageInfo();
                lep_failed.getLineageInfo(query);
                hlb=lep_failed.getHLBean();
                hlb.setQueryEndTime(Long.toString(currentTime));
                hlb.setQueryId(queryId);
                hlb.setQuery(query);
                hlb.setUser(user);
                hlb.setQueryStartTime(queryStartTime);
                hlb.setSuccess(false);
                hlb.setFailed(true);
                hlb.setHiveId(hiveId);
                if (executionEngine.equalsIgnoreCase("mr")) {
              	  hlb.setExecutionEngine("mapreduce");
                }
                if (executionEngine.equalsIgnoreCase("tez")) {
              	  hlb.setExecutionEngine("tez");
                }
                if (executionEngine.equalsIgnoreCase("spark")) {
              	  hlb.setExecutionEngine("spark");
                }
                fireAndForget(hookContext.getConf(), hlb, queryId);
            	break;
            default:
              //ignore
              break;
            }
          } catch (Exception e) {
            LOG.info("Failed to submit plan to DGC: " + StringUtils.stringifyException(e));
          }
        }
 
  public void fireAndForget(Configuration conf, HiveLineageBean hookData, String queryId) throws Exception {
			String postUri = "http://167.69.111.50:20810/HiveHookCollector/HookServlet"; 		
	  		if (conf.getTrimmed("aetna.hive.hook") != null) {
		  		postUri = conf.getTrimmed("aetna.hive.hook");
	  		} 
	  		Gson gson = new Gson();
	  		String gsonString = gson.toJson(hookData);
	  		LOG.debug("GSON String: "+gsonString);
	  		String encodedGsonQuery = URLEncoder.encode(gsonString, "UTF-8"); 
	  		String encodedQueryId = URLEncoder.encode(queryId, "UTF-8"); 
	        String postData = "hookdata=" + encodedGsonQuery+"&queryid="+encodedQueryId;
			// Create a trust manager that does not validate certificate chains
	        if (postUri.contains("https:")) {       	
	        	TrustManager[] trustAllCerts = new TrustManager[]{
	        			new X509TrustManager() {
	        				public java.security.cert.X509Certificate[] getAcceptedIssuers() {
	        					return null;
	        				}
	        				public void checkClientTrusted(
	        						java.security.cert.X509Certificate[] certs, String authType) {
	        				}
	        				public void checkServerTrusted(
	        						java.security.cert.X509Certificate[] certs, String authType) {
	        				}
	        			}
	        	};
	        	// Install the all-trusting trust manager
	        	try {
	        		SSLContext sc = SSLContext.getInstance("SSL");
	        		sc.init(null, trustAllCerts, new java.security.SecureRandom());
	        		HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
	        	} catch (Exception e) {
	        		e.printStackTrace();
	        	}
	        }
			URL url = new URL(postUri);
			LOG.debug("Post URI: "+postUri);
			DataOutputStream wr = null;
			//HttpURLConnection urlcon = null;
	        if (postUri.contains("https:")) {
				HttpsURLConnection urlcon = null;
				urlcon = (HttpsURLConnection)url.openConnection();
				urlcon.setRequestMethod("POST");
				urlcon.setRequestProperty("X-Requested-By", "HiveHook");
				urlcon.setRequestProperty("Content-Type","application/x-www-form-urlencoded"); 
				urlcon.setUseCaches(false);
				urlcon.setDoInput(true);
				urlcon.setDoOutput(true);
				wr = new DataOutputStream (urlcon.getOutputStream()); 
				LOG.debug("PostString: "+postData);
				//wr.writeBytes(postString.);   
				wr.write(postData.getBytes());
				
				wr.flush ();      
				wr.close ();
				
				
				InputStream is = urlcon.getInputStream();
				InputStreamReader isr = new InputStreamReader(is);

				int numCharsRead;
				char[] charArray = new char[1024];
				StringBuffer sb = new StringBuffer();
				while ((numCharsRead = isr.read(charArray)) > 0) {
					sb.append(charArray, 0, numCharsRead);
				}
				String result = sb.toString();

				LOG.debug("Post Response: "+result);
				isr.close();
				is.close();
				urlcon.disconnect();
	        } else {
				HttpURLConnection urlcon = null;
				urlcon = (HttpURLConnection)url.openConnection();
				urlcon.setRequestMethod("POST");
				urlcon.setRequestProperty("X-Requested-By", "HiveHook");
				urlcon.setRequestProperty("Content-Type","application/x-www-form-urlencoded"); 
				urlcon.setUseCaches(false);
				urlcon.setDoInput(true);
				urlcon.setDoOutput(true);
				wr = new DataOutputStream (urlcon.getOutputStream());
				LOG.debug("PostString: "+postData);
				//wr.writeBytes(postString.);   
				wr.write(postData.getBytes());
				
				wr.flush ();      
				wr.close ();
				
				
				InputStream is = urlcon.getInputStream();
				InputStreamReader isr = new InputStreamReader(is);

				int numCharsRead;
				char[] charArray = new char[1024];
				StringBuffer sb = new StringBuffer();
				while ((numCharsRead = isr.read(charArray)) > 0) {
					sb.append(charArray, 0, numCharsRead);
				}
				String result = sb.toString();

				LOG.debug("Post Response: "+result);
				isr.close();
				is.close();
				urlcon.disconnect();
	        }

			
		}
 
}


package com.hike.analytics.transform;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bsb.hike.analytics.hive.udtf.AbstractLogHandler;
import com.bsb.hike.analytics.hive.udtf.HikeUDTFConstants;
import com.bsb.hike.analytics.hive.udtf.HikeUDTFUtility;
import com.bsb.hike.analytics.hive.udtf.NormalizationHandler;
import com.bsb.hike.analytics.hive.udtf.UnparsedLogHandler;
import com.hike.analytics.common.Constants;
import com.hike.analytics.common.LogParserHandlerMap;

@SuppressWarnings("serial")
public class LoglineTransform {
	private static final Logger LOG = LoggerFactory.getLogger(LoglineTransform.class);

	Context con;
	
	public LoglineTransform (Context con) {
		this.con = con;
	}
	
	public ArrayList<Object[]> processElement(String logline) {
		String logType = null;
		String sourceIp = null;
		String loglineJson = null;
		ArrayList<Object[]> objList = null;
		Map<?, ?> root = null;
		try {
			String[] logLineSplit = logline.split("\\|");
			if (logLineSplit.length < 4)
				return null;
			logType = logLineSplit[1];
			sourceIp = logLineSplit[2];
			loglineJson = logLineSplit[3];
			root = createMap(loglineJson);
			AbstractLogHandler handler = null;

			if (HikeUDTFUtility.getBooleanValue(HikeUDTFConstants.DUPLICATE_UPLOAD, root, false)) {
				handler = new UnparsedLogHandler(logType);
				objList = handler.parseLog(root);
				// processOutput(c, objList, root, logType, sourceIp);
				return null;
			}

			if (root.containsKey("ver")) {
				handler = new NormalizationHandler(logType);
			}
			if (null == handler) {
				handler = LogParserHandlerMap.getLogHandler(logType);
			}

			if (handler == null) {
				if ("filedownload".equals(logType) || "filewebpreview".equals(logType) || "fileupload".equals(logType)
						|| "name".equals(logType))
					return null;
				else {
					LOG.warn("Unable to parse record for type: '" + logType + "'. Skipping record.");
					return null;
				}
			}

			objList = handler.parseLog(root);
			if (objList != null) {
				return objList;
			} else {
				handler = new UnparsedLogHandler(logType);
				objList = handler.parseLog(root);
				// processOutput(c, objList, root, logType, sourceIp);
			}
		} catch (Exception e) {
			LOG.warn("Exception " + e);
			if (StringUtils.isEmpty(logType))
				logType = Constants.UNKNOWN;
			if (StringUtils.isEmpty(sourceIp))
				sourceIp = Constants.UNKNOWN;
			if (StringUtils.isEmpty(loglineJson))
				loglineJson = Constants.UNKNOWN;
			if (root == null)
				root = new HashMap<Object, Object>();
			AbstractLogHandler handler = new UnparsedLogHandler(logType);
			objList = null;
			try {
				objList = handler.parseLog(root);
			} catch (HiveException e1) {
			}
			/*
			 * try { processOutput(c, objList, root, logType, sourceIp); } catch
			 * (HiveException e1) { }
			 */
		}
		return objList;
	}

	/*
	 * public void processOutput(ProcessContext c, ArrayList<Object[]> objList,
	 * Map<?, ?> root, String logType, String sourceIp) throws HiveException {
	 * Random random = new Random(); if (objList != null) { Iterator<Object[]>
	 * iter = objList.iterator(); while (iter.hasNext()) { Object[] row =
	 * iter.next(); LOG.debug("row : {}", row); String kingdom =
	 * HikeUDTFUtility.strValue(row[HikeUDTFConstants.KINGDOM_COLUMN_INDEX]);
	 * //String key = kingdom; String key = kingdom + "," +
	 * String.valueOf(random.nextInt(100));
	 * row[HikeUDTFConstants.LOG_TYPE_COLUMN_INDEX] = logType;
	 * row[HikeUDTFConstants.SRC_IP_COLUMN_INDEX] = sourceIp;
	 * c.output(KV.of(key, row)); } } } })); return result;
	 */

	public static Map<?, ?> createMap(String logLine) throws JsonParseException, JsonMappingException, IOException {
		Map<?, ?> root = null;
		ObjectMapper mapper = new ObjectMapper();
		root = (Map<?, ?>) mapper.readValue(CleanData.get().cleanJson(logLine), Map.class);
		return root;
	}
}

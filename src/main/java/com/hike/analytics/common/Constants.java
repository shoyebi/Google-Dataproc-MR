package com.hike.analytics.common;

public class Constants {
	public static final String MAIN_BUCKET_PATH = "shoyeb/dataflow-requests-correct";
	public static final String STAGING_BUCKET_PATH = "dataflow-requests-staging";
	public static final String BUCKET_NAME = "hike-analytics-hive-backup";
	public static final String MAIN_ORC_SCHEMA = "struct<row_id:string,phylum:string,class:string,order:string,family:string,genus:string,species:string,time_stamp:timestamp,rec_id:string,val_int:int,val_str:string,device_id:string,from_user:string,to_user:string,fr_country:string,to_country:string,fr_operator:string,to_operator:string,fr_circle:string,to_circle:string,user_state:int,variety:string,form:string,record_id:string,race:string,breed:string,division:string,section:string,tribe:string,series:string,census:bigint,population:bigint,capacities:bigint,states:bigint,cts:bigint,nwtype:int,app_ver:string,dev_type:string,dev_os:string,os_ver:string,source:string,msisdn:string,log_type:string,src_ip:string,kingdom:string,dt:date>";
	public static final String MAIN_TABLE_ID = "analytics-main";
	public static final String STAGING_TABLE_ID = "analytics-staging";
	public static final String UNPARSED = "unparsed";
	public static final String UNKNOWN = "unknown";
	public static final String KEY_SEPARATOR = "-";
}

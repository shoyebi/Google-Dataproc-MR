package com.hike.analytics.common;

import org.apache.orc.TypeDescription;

public class Utils {

	// public static final String typeStr =
	// "struct<row_id:string,phylum:string,class:string,order:string,family:string,genus:string,species:string,time_stamp:timestamp,rec_id:string,val_int:int,val_str:string,device_id:string,from_user:string,to_user:string,fr_country:string,to_country:string,fr_operator:string,to_operator:string,fr_circle:string,to_circle:string,user_state:int,variety:string,form:string,record_id:string,race:string,breed:string,division:string,section:string,tribe:string,series:string,census:bigint,population:bigint,capacities:bigint,states:bigint,cts:bigint,nwtype:int,app_ver:string,dev_type:string,dev_os:string,os_ver:string,source:string,msisdn:string,log_type:string,src_ip:string,kingdom:string,dt:date>";
	public static final String typeStr = "struct<row_id:string,phylum:string,kingdom:string,dt:string>";
	public static TypeDescription schema = TypeDescription.fromString(typeStr);
}

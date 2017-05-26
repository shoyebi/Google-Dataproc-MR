package com.hike.analytics.transform;


public class CleanData {

	private static CleanData cleanData = new CleanData();
	
	private CleanData() {
	}
	
	public static CleanData get() {
		return cleanData;
	}
	
	public String cleanJson(final String data) {
		String temp = data;
		temp = temp.replaceAll("\\n", "\\\\n");
		temp = temp.replaceAll("\\t", "\\\\t");
		return temp;
	}
	
	public static void main(String[] args) {
		String data = "{\"user_flag\": \"new\", \"uid\": \"U9z2WtaQwUFVWXJo\", \"type\": \"popup\", \"cat\": \"null\", \"pid\": \"1436349902sticker\", \"feature\": \"sticker\", \"ts\": 1426173067}";
		String data1 = "{\"dev_id\": \"and:1f25a21669c194e8119134b9c807bb33d21ec5cc\", \"d\": {\"md\": {\"info\": {\"rs\": 0, \"error\": \" (0) - java.net.SocketException: Socket closed\n\tat org.eclipse.paho.client.mqttv3.internal.ExceptionHelper.createMqttException(ExceptionHelper.java:42)\n\tat org.eclipse.paho.client.mqttv3.internal.ClientComms$ConnectBG.run(ClientComms.java:741)\n\tat java.lang.Thread.run(Thread.java:841)\nCaused by: java.net.SocketException: Socket closed\n\tat libcore.io.IoBridge.isConnected(IoBridge.java:237)\n\tat libcore.io.IoBridge.connectErrno(IoBridge.java:178)\n\tat libcore.io.IoBridge.connect(IoBridge.java:112)\n\tat java.net.PlainSocketImpl.connect(PlainSocketImpl.java:192)\n\tat java.net.PlainSocketImpl.connect(PlainSocketImpl.java:475)\n\tat java.net.Socket.connect(Socket.java:861)\n\tat org.eclipse.paho.client.mqttv3.internal.TCPNetworkModule.start(TCPNetworkModule.java:81)\n\tat org.eclipse.paho.client.mqttv3.internal.ClientComms$ConnectBG.run(ClientComms.java:723)\n\t... 1 more\n\"}, \"devArea\": \"exception_0_2\", \"sid\": 1436168046962}, \"cts\": 1436168127861, \"st\": \"conn\", \"tag\": \"mob\", \"et\": \"devEvent\", \"ep\": \"HIGH\"}, \"msisdn\": \"+918377862312\", \"ts\": 1436168290, \"os_version\": \"4.4.2\", \"app_ver\": \"android-3.9.8.33\", \"t\": \"le_android\", \"uid\": \"VW_3cR0lXUiialpD\"}";
		CleanData cleanData = new CleanData();
		System.out.println(cleanData.cleanJson(data));
		System.out.println(cleanData.cleanJson(data1));
	}
}

package com.zhangkun.balance.server;

public interface RegistProvider {
	
	public void regist(Object context) throws Exception;
	
	public void unRegist(Object context) throws Exception;

}

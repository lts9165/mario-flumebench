package com.iflytek.mario.tool;

public interface RpcHandler {
  public void init(String ip, int port);
  public void send();
}
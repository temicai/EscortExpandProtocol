#include <stdio.h>
#include <fstream>
#include <vector>
#include <map>
#include <string>
#include <Windows.h>
#include "EscortExpandProtocol.h"

#pragma comment(lib, "EscortExpandProtocol.lib")

typedef std::map<std::string, std::string> KVStringPair;

int loadConf(const char * pConfFile_, KVStringPair & kvPair_)
{
  int result = -1;
  std::fstream confFile;
  char szBuf[256] = { 0 };
  confFile.open(pConfFile_, std::ios::in);
  if (confFile.is_open()) {
    while (!confFile.eof()) {
      confFile.getline(szBuf, sizeof(szBuf), '\n');
      std::string str = szBuf;
      if (str[0] == '#') { //comment line
        continue;
      }
      size_t n = str.find_first_of('=');
      if (n != std::string::npos) {
        std::string keyStr = str.substr(0, n);
        std::string valueStr = str.substr(n + 1);
        kvPair_.emplace(keyStr, valueStr);
        result = 0;
      }
    }
  }
  confFile.close();
  return result;
}

char * readItem(KVStringPair kvPair_, const char * pItem_)
{
  if (!kvPair_.empty()) {
    if (pItem_ && strlen(pItem_)) {
      KVStringPair::iterator iter = kvPair_.find((std::string)pItem_);
      if (iter != kvPair_.end()) {
        std::string strValue = iter->second;
        size_t nSize = strValue.size();
        if (nSize) {
          char * pValue = (char *)malloc(nSize + 1);
          if (pValue) {
            memcpy_s(pValue, nSize + 1, strValue.c_str(), nSize);
            pValue[nSize] = '\0';
          }
          return pValue;
        }
      }
    }
  }
  return NULL;
}

int main(int argc, char ** argv)
{
  char szMsgSrvIp[20] = { 0 };
  unsigned short usMsgPort = 0;
  unsigned short usInteractPort = 0;
  char szBrokerURI[256] = { 0 };
  char szCfgFile[256] = { 0 };
  KVStringPair kvList;
  char szPath[256] = { 0 };
  GetModuleFileNameA(NULL, szPath, sizeof(szPath));
  char szDir[256] = { 0 };
  char szDrive[256] = { 0 };
  _splitpath_s(szPath, szDrive, sizeof(szDrive), szDir, sizeof(szDir), NULL, 0, NULL, 0);
  sprintf_s(szCfgFile, sizeof(szCfgFile), "%s%sconf\\server.data", szDrive, szDir);
  if (argc >= 2) {
    strcpy_s(szCfgFile, sizeof(szCfgFile), argv[1]);
  }
  
  if (loadConf(szCfgFile, kvList) == 0) {
    char * pMsgHost = readItem(kvList, "midware_ip");
    char * pMsgPort = readItem(kvList, "publish_port");
    char * pInteractPort = readItem(kvList, "talk_port");
    char * pBrokeURI = readItem(kvList, "broker_uri");
    if (pMsgHost) {
      strcpy_s(szMsgSrvIp, sizeof(szMsgSrvIp), pMsgHost);
      free(pMsgHost);
      pMsgHost = NULL;
    }
    if (pMsgPort) {
      usMsgPort = (unsigned short)atoi(pMsgPort);
      free(pMsgPort);
      pMsgPort = NULL;
    }
    if (pInteractPort) {
      usInteractPort = (unsigned short)atoi(pInteractPort);
      free(pInteractPort);
      pInteractPort = NULL;
    }
    if (pBrokeURI) {
      strcpy_s(szBrokerURI, sizeof(szBrokerURI), pBrokeURI);
      free(pBrokeURI);
      pBrokeURI = NULL;
    }
    int nInstVal = EEP_Start(szMsgSrvIp, usMsgPort, usInteractPort, szBrokerURI);
    if (nInstVal > 0) {
      printf("EEP_Start: %d\nconnect to %s,%hu|%hu\nconnect to activemq: %s\n", nInstVal,
        szMsgSrvIp, usMsgPort, usInteractPort, szBrokerURI);
      while (1) {
        char c = 0;
        sscanf_s("%c", &c, 1);
        if (c == 'q' || c == 'Q') {
          break;
        }
      }
      int nVal = EEP_Stop(nInstVal);
      printf("EEP_STOP: %d\n", nVal);
    }
  }
  else {
    printf("load config %s failed\n", szCfgFile);
  }
	return 0;
}
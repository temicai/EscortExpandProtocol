#ifndef ESCORT_EXPAND_PROTOCOL_8B0CA948524B48C196E83E0CE1F38165_H
#define ESCORT_EXPAND_PROTOCOL_8B0CA948524B48C196E83E0CE1F38165_H

#define MAX_INSTANCE_NUM 64

int __stdcall EEP_Start(
	const char * pMsgServerIp, 
	unsigned short usMsgDataPort, 
	unsigned short usMsgControlServerPort,
	const char * pMsgServerAddress2
);
int __stdcall EEP_Stop(
	int nInst
);
int __stdcall EEP_SetLogType(
	int nInst, 
	unsigned short usLogType
);

#endif
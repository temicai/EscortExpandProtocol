#include "EscortExpandProtocol.h"
#include "ExpandProtocolConcrete.h"

#include <Windows.h>

typedef std::map<unsigned int, expand_protocol::ExpandProtocolService *> EPServiceList;
EPServiceList g_srvInstList;
pthread_mutex_t g_mutex4SrvInstList;
char g_szDllPath[256] = { 0 };

BOOL APIENTRY DllMain(void * hInst_, DWORD dwReason_, void *)
{
	switch (dwReason_) {
		case DLL_PROCESS_ATTACH: {
			pthread_mutex_init(&g_mutex4SrvInstList, NULL);
			g_srvInstList.clear();
			g_szDllPath[0] = { 0 };
			char szExePath[256] = { 0 };
			if (GetModuleFileNameA((HMODULE)hInst_, szExePath, sizeof(szExePath)) != 0) {
				char szDrive[32] = { 0 };
				char szDir[256] = { 0 };
				_splitpath_s(szExePath, szDrive, 32, szDir, 256, NULL, 0, NULL, 0);
				sprintf_s(g_szDllPath, sizeof(g_szDllPath), "%s%s", szDrive, szDir);
			}
			break;
		}
		case DLL_PROCESS_DETACH: {
			if (!g_srvInstList.empty()) {
				EPServiceList::iterator iter = g_srvInstList.begin();
				while (iter != g_srvInstList.end()) {
					expand_protocol::ExpandProtocolService * pService = iter->second;
					if (pService) {
						pService->Stop();
						delete pService;
						pService = NULL;
					}
					iter = g_srvInstList.erase(iter);
				}
			}
			pthread_mutex_destroy(&g_mutex4SrvInstList);
			break;
		}
	}
	return TRUE;
}

int __stdcall EEP_Start(const char * pMsgSrvIp_, unsigned short usMsgDataPort_,
	unsigned short usMsgInteractorPort_, const char * pMsgSrvAddr2_)
{
	int result = 0;
	pthread_mutex_lock(&g_mutex4SrvInstList);
	if (g_srvInstList.size() < MAX_INSTANCE_NUM) {
		expand_protocol::ExpandProtocolService * pService
			= new expand_protocol::ExpandProtocolService(g_szDllPath);
		if (pService) {
			if (pService->Start(pMsgSrvIp_, usMsgDataPort_, usMsgInteractorPort_, pMsgSrvAddr2_) == 0) {
				unsigned int uiSrvInst = (unsigned int)pService;
				g_srvInstList.emplace(uiSrvInst, pService);
				result = uiSrvInst;
			}
			else {
				delete pService;
				pService = NULL;
			}
		}
	}
	pthread_mutex_unlock(&g_mutex4SrvInstList);
	return result;
}

int __stdcall EEP_Stop(int nInst)
{
	int result = -1;
	pthread_mutex_lock(&g_mutex4SrvInstList);
	if (!g_srvInstList.empty()) {
		EPServiceList::iterator iter = g_srvInstList.find((unsigned int)nInst);
		if (iter != g_srvInstList.end()) {
			expand_protocol::ExpandProtocolService * pService = iter->second;
			if (pService) {
				result = pService->Stop();
			}
		}
	}
	pthread_mutex_unlock(&g_mutex4SrvInstList);
	return result;
}

int __stdcall EEP_SetLogType(int nInst, unsigned short usLogType)
{
	int result = -1;
	pthread_mutex_lock(&g_mutex4SrvInstList);
	if (!g_srvInstList.empty()) {
		EPServiceList::iterator iter = g_srvInstList.find((unsigned int)nInst);
		if (iter != g_srvInstList.end()) {
			expand_protocol::ExpandProtocolService * pService = iter->second;
			if (pService) {
				pService->SetLogType(usLogType);
				result = 0;
			}
		}
	}
	pthread_mutex_unlock(&g_mutex4SrvInstList);
	return result;
}


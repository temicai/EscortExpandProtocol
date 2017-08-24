#ifndef EXPAND_PROTOCOL_CONCRETE_6E59B044460D4E74A4574A6AC38BD2D9_H
#define EXPAND_PROTOCOL_CONCRETE_6E59B044460D4E74A4574A6AC38BD2D9_H

#include <WinSock2.h>
#include <string>
#include <map>
#include <set>
#include <queue>

#include <time.h>

#include "zmq.h"
#include "czmq.h"
#include "document.h"
#include "pfLog.hh"

#define _TIMESPEC_DEFINED
#include "pthread.h"

#include "cms\Connection.h"
#include "cms\Session.h"
#include "cms\TextMessage.h"
#include "cms\BytesMessage.h"
#include "cms\MapMessage.h"
#include "cms\ExceptionListener.h"
#include "cms\MessageListener.h"

#include "decaf\lang\Thread.h"
#include "decaf\lang\Runnable.h"
#include "decaf\lang\Integer.h"
#include "decaf\lang\Long.h"
#include "decaf\util\concurrent\CountDownLatch.h"
#include "decaf\util\Date.h"

#include "activemq\core\ActiveMQConnectionFactory.h"
#include "activemq\core\ActiveMQConnection.h"
#include "activemq\library\ActiveMQCPP.h"
#include "activemq\transport\DefaultTransportListener.h"
#include "activemq\util\Config.h"

#include "EscortExpandProtocolCommon.h"
#include "escort_common.h"

#pragma comment(lib, "ws2_32.lib")
#pragma comment(lib, "pthreadVC2.lib")
#pragma comment(lib, "pfLog.lib")
#pragma comment(lib, "libzmq.lib")
#pragma comment(lib, "czmq.lib")

#ifdef _DEBUG 
#pragma comment(lib, "activemq-cppd.lib")
#else
#pragma comment(lib, "activemq-cpp.lib")
#endif

using namespace activemq;
using namespace activemq::core;
using namespace activemq::transport;
using namespace decaf::lang;
using namespace decaf::util;
using namespace decaf::util::concurrent;
using namespace cms;

#define DEFAULT_RPELY_

namespace expand_protocol
{
	typedef struct tagLogContext
	{
		char * pLogData;
		unsigned int uiDataLen;
		unsigned short usLogCategory;
		unsigned short usLogType;
		tagLogContext()
		{
			uiDataLen = 0;
			pLogData = NULL;
			usLogCategory = 0;
			usLogType = 0;
		}
		~tagLogContext()
		{
			if (uiDataLen && pLogData) {
				delete [] pLogData;
				pLogData = NULL;
				uiDataLen = 0;
			}
		}
	} LogContext;

	typedef void(*fConsumerMessageCallback)(std::string strDestURI, unsigned char * pData,
		unsigned int uiDataLen, void * pUserData);

	typedef void(*fConsumerMessageCallback2)(std::string strDestURI, std::string strMsg,
		void * pUserData);

	typedef std::map<std::string, EscortTask *> EscortTaskList;

	typedef std::map<std::string, Guarder *> EscortGuarderList;

	typedef std::map<std::string, OrganizationEx *> EscortOrganizationList;

	typedef std::map<std::string, WristletDevice *> EscortDeviceList;

	typedef struct tagConsumerMessage
	{
		std::string strDestURI;
		unsigned char * pData;
		unsigned int uiDataLen;
	} ConsumerMessage;

	typedef struct tagRequestStartTask
	{
		unsigned short usCmd;
		unsigned short usReqSeq;
		unsigned short usTaskType;
		unsigned short usDistanceLimit;	
		char szGuarderId[20];
		char szDeviceId[20];
		char szTarget[64];
		char szDestination[64];
		char szRequestTime[20];//yyyymmddhhnnss
	} RequestStartTask;

	typedef struct tagRequestStopTask
	{
		unsigned short usCmd;
		unsigned short usReqSeq;
		char szTaskId[20];
		char szRequestTime[20];
	} RequestStopTask;

	class SimpleAsyncConsumer : 
		public ExceptionListener, 
		public MessageListener, 
		public DefaultTransportListener
	{
	private:
		Connection * m_connection;
		Session * m_session;
		Destination * m_destination;
		MessageConsumer * m_consumer;
		std::string m_strBrokerURI;
		std::string m_strDestURI;
		bool m_bClientAck;
		bool m_bTopic;
		fConsumerMessageCallback m_fMsgCb;
		fConsumerMessageCallback2 m_fMsgCb2;
		void * m_pUserData;
		void * m_pUserData2;
		SimpleAsyncConsumer(const SimpleAsyncConsumer &);
		SimpleAsyncConsumer & operator=(const SimpleAsyncConsumer &);
		void cleanup();

	public:
		SimpleAsyncConsumer(const std::string & strBrokerURI, const std::string & strDestURI,
			bool bUseTopic, bool bClientAck = false);

		virtual ~SimpleAsyncConsumer();

		void close();

		void setConsumerMessageCallback(fConsumerMessageCallback fMsgCb_, void * pUserData);

		void setConsumerMessageCallback2(fConsumerMessageCallback2 fMsgCb_, void * pUserData);

		void runConsumer();

		virtual void onMessage(const Message * message);

		virtual void onException(const CMSException & ex AMQCPP_UNUSED);

		virtual void transportInterrupted() {}

		virtual void transportResumed() {}
	};

	class SimpleProducer : public Runnable
	{
	private:
		Connection * m_connection;
		Session * m_session;
		std::string m_strBrokerURI;
		bool m_bClientAck;
		SimpleProducer(const SimpleProducer &);
		SimpleProducer & operator=(const SimpleProducer &);
		void cleanup();
	public:
		SimpleProducer(const std::string & strBrokerURI, bool bClientAck = false);
		virtual ~SimpleProducer();
		void close();
		void send(std::string strDest, unsigned char * pData, unsigned int uiDataLen);
		void send(std::string strDest, std::string strData);
		virtual void run();
	};

	class ExpandProtocolService
	{
	public:
		ExpandProtocolService(const char * pDllDir_ = NULL);
		~ExpandProtocolService();
		int Start(const char *, unsigned short, unsigned short, const char *, const char * pDestURI = NULL);
		int Stop();
		void SetLogType(unsigned short usLogType);

		friend void consumerMsgCb(std::string, unsigned char *, unsigned int, void *);
		friend void * startDealLogThread(void *);
		friend void * startNetworkThread(void *);
		friend void * startDealConsumerMessageThread(void *);
		friend void * startDealTopicMessageThread(void *);
		friend void * startDealInteractMessageThread(void *);
		

	private:
		int m_nRun;
		
		unsigned int m_uiLogInst;
		unsigned short m_usLogType;
		
		zctx_t * m_ctx;
		void * m_subscriber;
		void * m_interactor;
		char m_szInteractorIdentity[64];

		char m_szMsgServerIp[20];
		unsigned short m_usMsgPort;
		unsigned short m_usInteractPort;

		char m_szBrokerAdderess[256];

		std::queue<LogContext *> m_logQue;
		pthread_mutex_t m_mutex4LogQue;
		pthread_cond_t m_cond4LogQue;

		std::queue<ConsumerMessage *> m_consumerMsgQue;
		pthread_mutex_t m_mutex4ConsumerMsgQue;
		pthread_cond_t m_cond4ConsumerMsgQue;

		std::queue<TopicMessage *> m_topicMsgQue;
		pthread_mutex_t m_mutex4TopicMsgQue;
		pthread_cond_t m_cond4TopicMsgQue;

		std::queue<InteractionMessage *> m_interactMsgQue;
		pthread_mutex_t m_mutex4InteractMsgQue;
		pthread_cond_t m_cond4InteractMsgQue;

		EscortTaskList m_taskList;
		pthread_mutex_t m_mutex4TaskList;
		EscortDeviceList m_deviceList;
		pthread_mutex_t m_mutex4DeviceList;
		EscortGuarderList m_guarderList;
		pthread_mutex_t m_mutex4GuarderList;
		EscortOrganizationList m_orgList;
		pthread_mutex_t m_mutex4OrgList;
		
		pthread_mutex_t m_mutex4LoadTaskList;
		pthread_mutex_t m_mutex4LoadOrgList;
		pthread_mutex_t m_mutex4LoadDeviceList;
		pthread_mutex_t m_mutex4LoadGuarderList;
		bool m_bLoadTaskList;
		bool m_bLoadOrgList;
		bool m_bLoadDeviceList;
		bool m_bLoadGuarderList;

		pthread_t m_pthdDealLog;
		pthread_t m_pthdDealNetwork;
		pthread_t m_pthdDealConsumerMsg;
		pthread_t m_pthdDealTopicMsg;
		pthread_t m_pthdDealInteractorMsg;

		unsigned int m_uiInteractSequence;
		pthread_mutex_t m_mutex4InteractSequence;

		SimpleAsyncConsumer * m_pMsgConsumer; //listen request
		SimpleProducer * m_pPubMsgProducer; //publish 
		SimpleProducer * m_pRepMsgProducer; //reply

		pthread_mutex_t m_mutex4TaskId;
		
	protected:
		void initLog(const char *);
		bool addLog(LogContext *);
		void writeLog(const char *, unsigned short, unsigned short);
		void dealLog();
		void decryptMessage(unsigned char * pData, unsigned int uiBeginIndex,
			unsigned int uiEndIndex, unsigned short usWord);
		void encryptMessage(unsigned char * pData, unsigned int uiBeginIndex,
			unsigned int uiEndIndex, unsigned short usWord);
		bool addConsumerMessage(ConsumerMessage *);
		void dealConsumerMessage();
		void handleTaskStart(expand_protocol::RequestStartTask *, std::string);
		void handleTaskStop(expand_protocol::RequestStopTask *, std::string);
		bool addTopicMessage(TopicMessage *);
		void dealTopicMessage();
		void handleTopicAliveMessage(TopicAliveMessage *, const char *);
		void handleTopicOnlineMessage(TopicOnlineMessage *, const char *);
		void handleTopicOfflineMessage(TopicOfflineMessage *, const char *);
		void handleTopicLocateGpsMessage(TopicLocateMessageGps *, const char *);
		void handleTopicLocateLbsMessage(TopicLocateMessageLbs *, const char *);
		void handleTopicLocateAppMessage(TopicLocateMessageApp *, const char *);
		void handleTopicAlarmLowpowerMessage(TopicAlarmMessageLowpower *, const char *);
		void handleTopicAlarmLooseMsg(TopicAlarmMessageLoose *, const char *);
		void handleTopicAlarmFleeMsg(TopicAlarmMessageFlee *, const char *);
		void handleTopicAlarmLocateLostMsg(TopicAlarmMessageLocateLost *, const char *);
		void handleTopicAlarmFenceMsg(TopicAlarmMessageFence *, const char *);
		void handleTopicDeviceBindMsg(TopicBindMessage *, const char *);
		void handleTopicTaskSubmitMsg(TopicTaskMessage *, const char *);
		void handleTopicTaskModifyMsg(TopicTaskModifyMessage *, const char *);
		void handleTopicTaskCloseMsg(TopicTaskCloseMessage *, const char *);
		void handleTopicLoginMsg(TopicLoginMessage *, const char *);
		void handleTopicLogoutMsg(TopicLogoutMessage *, const char *);

		bool addInteractMessage(InteractionMessage *);
		void dealInteractMessage();
		void dealNetwork();
		
		void loadTaskList();
		void loadOrgList();
		void loadGuarderList();
		void loadDeviceList();
		bool getLoadOrgList();
		void setLoadOrgList(bool);
		bool getLoadGuarderList();
		void setLoadGuarderList(bool);
		bool getLoadTaskList();
		void setLoadTaskList(bool);
		bool getLoadDeviceList();
		void setLoadDeviceList(bool);
		void clearTaskList();
		void clearOrgList();
		void clearGuarderList();
		void clearDeviceList();
		void reloadOrgList(bool);
		void findOrgChild(std::string strOrgId, std::set<std::string> & childList);

		unsigned int getNextInteractSequence();
		void sendInteractionMessage(const char *, unsigned int);
		void formatDatetime(unsigned long, char *, unsigned int);
		unsigned long makeDatetime(const char *);
		void generateTaskId(char *, unsigned int);
	};
}

#endif

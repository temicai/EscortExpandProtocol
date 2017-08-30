#include "ExpandProtocolConcrete.h"

std::string Utf8ToAnsi(LPCSTR utf8)
{
	int WLength = MultiByteToWideChar(CP_UTF8, 0, utf8, -1, NULL, NULL);
	LPWSTR pszW = (LPWSTR)_alloca((WLength + 1) * sizeof(WCHAR));
	//LPWSTR pszW = (LPWSTR)_malloca((WLength + 1) * sizeof(WCHAR));
	MultiByteToWideChar(CP_UTF8, 0, utf8, -1, pszW, WLength);
	pszW[WLength] = '\0';
	int ALength = WideCharToMultiByte(CP_ACP, 0, pszW, -1, NULL, 0, NULL, NULL);
	LPSTR pszA = (LPSTR)_alloca((ALength + 1) * sizeof(char));
	//LPSTR pszA = (LPSTR)_malloca((ALength + 1) * sizeof(char));
	WideCharToMultiByte(CP_ACP, 0, pszW, -1, pszA, ALength, NULL, NULL);
	pszA[ALength] = '\0';
	std::string retStr = pszA;
	return retStr;
}

std::string AnsiToUtf8(LPCSTR Ansi)
{
	int WLength = MultiByteToWideChar(CP_ACP, 0, Ansi, -1, NULL, 0);
	LPWSTR pszW = (LPWSTR)_alloca((WLength + 1) * sizeof(WCHAR));
	//LPWSTR pszW = (LPWSTR)_malloca((WLength + 1) * sizeof(WCHAR));
	MultiByteToWideChar(CP_ACP, 0, Ansi, -1, pszW, WLength);
	int ALength = WideCharToMultiByte(CP_UTF8, 0, pszW, -1, NULL, 0, NULL, NULL);
	LPSTR pszA = (LPSTR)_alloca(ALength + 1);
	//LPSTR pszA = (LPSTR)_malloca((ALength + 1) * sizeof(char));
	WideCharToMultiByte(CP_UTF8, 0, pszW, -1, pszA, ALength, NULL, NULL);
	pszA[ALength] = '\0';
	std::string retStr(pszA);
	return retStr;
}

expand_protocol::SimpleAsyncConsumer::SimpleAsyncConsumer(const std::string & strBrokerURI_,
	const std::string & strDestURI_, bool bUseTopic_, bool bClientAck_)
{
	m_strBrokerURI = strBrokerURI_;
	m_strDestURI = strDestURI_;
	m_bTopic = bUseTopic_;
	m_bClientAck = bClientAck_;
	m_fMsgCb = NULL;
	m_pUserData = NULL;
	m_connection = NULL;
	m_session = NULL;
	m_destination = NULL;
	m_consumer = NULL;
}

expand_protocol::SimpleAsyncConsumer::~SimpleAsyncConsumer()
{
	this->cleanup();
}

void expand_protocol::SimpleAsyncConsumer::close()
{
	this->cleanup();
}

void expand_protocol::SimpleAsyncConsumer::setConsumerMessageCallback(
	expand_protocol::fConsumerMessageCallback fConsumerMsgCb_, void * pUserData_)
{
	m_fMsgCb = fConsumerMsgCb_;
	m_pUserData = pUserData_;
}

void expand_protocol::SimpleAsyncConsumer::setConsumerMessageCallback2(
	expand_protocol::fConsumerMessageCallback2 fConsumerMsgCb_, void * pUserData_)
{
	m_fMsgCb2 = fConsumerMsgCb_;
	m_pUserData2 = pUserData_;
}

void expand_protocol::SimpleAsyncConsumer::runConsumer()
{
	try {
		ActiveMQConnectionFactory * connFactory = new ActiveMQConnectionFactory(m_strBrokerURI);
		m_connection = connFactory->createConnection();
		delete connFactory;
		connFactory = NULL;
		ActiveMQConnection * amqConnection = dynamic_cast<ActiveMQConnection *>(m_connection);
		if (amqConnection) {
			amqConnection->addTransportListener(this);
		}
		m_connection->start();
		m_connection->setExceptionListener(this);
		if (m_bClientAck) {
			m_session = m_connection->createSession(Session::CLIENT_ACKNOWLEDGE);
		}
		else {
			m_session = m_connection->createSession(Session::AUTO_ACKNOWLEDGE);
		}
		if (m_bTopic) {
			m_destination = m_session->createTopic(m_strDestURI);
		}
		else {
			m_destination = m_session->createQueue(m_strDestURI);
		}
		m_consumer = m_session->createConsumer(m_destination);
		m_consumer->setMessageListener(this);
	}
	catch (cms::CMSException & e) {
		e.printStackTrace();
	}
}

void expand_protocol::SimpleAsyncConsumer::onMessage(const Message * message_)
{
	try {
		const TextMessage * txtMsg = dynamic_cast<const TextMessage *>(message_);
		if (txtMsg) {
			std::string strText = txtMsg->getText();
			if (m_fMsgCb2) {
				m_fMsgCb2(m_strDestURI, strText, m_pUserData2);
			}
		}
		else {
			const BytesMessage * bytesMsg = dynamic_cast<const BytesMessage *>(message_);
			if (bytesMsg) {
				unsigned char * pMsgData = bytesMsg->getBodyBytes();
				unsigned int uiMsgDataLen = bytesMsg->getBodyLength();
				if (m_fMsgCb) {
					m_fMsgCb(m_strDestURI, pMsgData, uiMsgDataLen, m_pUserData);
				}
				delete[] pMsgData;
				pMsgData = NULL;
			}
		}
		if (m_bClientAck) {
			message_->acknowledge();
		}
	}
	catch (cms::CMSException & e) {
		e.printStackTrace();
	}
}

void expand_protocol::SimpleAsyncConsumer::onException(const CMSException & ex AMQCPP_UNUSED)
{

}

void expand_protocol::SimpleAsyncConsumer::cleanup()
{
	try {
		if (m_connection) {
			m_connection->close();
		}
	}
	catch (cms::CMSException & e) {
		e.printStackTrace();
		//e.getStackTraceString().c_str();
	}
	if (m_destination) {
		delete m_destination;
		m_destination = NULL;
	}
	if (m_session) {
		delete m_session;
		m_session = NULL;
	}
	if (m_connection) {
		delete m_connection;
		m_connection = false;
	}
	if (m_consumer) {
		delete m_consumer;
		m_consumer = NULL;
	}
}

expand_protocol::SimpleProducer::SimpleProducer(const std::string & strBrokerURI_,
	bool bClientAck_)
{
	m_strBrokerURI = strBrokerURI_;
	m_bClientAck = bClientAck_;
	m_connection = NULL;
	m_session = NULL;
}

expand_protocol::SimpleProducer::~SimpleProducer()
{
	this->cleanup();
}

void expand_protocol::SimpleProducer::run()
{
	ActiveMQConnectionFactory * pConnFactory = new ActiveMQConnectionFactory(m_strBrokerURI);
	try {
		m_connection = pConnFactory->createConnection();
		m_connection->start();
		delete pConnFactory;
		pConnFactory = NULL;
	}
	catch (cms::CMSException & e) {
		e.printStackTrace();
		delete pConnFactory;
		pConnFactory = NULL;
		throw e;
	}
	if (m_bClientAck) {
		m_session = m_connection->createSession(Session::CLIENT_ACKNOWLEDGE);
	}
	else {
		m_session = m_connection->createSession(Session::AUTO_ACKNOWLEDGE);
	}
}

void expand_protocol::SimpleProducer::close()
{
	this->cleanup();
}

void expand_protocol::SimpleProducer::send(std::string strDest_, unsigned char * pData_,
	unsigned int uiDataLen_, bool bTopic_)
{
	if (!strDest_.empty() && pData_ && uiDataLen_ && m_session) {
    cms::Destination * destination = NULL;
    if (bTopic_) {
      destination = m_session->createTopic(strDest_);
    }
    else {
      destination = m_session->createQueue(strDest_);
    }
		cms::MessageProducer * producer = m_session->createProducer(destination);
		cms::BytesMessage * bytesMsg = m_session->createBytesMessage(pData_, (int)uiDataLen_);
		try {
			producer->send(bytesMsg);
		}
		catch (cms::CMSException & e) {
			e.printStackTrace();
		}
		delete bytesMsg;
		bytesMsg = NULL;
		delete producer;
		producer = NULL;
		delete destination;
		destination = NULL;
	}
}

void expand_protocol::SimpleProducer::send(std::string strDest_, std::string strData_, 
  bool bTopic_)
{
	if (!strData_.empty() && !strDest_.empty()) {
    cms::Destination * destination = NULL; 
    if (bTopic_) {
      destination = m_session->createTopic(strDest_);
    }
    else {
      destination = m_session->createQueue(strDest_);
    }
		cms::MessageProducer * producer = m_session->createProducer(destination);
		TextMessage * txtMsg = m_session->createTextMessage(strData_);
		try {
			producer->send(txtMsg);
		}
		catch (cms::CMSException & e) {
			e.printStackTrace();
		}
		delete txtMsg;
		txtMsg = NULL;
		delete destination;
		destination = NULL;
		delete producer;
		producer = NULL;
	}
}

void expand_protocol::SimpleProducer::cleanup()
{
	try {
		if (m_connection) {
			m_connection->close();
		}
	}
	catch (cms::CMSException & e) {
		e.printStackTrace();
	}
	if (m_session) {
		delete m_session;
		m_session = NULL;
	}
	if (m_connection) {
		delete m_connection;
		m_connection = NULL;
	}
}

expand_protocol::ExpandProtocolService::ExpandProtocolService(const char * pDllDir_)
{
	activemq::library::ActiveMQCPP::initializeLibrary();

	pthread_mutex_init(&m_mutex4ConsumerMsgQue, NULL);
	pthread_mutex_init(&m_mutex4InteractMsgQue, NULL);
	pthread_mutex_init(&m_mutex4TopicMsgQue, NULL);
	pthread_mutex_init(&m_mutex4LogQue, NULL);
	
	pthread_mutex_init(&m_mutex4DeviceList, NULL);
	pthread_mutex_init(&m_mutex4GuarderList, NULL);
	pthread_mutex_init(&m_mutex4OrgList, NULL);
	pthread_mutex_init(&m_mutex4TaskList, NULL);
	
	pthread_mutex_init(&m_mutex4LoadDeviceList, NULL);
	pthread_mutex_init(&m_mutex4LoadGuarderList, NULL);
	pthread_mutex_init(&m_mutex4LoadOrgList, NULL);
	pthread_mutex_init(&m_mutex4LoadTaskList, NULL);
	
	pthread_mutex_init(&m_mutex4InteractSequence, NULL);
	pthread_mutex_init(&m_mutex4TaskId, NULL);

	m_nRun = 0;
	m_bLoadDeviceList = false;
	m_bLoadGuarderList = false;
	m_bLoadOrgList = false;
	m_bLoadTaskList = false;
	
	pthread_cond_init(&m_cond4ConsumerMsgQue, NULL);
	pthread_cond_init(&m_cond4InteractMsgQue, NULL);
	pthread_cond_init(&m_cond4LogQue, NULL);
	pthread_cond_init(&m_cond4TopicMsgQue, NULL);

	m_uiInteractSequence = 0;

	m_pthdDealConsumerMsg.p = NULL;
	m_pthdDealInteractorMsg.p = NULL;
	m_pthdDealLog.p = NULL;
	m_pthdDealNetwork.p = NULL;
	m_pthdDealTopicMsg.p = NULL;

	m_pMsgConsumer = NULL;
	m_pPubMsgProducer = NULL;
	m_pRepMsgProducer = NULL;

	memset(m_szBrokerAdderess, 0, sizeof(m_szBrokerAdderess));
	memset(m_szMsgServerIp, 0, sizeof(m_szMsgServerIp));
	
	m_usMsgPort = 0;
	m_usInteractPort = 0;

	m_ctx = zctx_new();
	m_subscriber = NULL;
	m_interactor = NULL;
	memset(m_szInteractorIdentity, 0, sizeof(m_szInteractorIdentity));

	char szPath[256] = { 0 };
	if (pDllDir_) {
		sprintf_s(szPath, sizeof(szPath), "%slog\\", pDllDir_);
		CreateDirectoryExA(".\\", szPath, NULL);
	}
	else {
		GetDllDirectoryA(sizeof(szPath), szPath);
		strcat_s(szPath, sizeof(szPath), "log\\");
		CreateDirectoryExA(".\\", szPath, NULL);
	}
	strcat_s(szPath, sizeof(szPath), "ExpandProtocolBroker\\");
	CreateDirectoryExA(".\\", szPath, NULL);
	m_uiLogInst = LOG_Init();
  m_usLogType = pf_logger::eLOGTYPE_FILE;
	if (m_uiLogInst) {
		pf_logger::LogConfig logConf;
		logConf.usLogPriority = pf_logger::eLOGPRIO_ALL;
		logConf.usLogType = m_usLogType;
		strcpy_s(logConf.szLogPath, sizeof(logConf.szLogPath), szPath);
		LOG_SetConfig(m_uiLogInst, logConf);
	}

	srand((unsigned int)time(NULL));
}

expand_protocol::ExpandProtocolService::~ExpandProtocolService()
{
	if (m_nRun) {
		Stop();
	}

	if (m_ctx) {
		zctx_destroy(&m_ctx);
		m_ctx = NULL;
	}

	clearTaskList();
	setLoadTaskList(false);
	clearOrgList();
	setLoadOrgList(false);
	clearDeviceList();
	setLoadDeviceList(false);
	clearGuarderList();
	setLoadGuarderList(false);

	pthread_cond_destroy(&m_cond4ConsumerMsgQue);
	pthread_cond_destroy(&m_cond4InteractMsgQue);
	pthread_cond_destroy(&m_cond4TopicMsgQue);
	pthread_cond_destroy(&m_cond4LogQue);

	pthread_mutex_destroy(&m_mutex4InteractMsgQue);
	pthread_mutex_destroy(&m_mutex4ConsumerMsgQue);
	pthread_mutex_destroy(&m_mutex4TopicMsgQue);
	pthread_mutex_destroy(&m_mutex4LogQue);

	pthread_mutex_destroy(&m_mutex4LoadDeviceList);
	pthread_mutex_destroy(&m_mutex4LoadGuarderList);
	pthread_mutex_destroy(&m_mutex4LoadOrgList);
	pthread_mutex_destroy(&m_mutex4LoadTaskList);

	pthread_mutex_destroy(&m_mutex4InteractSequence);
	pthread_mutex_destroy(&m_mutex4OrgList);
	pthread_mutex_destroy(&m_mutex4DeviceList);
	pthread_mutex_destroy(&m_mutex4GuarderList);
	pthread_mutex_destroy(&m_mutex4TaskList);

	pthread_mutex_destroy(&m_mutex4TaskId);

	if (m_uiLogInst) {
		LOG_Release(m_uiLogInst);
		m_uiLogInst = NULL;
	}

	activemq::library::ActiveMQCPP::shutdownLibrary();
}

int expand_protocol::ExpandProtocolService::Start(const char * pMsgSrvIp_, unsigned short usMsgPort_, 
	unsigned short usInteracPort_, const char * pBrokerURI_, const char * pDestURI_)
{
	int result = -1;
	if (m_nRun) {
		return 0;
	}
	if (!m_ctx) {
		m_ctx = zctx_new();
	}

	m_subscriber = zsocket_new(m_ctx, ZMQ_SUB);
	zsocket_set_subscribe(m_subscriber, "");
	zsocket_connect(m_subscriber, "tcp://%s:%u", (pMsgSrvIp_ && strlen(pMsgSrvIp_)) ? pMsgSrvIp_ : "127.0.0.1",
		usMsgPort_);
	m_interactor = zsocket_new(m_ctx, ZMQ_DEALER);
	zuuid_t * uuid = zuuid_new();
	const char * szUuid = zuuid_str(uuid);
	zsocket_set_identity(m_interactor, szUuid);
	zsocket_connect(m_interactor, "tcp://%s:%u", (pMsgSrvIp_ && strlen(pMsgSrvIp_)) ? pMsgSrvIp_ : "127.0.0.1",
		usInteracPort_);
	strcpy_s(m_szInteractorIdentity, sizeof(m_szInteractorIdentity), szUuid);
	zuuid_destroy(&uuid);

	loadOrgList();
	loadDeviceList();
	loadGuarderList();
	loadTaskList();

	m_nRun = 1;

	if (m_pthdDealLog.p == NULL) {
		pthread_create(&m_pthdDealLog, NULL, startDealLogThread, this);
	}
	if (m_pthdDealTopicMsg.p == NULL) {
		pthread_create(&m_pthdDealTopicMsg, NULL, startDealTopicMessageThread, this);
	}
	if (m_pthdDealInteractorMsg.p == NULL) {
		pthread_create(&m_pthdDealInteractorMsg, NULL, startDealInteractMessageThread, this);
	}
	if (m_pthdDealNetwork.p == NULL) {
		pthread_create(&m_pthdDealNetwork, NULL, startNetworkThread, this);
	}
	if (m_pthdDealConsumerMsg.p == NULL) {
		pthread_create(&m_pthdDealConsumerMsg, NULL, startDealConsumerMessageThread, this);
	}
	
	std::string strBrokerURI = pBrokerURI_;
	std::string strDestURI;
	if (pDestURI_ && strlen(pDestURI_)) {
		strDestURI = pDestURI_;
	}
	else {
		strDestURI = DEFAULT_LISTEN_DEST;
	}
	
	m_pMsgConsumer = new expand_protocol::SimpleAsyncConsumer(strBrokerURI, strDestURI, false);
	m_pMsgConsumer->setConsumerMessageCallback(expand_protocol::consumerMsgCb, this);
	m_pMsgConsumer->runConsumer();

	m_pPubMsgProducer = new expand_protocol::SimpleProducer(strBrokerURI, false);
	m_pPubMsgProducer->run();

	m_pRepMsgProducer = new expand_protocol::SimpleProducer(strBrokerURI, false);
	m_pRepMsgProducer->run();

	strcpy_s(m_szMsgServerIp, sizeof(m_szMsgServerIp), pMsgSrvIp_);
	m_usMsgPort = usMsgPort_;
	m_usInteractPort = usInteracPort_;

	strcpy_s(m_szBrokerAdderess, sizeof(m_szBrokerAdderess), pBrokerURI_);

	char szLog[512] = { 0 };
	sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]Broker start at %s:%hu|%hu, activemq broker=%s, "
		"destURI=%s\r\n", __FUNCTION__, __LINE__, pMsgSrvIp_, usMsgPort_, usInteracPort_, pBrokerURI_, 
		strDestURI.c_str());
	writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);

	return 0;
}

int expand_protocol::ExpandProtocolService::Stop()
{
	if (!m_nRun) {
		return 0;
	}
	char szLog[512] = { 0 };
	sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]Broker stop at %s:%hu|%hu, broker=%s\r\n", __FUNCTION__, 
		__LINE__, m_szMsgServerIp, m_usMsgPort, m_usInteractPort, m_szBrokerAdderess);
	writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);

	m_nRun = 0;
	memset(m_szBrokerAdderess, 0, sizeof(m_szBrokerAdderess));
	memset(m_szInteractorIdentity, 0, sizeof(m_szInteractorIdentity));
	memset(m_szMsgServerIp, 0, sizeof(m_szMsgServerIp));
	m_usInteractPort = m_usMsgPort = 0;

	m_pMsgConsumer->close();
	delete m_pMsgConsumer;
	m_pMsgConsumer = NULL;
	m_pPubMsgProducer->close();
	delete m_pPubMsgProducer;
	m_pPubMsgProducer = NULL;
	m_pRepMsgProducer->close();
	delete m_pRepMsgProducer;
	m_pRepMsgProducer = NULL;

	if (m_pthdDealNetwork.p) {
		pthread_join(m_pthdDealNetwork, NULL);
		m_pthdDealNetwork.p = NULL;
	}

	if (m_pthdDealLog.p) {
    pthread_cond_broadcast(&m_cond4LogQue);
		pthread_join(m_pthdDealLog, NULL);
		m_pthdDealNetwork.p = NULL;
	}

	if (m_pthdDealTopicMsg.p) {
    pthread_cond_broadcast(&m_cond4TopicMsgQue);
		pthread_join(m_pthdDealTopicMsg, NULL);
		m_pthdDealTopicMsg.p = NULL;
	}

	if (m_pthdDealInteractorMsg.p) {
    pthread_cond_broadcast(&m_cond4InteractMsgQue);
		pthread_join(m_pthdDealInteractorMsg, NULL);
		m_pthdDealInteractorMsg.p = NULL;
	}

	if (m_pthdDealConsumerMsg.p) {
    pthread_cond_broadcast(&m_cond4ConsumerMsgQue);
		pthread_join(m_pthdDealConsumerMsg, NULL);
		m_pthdDealConsumerMsg.p = NULL;
	}

	if (m_ctx) {
		if (m_subscriber) {
			zsocket_destroy(m_ctx, m_subscriber);
			m_subscriber = NULL;
		}
		if (m_interactor) {
			zsocket_destroy(m_ctx, m_interactor);
			m_interactor = NULL;
		}
		zctx_destroy(&m_ctx);
		m_ctx = NULL;
	}

	return 0;
}

void expand_protocol::ExpandProtocolService::SetLogType(unsigned short usLogType_)
{
	if (m_usLogType != usLogType_) {
		if (m_uiLogInst) {
			pf_logger::LogConfig logConf;
			LOG_GetConfig(m_uiLogInst, &logConf);
			if (logConf.usLogType != usLogType_) {
				logConf.usLogType = usLogType_;
			}
			LOG_SetConfig(m_uiLogInst, logConf);
			m_usLogType = usLogType_;
		}
	}
}

void expand_protocol::ExpandProtocolService::initLog(const char * pLogPath_)
{
	m_uiLogInst = LOG_Init();
	if (m_uiLogInst) {
		pf_logger::LogConfig logConf;
		strcpy_s(logConf.szLogPath, sizeof(logConf.szLogPath), pLogPath_);
		logConf.usLogPriority = pf_logger::eLOGPRIO_ALL;
		logConf.usLogType = pf_logger::eLOGTYPE_FILE;
		LOG_SetConfig(m_uiLogInst, logConf);
	}
}

bool expand_protocol::ExpandProtocolService::addLog(expand_protocol::LogContext * pLogCtx_)
{
	bool result = false;
	if (pLogCtx_ && pLogCtx_->pLogData && pLogCtx_->uiDataLen) {
		pthread_mutex_lock(&m_mutex4LogQue);
		m_logQue.emplace(pLogCtx_);
		if (m_logQue.size() == 1) {
			pthread_cond_signal(&m_cond4LogQue);
		}
		result = true;
		pthread_mutex_unlock(&m_mutex4LogQue);
	}
	return result;
}

void expand_protocol::ExpandProtocolService::writeLog(const char * pLogContent_,
	unsigned short usLogCategory_, unsigned short usLogType_)
{
	if (pLogContent_) {
		expand_protocol::LogContext * pLogCtx = new expand_protocol::LogContext();
		memset(pLogCtx, 0, sizeof(expand_protocol::LogContext));
		pLogCtx->uiDataLen = strlen(pLogContent_);
		pLogCtx->usLogCategory = usLogCategory_;
		pLogCtx->usLogType = usLogType_;
		pLogCtx->pLogData = new char[pLogCtx->uiDataLen + 1];
		memcpy_s(pLogCtx->pLogData, pLogCtx->uiDataLen + 1, pLogContent_, pLogCtx->uiDataLen);
		pLogCtx->pLogData[pLogCtx->uiDataLen] = '\0';
		if (!addLog(pLogCtx)) {
			if (pLogCtx) {
				if (pLogCtx->pLogData) {
					delete[] pLogCtx->pLogData;
					pLogCtx->pLogData = NULL;
				}
			}
			delete pLogCtx;
			pLogCtx = NULL;
		}
	}
}

void expand_protocol::ExpandProtocolService::dealLog()
{
	do {
		pthread_mutex_lock(&m_mutex4LogQue);
		while (m_nRun && m_logQue.empty()) {
			pthread_cond_wait(&m_cond4LogQue, &m_mutex4LogQue);
		}
		if (!m_nRun && m_logQue.empty()) {
			pthread_mutex_unlock(&m_mutex4LogQue);
			break;
		}
		expand_protocol::LogContext * pLogCtx = m_logQue.front();
		m_logQue.pop();
		pthread_mutex_unlock(&m_mutex4LogQue);
		if (pLogCtx) {
			if (pLogCtx->pLogData) {
				if (m_uiLogInst) {
					LOG_Log(m_uiLogInst, pLogCtx->pLogData, pLogCtx->usLogCategory, pLogCtx->usLogType);
				}
				delete[] pLogCtx->pLogData;
				pLogCtx->pLogData = NULL;
			}
			delete pLogCtx;
			pLogCtx = NULL;
		}
	} while (1);
}

void expand_protocol::ExpandProtocolService::decryptMessage(unsigned char * pData_,
	unsigned int uiBeginIndex_, unsigned int uiEndIndex_, unsigned short usWord_)
{
	if (uiEndIndex_ > uiBeginIndex_) {
		for (unsigned int i = uiBeginIndex_; i < uiEndIndex_; i++) {
			pData_[i] ^= usWord_;
			pData_[i] -= 1;
		}
	}
}

void expand_protocol::ExpandProtocolService::encryptMessage(unsigned char * pData_,
	unsigned int uiBeginIndex_, unsigned int uiEndIndex_, unsigned short usWord_)
{
	if (uiEndIndex_ > uiBeginIndex_) {
		for (unsigned int i = uiBeginIndex_; i < uiEndIndex_; i++) {
			pData_[i] += 1;
			pData_[i] ^= usWord_;
		}
	}
}

bool expand_protocol::ExpandProtocolService::addConsumerMessage(
	expand_protocol::ConsumerMessage * pConsumerMsg_)
{
	bool result = false;
	if (pConsumerMsg_ && pConsumerMsg_->pData) {
		pthread_mutex_lock(&m_mutex4ConsumerMsgQue);
		m_consumerMsgQue.emplace(pConsumerMsg_);
		if (m_consumerMsgQue.size() == 1) {
			pthread_cond_signal(&m_cond4ConsumerMsgQue);
		}
		result = true;
		pthread_mutex_unlock(&m_mutex4ConsumerMsgQue);
	}
	return result;
}

void expand_protocol::ExpandProtocolService::dealConsumerMessage()
{
	do {
		pthread_mutex_lock(&m_mutex4ConsumerMsgQue);
		while (m_nRun && m_consumerMsgQue.empty()) {
			pthread_cond_wait(&m_cond4ConsumerMsgQue, &m_mutex4ConsumerMsgQue);
		}
		if (!m_nRun && m_consumerMsgQue.empty()) {
			pthread_mutex_unlock(&m_mutex4ConsumerMsgQue);
			break;
		}
		expand_protocol::ConsumerMessage * pConsumerMsg = m_consumerMsgQue.front();
		m_consumerMsgQue.pop();
		pthread_mutex_unlock(&m_mutex4ConsumerMsgQue);
		if (pConsumerMsg) {
			if (pConsumerMsg->pData) {
				char szLog[1024] = { 0 };
				decryptMessage(pConsumerMsg->pData, 0, pConsumerMsg->uiDataLen, kReqSafeKey);
				std::string ansiStr = Utf8ToAnsi((char *)pConsumerMsg->pData);
				sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]receive message: %s\r\n",
					__FUNCTION__, __LINE__, ansiStr.c_str());
				writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
				rapidjson::Document doc;
				if (!doc.Parse(ansiStr.c_str()).HasParseError()) {
					unsigned short usCmd;
					std::string strDestURI;
					if (doc.HasMember("cmd")) {
						if (doc["cmd"].IsInt()) {
							usCmd = (unsigned short)doc["cmd"].GetInt();
						}
					}
					if (doc.HasMember("destURI")) {
						if (doc["destURI"].IsString()) {
							strDestURI = doc["destURI"].GetString();
						}
					}
					switch (usCmd) {
						case escort_protocol::CMD_START_TASK_REQ: {
							expand_protocol::RequestStartTask reqStartTask;
							memset(&reqStartTask, 0, sizeof(reqStartTask));
							if (doc.HasMember("guarder")) {
								if (doc["guarder"].IsString()) {
									if (doc["guarder"].GetStringLength()) {
										strcpy_s(reqStartTask.szGuarderId, sizeof(reqStartTask.szGuarderId),
											doc["guarder"].GetString());
									}
								}
							}
							if (doc.HasMember("deviceId")) {
								if (doc["deviceId"].IsString()) {
									if (doc["deviceId"].GetStringLength()) {
										strcpy_s(reqStartTask.szDeviceId, sizeof(reqStartTask.szDeviceId), 
											doc["deviceId"].GetString());
									}
								}
							}
							if (doc.HasMember("destination")) {
								if (doc["destination"].IsString()) {
									if (doc["destination"].GetStringLength()) {
										strcpy_s(reqStartTask.szDestination, sizeof(reqStartTask.szDestination),
											doc["destination"].GetString());
									}
								}
							}
							if (doc.HasMember("target")) {
								if (doc["target"].IsString()) {
									if (doc["target"].GetStringLength()) {
										strcpy_s(reqStartTask.szTarget, sizeof(reqStartTask.szTarget),
											doc["target"].GetString());
									}
								}
							}
							if (doc.HasMember("reqTime")) {
								if (doc["reqTime"].IsString()) {
									if (doc["reqTime"].GetStringLength()) {
										strcpy_s(reqStartTask.szRequestTime, sizeof(reqStartTask.szRequestTime),
											doc["reqTime"].GetString());
									}
								}
							}
							if (doc.HasMember("seq")) {
								if (doc["seq"].IsInt()) {
									reqStartTask.usReqSeq = (unsigned short)doc["seq"].GetInt();
								}
							}
							if (doc.HasMember("limit")) {
								if (doc["limit"].IsInt()) {
									reqStartTask.usDistanceLimit = (unsigned short)doc["limit"].GetInt();
								}
							}
							if (doc.HasMember("type")) {
								if (doc["type"].IsInt()) {
									reqStartTask.usTaskType = (unsigned short)doc["type"].GetInt();
								}
							}
							if (strlen(reqStartTask.szDeviceId) && strlen(reqStartTask.szGuarderId)
								&& strlen(reqStartTask.szRequestTime) && strlen(reqStartTask.szTarget)) {
								handleTaskStart(&reqStartTask, strDestURI);
							}
							else {
								sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]request task start parameter miss, "
									"deviceId=%s, guarder=%s, reqTime=%s, target=%s, destination=%s, seq=%hu, limit=%hu,"
									" type=%hu\r\n", __FUNCTION__, __LINE__, reqStartTask.szDeviceId,
									reqStartTask.szGuarderId, reqStartTask.szRequestTime, reqStartTask.szTarget,
									reqStartTask.szDestination, reqStartTask.usReqSeq, reqStartTask.usDistanceLimit,
									reqStartTask.usTaskType);
								writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
							}
							break;
						}
						case escort_protocol::CMD_STOP_TASK_REQ: {
							expand_protocol::RequestStopTask reqStopTask;
							memset(&reqStopTask, 0, sizeof(reqStopTask));
							if (doc.HasMember("req")) {
								if (doc["req"].IsInt()) {
									reqStopTask.usCmd = (unsigned short)doc["req"].GetInt();
								}
							}
							if (doc.HasMember("reqTime")) {
								if (doc["reqTime"].IsString()) {
									if (doc["reqTime"].GetStringLength()) {
										strcpy_s(reqStopTask.szRequestTime, sizeof(reqStopTask.szRequestTime),
											doc["reqTime"].GetString());
									}
								}
							}
							if (doc.HasMember("taskId")) {
								if (doc["taskId"].IsString()) {
									if (doc["taskId"].GetStringLength()) {
										strcpy_s(reqStopTask.szTaskId, sizeof(reqStopTask.szTaskId),
											doc["taskId"].GetString());
									}
								}
							}
							if (strlen(reqStopTask.szRequestTime) && strlen(reqStopTask.szTaskId)) {
								handleTaskStop(&reqStopTask, strDestURI);
							}
							else {
								sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]request stop task parameter miss,"
									" taskId=%s, reqTime=%s, seq=%hu\r\n", __FUNCTION__, __LINE__,
									reqStopTask.szTaskId, reqStopTask.szRequestTime, reqStopTask.usReqSeq);
								writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
							}
							break;
						}
						default: {
							sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]consume message unsupport cmd %hu\r\n",
								__FUNCTION__, __LINE__, usCmd);
							writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
							break;
						}
					}
				}
				else {
					sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]can't parse JSON error %d\r\n",
						__FUNCTION__, __LINE__, doc.GetParseError());
					writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
				}
				delete[] pConsumerMsg->pData;
				pConsumerMsg->pData = NULL;
			}
			delete pConsumerMsg;
			pConsumerMsg = NULL;
		}
	} while (1);
}

bool expand_protocol::ExpandProtocolService::addTopicMessage(TopicMessage * pTopicMsg_)
{
	bool result = false;
	if (pTopicMsg_ && strlen(pTopicMsg_->szMsgUuid) && strlen(pTopicMsg_->szMsgBody)) {
		pthread_mutex_lock(&m_mutex4TopicMsgQue);
		m_topicMsgQue.emplace(pTopicMsg_);
		if (m_topicMsgQue.size() == 1) {
			pthread_cond_signal(&m_cond4TopicMsgQue);
		}
		pthread_mutex_unlock(&m_mutex4TopicMsgQue);
		result = true;
	}
	return result;
}

void expand_protocol::ExpandProtocolService::dealTopicMessage()
{
	char szLog[1024] = {0};
	do {
		pthread_mutex_lock(&m_mutex4TopicMsgQue);
		while (m_nRun && m_topicMsgQue.empty()) {
			pthread_cond_wait(&m_cond4TopicMsgQue, &m_mutex4TopicMsgQue);
		}
		if (!m_nRun && m_topicMsgQue.empty()) {
			pthread_mutex_unlock(&m_mutex4TopicMsgQue);
			break;
		}
		TopicMessage * pTopicMsg = m_topicMsgQue.front();
		m_topicMsgQue.pop();
		pthread_mutex_unlock(&m_mutex4TopicMsgQue);
		if (pTopicMsg) {
			rapidjson::Document doc;
			switch (pTopicMsg->usMsgType) {
				case PUBMSG_DEVICE_ALIVE: {
					if (!doc.Parse(pTopicMsg->szMsgBody).HasParseError()) {
						TopicAliveMessage aliveMsg;
						memset(&aliveMsg, 0, sizeof(aliveMsg));
						if (doc.HasMember("factoryId")) {
							if (doc["factoryId"].IsString() && doc["factoryId"].GetStringLength()) {
								strcpy_s(aliveMsg.szFactoryId, sizeof(aliveMsg.szFactoryId),
									doc["factoryId"].GetString());
							}
						}
						if (doc.HasMember("deviceId")) {
							if (doc["deviceId"].IsString() && doc["deviceId"].GetStringLength()) {
								strcpy_s(aliveMsg.szDeviceId, sizeof(aliveMsg.szDeviceId), 
									doc["deviceId"].GetString());
							}
						}
						if (doc.HasMember("orgId")) {
							if (doc["orgId"].IsString() && doc["orgId"].GetStringLength()) {
								strcpy_s(aliveMsg.szOrg, sizeof(aliveMsg.szOrg), doc["orgId"].GetString());
							}
						}
						if (doc.HasMember("battery")) {
							if (doc["battery"].IsInt()) {
								aliveMsg.usBattery = (unsigned short)doc["battery"].GetInt();
							}
						}
						if (doc.HasMember("datetime")) {
							if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
								aliveMsg.ulMessageTime = makeDatetime(doc["datetime"].GetString());
							}
						}
						if (strlen(aliveMsg.szDeviceId) && strlen(aliveMsg.szOrg) && aliveMsg.ulMessageTime > 0){
							handleTopicAliveMessage(&aliveMsg);
						}
						else {
							sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]topic alive message miss parameter, deviceId=%s,"
								" orgId=%s, battery=%hu, datetime=%lu, uuid=%s, mark=%s, from=%s, seq=%hu\r\n", __FUNCTION__,
								__LINE__, aliveMsg.szDeviceId, aliveMsg.szOrg, aliveMsg.usBattery, aliveMsg.ulMessageTime,
								pTopicMsg->szMsgUuid, pTopicMsg->szMsgMark, pTopicMsg->szMsgFrom, pTopicMsg->usMsgSequence);
							writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
						}
					}	
					else {
						sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]parse topic alive message error=%d, body=%s, uuid=%s"
							", msgMark=%s, msgSeq=%hu, msgFrom=%s\r\n", __FUNCTION__, __LINE__, doc.GetParseError(), 
							pTopicMsg->szMsgBody, pTopicMsg->szMsgUuid, pTopicMsg->szMsgMark, pTopicMsg->usMsgSequence,
							pTopicMsg->szMsgFrom);
						writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
					}
					break;
				}
				case PUBMSG_DEVICE_LOCATE: {
					if (!doc.Parse(pTopicMsg->szMsgBody).HasParseError()) {
						int nSubType = -1;
						if (doc.HasMember("subType")) {
							if (doc["subType"].IsInt()) {
								nSubType = doc["subType"].GetInt();
							}
						}
						switch (nSubType) {
							case LOCATE_GPS: {
								TopicLocateMessageGps locateGps;
								memset(&locateGps, 0, sizeof(locateGps));
								if (doc.HasMember("factoryId")) {
									if (doc["factoryId"].IsString() && doc["factoryId"].GetStringLength()) {
										strcpy_s(locateGps.szFactoryId, sizeof(locateGps.szFactoryId),
											doc["factoryId"].GetString());
									}
								}
								if (doc.HasMember("deviceId")) {
									if (doc["deviceId"].IsString() && doc["deviceId"].GetStringLength()) {
										strcpy_s(locateGps.szDeviceId, sizeof(locateGps.szDeviceId),
											doc["deviceId"].GetString());
									}
								}
								if (doc.HasMember("orgId")) {
									if (doc["orgId"].IsString() && doc["orgId"].GetStringLength()) {
										strcpy_s(locateGps.szOrg, sizeof(locateGps.szOrg),
											doc["orgId"].GetString());
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										locateGps.ulMessageTime = makeDatetime(doc["datetime"].GetString());
									}
								}
								if (doc.HasMember("latitude")) {
									if (doc["latitude"].IsDouble()) {
										locateGps.dLat = doc["latitude"].GetDouble();
									}
								}
								if (doc.HasMember("lngitude")) {
									if (doc["lngitude"].IsDouble()) {
										locateGps.dLng = doc["lngitude"].GetDouble();
									}
								}
								if (doc.HasMember("speed")) {
									if (doc["speed"].IsDouble()) {
										locateGps.dSpeed = doc["speed"].GetDouble();
									}
								}
								if (doc.HasMember("direction")) {
									if (doc["direction"].IsInt()) {
										locateGps.nDirection = doc["direction"].GetInt();
									}
								}
								if (doc.HasMember("latType")) {
									if (doc["latType"].IsInt()) {
										locateGps.usLatType = (unsigned short)doc["latType"].GetInt();
									}
								}
								if (doc.HasMember("lngType")) {
									if (doc["lngType"].IsInt()) {
										locateGps.usLngType = (unsigned short)doc["lngType"].GetInt();
									}
								}
								if (doc.HasMember("sattelite")) {
									if (doc["sattelite"].IsInt()) {
										locateGps.usStattelite = (unsigned short)doc["sattelite"].GetInt();
									}
								}
								if (doc.HasMember("intensity")) {
									if (doc["intensity"].IsInt()) {
										locateGps.usIntensity = (unsigned short)doc["intensity"].GetInt();
									}
								}
								if (doc.HasMember("locateFlag")) {
									if (doc["locateFlag"].IsInt()) {
										locateGps.nFlag = doc["locateFlag"].GetInt();
									}
								}
								if (doc.HasMember("coordinate")) {
									if (doc["coordinate"].IsInt()) {
										locateGps.nCoordinate = doc["coordinate"].GetInt();
									}
								}
								if (doc.HasMember("battery")) {
									if (doc["battery"].IsInt()) {
										locateGps.usBattery = (unsigned short)doc["battery"].GetInt();
									}
								}
								if (strlen(locateGps.szDeviceId) && strlen(locateGps.szOrg) 
									&& locateGps.ulMessageTime > 0) {
									handleTopicLocateGpsMessage(&locateGps);
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]parse Topic GPS locate message miss "
										"parameter, deviceId=%s, orgId=%s, lat=%f, lng=%f, speed=%f, direction=%d, "
										"coordinate=%d, battery=%hu, locateFlag=%d, sattelite=%d, intensity=%d, msgUuid=%s,"
										" msgMark=%s, msgSeq=%hu, msgFrom=%s\r\n", __FUNCTION__, __LINE__, locateGps.szDeviceId,
										locateGps.szOrg, locateGps.dLat, locateGps.dLng, locateGps.dSpeed, locateGps.nDirection,
										locateGps.nCoordinate, locateGps.usBattery, locateGps.nFlag, locateGps.usStattelite,
										locateGps.usIntensity, pTopicMsg->szMsgUuid, pTopicMsg->szMsgMark, 
										pTopicMsg->usMsgSequence, pTopicMsg->szMsgFrom);
									writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								}
								break;
							}
							case LOCATE_LBS: {
								TopicLocateMessageLbs locateLbs;
								memset(&locateLbs, 0, sizeof(locateLbs));
								if (doc.HasMember("factoryId")) {
									if (doc["factoryId"].IsString() && doc["factoryId"].GetStringLength()) {
										strcpy_s(locateLbs.szFactoryId, sizeof(locateLbs.szFactoryId),
											doc["factoryId"].GetString());
									}
								}
								if (doc.HasMember("deviceId")) {
									if (doc["deviceId"].IsString() && doc["deviceId"].GetStringLength()) {
										strcpy_s(locateLbs.szDeviceId, sizeof(locateLbs.szDeviceId),
											doc["deviceId"].GetString());
									}
								}
								if (doc.HasMember("orgId")) {
									if (doc["orgId"].IsString() && doc["orgId"].GetStringLength()) {
										strcpy_s(locateLbs.szOrg, sizeof(locateLbs.szOrg), doc["orgId"].GetString());
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										locateLbs.ulMessageTime = makeDatetime(doc["datetime"].GetString());
									}
								}
								if (doc.HasMember("latitude")) {
									if (doc["latitude"].IsDouble()) {
										locateLbs.dLat = doc["latitude"].GetDouble();
									}
								}
								if (doc.HasMember("lngitude")) {
									if (doc["lngitude"].IsDouble()) {
										locateLbs.dLng = doc["lngitude"].GetDouble();
									}
								}
								if (doc.HasMember("latType")) {
									if (doc["latType"].IsInt()) {
										locateLbs.usLatType = (unsigned short)doc["latType"].GetInt();
									}
								}
								if (doc.HasMember("lngType")) {
									if (doc["lngType"].IsInt()) {
										locateLbs.usLngType = (unsigned short)doc["lngType"].GetInt();
									}
								}
								if (doc.HasMember("locateFlag")) {
									if (doc["locateFlag"].IsInt()) {
										locateLbs.usFlag = (unsigned short)doc["locateFlag"].GetInt();
									}
								}
								if (doc.HasMember("battery")) {
									if (doc["battery"].IsInt()) {
										locateLbs.usBattery = (unsigned short)doc["battery"].GetInt();
									}
								}
								if (doc.HasMember("precision")) {
									if (doc["precision"].IsInt()) {
										locateLbs.nPrecision = doc["precision"].GetInt();
									}
								}
								if (doc.HasMember("coordinate")) {
									if (doc["coordinate"].IsInt()) {
										locateLbs.nCoordinate = doc["coordinate"].GetInt();
									}
								}
								if (strlen(locateLbs.szDeviceId) && strlen(locateLbs.szOrg)
									&& locateLbs.ulMessageTime > 0) {
									handleTopicLocateLbsMessage(&locateLbs);
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]parse Topic GPS locate message miss "
										"parameter, deviceId=%s, orgId=%s, lat=%f, lng=%f, coordinate=%d, precision=%d, "
										"battery=%hu, locateFlag=%d, msgUuid=%s, msgMark=%s, msgSeq=%hu, msgFrom=%s\r\n",
										__FUNCTION__, __LINE__, locateLbs.szDeviceId, locateLbs.szOrg, locateLbs.dLat, 
										locateLbs.dLng, locateLbs.nCoordinate, locateLbs.nPrecision, locateLbs.usBattery, 
										locateLbs.usFlag, pTopicMsg->szMsgUuid, pTopicMsg->szMsgMark, pTopicMsg->usMsgSequence,
										pTopicMsg->szMsgFrom);
									writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								}
								break;
							}
							case LOCATE_APP: {
								TopicLocateMessageApp locateApp;
								memset(&locateApp, 0, sizeof(locateApp));
								if (doc.HasMember("factroyId")) {
									if (doc["factoryId"].IsString() && doc["factoryId"].GetStringLength()) {
										strcpy_s(locateApp.szFactoryId, sizeof(locateApp.szFactoryId),
											doc["factoryId"].GetString());
									}
								}
								if (doc.HasMember("deviceId")) {
									if (doc["deviceId"].IsString() && doc["deviceId"].GetStringLength()) {
										strcpy_s(locateApp.szDeviceId, sizeof(locateApp.szDeviceId),
											doc["deviceId"].GetString());
									}
								}
								if (doc.HasMember("orgId")) {
									if (doc["orgId"].IsString() && doc["orgId"].GetStringLength()) {
										strcpy_s(locateApp.szOrg, sizeof(locateApp.szOrg),
											doc["orgId"].GetString());
									}
								}
								if (doc.HasMember("taskId")) {
									if (doc["taskId"].IsString() && doc["taskId"].GetStringLength()) {
										strcpy_s(locateApp.szTaskId, sizeof(locateApp.szTaskId),
											doc["taskId"].GetString());
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										locateApp.ulMessageTime = makeDatetime(doc["factoryId"].GetString());
									}
								}
								if (doc.HasMember("latitude")) {
									if (doc["latitude"].IsDouble()) {
										locateApp.dLat = doc["latitude"].GetDouble();
									}
								}
								if (doc.HasMember("lngitude")) {
									if (doc["lngitude"].IsDouble()) {
										locateApp.dLng = doc["lngitude"].GetDouble();
									}
								}
								if (doc.HasMember("coordinate")) {
									if (doc["coordinate"].IsInt()) {
										locateApp.nCoordinate = doc["coordinate"].GetInt();
									}
								}
								if (doc.HasMember("battery")) {
									if (doc["battery"].IsInt()) {
										locateApp.usBattery = (unsigned short)doc["battery"].GetInt();
									}
								}
								if (strlen(locateApp.szDeviceId) && strlen(locateApp.szOrg)
									&& strlen(locateApp.szTaskId) && locateApp.ulMessageTime > 0) {
									handleTopicLocateAppMessage(&locateApp);
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]parse Topic locate app message miss "
										"parameter, deviceId=%s, orgId=%s, taskId=%s, datetime=%lu, lat=%f, lng=%f, "
										"battery=%hu, coordinate=%d, msgUuid=%s, msgMark=%s, msgSeq=%hu, msgFrom=%s\r\n",
										__FUNCTION__, __LINE__, locateApp.szDeviceId, locateApp.szOrg, locateApp.szTaskId,
										locateApp.ulMessageTime, locateApp.dLat, locateApp.dLng, locateApp.usBattery,
										locateApp.nCoordinate, pTopicMsg->szMsgUuid, pTopicMsg->szMsgMark,
										pTopicMsg->usMsgSequence, pTopicMsg->szMsgFrom);
									writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								}
								break;
							}
							default: {
								sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]parse topic locate message unsupport "
									"type=%d, body=%s, msgUuid=%s, msgMark=%s, msgSeq=%hu, msgFrom=%s\r\n", __FUNCTION__, 
									__LINE__, nSubType, pTopicMsg->szMsgBody, pTopicMsg->szMsgUuid, pTopicMsg->szMsgMark,
									pTopicMsg->usMsgSequence, pTopicMsg->szMsgFrom);
								writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								break;
							}
						}
					}
					else {
						sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]parse device locate message json error=%d,"
							" body=%s, msgUuid=%s, msgMark=%s, msgSeq=%hu, msgFrom=%s\r\n", __FUNCTION__, __LINE__,
							doc.GetParseError(), pTopicMsg->szMsgBody, pTopicMsg->szMsgUuid, pTopicMsg->szMsgMark,
							pTopicMsg->usMsgSequence, pTopicMsg->szMsgFrom);
						writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
					}
					break;
				}
				case PUBMSG_DEVICE_ALARM: {
					if (!doc.Parse(pTopicMsg->szMsgBody).HasParseError()) {
						int nSubType = -1;
						if (doc.HasMember("subType")) {
							if (doc["subType"].IsInt()) {
								nSubType = doc["subType"].GetInt();
							}
						}
						switch (nSubType) {
							case ALARM_DEVICE_LOWPOWER: {
								TopicAlarmMessageLowpower lpAlarmMsg;
								memset(&lpAlarmMsg, 0, sizeof(lpAlarmMsg));
								if (doc.HasMember("factoryId")) {
									if (doc["factoryId"].IsString() && doc["factoryId"].GetStringLength()) {
										strcpy_s(lpAlarmMsg.szFactoryId, sizeof(lpAlarmMsg.szFactoryId),
												doc["factoryId"].GetString());
									}
								}
								if (doc.HasMember("deviceId")) {
									if (doc["deviceId"].IsString() && doc["deviceId"].GetStringLength()) {
										strcpy_s(lpAlarmMsg.szDeviceId, sizeof(lpAlarmMsg.szDeviceId),
											doc["deviceId"].GetString());
									}
								}
								if (doc.HasMember("orgId")) {
									if (doc["orgId"].IsString() && doc["orgId"].GetStringLength()) {
										strcpy_s(lpAlarmMsg.szOrg, sizeof(lpAlarmMsg.szOrg), 
											doc["orgId"].GetString());
									}
								}
								if (doc.HasMember("battery")) {
									if (doc["battery"].IsInt()) {
										lpAlarmMsg.usBattery = (unsigned short)doc["battery"].GetInt();
									}
								}
								if (doc.HasMember("mode")) {
									if (doc["mode"].IsInt()) {
										lpAlarmMsg.usMode = (unsigned short)doc["mode"].GetInt();
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										lpAlarmMsg.ulMessageTime = makeDatetime(doc["datetime"].GetString());
									}
								}
								if (strlen(lpAlarmMsg.szDeviceId) && strlen(lpAlarmMsg.szOrg)
									&& lpAlarmMsg.ulMessageTime > 0) {
									handleTopicAlarmLowpowerMessage(&lpAlarmMsg);
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]parse Topic lowpower alarm miss"
										" parameter, deviceId=%s, orgId=%s, battery=%hu, mode=%hu, datetime=%lu, "
										"msgUuid=%s, msgMark=%s, msgSeq=%hu, msgFrom=%s\r\n", __FUNCTION__, __LINE__,
										lpAlarmMsg.szDeviceId, lpAlarmMsg.szOrg, lpAlarmMsg.usBattery, lpAlarmMsg.usMode,
										lpAlarmMsg.ulMessageTime, pTopicMsg->szMsgUuid, pTopicMsg->szMsgMark,
										pTopicMsg->usMsgSequence, pTopicMsg->szMsgFrom);
									writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								}
								break;
							}
							case ALARM_DEVICE_LOOSE: {
								TopicAlarmMessageLoose lsAlarmMsg;
								memset(&lsAlarmMsg, 0, sizeof(lsAlarmMsg));
								if (doc.HasMember("factoryId")) {
									if (doc["factoryId"].IsString() && doc["factoryId"].GetStringLength()) {
										strcpy_s(lsAlarmMsg.szFactoryId, sizeof(lsAlarmMsg.szFactoryId),
											doc["factoryId"].GetString());
									}
								}
								if (doc.HasMember("deviceId")) {
									if (doc["deviceId"].IsString() && doc["deviceId"].GetStringLength()) {
										strcpy_s(lsAlarmMsg.szDeviceId, sizeof(lsAlarmMsg.szDeviceId),
											doc["deviceId"].GetString());
									}
								}
								if (doc.HasMember("orgId")) {
									if (doc["orgId"].IsString() && doc["orgId"].GetStringLength()) {
										strcpy_s(lsAlarmMsg.szOrg, sizeof(lsAlarmMsg.szOrg),
											doc["orgId"].GetString());
									}
								}
								if (doc.HasMember("mode")) {
									if (doc["mode"].IsInt()) {
										lsAlarmMsg.usMode = (unsigned short)doc["mode"].GetInt();
									}
								}
								if (doc.HasMember("battery")) {
									if (doc["battery"].IsInt()) {
										lsAlarmMsg.usBattery = (unsigned short)doc["battery"].GetInt();
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										lsAlarmMsg.ulMessageTime = makeDatetime(doc["datetime"].GetString());
									
									}
								}
								if (strlen(lsAlarmMsg.szDeviceId) && strlen(lsAlarmMsg.szOrg) 
									&& lsAlarmMsg.ulMessageTime > 0) {
									handleTopicAlarmLooseMsg(&lsAlarmMsg);
								}
								break;
							}
							case ALARM_DEVICE_FLEE: {
								TopicAlarmMessageFlee fleeAlarmMsg;
								memset(&fleeAlarmMsg, 0, sizeof(TopicAlarmMessageFlee));
								if (doc.HasMember("factoryId")) {
									if (doc["factoryId"].IsString()) {
										size_t nSize = doc["factoryId"].GetStringLength();
										if (nSize) {
											strncpy_s(fleeAlarmMsg.szFactoryId, sizeof(fleeAlarmMsg.szFactoryId),
												doc["factoryId"].GetString(), nSize);
										}
									}
								}
								if (doc.HasMember("deviceId")) {
									if (doc["deviceId"].IsString()) {
										size_t nSize = doc["deviceId"].GetStringLength();
										if (nSize) {
											strncpy_s(fleeAlarmMsg.szDeviceId, sizeof(fleeAlarmMsg.szDeviceId),
												doc["deviceId"].GetString(), nSize);
										}
									}
								}
								if (doc.HasMember("orgId")) {
									if (doc["orgId"].IsString()) {
										size_t nSize = doc["orgId"].GetStringLength();
										if (nSize) {
											strncpy_s(fleeAlarmMsg.szOrg, sizeof(fleeAlarmMsg.szOrg),
												doc["orgId"].GetString(), nSize);
										}
									}
								}
								if (doc.HasMember("guarder")) {
									if (doc["guarder"].IsString()) {
										size_t nSize = doc["guarder"].GetStringLength();
										if (nSize) {
											strncpy_s(fleeAlarmMsg.szGuarder, sizeof(fleeAlarmMsg.szGuarder),
												doc["guarder"].GetString(), nSize);
										}
									}
								}
								if (doc.HasMember("taskId")) {
									if (doc["taskId"].IsString()) {
										size_t nSize = doc["taskId"].GetStringLength();
										if (nSize) {
											strncpy_s(fleeAlarmMsg.szTaskId, sizeof(fleeAlarmMsg.szTaskId),
												doc["taskId"].GetString(), nSize);
										}
									}
								}
								if (doc.HasMember("battery")) {
									if (doc["battery"].IsInt()) {
										fleeAlarmMsg.usBattery = (unsigned short)doc["battery"].GetInt();
									}
								}
								if (doc.HasMember("mode")) {
									if (doc["mode"].IsInt()) {
										fleeAlarmMsg.usMode = (unsigned short)doc["mode"].GetInt();
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										fleeAlarmMsg.ulMessageTime = makeDatetime(doc["datetime"].GetString());
									}
								}
								if (strlen(fleeAlarmMsg.szDeviceId) && strlen(fleeAlarmMsg.szGuarder) 
									&& strlen(fleeAlarmMsg.szTaskId) && strlen(fleeAlarmMsg.szOrg) 
									&& fleeAlarmMsg.ulMessageTime) {
									handleTopicAlarmFleeMsg(&fleeAlarmMsg);
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]parse flee message data miss,"
										" deviceId=%s, orgId=%s, guarder=%s, taskId=%s, mode=%hu, battery=%hu, "
										"datetime=%lu, msgUuid=%s, msgMark=%s, msgSeq=%hu, msgFrom=%s\r\n", 
										__FUNCTION__, __LINE__, fleeAlarmMsg.szDeviceId, fleeAlarmMsg.szOrg, 
										fleeAlarmMsg.szGuarder, fleeAlarmMsg.szTaskId, fleeAlarmMsg.usMode, 
										fleeAlarmMsg.usBattery, fleeAlarmMsg.ulMessageTime, pTopicMsg->szMsgUuid,
										pTopicMsg->szMsgMark, pTopicMsg->usMsgSequence, pTopicMsg->szMsgFrom);
									writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								}
								break;
							}
							case ALARM_LOCATE_LOST: {
								TopicAlarmMessageLocateLost alarmLocateLost;
								memset(&alarmLocateLost, 0, sizeof(alarmLocateLost));
								if (doc.HasMember("factoryId")) {
									if (doc["factoryId"].IsString() && doc["factoryId"].GetStringLength()) {
										strcpy_s(alarmLocateLost.szFactoryId, sizeof(alarmLocateLost.szFactoryId),
											doc["factoryId"].GetString());
									}
								}
								if (doc.HasMember("deviceId")) {
									if (doc["deviceId"].IsString() && doc["deviceId"].GetStringLength()) {
										strcpy_s(alarmLocateLost.szDeviceId, sizeof(alarmLocateLost.szDeviceId),
											doc["deviceId"].GetString());
									}
								}
								if (doc.HasMember("orgId")) {
									if (doc["orgId"].IsString() && doc["orgId"].GetStringLength()) {
										strcpy_s(alarmLocateLost.szOrg, sizeof(alarmLocateLost.szOrg),
											doc["orgId"].GetString());
									}
								}
								if (doc.HasMember("guarder")) {
									if (doc["guarder"].IsString() && doc["guarder"].GetStringLength()) {
										strcpy_s(alarmLocateLost.szGuarder, sizeof(alarmLocateLost.szGuarder),
											doc["guarder"].GetString());
									}
								}
								if (doc.HasMember("battery")) {
									if (doc["battery"].IsInt()) {
										alarmLocateLost.usDeviceBattery = (unsigned short)doc["battery"].GetInt();
									}
								}
								if (doc.HasMember("mode")) {
									if (doc["mode"].IsInt()) {
										alarmLocateLost.usAlarmMode = (unsigned short)doc["mode"].GetInt();
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										alarmLocateLost.ulMessageTime = makeDatetime(doc["datetime"].GetString());
									}
								}
								if (strlen(alarmLocateLost.szDeviceId) && strlen(alarmLocateLost.szOrg)
									&& strlen(alarmLocateLost.szGuarder) && alarmLocateLost.ulMessageTime > 0) {
									handleTopicAlarmLocateLostMsg(&alarmLocateLost);
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]parse Topic alarm locate lost "
										"data miss, deviceId=%s, org=%s, guarder=%s, battery=%hu, datetime=%lu, "
										"msgUuid=%s, msgMark=%s, msgSeq=%hu, msgFrom=%s\r\n", __FUNCTION__, __LINE__,
										alarmLocateLost.szDeviceId, alarmLocateLost.szOrg, alarmLocateLost.szGuarder,
										alarmLocateLost.usDeviceBattery, alarmLocateLost.ulMessageTime, 
										pTopicMsg->szMsgUuid, pTopicMsg->szMsgMark, pTopicMsg->usMsgSequence,
										pTopicMsg->szMsgFrom);
									writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								}
								break;
							}
							case ALARM_DEVICE_FENCE: {
								TopicAlarmMessageFence fenceAlarmMsg;
								memset(&fenceAlarmMsg, 0, sizeof(fenceAlarmMsg));
								if (doc.HasMember("factoryId")) {
									if (doc["factoryId"].IsString()) {
										size_t nSize = doc["factoryId"].GetStringLength();
										if (nSize) {
											strcpy_s(fenceAlarmMsg.szFactoryId, sizeof(fenceAlarmMsg.szFactoryId),
												doc["factoryId"].GetString());
										}
									}
								}
								if (doc.HasMember("deviceId")) {
									if (doc["deviceId"].IsString()) {
										size_t nSize = doc["deviceId"].GetStringLength();
										if (nSize) {
											strcpy_s(fenceAlarmMsg.szDeviceId, sizeof(fenceAlarmMsg.szDeviceId),
												doc["deviceId"].GetString());
										}
									}
								}
								if (doc.HasMember("orgId")) {
									if (doc["orgId"].IsString()) {
										size_t nSize = doc["orgId"].GetStringLength();
										if (nSize) {
											strcpy_s(fenceAlarmMsg.szOrgId, sizeof(fenceAlarmMsg.szOrgId), 
												doc["orgId"].GetString());
										}
									}
								}
								if (doc.HasMember("fenceId")) {
									if (doc["fenceId"].IsString()) {
										size_t nSize = doc["fenceId"].GetStringLength();
										if (nSize) {
											strcpy_s(fenceAlarmMsg.szFenceId, sizeof(fenceAlarmMsg.szFenceId), 
												doc["fenceId"].GetString());
										}
									}
								}
								if (doc.HasMember("fenceTaskId")) {
									if (doc["fenceTaskId"].IsString()) {
										size_t nSize = doc["fenceTaskId"].GetStringLength();
										if (nSize) {
											strcpy_s(fenceAlarmMsg.szFenceTaskId, sizeof(fenceAlarmMsg.szFenceTaskId),
												doc["fenceTaskId"].GetString());
										}
									}
								}
								if (doc.HasMember("latitude")) {
									if (doc["latitude"].IsDouble()) {
										fenceAlarmMsg.dLatitude = doc["latitude"].GetDouble();
									}
								}
								if (doc.HasMember("latType")) {
									if (doc["latType"].IsInt()) {
										fenceAlarmMsg.usLatType = (unsigned short)doc["latType"].GetInt();
									}
								}
								if (doc.HasMember("lngitude")) {
									if (doc["lngitude"].IsDouble()) {
										fenceAlarmMsg.dLngitude = doc["lngitude"].GetDouble();
									}
								}
								if (doc.HasMember("lngType")) {
									if (doc["lngType"].IsInt()) {
										fenceAlarmMsg.usLngType = (unsigned short)doc["lngType"].GetInt();
									}
								}
								if (doc.HasMember("coordinate")) {
									if (doc["coordinate"].IsInt()) {
										fenceAlarmMsg.nCoordinate = (int8_t)doc["coordinate"].GetInt();
									}
								}
								if (doc.HasMember("locateType")) {
									if (doc["locateType"].IsInt()) {
										fenceAlarmMsg.nLocateType = (int8_t)doc["locateType"].GetInt();
									}
								}
								if (doc.HasMember("policy")) {
									if (doc["policy"].IsInt()) {
										fenceAlarmMsg.nPolicy = (int8_t)doc["policy"].GetInt();
									}
								}
								if (doc.HasMember("mode")) {
									if (doc["mode"].IsInt()) {
										fenceAlarmMsg.nMode = (int8_t)doc["mode"].GetInt();
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										fenceAlarmMsg.ulMessageTime = makeDatetime(doc["datetime"].GetString());
									}
								}
								if (strlen(fenceAlarmMsg.szOrgId) && strlen(fenceAlarmMsg.szFactoryId)
									&& fenceAlarmMsg.ulMessageTime > 0 && strlen(fenceAlarmMsg.szDeviceId)
									&& strlen(fenceAlarmMsg.szFenceTaskId) && strlen(fenceAlarmMsg.szFenceId)
									&& fenceAlarmMsg.nLocateType != LOCATE_APP
									&& fenceAlarmMsg.dLatitude > 0.00 && fenceAlarmMsg.dLngitude > 0.00) {
									handleTopicAlarmFenceMsg(&fenceAlarmMsg);
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]parse fence alarm, "
										"data parameter miss, deviceId=%s, factoryId=%s, orgId=%s, fenceTaskId=%s, "
										"fenceId=%s, locateType=%d, latitude=%f, lngitude=%f, policy=%d, mode=%d, "
										"datetime=%lu, msgUuid=%s, msgMark=%s, msgSeq=%hu, msgFrom=%s\r\n",
										__FUNCTION__, __LINE__, fenceAlarmMsg.szDeviceId, fenceAlarmMsg.szFactoryId,
										fenceAlarmMsg.szOrgId, fenceAlarmMsg.szFenceTaskId, fenceAlarmMsg.szFenceId,
										fenceAlarmMsg.nLocateType, fenceAlarmMsg.dLatitude, fenceAlarmMsg.dLngitude,
										fenceAlarmMsg.nPolicy, fenceAlarmMsg.nMode, fenceAlarmMsg.ulMessageTime,
										pTopicMsg->szMsgUuid, pTopicMsg->szMsgMark, pTopicMsg->usMsgSequence, 
										pTopicMsg->szMsgFrom);
									writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
								}
								break;
							}
							default: {
								sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic alarm message error,"
									" not support type=%d\r\n", __FUNCTION__, __LINE__, nSubType);
								writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								break;
							}
						}
					}
					else {
						sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]parse Topic alarm message error=%d, msgUuid=%s,"
							" msgMark=%s, msgSeq=%hu, msgFrom=%s\r\n", __FUNCTION__, __LINE__, doc.GetParseError(),
							pTopicMsg->szMsgUuid, pTopicMsg->szMsgMark, pTopicMsg->usMsgSequence, pTopicMsg->szMsgFrom);
						writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
					}
					break;
				}
				case PUBMSG_DEVICE_BIND: {
					if (!doc.Parse(pTopicMsg->szMsgBody).HasParseError()) {
						TopicBindMessage devBindMsg;
						bool bValidDevice = false;
						bool bValidGuarder = false;
						bool bValidMode = false;
						bool bValidDatetime = false;
						char szDatetime[20] = { 0 };
						if (doc.HasMember("factoryId")) {
							if (doc["factoryId"].IsString() && doc["factoryId"].GetStringLength()) {
								strcpy_s(devBindMsg.szFactoryId, sizeof(devBindMsg.szFactoryId),
									doc["factoryId"].GetString());
							}
						}
						if (doc.HasMember("deviceId")) {
							if (doc["deviceId"].IsString() && doc["deviceId"].GetStringLength()) {
								strcpy_s(devBindMsg.szDeviceId, sizeof(devBindMsg.szDeviceId),
									doc["deviceId"].GetString());
							}
						}
						if (doc.HasMember("orgId")) {
							if (doc["orgId"].IsString() && doc["orgId"].GetStringLength()) {
								strcpy_s(devBindMsg.szOrg, sizeof(devBindMsg.szOrg), doc["orgId"].GetString());
							}
						}
						if (doc.HasMember("guarder")) {
							if (doc["guarder"].IsString() && doc["guarder"].GetStringLength()) {
								strcpy_s(devBindMsg.szGuarder, sizeof(devBindMsg.szGuarder),
									doc["guarder"].GetString());
							}
						}
						if (doc.HasMember("mode")) {
							if (doc["mode"].IsInt()) {
								devBindMsg.usMode = (unsigned short)doc["mode"].GetInt();
								bValidMode = true;
							}
						}
						if (doc.HasMember("battery")) {
							if (doc["battery"].IsInt()) {
								devBindMsg.usBattery = (unsigned short)doc["battery"].GetInt();
							}
						}
						if (doc.HasMember("datetime")) {
							if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
								devBindMsg.ulMessageTime = makeDatetime(doc["datetime"].GetString());
							}
						}
						if (strlen(devBindMsg.szDeviceId) && strlen(devBindMsg.szGuarder) 
							&& strlen(devBindMsg.szOrg) && devBindMsg.ulMessageTime > 0) {
							handleTopicDeviceBindMsg(&devBindMsg);
						}
						else {
							sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]parse Topic device bind message data"
								" miss parameter, factoryId=%s, deviceId=%s, org=%s, guarder=%s, mode=%hu, "
								"battery=%hu, datetime=%lu, msgMark=%s, msgSeq=%u, msgUuid=%s, msgFrom=%s\r\n",
								__FUNCTION__, __LINE__, devBindMsg.szFactoryId, devBindMsg.szDeviceId, 
								devBindMsg.szOrg, devBindMsg.szGuarder, devBindMsg.usMode, devBindMsg.usBattery,
								devBindMsg.ulMessageTime, pTopicMsg->szMsgMark, pTopicMsg->usMsgSequence, 
								pTopicMsg->szMsgUuid, pTopicMsg->szMsgFrom);
							writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
						}
					}
					else {
						sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic device bind message"
							"error=%d, body=%s, msgUuid=%s, msgMark=%s, msgSeq=%hu, msgFrom=%s\r\n", __FUNCTION__, 
							__LINE__, doc.GetParseError(), pTopicMsg->szMsgBody, pTopicMsg->szMsgUuid, 
							pTopicMsg->szMsgMark, pTopicMsg->usMsgSequence, pTopicMsg->szMsgFrom);
						writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
					}
					break;
				}
				case PUBMSG_DEVICE_ONLINE: {
					if (!doc.Parse(pTopicMsg->szMsgBody).HasParseError()) {
						TopicOnlineMessage onlineMsg;
						memset(&onlineMsg, 0, sizeof(onlineMsg));
						if (doc.HasMember("factoryId")) {
							if (doc["factoryId"].IsString() && doc["factoryId"].GetStringLength()) {
								strcpy_s(onlineMsg.szFactoryId, sizeof(onlineMsg.szFactoryId),
									doc["factoryId"].GetString());
							}
						}
						if (doc.HasMember("deviceId")) {
							if (doc["deviceId"].IsString() && doc["deviceId"].GetStringLength()) {
								strcpy_s(onlineMsg.szDeviceId, sizeof(onlineMsg.szDeviceId),
									doc["deviceId"].GetString());
							}
						}
						if (doc.HasMember("orgId")) {
							if (doc["orgId"].IsString() && doc["orgId"].GetStringLength()) {
								strcpy_s(onlineMsg.szOrg, sizeof(onlineMsg.szOrg), doc["orgId"].GetString());
							}
						}
						if (doc.HasMember("battery")) {
							if (doc["battery"].IsInt()) {
								onlineMsg.usBattery = (unsigned short)doc["battery"].GetInt();
							}
						}
						if (doc.HasMember("datetime")) {
							if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
									onlineMsg.ulMessageTime = makeDatetime(doc["datetime"].GetString());
							}
						}
						if (strlen(onlineMsg.szDeviceId) && strlen(onlineMsg.szOrg)
							&& onlineMsg.ulMessageTime > 0) {
							handleTopicOnlineMessage(&onlineMsg);
						}
						else {
							sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]topic online message miss parameter, "
								"deviceId=%s, orgId=%s, battery=%hu, datetime=%lu, uuid=%s, mark=%s, from=%s, seq=%hu\r\n",
								__FUNCTION__, __LINE__, onlineMsg.szDeviceId, onlineMsg.szOrg, onlineMsg.usBattery, 
								onlineMsg.ulMessageTime, pTopicMsg->szMsgUuid, pTopicMsg->szMsgMark, pTopicMsg->szMsgFrom,
								pTopicMsg->usMsgSequence);
							writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
						}
					}
					else {
						sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]parse topic online message error=%d, body=%s, "
							"uuid=%s, mark=%s, from=%s, seq=%hu\r\n", __FUNCTION__, __LINE__, doc.GetParseError(), 
							pTopicMsg->szMsgBody, pTopicMsg->szMsgUuid, pTopicMsg->szMsgFrom, pTopicMsg->szMsgMark, 
							pTopicMsg->usMsgSequence);
						writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
					}
					break;
				}
				case PUBMSG_DEVICE_OFFLINE: {
					if (!doc.Parse(pTopicMsg->szMsgBody).HasParseError()) {
						TopicOfflineMessage offlineMsg;
						memset(&offlineMsg, 0, sizeof(offlineMsg));
						if (doc.HasMember("factoryId")) {
							if (doc["factoryId"].IsString()) {
								size_t nSize = doc["factoryId"].GetStringLength();
								if (nSize) {
									strncpy_s(offlineMsg.szFactoryId, sizeof(offlineMsg.szFactoryId),
										doc["factoryId"].GetString(), nSize);
								}
							}
						}
						if (doc.HasMember("deviceId")) {
							if (doc["deviceId"].IsString() && doc["deviceId"].GetStringLength()) {
								strcpy_s(offlineMsg.szDeviceId, sizeof(offlineMsg.szDeviceId),
									doc["deviceId"].GetString());
							}
						}
						if (doc.HasMember("orgId")) {
							if (doc["orgId"].IsString() && doc["orgId"].GetStringLength()) {
								strcpy_s(offlineMsg.szOrg, sizeof(offlineMsg.szOrg), doc["orgId"].GetString());
							}
						}
						if (doc.HasMember("datetime")) {
							if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
								offlineMsg.ulMessageTime = makeDatetime(doc["datetime"].GetString());
							}
						}
						if (strlen(offlineMsg.szDeviceId) && strlen(offlineMsg.szOrg) && offlineMsg.ulMessageTime > 0) {
							handleTopicOfflineMessage(&offlineMsg);
						}
						else {
							sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]topic offline message miss parameter, deviceId=%s,"
								" orgId=%s, datetime=%lu, uuid=%s, mark=%s, from=%s, seq=%hu\r\n", __FUNCTION__, __LINE__, 
								offlineMsg.szDeviceId, offlineMsg.szOrg, offlineMsg.ulMessageTime, pTopicMsg->szMsgUuid, 
								pTopicMsg->szMsgMark, pTopicMsg->szMsgFrom, pTopicMsg->usMsgSequence);
							writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
						}
					}
					else {
						sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]parse topic offline message error=%d, body=%s, "
							"uuid=%s, from=%s, mark=%s, seq=%hu\r\n", __FUNCTION__, __LINE__, doc.GetParseError(), 
							pTopicMsg->szMsgBody, pTopicMsg->szMsgUuid, pTopicMsg->szMsgFrom, pTopicMsg->szMsgMark, 
							pTopicMsg->usMsgSequence);
						writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
					}
					break;
				}
				case PUBMSG_TASK: {
					if (!doc.Parse(pTopicMsg->szMsgBody).HasParseError()) {
						int nSubType = -1;
						if (doc.HasMember("subType")) {
							if (doc["subType"].IsInt()) {
								nSubType = doc["subType"].GetInt();
							}
							switch (nSubType) {
								case TASK_OPT_SUBMIT: {
                  if (strcmp(pTopicMsg->szMsgFrom, m_szInteractorIdentity) != 0) {
                    TopicTaskMessage taskMsg;
                    memset(&taskMsg, 0, sizeof(taskMsg));
                    char szDatetime[20] = { 0 };
                    if (doc.HasMember("taskId")) {
                      if (doc["taskId"].IsString() && doc["taskId"].GetStringLength()) {
                        strcpy_s(taskMsg.szTaskId, sizeof(taskMsg.szTaskId), doc["taskId"].GetString());
                      }
                    }
                    if (doc.HasMember("factoryId")) {
                      if (doc["factoryId"].IsString() && doc["factoryId"].GetStringLength()) {
                        strcpy_s(taskMsg.szFactoryId, sizeof(taskMsg.szFactoryId), doc["factoryId"].GetString());
                      }
                    }
                    if (doc.HasMember("deviceId")) {
                      if (doc["deviceId"].IsString() && doc["deviceId"].GetStringLength()) {
                        strcpy_s(taskMsg.szDeviceId, sizeof(taskMsg.szDeviceId), doc["deviceId"].GetString());
                      }
                    }
                    if (doc.HasMember("orgId")) {
                      if (doc["orgId"].IsString() && doc["orgId"].GetStringLength()) {
                        strcpy_s(taskMsg.szOrg, sizeof(taskMsg.szOrg), doc["orgId"].GetString());
                      }
                    }
                    if (doc.HasMember("guarder")) {
                      if (doc["guarder"].IsString() && doc["guarder"].GetStringLength()) {
                        strcpy_s(taskMsg.szGuarder, sizeof(taskMsg.szGuarder), doc["guarder"].GetString());
                      }
                    }
                    if (doc.HasMember("taskType")) {
                      if (doc["taskType"].IsInt()) {
                        taskMsg.usTaskType = (unsigned short)doc["taskType"].GetInt();
                      }
                    }
                    if (doc.HasMember("limit")) {
                      if (doc["limit"].IsInt()) {
                        taskMsg.usTaskLimit = (unsigned short)doc["limit"].GetInt();
                      }
                    }
                    if (doc.HasMember("destination")) {
                      if (doc["destination"].IsString() && doc["destination"].GetStringLength()) {
                        strcpy_s(taskMsg.szDestination, sizeof(taskMsg.szDestination),
                          doc["destination"].GetString());
                      }
                    }
                    if (doc.HasMember("target")) {
                      if (doc["target"].IsString() && doc["target"].GetStringLength()) {
                        strcpy_s(taskMsg.szTarget, sizeof(taskMsg.szTarget), doc["target"].GetString());
                      }
                    }
                    if (doc.HasMember("handset")) {
                      if (doc["handset"].IsString() && doc["handset"].GetStringLength()) {
                        strcpy_s(taskMsg.szHandset, sizeof(taskMsg.szHandset), doc["handset"].GetString());
                      }
                    }
                    if (doc.HasMember("datetime")) {
                      if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
                        taskMsg.ulMessageTime = makeDatetime(doc["datetime"].GetString());
                      }
                    }
                    if (strlen(taskMsg.szDeviceId) && strlen(taskMsg.szTaskId) && strlen(taskMsg.szGuarder)
                      && strlen(taskMsg.szOrg) && taskMsg.ulMessageTime > 0) {
                      handleTopicTaskSubmitMsg(&taskMsg);
                    }
                    else {
                      sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]parse Topic task message data miss parameter,"
                        " taskId=%s, guarder=%s, deviceId=%s, factoryId=%s, datetime=%s, msgTopic=%s, msgSeq=%hu, "
                        "msgUuid=%s, msgFrom=%s\r\n", __FUNCTION__, __LINE__, taskMsg.szTaskId, taskMsg.szGuarder,
                        taskMsg.szDeviceId, taskMsg.szFactoryId, szDatetime, pTopicMsg->szMsgMark,
                        pTopicMsg->usMsgSequence, pTopicMsg->szMsgUuid, pTopicMsg->szMsgFrom);
                      writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
                    }
                  }
									break;
								}
								case TASK_OPT_MODIFY: {
									TopicTaskModifyMessage taskModifyMsg;
									memset(&taskModifyMsg, 0, sizeof(TopicTaskModifyMessage));
									if (doc.HasMember("taskId")) {
										if (doc["taskId"].IsString() && doc["taskId"].GetStringLength()) {
											strcpy_s(taskModifyMsg.szTaskId, sizeof(taskModifyMsg.szTaskId), doc["taskId"].GetString());
										}
									}
									if (doc.HasMember("handset")) {
										if (doc["handset"].IsString() && doc["handset"].GetStringLength()) {
											strcpy_s(taskModifyMsg.szHandset, sizeof(taskModifyMsg.szHandset), doc["handset"].GetString());
										}
									}
									if (doc.HasMember("datetime")) {
										if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
											taskModifyMsg.ulMessageTime = makeDatetime(doc["datetime"].GetString());
										}
									}
									if (strlen(taskModifyMsg.szTaskId) && taskModifyMsg.ulMessageTime > 0) {
										handleTopicTaskModifyMsg(&taskModifyMsg);
									}
									else {
										sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]parse Topic task modify message miss parameter, "
											"taskId=%s, datetime=%lu, handset=%s, msgTopic=%s, msgSeq=%hu, msgUuid=%s, msgFrom=%s\r\n", 
											__FUNCTION__, __LINE__, taskModifyMsg.szTaskId, taskModifyMsg.ulMessageTime, 
											taskModifyMsg.szHandset, pTopicMsg->szMsgMark, pTopicMsg->usMsgSequence, pTopicMsg->szMsgUuid,
											pTopicMsg->szMsgFrom);
										writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									}
									break;
								}
								case TASK_OPT_CLOSE: {
                  if (strcmp(pTopicMsg->szMsgFrom, m_szInteractorIdentity) != 0) {
                    TopicTaskCloseMessage taskCloseMsg;
                    memset(&taskCloseMsg, 0, sizeof(TopicTaskCloseMessage));
                    if (doc.HasMember("taskId")) {
                      if (doc["taskId"].IsString() && doc["taskId"].GetStringLength()) {
                        strcpy_s(taskCloseMsg.szTaskId, sizeof(taskCloseMsg.szTaskId), doc["taskId"].GetString());
                      }
                    }
                    if (doc.HasMember("state")) {
                      if (doc["state"].IsInt()) {
                        taskCloseMsg.nClose = doc["state"].GetInt();
                      }
                    }
                    if (doc.HasMember("datetime")) {
                      if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
                        taskCloseMsg.ulMessageTime = makeDatetime(doc["datetime"].GetString());
                      }
                    }
                    if (strlen(taskCloseMsg.szTaskId) && taskCloseMsg.ulMessageTime > 0) {
                      handleTopicTaskCloseMsg(&taskCloseMsg);
                    }
                    else {
                      sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]parse topic task close message miss "
                        "parameter, taskId=%s, state=%d, datetime=%lu, msgTopic=%s, msgSeq=%hu, msgUuid=%s, "
                        "msgFrom=%s\r\n", __FUNCTION__, __LINE__, taskCloseMsg.szTaskId, taskCloseMsg.nClose,
                        taskCloseMsg.ulMessageTime, pTopicMsg->szMsgMark, pTopicMsg->usMsgSequence,
                        pTopicMsg->szMsgUuid, pTopicMsg->szMsgFrom);
                      writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
                    }
                  }
                  break;
								}
								default: {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]parse task message subType not "
										"support=%d, msgTopic=%s, msgSeq=%hu, msgUuid=%s, msgFrom=%s\r\n\r\n", __FUNCTION__, 
										__LINE__, nSubType, pTopicMsg->szMsgMark, pTopicMsg->usMsgSequence, pTopicMsg->szMsgUuid,
										pTopicMsg->szMsgFrom);
									writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									break;
								}
							}
						}
					}
					else {
						sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]parse topic task message json error=%d,"
							" msgUuid=%s, msgMark=%s, msgSeq=%hu, msgFrom=%s\r\n", __FUNCTION__, __LINE__, 
							doc.GetParseError(), pTopicMsg->szMsgUuid, pTopicMsg->szMsgMark, pTopicMsg->usMsgSequence, 
							pTopicMsg->szMsgFrom);
						writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
					}
					break;
				}
				case PUBMSG_BUFFER_MODIFY: {
					if (!doc.Parse(pTopicMsg->szMsgBody).HasParseError()) {
						int nObject = 0;
						int nOperate = 0;
						if (doc.HasMember("object")) {
							if (doc["object"].IsInt()) {
								nObject = doc["object"].GetInt();
							}
						}
						if (doc.HasMember("operate")) {
							if (doc["operate"].IsInt()) {
								nOperate = doc["operate"].GetInt();
							}
						}
						switch (nObject) {
							case BUFFER_GUARDER: {
								size_t nGuarderSize = sizeof(Guarder);
								Guarder guarder;
								memset(&guarder, 0, nGuarderSize);
								char szDateTime[20] = { 0 };
								if (doc.HasMember("guarder")) {
									if (doc["guarder"].IsString() && doc["guarder"].GetStringLength()) {
										strcpy_s(guarder.szId, sizeof(guarder.szId), doc["guarder"].GetString());
									}
								}
								if (doc.HasMember("orgId")) {
									if (doc["orgId"].IsString() && doc["orgId"].GetStringLength()) {
										strcpy_s(guarder.szOrg, sizeof(guarder.szOrg), doc["orgId"].GetString());
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										strcpy_s(szDateTime, sizeof(szDateTime), doc["datetime"].GetString());
									}
								}
								if (nOperate == BUFFER_OPERATE_DELETE) {
									if (strlen(guarder.szId) && strlen(guarder.szOrg) && strlen(szDateTime)) {
										pthread_mutex_lock(&m_mutex4GuarderList);
										std::string strGuarderId = guarder.szId;
										if (m_guarderList.empty()) {
											expand_protocol::EscortGuarderList::iterator iter = m_guarderList.find(strGuarderId);
											if (iter != m_guarderList.end()) {
												Guarder * pGuarder = iter->second;
												if (pGuarder) {
													delete pGuarder;
													pGuarder = NULL;
												}
												m_guarderList.erase(iter);
											}
										}
										pthread_mutex_unlock(&m_mutex4GuarderList);
									}
									else {
										sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]parse Topic Buffer Modify, object=%u,"
											" operate=%d, guarder=%s, org=%s, datetime=%s, msgUuid=%s, msgMark=%s, msgSeq=%hu,"
											" msgFrom=%s\r\n", __FUNCTION__, __LINE__, BUFFER_GUARDER, nOperate, guarder.szId, 
											guarder.szOrg, szDateTime, pTopicMsg->szMsgUuid, pTopicMsg->szMsgMark, 
											pTopicMsg->usMsgSequence, pTopicMsg->szMsgFrom);
										writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									}
								}
								else {
									if (doc.HasMember("name")) {
										if (doc["name"].IsString() && doc["name"].GetStringLength()) {
											strcpy_s(guarder.szTagName, sizeof(guarder.szTagName), doc["name"].GetString());
										}
									}
									if (doc.HasMember("passwd")) {
										if (doc["passwd"].IsString() && doc["passwd"].GetStringLength()) {
											strcpy_s(guarder.szPassword, sizeof(guarder.szPassword),
												doc["passwd"].GetString());
										}
									}
									if (doc.HasMember("roleType")) {
										if (doc["roleType"].IsInt()) {
											guarder.usRoleType = (unsigned short)doc["roleType"].GetInt();
										}
									}
									if (strlen(guarder.szId) && strlen(guarder.szOrg) && strlen(guarder.szTagName)
										&& strlen(guarder.szPassword) && strlen(szDateTime)){
										if (nOperate == BUFFER_OPERATE_NEW) {
											std::string strGuarderId = guarder.szId;
											pthread_mutex_lock(&m_mutex4GuarderList);
											expand_protocol::EscortGuarderList::iterator iter = m_guarderList.find(strGuarderId);
											if (iter != m_guarderList.end()) {
												sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]Topic Buffer Modify"
													" message, object=%u, operate=%u, guarder=%s, name=%s, passwd=%s, orgId=%s,"
													" roleType=%hu already exists in the guarderList\r\n", __FUNCTION__,
													__LINE__, BUFFER_GUARDER, BUFFER_OPERATE_NEW, guarder.szId,
													guarder.szTagName, guarder.szPassword, guarder.szOrg, guarder.usRoleType);
												writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
											}
											else {
												Guarder * pGuarder = new Guarder();
												memset(pGuarder, 0, nGuarderSize);
												strncpy_s(pGuarder->szId, sizeof(pGuarder->szId), guarder.szId,
													strlen(guarder.szId));
												strncpy_s(pGuarder->szTagName, sizeof(pGuarder->szTagName),
													guarder.szTagName, strlen(guarder.szTagName));
												strncpy_s(pGuarder->szOrg, sizeof(pGuarder->szOrg), guarder.szOrg,
													strlen(guarder.szOrg));
												strncpy_s(pGuarder->szPassword, sizeof(pGuarder->szPassword),
													guarder.szPassword, strlen(guarder.szPassword));
												pGuarder->szLink[0] = '\0';
												pGuarder->szTaskId[0] = '\0';
												pGuarder->szBindDevice[0] = '\0';
												pGuarder->szCurrentSession[0] = '\0';
												pGuarder->szTaskStartTime[0] = '\0';
												pGuarder->usState = STATE_GUARDER_FREE;
												pGuarder->usRoleType = guarder.usRoleType;
												m_guarderList.emplace(strGuarderId, pGuarder);
												sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]Topic Buffer Modify message"
													", object=%u, operate=%u, guarder=%s, name=%s, passwd=%s, orgId=%s, roleType=%hu\r\n",
													__FUNCTION__, __LINE__, BUFFER_GUARDER, BUFFER_OPERATE_NEW, guarder.szId,
													guarder.szTagName, guarder.szPassword, guarder.szOrg, guarder.usRoleType);
												writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
											}
											pthread_mutex_unlock(&m_mutex4GuarderList);
										}
										else if (nOperate == BUFFER_OPERATE_UPDATE) {
											pthread_mutex_lock(&m_mutex4GuarderList);
											std::string strGuarderId = guarder.szId;
											expand_protocol::EscortGuarderList::iterator iter = m_guarderList.find(strGuarderId);
											if (iter != m_guarderList.end()) {
												Guarder * pGuarder = iter->second;
												if (pGuarder) {
													if (strcmp(pGuarder->szTagName, guarder.szTagName) != 0) {
														strncpy_s(pGuarder->szTagName, sizeof(pGuarder->szTagName),
															guarder.szTagName, strlen(guarder.szTagName));
													}
													if (strcmp(pGuarder->szPassword, guarder.szPassword) != 0) {
														strncpy_s(pGuarder->szPassword, sizeof(pGuarder->szPassword),
															guarder.szPassword, strlen(guarder.szPassword));
													}
													if (strcmp(pGuarder->szOrg, guarder.szOrg) != 0) {
														strncpy_s(pGuarder->szOrg, sizeof(pGuarder->szOrg), guarder.szOrg,
															strlen(guarder.szOrg));
													}
													if (pGuarder->usRoleType != guarder.usRoleType) {
														pGuarder->usRoleType = guarder.usRoleType;
													}
													sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]Topic Buffer Modify message, "
														"object=%u, operate=%u, guarder=%s, name=%s, passwd=%s, orgId=%s, roleType=%hu\r\n",
														__FUNCTION__, __LINE__, BUFFER_GUARDER, BUFFER_OPERATE_UPDATE, guarder.szId,
														guarder.szTagName, guarder.szPassword, guarder.szOrg, guarder.usRoleType);
													writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
												}
												else {
													sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]Topic Buffer Modify message, "
														"object=%u, operate=%u, guarder=%s, name=%s, passwd=%s, orgId=%s, not found in "
														"guarderList\r\n", __FUNCTION__, __LINE__, BUFFER_GUARDER, BUFFER_OPERATE_UPDATE,
														guarder.szId, guarder.szTagName, guarder.szPassword, guarder.szOrg);
													writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
												}
											}
											pthread_mutex_unlock(&m_mutex4GuarderList);
										}
									}
								}
								break;
							}
							case BUFFER_DEVICE: {
								size_t nDeviceSize = sizeof(WristletDevice);
								WristletDevice device;
								char szDatetime[20] = { 0 };
								if (doc.HasMember("deviceId")) {
									if (doc["deviceId"].IsString() && doc["deviceId"].GetStringLength()) {
										strcpy_s(device.deviceBasic.szDeviceId, sizeof(device.deviceBasic.szDeviceId),
											doc["deviceId"].GetString());
									}
								}
								if (doc.HasMember("factoryId")) {
									if (doc["factoryId"].IsString() && doc["factoryId"].GetStringLength()) {
										strcpy_s(device.deviceBasic.szFactoryId, sizeof(device.deviceBasic.szFactoryId),
											doc["factoryId"].GetString());
									}
								}
								if (doc.HasMember("orgId")) {
									if (doc["orgId"].IsString() && doc["orgId"].GetStringLength()) {
										strcpy_s(device.szOrganization, sizeof(device.szOrganization),
											doc["orgId"].GetString());
									}
								}
								if (doc.HasMember("battery")) {
									if (doc["battery"].IsInt()) {
										device.deviceBasic.nBattery = (unsigned char)doc["battery"].GetInt();
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											strncpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString(), nSize);
										}
									}
								}
								if (strlen(device.deviceBasic.szDeviceId) && strlen(device.szOrganization)
									&& strlen(szDatetime)) {
									std::string strDeviceId = device.deviceBasic.szDeviceId;
									if (nOperate == BUFFER_OPERATE_NEW) {
										pthread_mutex_lock(&m_mutex4DeviceList);
										expand_protocol::EscortDeviceList::iterator iter = m_deviceList.find(strDeviceId);
										if (iter != m_deviceList.end()) {

											WristletDevice * pDevice = iter->second;
											if (pDevice) {
												if (strlen(pDevice->szOrganization) == 0) {
													strncpy_s(pDevice->szOrganization, sizeof(pDevice->szOrganization),
														device.szOrganization, strlen(device.szOrganization));
												}
												if (strcmp(pDevice->deviceBasic.szFactoryId, device.deviceBasic.szFactoryId) != 0) {
													strncpy_s(pDevice->deviceBasic.szFactoryId,
														sizeof(pDevice->deviceBasic.szFactoryId),
														device.deviceBasic.szFactoryId,
														strlen(device.deviceBasic.szFactoryId));
												}
												sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]topic Buffer modify messsage, "
													"object=%u, operate=%d, deviceId=%s, factoryId=%s, orgId=%s, datetime=%s, "
													"already found in the deviceList\r\n", __FUNCTION__, __LINE__, BUFFER_DEVICE,
													nOperate, device.deviceBasic.szDeviceId, device.deviceBasic.szFactoryId,
													device.szOrganization, szDatetime);
												writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
											}
											else {
												pDevice = (WristletDevice *)zmalloc(nDeviceSize);
												memset(pDevice, 0, nDeviceSize);
												strncpy_s(pDevice->deviceBasic.szDeviceId, sizeof(pDevice->deviceBasic.szDeviceId),
													device.deviceBasic.szDeviceId, strlen(device.deviceBasic.szDeviceId));
												strncpy_s(pDevice->deviceBasic.szFactoryId, sizeof(pDevice->deviceBasic.szFactoryId),
													device.deviceBasic.szFactoryId, strlen(device.deviceBasic.szFactoryId));
												strncpy_s(pDevice->szOrganization, sizeof(pDevice->szOrganization),
													device.szOrganization, strlen(device.szOrganization));
												pDevice->deviceBasic.nBattery = 0;
												pDevice->deviceBasic.nStatus = DEV_ONLINE;
												pDevice->deviceBasic.ulLastActiveTime = 0;
												pDevice->nLastLocateType = 0;
												pDevice->szBindGuard[0] = '\0';
												pDevice->szLinkId[0] = '\0';
												pDevice->ulBindTime = 0;
												pDevice->ulLastFleeAlertTime = 0;
												pDevice->ulLastDeviceLocateTime = 0;
												pDevice->ulLastGuarderLocateTime = 0;
												pDevice->ulLastLooseAlertTime = 0;
												pDevice->ulLastLowPowerAlertTime = 0;
												pDevice->devicePosition.dLatitude = pDevice->devicePosition.dLngitude = 0.000000;
												pDevice->devicePosition.usLatType = pDevice->devicePosition.usLngType = 1;
												pDevice->devicePosition.nPrecision = 0;
												pDevice->guardPosition.dLatitude = pDevice->guardPosition.dLngitude = 0.000000;
												pDevice->guardPosition.usLatType = pDevice->guardPosition.usLngType = 1;
												pDevice->guardPosition.nPrecision = 0;
												pDevice->nDeviceFenceState = 0;
												pDevice->nDeviceHasFence = 0;
												std::string strDeviceId = device.deviceBasic.szDeviceId;
												m_deviceList.emplace(strDeviceId, pDevice);
												sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]topic Buffer modify message,"
													" object=%u, operate=%d, deviceId=%s, factoryId=%s, orgId=%s, datetime=%s\r\n",
													__FUNCTION__, __LINE__, BUFFER_DEVICE, nOperate, device.deviceBasic.szDeviceId,
													device.deviceBasic.szFactoryId, device.szOrganization, szDatetime);
												writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
											}
										}
										pthread_mutex_unlock(&m_mutex4DeviceList);
									}
									else if (nOperate == BUFFER_OPERATE_UPDATE) {
										pthread_mutex_lock(&m_mutex4DeviceList);
										expand_protocol::EscortDeviceList::iterator iter = m_deviceList.find(strDeviceId);
										if (iter != m_deviceList.end()) {
											WristletDevice * pDevice = iter->second;
											if (pDevice) {
												if (strcmp(pDevice->deviceBasic.szFactoryId, device.deviceBasic.szFactoryId) != 0) {
													strncpy_s(pDevice->deviceBasic.szFactoryId,
														sizeof(pDevice->deviceBasic.szFactoryId),
														device.deviceBasic.szFactoryId,
														strlen(device.deviceBasic.szFactoryId));
												}
												if (strcmp(pDevice->szOrganization, device.szOrganization) != 0) {
													strncpy_s(pDevice->szOrganization, sizeof(pDevice->szOrganization),
														device.szOrganization, strlen(device.szOrganization));
												}
												sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]Topic Buffer modify message, "
													"object=%u, operate=%u, deviceId=%s, factoryId=%s, orgId=%s, datetime=%s\r\n",
													__FUNCTION__, __LINE__, BUFFER_DEVICE, BUFFER_OPERATE_UPDATE,
													device.deviceBasic.szDeviceId, device.deviceBasic.szFactoryId,
													device.szOrganization, szDatetime);
												writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
											}
											else {
												sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]Topic Buffer modify message,"
													"object=%u, operate=%u, deviceId=%s, factoryId=%s, orgId=%s, datetime=%s,"
													" not find int the deviceList\r\n", __FUNCTION__, __LINE__, BUFFER_DEVICE,
													BUFFER_OPERATE_UPDATE, device.deviceBasic.szDeviceId, device.deviceBasic.szFactoryId,
													device.szOrganization, szDatetime);
												writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
											}
										}
										pthread_mutex_unlock(&m_mutex4DeviceList);
									}
									else if (nOperate == BUFFER_OPERATE_DELETE) {
										pthread_mutex_lock(&m_mutex4DeviceList);
										expand_protocol::EscortDeviceList::iterator iter = m_deviceList.find(strDeviceId);
										if (iter != m_deviceList.end()) {
											WristletDevice * pDevice = iter->second;
											if (pDevice) {
												delete pDevice;
												pDevice = NULL;
											}
											m_deviceList.erase(iter);
										}
										pthread_mutex_unlock(&m_mutex4DeviceList);
									}
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]Topic Buffer modify message, object=%u,"
										" operate=%d, parameter miss, deviceId=%s, factoryId=%s, orgId=%s, datetime=%s\r\n",
										__FUNCTION__, __LINE__, BUFFER_DEVICE, nOperate, device.deviceBasic.szDeviceId, 
										device.deviceBasic.szFactoryId, device.szOrganization, szDatetime);
									writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								}
								break;
							}
							case BUFFER_ORG: {
								size_t nOrgSize = sizeof(OrganizationEx);
								char szDatetime[20] = { 0 };
								if (nOperate == BUFFER_OPERATE_NEW) {
									OrganizationEx * pOrg =  new OrganizationEx ();
									memset(pOrg, 0, nOrgSize);
									pOrg->childList.clear();
									if (doc.HasMember("orgId")) {
										if (doc["orgId"].IsString()) {
											size_t nSize = doc["orgId"].GetStringLength();
											if (nSize) {
												strcpy_s(pOrg->org.szOrgId, sizeof(pOrg->org.szOrgId), doc["orgId"].GetString());
											}
										}
									}
									if (doc.HasMember("orgName")) {
										if (doc["orgName"].IsString()) {
											size_t nSize = doc["orgName"].GetStringLength();
											if (nSize) {
												strcpy_s(pOrg->org.szOrgName, sizeof(pOrg->org.szOrgName),
													doc["orgName"].GetString());
											}
										}
									}
									if (doc.HasMember("parentId")) {
										if (doc["parentId"].IsString()) {
											size_t nSize = doc["parentId"].GetStringLength();
											if (nSize) {
												strcpy_s(pOrg->org.szParentOrgId, sizeof(pOrg->org.szParentOrgId),
													doc["parentId"].GetString());
											}
										}
									}
									if (doc.HasMember("datetime")) {
										if (doc["datetime"].IsString()) {
											size_t nSize = doc["datetime"].GetStringLength();
											if (nSize) {
												strcpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString());
											}
										}
									}
									if (strlen(pOrg->org.szOrgId) && strlen(szDatetime)) {
										std::string strOrgId = pOrg->org.szOrgId;
										sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]topic buffer add new org, id=%s, "
											" orgName=%s, parentId=%s\r\n", __FUNCTION__, __LINE__, pOrg->org.szOrgId,
											pOrg->org.szOrgName, pOrg->org.szParentOrgId);
										writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
										pthread_mutex_lock(&m_mutex4OrgList);
										std::map<std::string, OrganizationEx *>::iterator iter = m_orgList.find(strOrgId);
										if (iter != m_orgList.end()) {
											free(pOrg);
											pOrg = NULL;
										}
										else {
											m_orgList.emplace(strOrgId, pOrg);
										}
										pthread_mutex_unlock(&m_mutex4OrgList);
										reloadOrgList(true);
									}
								}
								else if (nOperate == BUFFER_OPERATE_UPDATE) {
									Organization org;
									memset(&org, 0, sizeof(org));
									if (doc.HasMember("orgId")) {
										if (doc["orgId"].IsString()) {
											size_t nSize = doc["orgId"].GetStringLength();
											if (nSize) {
												strcpy_s(org.szOrgId, sizeof(org.szOrgId), doc["orgId"].GetString());
											}
										}
									}
									if (doc.HasMember("orgName")) {
										if (doc["orgName"].IsString()) {
											size_t nSize = doc["orgId"].GetStringLength();
											if (nSize) {
												strcpy_s(org.szOrgName, sizeof(org.szOrgName), doc["orgName"].GetString());
											}
										}
									}
									if (doc.HasMember("parentId")) {
										if (doc["parentId"].IsString()) {
											size_t nSize = doc["parentId"].GetStringLength();
											if (nSize) {
												strcpy_s(org.szParentOrgId, sizeof(org.szParentOrgId),
													doc["parentId"].GetString());
											}
										}
									}
									if (doc.HasMember("datetime")) {
										if (doc["datetime"].IsString()) {
											size_t nSize = doc["datetime"].GetStringLength();
											if (nSize) {
												strcpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString());
											}
										}
									}
									if (strlen(org.szOrgId) && strlen(szDatetime)) {
										std::string strOrgId = org.szOrgId;
										bool bReload = false;
										pthread_mutex_lock(&m_mutex4OrgList);
										std::map<std::string, OrganizationEx *>::iterator iter = m_orgList.find(strOrgId);
										if (iter != m_orgList.end()) {
											OrganizationEx * pOrg = iter->second;
											if (pOrg) {
												if (strcmp(pOrg->org.szOrgName, org.szOrgName) != 0) {
													strcpy_s(pOrg->org.szOrgName, sizeof(pOrg->org.szOrgName), org.szOrgName);
												}
												if (strcmp(pOrg->org.szParentOrgId, org.szParentOrgId) != 0) {
													strcpy_s(pOrg->org.szParentOrgId, sizeof(pOrg->org.szParentOrgId),
														org.szParentOrgId);
													bReload = true;
												}
											}
										}
										pthread_mutex_unlock(&m_mutex4OrgList);
										if (bReload) {
											reloadOrgList(true);
										}
										sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]topic buffer update org, orgId=%s, "
											"orgName=%s, parentId=%s\r\n", __FUNCTION__, __LINE__, org.szOrgId, org.szOrgName,
											org.szParentOrgId);
										writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
									}
								}
								else if (nOperate == BUFFER_OPERATE_DELETE) {
									char szOrgId[40] = { 0 };
									if (doc.HasMember("orgId")) {
										if (doc["orgId"].IsString()) {
											size_t nSize = doc["orgId"].GetStringLength();
											if (nSize) {
												strcpy_s(szOrgId, sizeof(szOrgId), doc["orgId"].GetString());
											}
										}
									}
									if (strlen(szOrgId)) {
										std::string strOrgId = szOrgId;
										bool bReload = false;
										pthread_mutex_lock(&m_mutex4OrgList);
										std::map<std::string, OrganizationEx *>::iterator iter = m_orgList.find(strOrgId);
										if (iter != m_orgList.end()) {
											OrganizationEx * pOrg = iter->second;
											if (pOrg) {
												delete pOrg;
												pOrg = NULL;
											}
											m_orgList.erase(iter);
										}
										pthread_mutex_unlock(&m_mutex4OrgList);
										if (bReload) {
											reloadOrgList(true);
										}
										sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]topic buffer delete org, orgId=%s\r\n",
											__FUNCTION__, __LINE__, szOrgId);
										writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
									}
								}
								break;
							}
							default: {
								sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]Topic Buffer modify message, object=%d not "
									"deal, msgUuid=%s, msgMark=%s, msgSeq=%hu, msgFrom=%s\r\n", __FUNCTION__, __LINE__, 
									nObject, pTopicMsg->szMsgUuid, pTopicMsg->szMsgMark, pTopicMsg->usMsgSequence, 
									pTopicMsg->szMsgFrom);
								writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
							}
						}
					}
					else {
						sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]parse Topic buffer modify message "
							"error=%d, msgUuid=%s, msgMark=%s, msgSeq=%hu, msgFrom=%s\r\n", __FUNCTION__, 
							__LINE__, doc.HasParseError(), pTopicMsg->szMsgUuid, pTopicMsg->szMsgMark,
							pTopicMsg->usMsgSequence, pTopicMsg->szMsgFrom);
						writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
					}
					break;
				}
				case PUBMSG_ACCOUNT_LOGIN: {
					break;
				}
				case PUBMSG_ACCOUNT_LOGOUT: {
					break;
				}
				default: {
					break;
				}
			}
			delete pTopicMsg;
			pTopicMsg = NULL;
		}
	} while (1);
}

void expand_protocol::ExpandProtocolService::handleTopicAliveMessage(TopicAliveMessage * pAliveMsg_)
{
	char szLog[512] = { 0 };
	if (pAliveMsg_ && strlen(pAliveMsg_->szDeviceId) && strlen(pAliveMsg_->szOrg)) {
		if (getLoadDeviceList()) {
			bool bValidDevice = false;
			std::string strDeviceId = pAliveMsg_->szDeviceId;
			pthread_mutex_lock(&m_mutex4DeviceList);
			EscortDeviceList::iterator iter = m_deviceList.find(strDeviceId);
			if (iter != m_deviceList.end()) {
				WristletDevice * pDevice = iter->second;
				if (pDevice) {
					bValidDevice = true;
					pDevice->deviceBasic.ulLastActiveTime = pAliveMsg_->ulMessageTime;
					pDevice->deviceBasic.nBattery = pAliveMsg_->usBattery;
					if (pDevice->deviceBasic.nOnline == 0) {
						pDevice->deviceBasic.nOnline = 1;
					}
					if (strcmp(pAliveMsg_->szOrg, pDevice->szOrganization) != 0) {
						strcpy_s(pDevice->szOrganization, sizeof(pDevice->szOrganization), pAliveMsg_->szOrg);
					}
					sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]deal alive message, deviceId=%s, org=%s, "
						"battery=%hu, status=%hu, datetime=%lu\r\n", __FUNCTION__, __LINE__, pAliveMsg_->szDeviceId,
						pAliveMsg_->szOrg, pAliveMsg_->usBattery, pDevice->deviceBasic.nStatus, pAliveMsg_->ulMessageTime);
					writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
				}
				else {
					sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]deal alive message, not find deviceId=%s, "
						"datetime=%lu\r\n", __FUNCTION__, __LINE__, pAliveMsg_->szDeviceId, pAliveMsg_->ulMessageTime);
					writeLog(szLog, pf_logger::eLOGCATEGORY_WARN, m_usLogType);
				}
			}
			pthread_mutex_unlock(&m_mutex4DeviceList);
		}
	}
}

void expand_protocol::ExpandProtocolService::handleTopicOnlineMessage(TopicOnlineMessage * pMsg_)
{
	char szLog[512] = { 0 };
	if (pMsg_ && strlen(pMsg_->szDeviceId) && strlen(pMsg_->szOrg)) {
		if (getLoadDeviceList()) {
			std::string strDeviceId = pMsg_->szDeviceId;
			bool bValidDevice = false;
			char szDatetime[20] = { 0 };
			formatDatetime(pMsg_->ulMessageTime, szDatetime, sizeof(szDatetime));
			pthread_mutex_lock(&m_mutex4DeviceList);
			EscortDeviceList::iterator iter = m_deviceList.find(strDeviceId);
			if (iter != m_deviceList.end()) {
				WristletDevice * pDevice = iter->second;
				if (pDevice) {
					bValidDevice = false;
					pDevice->deviceBasic.ulLastActiveTime = pMsg_->ulMessageTime;
					pDevice->deviceBasic.nBattery = pMsg_->usBattery;
					if (pDevice->deviceBasic.nOnline == 0) {
						pDevice->deviceBasic.nOnline = 1;
					}
					if (strcmp(pMsg_->szOrg, pDevice->szOrganization) != 0) {
						strcpy_s(pDevice->szOrganization, sizeof(pDevice->szOrganization), pMsg_->szOrg);
					}
					sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]deal online message, deviceId=%s, orgId=%s, "
						"battery=%hu, status=%hu, datetime=%s\r\n", __FUNCTION__, __LINE__, pMsg_->szDeviceId,
						pMsg_->szOrg, pMsg_->usBattery, pDevice->deviceBasic.nStatus, szDatetime);
					writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
				}
				else {
					sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]deal online message, not find deviceId=%s, "
						"datetime=%s\r\n", __FUNCTION__, __LINE__, pMsg_->szDeviceId, szDatetime);
					writeLog(szLog, pf_logger::eLOGCATEGORY_WARN, m_usLogType);
				}
			}
			pthread_mutex_unlock(&m_mutex4DeviceList);
			if (bValidDevice) {
				char szPubMsg[512] = { 0 };
				sprintf_s(szPubMsg, sizeof(szPubMsg), "{\"cmd\":%d,\"deviceId\":\"%s\",\"battery\":%hu,"
					"\"datetime\":\"%s\"}", escort_protocol::CMD_PUB_DEVICE_ONLINE, pMsg_->szDeviceId,
					pMsg_->usBattery, szDatetime);
				std::string strPubMsgUtf = AnsiToUtf8(szPubMsg);
				size_t nLen = strPubMsgUtf.size();
				unsigned char * pPubMsg = new unsigned char[nLen + 1];
				memcpy_s(pPubMsg, nLen + 1, strPubMsgUtf.c_str(), nLen);
				pPubMsg[nLen] = '\0';
				encryptMessage(pPubMsg, 0, nLen, kPubSafeKey);
				std::string strDestURI = DEFAULT_PUBLISH_DEST;
				if (m_pPubMsgProducer) {
					m_pPubMsgProducer->send(strDestURI, pPubMsg, nLen, true);
					sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]publish device online to %s, deviceId=%s, battery=%hu,"
						" datetime=%s\r\n", __FUNCTION__, __LINE__, strDestURI.c_str(), pMsg_->szDeviceId, pMsg_->usBattery,
						szDatetime);
				}
				writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
				delete [] pPubMsg;
				pPubMsg = NULL;
			}
		}
	}
}

void expand_protocol::ExpandProtocolService::handleTopicOfflineMessage(TopicOfflineMessage * pMsg_)
{
	char szLog[256] = { 0 };
	if (pMsg_ && strlen(pMsg_->szDeviceId)) {
		if (getLoadDeviceList()) {
			std::string strDeviceId = pMsg_->szDeviceId;
			char szDatetime[20] = { 0 };
			formatDatetime(pMsg_->ulMessageTime, szDatetime, sizeof(szDatetime));
			bool bValidDevice = false;
			pthread_mutex_lock(&m_mutex4DeviceList);
			EscortDeviceList::iterator iter = m_deviceList.find(strDeviceId);
			if (iter != m_deviceList.end()) {
				WristletDevice * pDevice = iter->second;
				if (pDevice) {
					if (pDevice->deviceBasic.nOnline == 1) {
						pDevice->deviceBasic.nOnline = 0;
					}
					bValidDevice = true;
					sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]deal offline message, deviceId=%s, orgId=%s, "
						"datetime=%s\r\n", __FUNCTION__, __LINE__, pMsg_->szDeviceId, pMsg_->szOrg, szDatetime);
					writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
				}
			}
			else {
				sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]deal offline message, not find deviceId=%s, "
					"datetime=%s\r\n", __FUNCTION__, __LINE__, pMsg_->szDeviceId, szDatetime);
				writeLog(szLog, pf_logger::eLOGCATEGORY_WARN, m_usLogType);
			}
			pthread_mutex_unlock(&m_mutex4DeviceList);
			if (bValidDevice) {
				char szPubMsg[512] = { 0 };
				sprintf_s(szPubMsg, sizeof(szPubMsg), "{\"cmd\":%d,\"deviceId\":\"%s\",\"datetime\":\"%s\"}", 
					escort_protocol::CMD_PUB_DEVICE_OFFLINE, pMsg_->szDeviceId, szDatetime);
				std::string strPubMsgUtf = AnsiToUtf8(szPubMsg);
				size_t nLen = strPubMsgUtf.size();
				unsigned char * pPubMsg = new unsigned char[nLen + 1];
				memcpy_s(pPubMsg, nLen + 1, strPubMsgUtf.c_str(), nLen);
				pPubMsg[nLen] = '\0';
				encryptMessage(pPubMsg, 0, nLen, kPubSafeKey);
				std::string strDestURI = DEFAULT_PUBLISH_DEST;
        if (m_pPubMsgProducer) {
          m_pPubMsgProducer->send(strDestURI, pPubMsg, nLen, true);
          sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]publish device offline to %s, deviceId=%s, "
            "datetime=%s\r\n", __FUNCTION__, __LINE__, strDestURI.c_str(), pMsg_->szDeviceId, szDatetime);
          writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
        }
				delete[] pPubMsg;
				pPubMsg = NULL;
			}
		}
	}
}

void expand_protocol::ExpandProtocolService::handleTopicDeviceBindMsg(TopicBindMessage * pMsg_)
{
	if (pMsg_ && strlen(pMsg_->szDeviceId) && strlen(pMsg_->szGuarder)) {
		if (getLoadDeviceList() && getLoadGuarderList()) {
			std::string strDeviceId = pMsg_->szDeviceId;
			std::string strGuarderId = pMsg_->szGuarder;
			{
				pthread_mutex_lock(&m_mutex4DeviceList);
				EscortDeviceList::iterator iter = m_deviceList.find(strDeviceId);
				if (iter != m_deviceList.end()) {
					WristletDevice * pDevice = iter->second;
					if (pDevice) {
						if (pDevice->deviceBasic.nOnline == 0) {
							pDevice->deviceBasic.nOnline = 1;
						}
						pDevice->deviceBasic.nBattery = pMsg_->usBattery;
						pDevice->deviceBasic.ulLastActiveTime = pMsg_->ulMessageTime;
						if (pMsg_->usMode == 0) {
							strcpy_s(pDevice->szBindGuard, sizeof(pDevice->szBindGuard), strGuarderId.c_str());
							pDevice->ulBindTime = pMsg_->ulMessageTime;
						}
						else {
							memset(pDevice->szBindGuard, 0, sizeof(pDevice->szBindGuard));
							pDevice->ulBindTime = 0;
						}
					}
				}
				pthread_mutex_unlock(&m_mutex4DeviceList);
			}
			{
				pthread_mutex_lock(&m_mutex4GuarderList);
				EscortGuarderList::iterator iter = m_guarderList.find(strGuarderId);
				if (iter != m_guarderList.end()) {
					Guarder * pGuarder = iter->second;
					if (pGuarder) {
						if (pMsg_->usMode == 0) {
							strcpy_s(pGuarder->szBindDevice, sizeof(pGuarder->szBindDevice), strDeviceId.c_str());
							pGuarder->usState = STATE_GUARDER_BIND;
						}
						else {
							memset(pGuarder->szBindDevice, 0, sizeof(pGuarder->szBindDevice));
							pGuarder->usState = STATE_GUARDER_FREE;
						}
					}
				}
				pthread_mutex_unlock(&m_mutex4GuarderList);
			}
			char szLog[512] = { 0 };
			sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]deal bind message, deviceId=%s, orgId=%s, guarder=%s,"
				" mode=%hu, datetime=%lu\r\n", __FUNCTION__, __LINE__, pMsg_->szDeviceId, pMsg_->szOrg, 
				pMsg_->szGuarder, pMsg_->usMode, pMsg_->ulMessageTime);
			writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		}
	}
}

void expand_protocol::ExpandProtocolService::handleTopicTaskSubmitMsg(TopicTaskMessage * pMsg_)
{
	if (pMsg_ && strlen(pMsg_->szTaskId) && strlen(pMsg_->szDeviceId) 
		&& strlen(pMsg_->szGuarder) && pMsg_->ulMessageTime > 0) {
		if (getLoadDeviceList() && getLoadGuarderList() && getLoadTaskList()) {
			std::string strTaskId = pMsg_->szTaskId;
			std::string strDeviceId = pMsg_->szDeviceId;
			std::string strGuarderId = pMsg_->szGuarder;
			char szDatetime[20] = { 0 };
			formatDatetime(pMsg_->ulMessageTime, szDatetime, sizeof(szDatetime));
			{
				pthread_mutex_lock(&m_mutex4TaskList);
				EscortTaskList::iterator iter = m_taskList.find(strTaskId);
				if (iter == m_taskList.end()) {
					EscortTask * pTask = new EscortTask();
					memset(pTask, 0, sizeof(EscortTask));
					strcpy_s(pTask->szTaskId, sizeof(pTask->szTaskId), pMsg_->szTaskId);
					strcpy_s(pTask->szDeviceId, sizeof(pTask->szDeviceId), pMsg_->szDeviceId);
					strcpy_s(pTask->szFactoryId, sizeof(pTask->szFactoryId), pMsg_->szFactoryId);
					strcpy_s(pTask->szGuarder, sizeof(pTask->szGuarder), pMsg_->szGuarder);
					strcpy_s(pTask->szOrg, sizeof(pTask->szOrg), pMsg_->szOrg);
					strcpy_s(pTask->szTarget, sizeof(pTask->szTarget), pMsg_->szTarget);
					strcpy_s(pTask->szDestination, sizeof(pTask->szDestination), pMsg_->szDestination);
					pTask->nTaskLimitDistance = (uint8_t)pMsg_->usTaskLimit;
					pTask->nTaskType = (uint8_t)pMsg_->usTaskType;
					if (strlen(pMsg_->szHandset)) {
						strcpy_s(pTask->szHandset, sizeof(pTask->szHandset), pMsg_->szHandset);
						pTask->nTaskMode = 1;
					}
					strcpy_s(pTask->szTaskStartTime, sizeof(pTask->szTaskStartTime), szDatetime);
					m_taskList.emplace(strTaskId, pTask);
				}
				pthread_mutex_unlock(&m_mutex4TaskList);
			}
			{
				pthread_mutex_lock(&m_mutex4DeviceList);
				EscortDeviceList::iterator iter = m_deviceList.find(strDeviceId);
				if (iter != m_deviceList.end()) {
					WristletDevice * pDevice = iter->second;
					if (pDevice) {
						pDevice->deviceBasic.nStatus = DEV_GUARD;
						if (pDevice->deviceBasic.nLooseStatus) {
							pDevice->deviceBasic.nStatus += DEV_LOOSE;
						}
						if (pDevice->deviceBasic.nBattery < 20) {
							pDevice->deviceBasic.nStatus += DEV_LOWPOWER;
						}
					}
				}
				pthread_mutex_unlock(&m_mutex4DeviceList);
			}
			{
				pthread_mutex_lock(&m_mutex4GuarderList);
				EscortGuarderList::iterator iter = m_guarderList.find(strGuarderId);
				if (iter != m_guarderList.end()) {
					Guarder * pGuarder = iter->second;
					if (pGuarder) {
						pGuarder->usState = STATE_GUARDER_DUTY;
						strcpy_s(pGuarder->szTaskId, sizeof(pGuarder->szTaskId), pMsg_->szTaskId);
						strcpy_s(pGuarder->szTaskStartTime, sizeof(pGuarder->szTaskStartTime), szDatetime);
					}
				}
				pthread_mutex_unlock(&m_mutex4GuarderList);
			}
			char szLog[512] = { 0 };
			sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]deal submit task, taskId=%s, deviceId=%s, "
        "orgId=%s, guarderId=%s, datetime=%lu\r\n", __FUNCTION__, __LINE__, pMsg_->szTaskId, 
        pMsg_->szDeviceId, pMsg_->szOrg, pMsg_->szGuarder, pMsg_->ulMessageTime);
			writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
			char szPubMsg[512] = { 0 };
			sprintf_s(szPubMsg, sizeof(szPubMsg), "{\"cmd\":%d,\"taskId\":\"%s\",\"deviceId\":\"%s\","
				"\"guarder\":\"%s\",\"datetime\":\"%s\"}", escort_protocol::CMD_PUB_TASK_START, 
        pMsg_->szTaskId, pMsg_->szDeviceId, pMsg_->szGuarder, szDatetime);
			std::string strPubMsgUtf = AnsiToUtf8(szPubMsg);
			size_t nLen = strPubMsgUtf.size();
			unsigned char * pPubMsg = new unsigned char[nLen + 1];
			memcpy_s(pPubMsg, nLen + 1, strPubMsgUtf.c_str(), nLen);
			pPubMsg[nLen] = '\0';
			encryptMessage(pPubMsg, 0, nLen, kPubSafeKey);
			std::string strDestURI = DEFAULT_PUBLISH_DEST;
      if (m_pPubMsgProducer) {
        m_pPubMsgProducer->send(strDestURI, pPubMsg, nLen, true);
        sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]publish start task to %s, taskId=%s, "
          "deviceId=%s, guarder=%s, datetime=%s\r\n", __FUNCTION__, __LINE__, strDestURI.c_str(),
          pMsg_->szTaskId, pMsg_->szDeviceId, pMsg_->szGuarder, szDatetime);
        writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
      }
      delete [] pPubMsg;
			pPubMsg = NULL;
		}
	}
}

void expand_protocol::ExpandProtocolService::handleTopicTaskModifyMsg(
  TopicTaskModifyMessage * pMsg_)
{
	if (pMsg_ && strlen(pMsg_->szTaskId) && pMsg_->ulMessageTime > 0) {
		std::string strTaskId = pMsg_->szTaskId;
		pthread_mutex_lock(&m_mutex4TaskList);
		EscortTaskList::iterator iter = m_taskList.find(strTaskId);
		if (iter != m_taskList.end()) {
			EscortTask * pTask = iter->second;
			if (pTask) {
				if (pTask->nTaskMode == 0) {
					if (strlen(pMsg_->szHandset)) {
						strcpy_s(pTask->szHandset, sizeof(pTask->szHandset), pMsg_->szHandset);
						pTask->nTaskMode = 1;
					}
				}
				else {
					if (strlen(pMsg_->szHandset) == 0) {
						memset(pTask->szHandset, 0, sizeof(pTask->szHandset));
						pTask->nTaskMode = 0;
					}
					else {
						if (strlen(pMsg_->szHandset) && strcmp(pMsg_->szHandset, pTask->szHandset) != 0) {
							strcpy_s(pTask->szHandset, sizeof(pTask->szHandset), pMsg_->szHandset);
						}
					}
				}
			}
		}
		pthread_mutex_unlock(&m_mutex4TaskList);
	}
}

void expand_protocol::ExpandProtocolService::handleTopicTaskCloseMsg(TopicTaskCloseMessage * pMsg_)
{
	if (pMsg_ && strlen(pMsg_->szTaskId)) {
		std::string strTaskId = pMsg_->szTaskId;
		char szDeviceId[20] = { 0 };
		char szGuarder[20] = { 0 };
		char szDatetime[20] = { 0 };
		formatDatetime(pMsg_->ulMessageTime, szDatetime, sizeof(szDatetime));
		pthread_mutex_lock(&m_mutex4TaskList);
		EscortTaskList::iterator iter = m_taskList.find(strTaskId);
		if (iter != m_taskList.end()) {
			EscortTask * pTask = iter->second;
			if (pTask) {
				strcpy_s(szDeviceId, sizeof(szDeviceId), pTask->szDeviceId);
				strcpy_s(szGuarder, sizeof(szGuarder), pTask->szGuarder);
				delete pTask;
				pTask = NULL;
			}
			m_taskList.erase(iter);
		}
		pthread_mutex_unlock(&m_mutex4TaskList);
		if (strlen(szDeviceId)) {
			pthread_mutex_lock(&m_mutex4DeviceList);
			std::string strDeviceId = szDeviceId;
			EscortDeviceList::iterator iter = m_deviceList.find(strDeviceId);
			if (iter != m_deviceList.end()) {
				WristletDevice * pDevice = iter->second;
				if (pDevice) {
					if (pDevice->deviceBasic.nOnline == 0) {
						pDevice->deviceBasic.nOnline = 1;
					}
					pDevice->deviceBasic.nStatus = DEV_ONLINE;
					if (pDevice->deviceBasic.nLooseStatus == 1) {
						pDevice->deviceBasic.nStatus += DEV_LOOSE;
					}
					if (pDevice->deviceBasic.nBattery < BATTERY_THRESHOLD) {
						pDevice->deviceBasic.nStatus += DEV_LOWPOWER;
					}
					memset(pDevice->szBindGuard, 0, sizeof(pDevice->szBindGuard));
					pDevice->ulBindTime = 0;
				}
			}
			pthread_mutex_unlock(&m_mutex4DeviceList);
		}
		if (strlen(szGuarder)) {
			std::string strGuarder = szGuarder;
			pthread_mutex_lock(&m_mutex4GuarderList);
			EscortGuarderList::iterator iter = m_guarderList.find(strGuarder);
			if (iter != m_guarderList.end()) {
				Guarder * pGuarder = iter->second;
				if (pGuarder) {
					pGuarder->usState = STATE_GUARDER_FREE;
					memset(pGuarder->szTaskId, 0, sizeof(pGuarder->szTaskId));
					memset(pGuarder->szTaskStartTime, 0, sizeof(pGuarder->szTaskStartTime));
				}
			}
			pthread_mutex_unlock(&m_mutex4GuarderList);
		}
		char szLog[512] = { 0 };
		sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]deal topic task close, taskId=%s, deviceId=%s,"
      " guarder=%s, datetime=%s\r\n", __FUNCTION__, __LINE__, pMsg_->szTaskId, szDeviceId, 
      szGuarder, szDatetime);
		writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		char szPubMsg[512] = { 0 };
		sprintf_s(szPubMsg, sizeof(szPubMsg), "{\"cmd\":%d,\"taskId\":\"%s\",\"deviceId\":\"%s\","
      "\"guarder\":\"%s\",\"datetime\":\"%s\"}", escort_protocol::CMD_PUB_TASK_STOP, 
      pMsg_->szTaskId, szDeviceId, szGuarder, szDatetime);
		std::string strPubMsgUtf = AnsiToUtf8(szPubMsg);
		size_t nLen = strPubMsgUtf.size();
		unsigned char * pPubMsg = new unsigned char[nLen + 1];
		memcpy_s(pPubMsg, nLen + 1, strPubMsgUtf.c_str(), nLen);
		pPubMsg[nLen] = '\0';
		encryptMessage(pPubMsg, 0, nLen, kPubSafeKey);
		std::string strDestURI = DEFAULT_PUBLISH_DEST;
    if (m_pPubMsgProducer) {
      m_pPubMsgProducer->send(strDestURI, pPubMsg, nLen, true);
      sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]publish close task to %s, taskId=%s, "
        "deviceId=%s, guarder=%s, datetime=%s\r\n", __FUNCTION__, __LINE__, strDestURI.c_str(), 
        pMsg_->szTaskId, szDeviceId, szGuarder, szDatetime);
      writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
    }
    delete pPubMsg;
		pPubMsg = NULL;
	}
}

void expand_protocol::ExpandProtocolService::handleTopicLocateAppMessage(
  TopicLocateMessageApp * pMsg_)
{
	if (pMsg_ && strlen(pMsg_->szDeviceId) && strlen(pMsg_->szTaskId) && pMsg_->dLat > 0 
    && pMsg_->dLng > 0) {
		std::string strDeviceId = pMsg_->szDeviceId;
		char szDatetime[20] = { 0 };
		char szGuarder[20] = { 0 };
		formatDatetime(pMsg_->ulMessageTime, szDatetime, sizeof(szDatetime));
		pthread_mutex_lock(&m_mutex4DeviceList);
		EscortDeviceList::iterator iter = m_deviceList.find(strDeviceId);
		if (iter != m_deviceList.end()) {
			WristletDevice * pDevice = iter->second;
			if (pDevice) {
				strcpy_s(szGuarder, sizeof(szGuarder), pDevice->szBindGuard);
				if (pDevice->deviceBasic.ulLastActiveTime < pMsg_->ulMessageTime) {
					pDevice->deviceBasic.ulLastActiveTime = pMsg_->ulMessageTime;
				}
				if (pDevice->ulLastGuarderLocateTime < pMsg_->ulMessageTime) {
					pDevice->ulLastGuarderLocateTime = pMsg_->ulMessageTime;
					pDevice->devicePosition.dLatitude = pMsg_->dLat;
					pDevice->devicePosition.dLngitude = pMsg_->dLng;
					pDevice->devicePosition.nCoordinate = pMsg_->nCoordinate;
				}
			}
		}
		pthread_mutex_unlock(&m_mutex4DeviceList);
		char szLog[512] = { 0 };
		sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]deal locate app message, deviceId=%s, "
      "taskId=%s, lat=%f, lng=%f, coordinate=%d, datetime=%s\r\n", __FUNCTION__, __LINE__, 
      pMsg_->szDeviceId, pMsg_->szTaskId, pMsg_->dLat, pMsg_->dLng, pMsg_->nCoordinate, 
      szDatetime);
		writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		char szPubMsg[512] = { 0 };
		sprintf_s(szPubMsg, sizeof(szPubMsg), "{\"cmd\":%d,\"subType\":%d,\"deviceId\":\"%s\","
      "\"guarder\":\"%s\",\"taskId\":\"%s\",\"lat\":%f,\"lng\":%f,\"coordinate\":%d,"
      "\"datetime\":\"%s\"}", escort_protocol::CMD_PUB_DEVICE_LOCATE, LOCATE_APP, 
      pMsg_->szDeviceId, szGuarder, pMsg_->szTaskId, pMsg_->dLat, pMsg_->dLng, 
      pMsg_->nCoordinate, szDatetime);
		std::string strPubMsgUtf = AnsiToUtf8(szPubMsg);
		size_t nLen = strPubMsgUtf.size();
		unsigned char * pPubMsg = new unsigned char[nLen + 1];
		memcpy_s(pPubMsg, nLen + 1, strPubMsgUtf.c_str(), nLen);
		pPubMsg[nLen] = '\0';
		encryptMessage(pPubMsg, 0, nLen, kPubSafeKey);
		std::string strDestURI = DEFAULT_PUBLISH_DEST;
    if (m_pPubMsgProducer) {
      m_pPubMsgProducer->send(strDestURI, pPubMsg, nLen, true);
      sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]publish app locate to %s, deviceId=%s,"
        " taskId=%s, guarder=%s, lat=%f, lng=%f, coordinate=%d, datetime=%s\r\n", __FUNCTION__,
        __LINE__, strDestURI.c_str(), pMsg_->szDeviceId, pMsg_->szTaskId, szGuarder, 
        pMsg_->dLat, pMsg_->dLng, pMsg_->nCoordinate, szDatetime);
      writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
    }
		delete[] pPubMsg;
		pPubMsg = NULL;
	}
}

void expand_protocol::ExpandProtocolService::handleTopicLocateGpsMessage(
  TopicLocateMessageGps * pMsg_)
{
	if (pMsg_ && strlen(pMsg_->szDeviceId) && pMsg_->dLat > 0.0 && pMsg_->dLng > 0.0 
    && pMsg_->ulMessageTime > 0) {
		std::string strDeviceId = pMsg_->szDeviceId;
		char szDatetime[20] = { 0 };
		formatDatetime(pMsg_->ulMessageTime, szDatetime, sizeof(szDatetime));
		pthread_mutex_lock(&m_mutex4DeviceList);
		EscortDeviceList::iterator iter = m_deviceList.find(strDeviceId);
		if (iter != m_deviceList.end()) {
			WristletDevice * pDevice = iter->second;
			if (pDevice) {
				if (pDevice->deviceBasic.ulLastActiveTime < pMsg_->ulMessageTime) {
					pDevice->deviceBasic.ulLastActiveTime = pMsg_->ulMessageTime;
					pDevice->deviceBasic.nBattery = pMsg_->usBattery;
				}
				if (pDevice->ulLastDeviceLocateTime < pMsg_->ulMessageTime) {
					pDevice->ulLastDeviceLocateTime = pMsg_->ulMessageTime;
					pDevice->devicePosition.dLatitude = pMsg_->dLat;
					pDevice->devicePosition.dLngitude = pMsg_->dLng;
					pDevice->devicePosition.nCoordinate = pMsg_->nCoordinate;
					pDevice->nLastLocateType = LOCATE_GPS;
				}
			}
		}
		pthread_mutex_unlock(&m_mutex4DeviceList);
		char szLog[512] = { 0 };
		sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]deal locate gps message, deviceId=%s,"
      " lat=%f, lng=%f, coordinate=%d, datetime=%s\r\n", __FUNCTION__, __LINE__, 
      pMsg_->szDeviceId, pMsg_->dLat, pMsg_->dLng, pMsg_->nCoordinate, szDatetime);
		writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		char szPubMsg[512] = { 0 };
		sprintf_s(szPubMsg, sizeof(szPubMsg), "{\"cmd\":%d,\"subType\":%d,\"deviceId\":\"%s\","
      "\"lat\":%f,\"lng\":%f,\"coordinate\":%d,\"sattelite\":%hu,\"intensity\":%hu,"
      "\"speed\":%f,\"direction\":%d,\"datetime\":\"%s\"}",
			escort_protocol::CMD_PUB_DEVICE_LOCATE, LOCATE_GPS, pMsg_->szDeviceId, pMsg_->dLat,
      pMsg_->dLng, pMsg_->nCoordinate, pMsg_->usStattelite, pMsg_->usIntensity, pMsg_->dSpeed,
      pMsg_->nDirection, szDatetime);
		std::string strPubMsgUtf = AnsiToUtf8(szPubMsg);
		size_t nLen = strPubMsgUtf.size();
		unsigned char * pPubMsg = new unsigned char[nLen + 1];
		memcpy_s(pPubMsg, nLen + 1, strPubMsgUtf.c_str(), nLen);
		pPubMsg[nLen] = '\0';
		encryptMessage(pPubMsg, 0, nLen, kPubSafeKey);
		std::string strDestURI = DEFAULT_PUBLISH_DEST;
    if (m_pPubMsgProducer) {
      m_pPubMsgProducer->send(strDestURI, pPubMsg, nLen, true);
      sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]publish gps locate to %s, deviceId=%s,"
        " lat=%f, lng=%f, coordinate=%d, speed=%f, direction=%d, sattelite=%hu, intensity=%hu,"
        " datetime=%s\r\n", __FUNCTION__, __LINE__, strDestURI.c_str(), pMsg_->szDeviceId, 
        pMsg_->dLat, pMsg_->dLng, pMsg_->nCoordinate, pMsg_->dSpeed, pMsg_->nDirection, 
        pMsg_->usStattelite, pMsg_->usIntensity, szDatetime);
      writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
    }
    delete[] pPubMsg;
		pPubMsg = NULL;
	}
}

void expand_protocol::ExpandProtocolService::handleTopicLocateLbsMessage(
  TopicLocateMessageLbs * pMsg_)
{
	if (pMsg_ && strlen(pMsg_->szDeviceId) && pMsg_->dLat > 0.0 && pMsg_->dLng > 0.0
    && pMsg_->ulMessageTime > 0) {
		std::string strDeviceId = pMsg_->szDeviceId;
		char szDatetime[20] = { 0 };
		formatDatetime(pMsg_->ulMessageTime, szDatetime, sizeof(szDatetime));
		pthread_mutex_lock(&m_mutex4DeviceList);
		EscortDeviceList::iterator iter = m_deviceList.find(strDeviceId);
		if (iter != m_deviceList.end()) {
			WristletDevice * pDevice = iter->second;
			if (pDevice) {
				if (pDevice->deviceBasic.ulLastActiveTime < pMsg_->ulMessageTime) {
					pDevice->deviceBasic.ulLastActiveTime = pMsg_->ulMessageTime;
					pDevice->deviceBasic.nBattery = pMsg_->usBattery;
				}
				if (pDevice->ulLastDeviceLocateTime < pMsg_->ulMessageTime) {
					pDevice->ulLastDeviceLocateTime = pMsg_->ulMessageTime;
					pDevice->nLastLocateType = LOCATE_LBS;
					pDevice->devicePosition.dLatitude = pMsg_->dLat;
					pDevice->devicePosition.dLngitude = pMsg_->dLng;
					pDevice->devicePosition.nCoordinate = pMsg_->nCoordinate;
				}
			}
		}
		pthread_mutex_unlock(&m_mutex4DeviceList);
		char szLog[512] = { 0 };
		sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]deal locate lbs message, deviceId=%s,"
      " lat=%f, lng=%f, coordinate=%d, datetime=%s\r\n", __FUNCTION__, __LINE__, 
      pMsg_->szDeviceId, pMsg_->dLat, pMsg_->dLng, pMsg_->nCoordinate, szDatetime);
		writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		char szPubMsg[512] = { 0 };
		sprintf_s(szPubMsg, sizeof(szPubMsg), "{\"cmd\":%d,\"subType\":%d,\"deviceId\":\"%s\","
      "\"lat\":%f,\"lng\":%f,\"coordinate\":%d,\"datetime\":\"%s\"}", 
      escort_protocol::CMD_PUB_DEVICE_LOCATE, LOCATE_LBS, pMsg_->szDeviceId, pMsg_->dLat, 
      pMsg_->dLng, pMsg_->nCoordinate, szDatetime);
		std::string strPubMsgUtf = AnsiToUtf8(szPubMsg);
		size_t nLen = strPubMsgUtf.size();
		unsigned char * pPubMsg = new unsigned char[nLen + 1];
		memcpy_s(pPubMsg, nLen + 1, strPubMsgUtf.c_str(), nLen);
		pPubMsg[nLen] = '\0';
		encryptMessage(pPubMsg, 0, nLen, kPubSafeKey);
		std::string strDestURI = DEFAULT_PUBLISH_DEST;
    if (m_pPubMsgProducer) {
      m_pPubMsgProducer->send(strDestURI, pPubMsg, nLen, true);
      sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]publish gps locate to %s, deviceId=%s,"
        " lat=%f, lng=%f, coordinate=%d, datetime=%s\r\n", __FUNCTION__, __LINE__, 
        strDestURI.c_str(), pMsg_->szDeviceId, pMsg_->dLat, pMsg_->dLng, pMsg_->nCoordinate,
        szDatetime);
      writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
    }
		delete[] pPubMsg;
		pPubMsg = NULL;
	}
}

void expand_protocol::ExpandProtocolService::handleTopicAlarmLowpowerMessage(
  TopicAlarmMessageLowpower * pMsg_)
{
	if (pMsg_ && strlen(pMsg_->szDeviceId)) {
		std::string strDeviceId = pMsg_->szDeviceId;
		char szDatetime[20] = { 0 };
		formatDatetime(pMsg_->ulMessageTime, szDatetime, sizeof(szDatetime));
		pthread_mutex_lock(&m_mutex4DeviceList);
		EscortDeviceList::iterator iter = m_deviceList.find(strDeviceId);
		if (iter != m_deviceList.end()) {
			WristletDevice * pDevice = iter->second;
			if (pDevice) {
				if (pDevice->deviceBasic.ulLastActiveTime < pMsg_->ulMessageTime) {
					pDevice->deviceBasic.ulLastActiveTime = pMsg_->ulMessageTime;
					pDevice->deviceBasic.nBattery = pMsg_->usBattery;
					if (pMsg_->usMode == 0) {
						pDevice->ulLastLowPowerAlertTime = pMsg_->ulMessageTime;
					}
				}
			}
		}
		pthread_mutex_unlock(&m_mutex4DeviceList);
		char szLog[512] = { 0 };
		sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]deal alram lowpower message, deviceId=%s,"
      " battery=%hu, mode=%hu, datetime=%s\r\n", __FUNCTION__, __LINE__, pMsg_->szDeviceId, 
      pMsg_->usBattery, pMsg_->usMode, szDatetime);
		writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		char szPubMsg[512] = { 0 };
		sprintf_s(szPubMsg, sizeof(szPubMsg), "{\"cmd\":%d,\"subType\":%d,\"deviceId\":\"%s\","
      "\"battery\":%hu,\"mode\":%hu,\"datetime\":\"%s\"}", escort_protocol::CMD_PUB_DEVICE_ALARM,
      ALARM_DEVICE_LOWPOWER, 
			pMsg_->szDeviceId, pMsg_->usBattery, pMsg_->usMode, szDatetime);
		std::string strPubMsgUtf = AnsiToUtf8(szPubMsg);
		size_t nLen = strPubMsgUtf.size();
		unsigned char * pPubMsg = new unsigned char[nLen + 1];
		memcpy_s(pPubMsg, nLen + 1, strPubMsgUtf.c_str(), nLen);
		pPubMsg[nLen] = '\0';
		encryptMessage(pPubMsg, 0, nLen, kPubSafeKey);
		std::string strDestURI = DEFAULT_PUBLISH_DEST;
    if (m_pPubMsgProducer) {
      m_pPubMsgProducer->send(strDestURI, pPubMsg, nLen, true);
      sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]publish alarm lowpower to %s, deviceId=%s,"
        " battery=%hu, mode=%hu, datetime=%s\r\n", __FUNCTION__, __LINE__, strDestURI.c_str(), 
        pMsg_->szDeviceId, pMsg_->usBattery, pMsg_->usMode, szDatetime);
      writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
    }
		delete[] pPubMsg;
		pPubMsg = NULL;
	}
}

void expand_protocol::ExpandProtocolService::handleTopicAlarmLooseMsg(
  TopicAlarmMessageLoose * pMsg_)
{
	if (pMsg_ && strlen(pMsg_->szDeviceId) && pMsg_->ulMessageTime > 0)  {
		std::string strDeviceId = pMsg_->szDeviceId;
		char szDatetime[20] = { 0 };
		formatDatetime(pMsg_->ulMessageTime, szDatetime, sizeof(szDatetime));
		pthread_mutex_lock(&m_mutex4DeviceList);
		EscortDeviceList::iterator iter = m_deviceList.find(strDeviceId);
		if (iter != m_deviceList.end()) {
			WristletDevice * pDevice = iter->second;
			if (pDevice) {
				if (pDevice->deviceBasic.ulLastActiveTime < pMsg_->ulMessageTime) {
					pDevice->deviceBasic.ulLastActiveTime = pMsg_->ulMessageTime;
					pDevice->deviceBasic.nBattery = pMsg_->usBattery;
					if (pMsg_->usMode == 0) {
						pDevice->ulLastLooseAlertTime = pMsg_->ulMessageTime;
						pDevice->deviceBasic.nLooseStatus = 1;
						if ((pDevice->deviceBasic.nStatus & DEV_LOOSE) == 0) {
							pDevice->deviceBasic.nStatus += DEV_LOOSE;
						}
					}
					else {
						pDevice->deviceBasic.nLooseStatus = 0;
						if ((pDevice->deviceBasic.nStatus & DEV_LOOSE) == DEV_LOOSE) {
							pDevice->deviceBasic.nStatus -= DEV_LOOSE;
						}
					}
				}
			}
		}
		pthread_mutex_unlock(&m_mutex4DeviceList);
		char szLog[512] = { 0 };
		sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]deal alram loose message, deviceId=%s,"
      " battery=%hu, mode=%hu, datetime=%s\r\n", __FUNCTION__, __LINE__, pMsg_->szDeviceId,
      pMsg_->usBattery, pMsg_->usMode, szDatetime);
		writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		char szPubMsg[512] = { 0 };
		sprintf_s(szPubMsg, sizeof(szPubMsg), "{\"cmd\":%d,\"subType\":%d,\"deviceId\":\"%s\","
      "\"battery\":%hu,\"mode\":%d,\"datetime\":\"%s\"}", escort_protocol::CMD_PUB_DEVICE_ALARM,
      ALARM_DEVICE_LOOSE, pMsg_->szDeviceId, pMsg_->usBattery, pMsg_->usMode, szDatetime);
		std::string strPubMsgUtf = AnsiToUtf8(szPubMsg);
		size_t nLen = strPubMsgUtf.size();
		unsigned char * pPubMsg = new unsigned char[nLen + 1];
		memcpy_s(pPubMsg, nLen + 1, strPubMsgUtf.c_str(), nLen);
		pPubMsg[nLen] = '\0';
		encryptMessage(pPubMsg, 0, nLen, kPubSafeKey);
		std::string strDestURI = DEFAULT_PUBLISH_DEST;
    if (m_pPubMsgProducer) {
      m_pPubMsgProducer->send(strDestURI, pPubMsg, nLen, true);
      sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]publish loose message to %s, deviceId=%s,"
        " battery=%hu, mode=%hu, datetime=%s\r\n", __FUNCTION__, __LINE__, strDestURI.c_str(),
        pMsg_->szDeviceId, pMsg_->usBattery, pMsg_->usMode, szDatetime);
      writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
    }
		delete[] pPubMsg;
		pPubMsg = NULL;
	}
}

void expand_protocol::ExpandProtocolService::handleTopicAlarmFleeMsg(
  TopicAlarmMessageFlee * pMsg_)
{
	if (pMsg_ && strlen(pMsg_->szDeviceId) && strlen(pMsg_->szTaskId) 
    && pMsg_->ulMessageTime > 0) {
		std::string strDeviceId = pMsg_->szDeviceId;
		std::string strTaskId = pMsg_->szTaskId;
		char szDatetime[20] = { 0 };
		formatDatetime(pMsg_->ulMessageTime, szDatetime, sizeof(szDatetime));
		{
			pthread_mutex_lock(&m_mutex4DeviceList);
			EscortDeviceList::iterator iter = m_deviceList.find(strDeviceId);
			if (iter != m_deviceList.end()) {
				WristletDevice * pDevice = iter->second;
				if (pDevice) {
					if (pDevice->deviceBasic.ulLastActiveTime < pMsg_->ulMessageTime) {
						pDevice->deviceBasic.ulLastActiveTime = pMsg_->ulMessageTime;
						pDevice->deviceBasic.nBattery = pMsg_->usBattery;
						if (pMsg_->usMode = 0) {
							pDevice->ulLastFleeAlertTime = pMsg_->ulMessageTime;
							pDevice->deviceBasic.nStatus = DEV_FLEE;
						}
						else {
							pDevice->deviceBasic.nStatus = DEV_GUARD;
						}
						if (pDevice->deviceBasic.nBattery < 20) {
							pDevice->deviceBasic.nStatus += DEV_LOWPOWER;
						}
						if (pDevice->deviceBasic.nLooseStatus == 1) {
							pDevice->deviceBasic.nStatus += DEV_LOOSE;
						}
					}
				}
			}
			pthread_mutex_unlock(&m_mutex4DeviceList);
		}
		{
			pthread_mutex_lock(&m_mutex4TaskList);
			EscortTaskList::iterator iter = m_taskList.find(strTaskId);
			if (iter != m_taskList.end()) {
				EscortTask * pTask = iter->second;
				if (pTask) {
					if (pMsg_->usMode == 0) {
						pTask->nTaskFlee = 1;
					}
					else {
						pTask->nTaskFlee = 0;
					}
				}
			}
			pthread_mutex_unlock(&m_mutex4TaskList);
		}
		char szLog[512] = { 0 };
		sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]deal alarm flee, deviceId=%s, taskId=%s,"
      " mode=%hu, guarder=%s, datetime=%s", __FUNCTION__, __LINE__, pMsg_->szDeviceId, 
      pMsg_->szTaskId, pMsg_->usMode, pMsg_->szGuarder, szDatetime);
		writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		char szPubMsg[512] = { 0 };
		sprintf_s(szPubMsg, sizeof(szPubMsg), "{\"cmd\":%d,\"subType\":%d,\"deviceId\":\"%s\","
      "\"taskId\":\"%s\",\"guarder\":\"%s\",\"mode\":%hu,\"battery\":%hu,\"datetime\":\"%s\"}",
      escort_protocol::CMD_PUB_DEVICE_ALARM, ALARM_DEVICE_FLEE, pMsg_->szDeviceId, 
      pMsg_->szTaskId, pMsg_->szGuarder, pMsg_->usMode, pMsg_->usBattery, szDatetime);
		std::string strPubMsgUtf = AnsiToUtf8(szPubMsg);
		size_t nLen = strPubMsgUtf.size();
		unsigned char * pPubMsg = new unsigned char[nLen + 1];
		memcpy_s(pPubMsg, nLen + 1, strPubMsgUtf.c_str(), nLen);
		pPubMsg[nLen] = '\0';
		encryptMessage(pPubMsg, 0, nLen, kPubSafeKey);
		std::string strDestURI = DEFAULT_PUBLISH_DEST;
    if (m_pPubMsgProducer) {
      m_pPubMsgProducer->send(strDestURI, pPubMsg, nLen, true);
      sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]publish flee message to %s, deviceId=%s, "
        "taskId=%s, guarder=%s  battery=%hu, mode=%hu, datetime=%s\r\n", __FUNCTION__, 
        __LINE__, strDestURI.c_str(), pMsg_->szDeviceId, pMsg_->szTaskId, pMsg_->szGuarder, 
        pMsg_->usBattery, pMsg_->usMode, szDatetime);
      writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
    }
		delete[] pPubMsg;
		pPubMsg = NULL;
	}
}

void expand_protocol::ExpandProtocolService::handleTopicAlarmLocateLostMsg(
  TopicAlarmMessageLocateLost * pMsg_)
{

}

void expand_protocol::ExpandProtocolService::handleTopicAlarmFenceMsg(TopicAlarmMessageFence * pMsg_)
{
	if (pMsg_ && strlen(pMsg_->szDeviceId) && strlen(pMsg_->szFenceTaskId) && strlen(pMsg_->szFenceId)) {
		std::string strDeviceId = pMsg_->szDeviceId;
		char szDatetime[20] = { 0 };
		formatDatetime(pMsg_->ulMessageTime, szDatetime, sizeof(szDatetime));
		pthread_mutex_lock(&m_mutex4DeviceList);
		EscortDeviceList::iterator iter = m_deviceList.find(strDeviceId);
		if (iter != m_deviceList.end()) {
			WristletDevice * pDevice = iter->second;
			if (pDevice) {
				if (pMsg_->nMode == 0) {
					pDevice->nDeviceFenceState = 1;
				}
				else {
					pDevice->nDeviceFenceState = 0;
				}
			}
		}
		pthread_mutex_unlock(&m_mutex4DeviceList);
	}
}

void expand_protocol::ExpandProtocolService::handleTopicLoginMsg(TopicLoginMessage * pMsg_)
{

}

void expand_protocol::ExpandProtocolService::handleTopicLogoutMsg(TopicLogoutMessage * pMsg_)
{

}

bool expand_protocol::ExpandProtocolService::addInteractMessage(InteractionMessage * pInteractMsg_)
{
	bool result = false;
	if (pInteractMsg_ && pInteractMsg_->pMsgContents && pInteractMsg_->uiContentLens) {
		pthread_mutex_lock(&m_mutex4InteractMsgQue);
		m_interactMsgQue.emplace(pInteractMsg_);
		if (m_interactMsgQue.size() == 1) {
			pthread_cond_signal(&m_cond4InteractMsgQue);
		}
		pthread_mutex_unlock(&m_mutex4InteractMsgQue);
		result = true;
	}
	return result;
}

void expand_protocol::ExpandProtocolService::dealInteractMessage()
{
	char szLog[512] = { 0 };
	do {
		pthread_mutex_lock(&m_mutex4InteractMsgQue);
		while (m_nRun && m_interactMsgQue.empty()) {
			pthread_cond_wait(&m_cond4InteractMsgQue, &m_mutex4InteractMsgQue);
		}
		if (!m_nRun && m_interactMsgQue.empty()) {
			pthread_mutex_unlock(&m_mutex4InteractMsgQue);
			break;
		}
		InteractionMessage * pInteractMsg = m_interactMsgQue.front();
		m_interactMsgQue.pop();
		pthread_mutex_unlock(&m_mutex4InteractMsgQue);
		if (pInteractMsg) {
			if (pInteractMsg->pMsgContents && pInteractMsg->uiContentCount && pInteractMsg->uiContentLens) {
				for (unsigned int i = 0; i < pInteractMsg->uiContentCount; ++i) {
					rapidjson::Document doc;
					if (!doc.Parse(pInteractMsg->pMsgContents[i]).HasParseError()) {
						bool bValidMsg = false;
						if (doc.HasMember("mark") && doc.HasMember("version")) {
							if (doc["mark"].IsString()) {
								if (doc["mark"].GetStringLength()) {
									if (strcmp(doc["mark"].GetString(), "EC") == 0) {
										bValidMsg = true;
									}
								}
							}
						}
						if (bValidMsg) {
							int nType = 0, nSeq = 0;
							char szDatetime[20] = { 0 };
							if (doc.HasMember("type")) {
								if (doc["type"].IsInt()) {
									nType = doc["type"].GetInt();
								}
							}
							if (doc.HasMember("seq")) {
								if (doc["seq"].IsInt()) {
									nSeq = doc["seq"].GetInt();
								}
							}
							if (doc.HasMember("datetime")) {
								if (doc["datetime"].IsString()) {
									if (doc["datetime"].GetStringLength()) {
										strcpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString());
									}
								}
							}
							switch (nType) {
								case MSG_SUB_ALIVE: {
									break;
								}
								case MSG_SUB_SNAPSHOT: {
									break;
								}
								case MSG_SUB_REPORT: {
									break;
								}
								case MSG_SUB_REQUEST: {
									break;
								}
								case MSG_SUB_GETINFO: {
									int nGetType = 0, nRetCount = 0;
									if (doc.HasMember("getType")) {
										if (doc["getType"].IsInt()) {
											nGetType = doc["getType"].GetInt();
										}
									}
									if (doc.HasMember("retCount")) {
										if (doc["retCount"].IsInt()) {
											nRetCount = doc["retCount"].GetInt();
										}
									}
									if (nGetType && nRetCount > 0) {
										if (doc.HasMember("list")) {
											switch (nGetType) {
											  case BUFFER_ORG: {
												  pthread_mutex_lock(&m_mutex4OrgList);
												  for (int i = 0; i < nRetCount; i++) {
													  Organization org;
													  memset(&org, 0, sizeof(org));
													  if (doc["list"][i].HasMember("orgId")) {
														  if (doc["list"][i]["orgId"].IsString()
															  && doc["list"][i]["orgId"].GetStringLength()) {
															  strcpy_s(org.szOrgId, sizeof(org.szOrgId),
																  doc["list"][i]["orgId"].GetString());
														  }
													  }
													  if (doc["list"][i].HasMember("orgName")) {
														  if (doc["list"][i]["orgName"].IsString()
															  && doc["list"][i]["orgName"].GetStringLength()) {
															  strcpy_s(org.szOrgName, sizeof(org.szOrgName),
																  doc["list"][i]["orgName"].GetString());
														  }
													  }
													  if (doc["list"][i].HasMember("parentId")) {
														  if (doc["list"][i]["parentId"].IsString()
															  && doc["list"][i]["parentId"].GetStringLength()) {
															  strcpy_s(org.szParentOrgId, sizeof(org.szParentOrgId),
																  doc["list"][i]["parentId"].GetString());
														  }
													  }
													  if (strlen(org.szOrgId)) {
														  OrganizationEx * pOrg = new OrganizationEx();
														  memcpy_s(&pOrg->org, sizeof(Organization), &org, sizeof(Organization));
														  std::string strOrgId = org.szOrgId;
														  m_orgList.emplace(strOrgId, pOrg);
														  sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]load org, orgId=%s, orgName=%s,"
															  " parentId=%s\r\n", __FUNCTION__, __LINE__, org.szOrgId, org.szOrgName,
															  org.szParentOrgId);
														  writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
													  }
													  else {
														  sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]load org parameter miss, "
															  "orgId=%s, orgName=%s, parentId=%s\r\n", __FUNCTION__, __LINE__,
															  org.szOrgId, org.szOrgName, org.szParentOrgId);
														  writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
													  }
												  }
												  pthread_mutex_unlock(&m_mutex4OrgList);
												  setLoadOrgList(true);
												  reloadOrgList(false);
												  break;
											  }
											  case BUFFER_DEVICE: {
												  size_t nDeviceSize = sizeof(WristletDevice);
												  pthread_mutex_lock(&m_mutex4DeviceList);
												  for (int i = 0; i < nRetCount; i++) {
													  WristletDevice device;
													  memset(&device, 0, nDeviceSize);
													  if (doc["list"][i].HasMember("factoryId")) {
														  if (doc["list"][i]["factoryId"].IsString()
															  && doc["list"][i]["factoryId"].GetStringLength()) {
															  strcpy_s(device.deviceBasic.szFactoryId,
																  sizeof(device.deviceBasic.szFactoryId),
																  doc["list"][i]["factoryId"].GetString());
														  }
													  }
													  if (doc["list"][i].HasMember("deviceId")) {
														  if (doc["list"][i]["deviceId"].IsString()
															  && doc["list"][i]["deviceId"].GetStringLength()) {
															  strcpy_s(device.deviceBasic.szDeviceId,
																  sizeof(device.deviceBasic.szDeviceId),
																  doc["list"][i]["deviceId"].GetString());
														  }
													  }
													  if (doc["list"][i].HasMember("status")) {
														  if (doc["list"][i]["status"].IsUint()) {
															  device.deviceBasic.nStatus =
																  (unsigned short)doc["list"][i]["status"].GetUint();
														  }
													  }
													  if (doc["list"][i].HasMember("battery")) {
														  if (doc["list"][i]["battery"].IsUint()) {
															  device.deviceBasic.nBattery = (unsigned short)doc["list"][i]["battery"].GetUint();
														  }
													  }
													  if (doc["list"][i].HasMember("loose")) {
														  if (doc["list"][i]["loose"].IsUint()) {
															  device.deviceBasic.nLooseStatus = (unsigned short)doc["list"][i]["loose"].GetUint();
														  }
													  }
														if (doc["list"][i].HasMember("online")) {
															if (doc["list"][i]["online"].IsUint()) {
																device.deviceBasic.nOnline = (unsigned short)doc["list"][i]["online"].GetUint();
															}
														}
														if (doc["list"][i].HasMember("lastActiveTime")) {
														  if (doc["list"][i]["lastActiveTime"].IsUint()) {
															  device.deviceBasic.ulLastActiveTime =
																  (unsigned long)doc["list"][i]["lastActiveTime"].GetUint();
														  }
													  }
													  if (doc["list"][i].HasMember("link")) {
														  if (doc["list"][i]["link"].IsString()
															  && doc["list"][i]["link"].GetStringLength()) {
															  strcpy_s(device.szLinkId, sizeof(device.szLinkId),
																  doc["list"][i]["link"].GetString());
														  }
													  }
													  if (doc["list"][i].HasMember("org")) {
														  if (doc["list"][i]["org"].IsString()
															  && doc["list"][i]["org"].GetStringLength()) {
															  strcpy_s(device.szOrganization, sizeof(device.szOrganization),
																  doc["list"][i]["org"].GetString());
														  }
													  }
													  if (doc["list"][i].HasMember("lastLocateType")) {
														  if (doc["list"][i]["lastLocateType"].IsUint()) {
															  device.nLastLocateType = doc["list"][i]["lastLocateType"].GetUint();
														  }
													  }
													  if (doc["list"][i].HasMember("lastDeviceLocateTime")) {
														  if (doc["list"][i]["lastDeviceLocateTime"].IsUint()) {
															  device.ulLastDeviceLocateTime =
																  (unsigned long)doc["list"][i]["lastDeviceLocateTime"].GetUint();
														  }
													  }
													  if (doc["list"][i].HasMember("lastGuarderLocateTime")) {
														  if (doc["list"][i]["lastGuarderLocateTime"].IsUint()) {
															  device.ulLastGuarderLocateTime =
																  (unsigned long)doc["list"][i]["lastGuarderLocateTime"].GetUint();
														  }
													  }
													  if (doc["list"][i].HasMember("lastLowpowerAlertTime")) {
														  if (doc["list"][i]["lastLowpowerAlertTime"].IsUint()) {
															  device.ulLastLowPowerAlertTime =
																  (unsigned long)doc["list"][i]["lastLowpowerAlertTime"].GetUint();
														  }
													  }
													  if (doc["list"][i].HasMember("lastLooseAlertTime")) {
														  if (doc["list"][i]["lastLooseAlertTime"].IsUint()) {
															  device.ulLastLooseAlertTime =
																  (unsigned long)doc["list"][i]["lastLooseAlertTime"].GetUint();
														  }
													  }
													  if (doc["list"][i].HasMember("lastFleeAlertTime")) {
														  if (doc["list"][i]["lastFleeAlertTime"].IsUint()) {
															  device.ulLastFleeAlertTime =
																  (unsigned long)doc["list"][i]["lastFleeAlertTime"].GetUint();
														  }
													  }
													  if (doc["list"][i].HasMember("bindTime")) {
														  if (doc["list"][i]["bindTime"].IsUint()) {
															  device.ulBindTime = (unsigned long)doc["list"][i]["bindTime"].GetUint();
														  }
													  }
													  if (doc["list"][i].HasMember("bindGuarder")) {
														  if (doc["list"][i]["bindGuarder"].IsString()
															  && doc["list"][i]["bindGuarder"].GetStringLength()) {
															  strcpy_s(device.szBindGuard, sizeof(device.szBindGuard),
																  doc["list"][i]["bindGuarder"].GetString());
														  }
													  }
													  if (doc["list"][i].HasMember("devLat")) {
														  if (doc["list"][i]["devLat"].IsDouble()) {
															  device.devicePosition.dLatitude = doc["list"][i]["devLat"].GetDouble();
														  }
													  }
													  if (doc["list"][i].HasMember("devLng")) {
														  if (doc["list"][i]["devLng"].IsDouble()) {
															  device.devicePosition.dLngitude = doc["list"][i]["devLng"].GetDouble();
														  }
													  }
													  if (doc["list"][i].HasMember("devCoordinate")) {
														  if (doc["list"][i]["devCoordinate"].IsInt()) {
															  device.devicePosition.nCoordinate = doc["list"][i]["devCoordinate"].GetInt();
														  }
													  }
													  if (doc["list"][i].HasMember("guarderLat")) {
														  if (doc["list"][i]["guarderLat"].IsDouble()) {
															  device.guardPosition.dLatitude = doc["list"][i]["guarderLat"].GetDouble();
														  }
													  }
													  if (doc["list"][i].HasMember("guarderLng")) {
														  if (doc["list"][i]["guarderLng"].IsDouble()) {
															  device.guardPosition.dLngitude = doc["list"][i]["guarderLng"].GetDouble();
														  }
													  }
													  if (doc["list"][i].HasMember("guarderCoordinate")) {
														  if (doc["list"][i]["guarderCoordinate"].IsInt()) {
															  device.guardPosition.nCoordinate =
																  doc["list"][i]["guarderCoordinate"].GetInt();
														  }
													  }
													  if (doc["list"][i].HasMember("imei")) {
														  if (doc["list"][i]["imei"].IsString()
															  && doc["list"][i]["imei"].GetStringLength()) {
															  strcpy_s(device.deviceBasic.szDeviceImei,
																  sizeof(device.deviceBasic.szDeviceImei),
																  doc["list"][i]["imei"].GetString());
														  }
													  }
													  if (doc["list"][i].HasMember("mnc")) {
														  if (doc["list"][i]["mnc"].IsInt()) {
															  device.deviceBasic.nDeviceMnc = doc["list"][i]["mnc"].GetInt();
														  }
													  }
													  if (doc["list"][i].HasMember("fenceHave")) {
														  if (doc["list"][i]["fenceHave"].IsInt()) {
															  device.nDeviceHasFence = doc["list"][i]["fenceHave"].GetInt();
														  }
													  }
													  if (doc["list"][i].HasMember("fenceAlarm")) {
														  if (doc["list"][i]["fenceAlarm"].IsInt()) {
															  device.nDeviceFenceState = doc["list"][i]["fenceAlarm"].GetInt();
														  }
													  }
													  if (strlen(device.deviceBasic.szDeviceId)
														  && strlen(device.deviceBasic.szFactoryId)) {
														  WristletDevice * pDevice = new WristletDevice();
														  memcpy_s(pDevice, nDeviceSize, &device, nDeviceSize);
														  pDevice->devicePosition.usLatType = pDevice->devicePosition.usLngType = 1;
														  pDevice->guardPosition.usLatType = pDevice->guardPosition.usLngType = 1;
														  std::string strDeviceId = (std::string)pDevice->deviceBasic.szDeviceId;
														  m_deviceList.emplace(strDeviceId, pDevice);
														  sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]load device, id=%s, factory=%s,"
															  " org=%s, status=%hu, online=%hu, battery=%hu, imei=%s, mnc=%d\r\n", 
																__FUNCTION__, __LINE__, pDevice->deviceBasic.szDeviceId, 
																pDevice->deviceBasic.szFactoryId, pDevice->szOrganization, 
																pDevice->deviceBasic.nStatus, pDevice->deviceBasic.nOnline,
															  pDevice->deviceBasic.nBattery, pDevice->deviceBasic.szDeviceImei,
															  pDevice->deviceBasic.nDeviceMnc);
														  writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
													  }
													  else {
														  sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]load device data miss, id=%s, "
															  "factory=%s, org=%s, status=%hu, online=%hu, battery=%hu, imei=%s, mnc=%d\r\n",
															  __FUNCTION__, __LINE__, device.deviceBasic.szDeviceId,
															  device.deviceBasic.szFactoryId, device.szOrganization,
															  device.deviceBasic.nStatus, device.deviceBasic.nOnline, 
																device.deviceBasic.nBattery, device.deviceBasic.szDeviceImei, 
																device.deviceBasic.nDeviceMnc);
														  writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
													  }
												  }
												  pthread_mutex_unlock(&m_mutex4DeviceList);
												  setLoadDeviceList(true);
												  break;
											  }
											  case BUFFER_GUARDER: {
												  size_t nGuarderSize = sizeof(Guarder);
												  Guarder guarder;
												  memset(&guarder, 0, nGuarderSize);
												  for (int i = 0; i < nRetCount; i++) {
													  if (doc["list"][i].HasMember("id")) {
														  if (doc["list"][i]["id"].IsString()
															  && doc["list"][i]["id"].GetStringLength()) {
															  strcpy_s(guarder.szId, sizeof(guarder.szId),
																  doc["list"][i]["id"].GetString());
														  }
													  }
													  if (doc["list"][i].HasMember("name")) {
														  if (doc["list"][i]["name"].IsString()
															  && doc["list"][i]["name"].GetStringLength()) {
															  strcpy_s(guarder.szTagName, sizeof(guarder.szTagName),
																  doc["list"][i]["name"].GetString());
														  }
													  }
													  if (doc["list"][i].HasMember("password")) {
														  if (doc["list"][i]["password"].IsString()
															  && doc["list"][i]["password"].GetStringLength()) {
															  strcpy_s(guarder.szPassword, sizeof(guarder.szPassword),
																  doc["list"][i]["password"].GetString());
														  }
													  }
													  if (doc["list"][i].HasMember("org")) {
														  if (doc["list"][i]["org"].IsString()
															  && doc["list"][i]["org"].GetStringLength()) {
															  strcpy_s(guarder.szOrg, sizeof(guarder.szOrg),
																  doc["list"][i]["org"].GetString());
														  }
													  }
													  if (doc["list"][i].HasMember("state")) {
														  if (doc["list"][i]["state"].IsInt()) {
															  guarder.usState = (unsigned short)doc["list"][i]["state"].GetInt();
														  }
													  }
													  if (doc["list"][i].HasMember("roleType")) {
														  if (doc["list"][i]["roleType"].IsInt()) {
															  guarder.usRoleType = (unsigned short)doc["list"][i]["roleType"].GetInt();
														  }
													  }
													  if (doc["list"][i].HasMember("session")) {
														  if (doc["list"][i]["session"].IsString()
															  && doc["list"][i]["session"].GetStringLength()) {
															  strcpy_s(guarder.szCurrentSession, sizeof(guarder.szCurrentSession),
																  doc["list"][i]["session"].GetString());
														  }
													  }
													  if (doc["list"][i].HasMember("device")) {
														  if (doc["list"][i]["device"].IsString()
															  && doc["list"][i]["device"].GetStringLength()) {
															  strcpy_s(guarder.szBindDevice, sizeof(guarder.szBindDevice),
																  doc["list"][i]["device"].GetString());
														  }
													  }
													  if (doc["list"][i].HasMember("task")) {
														  if (doc["list"][i]["task"].IsString()
															  && doc["list"][i]["task"].GetStringLength()) {
															  strcpy_s(guarder.szTaskId, sizeof(guarder.szTaskId),
																  doc["list"][i]["task"].GetString());
														  }
													  }
													  if (doc["list"][i].HasMember("taskStartTime")) {
														  if (doc["list"][i]["taskStartTime"].IsString()
															  && doc["list"][i]["taskStartTime"].GetStringLength()) {
															  strcpy_s(guarder.szTaskStartTime, sizeof(guarder.szTaskStartTime),
																  doc["list"][i]["taskStartTime"].GetString());
														  }

													  }
													  if (doc["list"][i].HasMember("link")) {
														  if (doc["list"][i]["link"].IsString()
															  && doc["list"][i]["link"].GetStringLength()) {
															  strcpy_s(guarder.szLink, sizeof(guarder.szLink),
																  doc["list"][i]["guarder"].GetString());
														  }
													  }
													  if (doc["list"][i].HasMember("phone")) {
														  if (doc["list"][i]["phone"].IsString()
															  && doc["list"][i]["phone"].GetStringLength()) {
															  strcpy_s(guarder.szPhoneCode, sizeof(guarder.szPhoneCode),
																  doc["list"][i]["phone"].GetString());
														  }
													  }
													  if (strlen(guarder.szId) && strlen(guarder.szTagName)
														  && strlen(guarder.szPassword)) {
														  Guarder * pGuarder = new Guarder();
														  memset(pGuarder, 0, nGuarderSize);
														  memcpy_s(pGuarder, nGuarderSize, &guarder, nGuarderSize);
														  m_guarderList.emplace((std::string)pGuarder->szId, pGuarder);
														  sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]load guarder, id=%s, name=%s,"
															  " roleType=%hu, state=%hu, session=%s, org=%s, device=%s, task=%s, "
															  "startTime=%s\r\n", __FUNCTION__, __LINE__, guarder.szId, guarder.szTagName, 
															  guarder.usRoleType, guarder.usState, guarder.szCurrentSession, guarder.szOrg,
															  guarder.szBindDevice, guarder.szTaskId, guarder.szTaskStartTime);
														  writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
													  }
													  else {
														  sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]load guarder data miss, id=%s,"
															  " name=%s, roleType=%hu, state=%hu, session=%s, org=%s, device=%s, task=%s, "
															  "startTime=%s\r\n", __FUNCTION__, __LINE__, guarder.szId, guarder.szTagName, 
															  guarder.usRoleType, guarder.usState, guarder.szCurrentSession, guarder.szOrg,
															  guarder.szBindDevice, guarder.szTaskId, guarder.szTaskStartTime);
														  writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
													  }
												  }
												  setLoadGuarderList(true);
												  break;
											  }
											  case BUFFER_TASK: {
												  size_t nTaskSize = sizeof(EscortTask);
												  for (int i = 0; i < nRetCount; i++) {
													  EscortTask task;
													  memset(&task, 0, sizeof(EscortTask));
													  if (doc["list"][i].HasMember("taskId")) {
														  if (doc["list"][i]["taskId"].IsString()
															  && doc["list"][i]["taskId"].GetStringLength()) {
															  strcpy_s(task.szTaskId, sizeof(task.szTaskId),
																  doc["list"][i]["taskId"].GetString());
														  }
													  }
													  if (doc["list"][i].HasMember("taskType")) {
														  if (doc["list"][i]["taskType"].IsInt()) {
															  task.nTaskType = (uint8_t)doc["list"][i]["taskType"].GetInt();
														  }
													  }
													  if (doc["list"][i].HasMember("taskLimit")) {
														  if (doc["list"][i]["taskLimit"].IsInt()) {
															  task.nTaskLimitDistance = (uint8_t)doc["list"][i]["taskLimit"].GetInt();
														  }
													  }
													  if (doc["list"][i].HasMember("taskFlee")) {
														  if (doc["list"][i]["taskFlee"].IsInt()) {
															  task.nTaskFlee = (uint8_t)doc["list"][i]["taskFlee"].GetInt();
														  }
													  }
													  if (doc["list"][i].HasMember("guarder")) {
														  if (doc["list"][i]["guarder"].IsString()
															  && doc["list"][i]["guarder"].GetStringLength()) {
															  strcpy_s(task.szGuarder, sizeof(task.szGuarder),
																  doc["list"][i]["guarder"].GetString());
														  }

													  }
													  if (doc["list"][i].HasMember("factoryId")) {
														  if (doc["list"][i]["factoryId"].IsString()
															  && doc["list"][i]["factoryId"].GetStringLength()) {
															  strcpy_s(task.szFactoryId, sizeof(task.szFactoryId),
																  doc["list"][i]["factoryId"].GetString());
														  }
													  }
													  if (doc["list"][i].HasMember("deviceId")) {
														  if (doc["list"][i]["deviceId"].IsString()
															  && doc["list"][i]["deviceId"].GetStringLength()) {
															  strcpy_s(task.szDeviceId, sizeof(task.szDeviceId),
																  doc["list"][i]["deviceId"].GetString());
														  }
													  }
													  if (doc["list"][i].HasMember("orgId")) {
														  if (doc["list"][i]["orgId"].IsString()
															  && doc["list"][i]["orgId"].GetStringLength()) {
															  strcpy_s(task.szOrg, sizeof(task.szOrg),
																  doc["list"][i]["orgId"].GetString());
														  }
													  }
													  if (doc["list"][i].HasMember("taskStartTime")) {
														  if (doc["list"][i]["taskStartTime"].IsString()
															  && doc["list"][i]["taskStartTime"].GetStringLength()) {
															  strcpy_s(task.szTaskStartTime, sizeof(task.szTaskStartTime),
																  doc["list"][0]["taskStartTime"].GetString());
														  }
													  }
													  if (doc["list"][i].HasMember("target")) {
														  if (doc["list"][i]["target"].IsString()
															  && doc["list"][i]["target"].GetStringLength()) {
															  strcpy_s(task.szTarget, sizeof(task.szTarget),
																  doc["list"][i]["target"].GetString());
														  }
													  }
													  if (doc["list"][i].HasMember("destination")) {
														  if (doc["list"][i]["destination"].IsString()
															  && doc["list"][i]["destination"].GetStringLength()) {
															  strcpy_s(task.szDestination, sizeof(task.szDestination),
																  doc["list"][i]["destination"].GetString());
														  }
													  }
													  if (doc["list"][i].HasMember("handset")) {
														  if (doc["list"][i]["handset"].IsString()
															  && doc["list"][i]["handset"].GetStringLength()) {
															  strcpy_s(task.szTarget, sizeof(task.szTarget),
																  doc["list"][i]["handset"].GetString());
														  }
													  }
													  if (doc["list"][i].HasMember("taskMode")) {
														  if (doc["list"][i]["taskMode"].IsInt()) {
															  task.nTaskMode = doc["list"][i]["taskMode"].GetInt();
														  }
													  }
													  if (strlen(task.szTaskId) && strlen(task.szGuarder)
														  && strlen(task.szDeviceId) && strlen(task.szTaskStartTime)) {
														  EscortTask * pTask = new EscortTask();
														  memcpy_s(pTask, nTaskSize, &task, nTaskSize);
														  m_taskList.emplace((std::string)pTask->szTaskId, pTask);
														  sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]load task taskId=%s, "
															  "device=%s, gaurder=%s, org=%s, startTime=%s, type=%d, limit=%d, "
															  "target=%s, destination=%s, flee=%d, handset=%s\r\n", __FUNCTION__,
															  __LINE__, task.szTaskId, task.szDeviceId, task.szGuarder, task.szOrg,
															  task.szTaskStartTime, task.nTaskType, task.nTaskLimitDistance,
															  task.szTarget, task.szDestination, task.nTaskFlee, task.szHandset);
														  writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
													  }
													  else {
														  sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]load task data miss, "
															  "taskId=%s, device=%s, gaurder=%s, org=%s, startTime=%s, type=%d, limit=%d,"
															  " target=%s, destination=%s, flee=%d, handset=%s\r\n", __FUNCTION__,
															  __LINE__, task.szTaskId, task.szDeviceId, task.szGuarder, task.szOrg,
															  task.szTaskStartTime, task.nTaskType, task.nTaskLimitDistance,
															  task.szTarget, task.szDestination, task.nTaskFlee, task.szHandset);
														  writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
													  }
												  }
												  setLoadTaskList(true);
												  break;
											  }
											  default: {
												  sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]subGetInfo not used buffer "
													  "type for %d\r\n", __FUNCTION__, __LINE__, nGetType);
												  writeLog(szLog, pf_logger::eLOGCATEGORY_WARN, m_usLogType);
												  break;
											  }
											}
										}
									}
									break;
								}
								case MSG_SUB_SETINFO: {
									break;
								}
								default: {
									break;
								}
							}
						}
						else {
							sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]receive a invalid message",
								__FUNCTION__, __LINE__);
							writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
						}
					}
					else {
						sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]parse interaction message JSON "
							"error=%d\r\n", __FUNCTION__, __LINE__, doc.GetParseError());
						writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
					}
				}
				for (unsigned int i = 0; i < pInteractMsg->uiContentCount; ++i) {
					if (pInteractMsg->pMsgContents[i]) {
						delete[] pInteractMsg->pMsgContents[i];
						pInteractMsg->pMsgContents = NULL;
					}
				}
				delete[] pInteractMsg->pMsgContents;
				pInteractMsg->pMsgContents = NULL;
				pInteractMsg->uiContentCount = 0;
				pInteractMsg->uiContentLens = 0;
			}
			delete pInteractMsg;
			pInteractMsg = NULL;
		}
	} while (1);
}

void expand_protocol::ExpandProtocolService::dealNetwork()
{
	zmq_pollitem_t items[] = { {m_subscriber, 0, ZMQ_POLLIN, 0}, {m_interactor, 0, ZMQ_POLLIN, 0}};
	while (m_nRun) {
		int rc = zmq_poll(items, 2, 1000 * ZMQ_POLL_MSEC);
		if (rc == -1 && errno == ETERM) {
			break;
		}
		if (items[0].revents & ZMQ_POLLIN) {
			zmsg_t * subMsg = zmsg_recv(m_subscriber);
			if (subMsg) {
				zframe_t * frame_mark = zmsg_pop(subMsg);
				zframe_t * frame_seq = zmsg_pop(subMsg);
				zframe_t * frame_type = zmsg_pop(subMsg);
				zframe_t * frame_uuid = zmsg_pop(subMsg);
				zframe_t * frame_body = zmsg_pop(subMsg);
				zframe_t * frame_from = zmsg_pop(subMsg);
				char * strMark = zframe_strdup(frame_mark);
				char * strSeq = zframe_strdup(frame_seq);
				char * strType = zframe_strdup(frame_type);
				char * strUuid = zframe_strdup(frame_uuid);
				char * strBody = zframe_strdup(frame_body);
				char * strFrom = zframe_strdup(frame_from);
				TopicMessage * pTopicMsg = new TopicMessage();
				strcpy_s(pTopicMsg->szMsgMark, sizeof(pTopicMsg->szMsgMark), strMark);
				pTopicMsg->usMsgSequence = (unsigned short)atoi(strSeq);
				pTopicMsg->usMsgType = (unsigned short)atoi(strType);
				strcpy_s(pTopicMsg->szMsgUuid, sizeof(pTopicMsg->szMsgUuid), strUuid);
				strcpy_s(pTopicMsg->szMsgBody, sizeof(pTopicMsg->szMsgBody), strBody);
				strcpy_s(pTopicMsg->szMsgFrom, sizeof(pTopicMsg->szMsgFrom), strFrom);
				if (!addTopicMessage(pTopicMsg)) {
					delete pTopicMsg;
					pTopicMsg = NULL;
				}
				zframe_destroy(&frame_mark);
				zframe_destroy(&frame_seq);
				zframe_destroy(&frame_type);
				zframe_destroy(&frame_uuid);
				zframe_destroy(&frame_body);
				zframe_destroy(&frame_from);
				zmsg_destroy(&subMsg);
				free(strMark);
				free(strSeq);
				free(strType);
				free(strUuid);
				free(strBody);
				free(strFrom);
			}
		}
		if (items[1].revents & ZMQ_POLLIN) {
			zmsg_t * interactor_msg = zmsg_recv(m_interactor);
			if (interactor_msg) {
				size_t nCount = zmsg_size(interactor_msg);
				if (nCount) {
					zframe_t ** frame_replys = new zframe_t * [nCount];
					InteractionMessage * pMsg = new InteractionMessage();
					pMsg->uiContentCount = nCount;
					pMsg->uiContentLens = new unsigned int[nCount];
					pMsg->pMsgContents = new char *[nCount];
					for (size_t i = 0; i < nCount; i++) {
						frame_replys[i] = zmsg_pop(interactor_msg);
						pMsg->uiContentLens[i] = zframe_size(frame_replys[i]);
						pMsg->pMsgContents[i] = zframe_strdup(frame_replys[i]);
						zframe_destroy(&frame_replys[i]);
					}
					if (!addInteractMessage(pMsg)) {
						for (size_t i = 0; i < nCount; i++) {
							free (pMsg->pMsgContents[i]);
							pMsg->pMsgContents[i] = NULL;
						}
						delete[] pMsg->pMsgContents;
						pMsg->pMsgContents = NULL;
						delete[] pMsg->uiContentLens;
						pMsg->uiContentLens = NULL;
						delete pMsg;
						pMsg = NULL;
					}
					delete [] frame_replys;
					frame_replys = NULL;
				}
				zmsg_destroy(&interactor_msg);
			}
		}
	}
}

void expand_protocol::ExpandProtocolService::handleTaskStart(expand_protocol::RequestStartTask * pReqStartTask_,
	std::string strDest_)
{
	if (pReqStartTask_ && strlen(pReqStartTask_->szDeviceId) && strlen(pReqStartTask_->szGuarderId)) {
		std::string strGuarder = pReqStartTask_->szGuarderId;
		std::string strDevice = pReqStartTask_->szDeviceId;
		int result = escort_protocol::ERROR_NO;
		bool bValidGuarder = false;
		bool bGenerateTaskId = false;
		char szTaskId[16] = { 0 };
		char szExistDeviceId[20] = { 0 };
		char szFactoryId[4] = { 0 };
		char szOrg[40] = { 0 };
		char szExistGuarder[20] = { 0 };
		if (getLoadGuarderList()) {
			unsigned long ulReqTime = makeDatetime(pReqStartTask_->szRequestTime);
			unsigned long ulNow = (unsigned long)time(NULL);
			if (abs((double)ulNow - ulReqTime) <= 90) {
				pthread_mutex_lock(&m_mutex4GuarderList);
				expand_protocol::EscortGuarderList::iterator iter = m_guarderList.find(strGuarder);
				if (iter != m_guarderList.end()) {
					Guarder * pGuarder = iter->second;
					if (pGuarder) {
						if (pGuarder->usState == 0) {
							bValidGuarder = true;
						}
						else {
							if (strlen(pGuarder->szTaskId)) {
								strcpy_s(szTaskId, sizeof(szTaskId), pGuarder->szTaskId);
								strcpy_s(szExistDeviceId, sizeof(szExistDeviceId), pGuarder->szBindDevice);
								strcpy_s(szExistGuarder, sizeof(szExistGuarder), pGuarder->szId);
								result = escort_protocol::ERROR_GUARDER_ALREADY_ON_DUTY;
							}
							else {
								if (strlen(pGuarder->szBindDevice)) {
									if (strcmp(pGuarder->szBindDevice, pReqStartTask_->szDeviceId) != 0) {
										strcpy_s(szExistDeviceId, sizeof(szExistDeviceId), pGuarder->szBindDevice);
										result = escort_protocol::ERROR_GUARDER_ALREADY_BIND_DEVICE;
									}
									else {
										bValidGuarder = true;
									}
								}
								else {
									result = escort_protocol::ERROR_DEVICE_IS_INVALID;
								}
							}
						}
					}
				}
				else {
					result = escort_protocol::ERROR_GUARDER_IS_INVALID;
				}
				pthread_mutex_unlock(&m_mutex4GuarderList);
			}
			else {
				result = escort_protocol::ERROR_REQUEST_TIME_EXPIRED;
			}
		}
		else {
			result = escort_protocol::ERROR_GUARDER_LIST_NOT_LOAD;
		}
		if (bValidGuarder) {
			if (getLoadDeviceList()) {
				pthread_mutex_lock(&m_mutex4DeviceList);
				expand_protocol::EscortDeviceList::iterator iter = m_deviceList.find(strDevice);
				if (iter != m_deviceList.end()) {
					WristletDevice * pDevice = iter->second;
					if (pDevice) {
						if (pDevice->deviceBasic.nOnline == 0) {
							result = escort_protocol::ERROR_DEVICE_IS_OFFLINE;
						}
						else {
							if ((pDevice->deviceBasic.nStatus & DEV_GUARD) == DEV_GUARD
								|| (pDevice->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE) {
								if (strcmp(pDevice->szBindGuard, pReqStartTask_->szGuarderId) == 0) {
									result = escort_protocol::ERROR_GUARDER_ALREADY_ON_DUTY;
								}
								else {
									strcpy_s(szExistGuarder, sizeof(szExistGuarder), pDevice->szBindGuard);
									result = escort_protocol::ERROR_DEVICE_IS_OCCUPIED;
								}
							}
							else if ((pDevice->deviceBasic.nStatus & DEV_ONLINE) == DEV_ONLINE) {
								if (strlen(pDevice->szBindGuard)) {
									if (strcmp(pDevice->szBindGuard, pReqStartTask_->szGuarderId) == 0) {
										bGenerateTaskId = true;
									}
									else {
										result = escort_protocol::ERROR_DEVICE_IS_OCCUPIED;
										strcpy_s(szExistGuarder, sizeof(szExistGuarder), pDevice->szBindGuard);
									}
								}
								else {
									bGenerateTaskId = true;
								}
							}
						}
					}
				}
				else {
					result = escort_protocol::ERROR_DEVICE_IS_INVALID;
				}
				pthread_mutex_unlock(&m_mutex4DeviceList);
			}
			else {
				result = escort_protocol::ERROR_DEVICE_LIST_NOT_LOAD;
			}
		}
		if (bGenerateTaskId) {
			generateTaskId(szTaskId, sizeof(szTaskId));
			strcpy_s(szExistDeviceId, sizeof(szExistDeviceId), pReqStartTask_->szDeviceId);
			strcpy_s(szExistGuarder, sizeof(szExistGuarder), pReqStartTask_->szGuarderId);
		}
		//handle other work
		if (result == escort_protocol::ERROR_NO) {
			{
				pthread_mutex_lock(&m_mutex4GuarderList);
				expand_protocol::EscortGuarderList::iterator iter = m_guarderList.find(strGuarder);
				if (iter != m_guarderList.end()) {
					Guarder * pGuarder = iter->second;
					if (pGuarder) {
						pGuarder->usState = STATE_GUARDER_DUTY;
						strcpy_s(pGuarder->szBindDevice, sizeof(pGuarder->szBindDevice), pReqStartTask_->szDeviceId);
						strcpy_s(pGuarder->szTaskId, sizeof(pGuarder->szTaskId), szTaskId);
						strcpy_s(pGuarder->szTaskStartTime, sizeof(pGuarder->szTaskStartTime), pReqStartTask_->szRequestTime);
					}
				}
				pthread_mutex_unlock(&m_mutex4GuarderList);
			}
			{
				pthread_mutex_lock(&m_mutex4DeviceList);
				expand_protocol::EscortDeviceList::iterator iter = m_deviceList.find(strDevice);
				if (iter != m_deviceList.end()) {
					WristletDevice * pDevice = iter->second;
					if (pDevice) {
						strcpy_s(pDevice->szBindGuard, sizeof(pDevice->szBindGuard), pReqStartTask_->szGuarderId);
						pDevice->ulBindTime = makeDatetime(pReqStartTask_->szRequestTime);
						pDevice->deviceBasic.nStatus = pDevice->deviceBasic.nStatus - DEV_ONLINE + DEV_GUARD;
						strcpy_s(szFactoryId, sizeof(szFactoryId), pDevice->deviceBasic.szFactoryId);
						strcpy_s(szOrg, sizeof(szOrg), pDevice->szOrganization);
					}
				}
				pthread_mutex_unlock(&m_mutex4DeviceList);
			}
			{
				EscortTask * pTask = new EscortTask();
				memset(pTask, 0, sizeof(EscortTask));
				strcpy_s(pTask->szTaskId, sizeof(pTask->szTaskId), szTaskId);
				strcpy_s(pTask->szDeviceId, sizeof(pTask->szDeviceId), pReqStartTask_->szDeviceId);
				strcpy_s(pTask->szFactoryId, sizeof(pTask->szFactoryId), szFactoryId);
				strcpy_s(pTask->szGuarder, sizeof(pTask->szGuarder), pReqStartTask_->szGuarderId);
				strcpy_s(pTask->szOrg, sizeof(pTask->szOrg), szOrg);
				strcpy_s(pTask->szTarget, sizeof(pTask->szTarget), pReqStartTask_->szTarget);
				strcpy_s(pTask->szDestination, sizeof(pTask->szDestination), pReqStartTask_->szDestination);
				strcpy_s(pTask->szTaskStartTime, sizeof(pTask->szTaskStartTime), pReqStartTask_->szRequestTime);
				pTask->nTaskLimitDistance = (uint8_t)pReqStartTask_->usDistanceLimit;
				pTask->nTaskType = (uint8_t)pReqStartTask_->usTaskType;
				pthread_mutex_lock(&m_mutex4TaskList);
				m_taskList.emplace(std::string(szTaskId), pTask);
				pthread_mutex_unlock(&m_mutex4TaskList);
			}
			char szBindMsg[512] = { 0 };
			sprintf_s(szBindMsg, sizeof(szBindMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%u,"
				"\"datetime\":\"%s\",\"report\":[{\"subType\":%d,\"factoryId\":\"%s\",\"deviceId\":\"%s\","
				"\"guarder\":\"%s\"}]}", MSG_SUB_REPORT, getNextInteractSequence(), pReqStartTask_->szRequestTime,
				SUB_REPORT_DEVICE_BIND, szFactoryId, pReqStartTask_->szDeviceId, pReqStartTask_->szGuarderId);
			sendInteractionMessage(szBindMsg, strlen(szBindMsg));
			char szTaskMsg[512] = { 0 };
			sprintf_s(szTaskMsg, sizeof(szTaskMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%u,"
				"\"datetime\":\"%s\",\"report\":[{\"subType\":%d,\"taskId\":\"%s\",\"taskType\":%hu,\"limit\":%hu,"
				"\"factoryId\":\"%s\",\"deviceId\":\"%s\",\"guarder\":\"%s\",\"destination\":\"%s\",\"target\":\"%s\","
				"\"handset\":\"\"}]}", MSG_SUB_REPORT, getNextInteractSequence(), pReqStartTask_->szRequestTime,
				SUB_REPORT_TASK, szTaskId, pReqStartTask_->usTaskType, pReqStartTask_->usDistanceLimit, szFactoryId,
				pReqStartTask_->szDeviceId, pReqStartTask_->szGuarderId, pReqStartTask_->szDestination,
				pReqStartTask_->szTarget);
			sendInteractionMessage(szTaskMsg, strlen(szTaskMsg));
		}

		char szReply[512] = { 0 };
		std::string strDestURI = strDest_;
		if (strDestURI.empty()) {
			strDestURI = DEFAULT_REPLY_DEST;
		}
		sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%hu,\"retCode\":%d,\"seq\":%hu,\"taskId\":\"%s\","
			"\"deviceId\":\"%s\",\"guarder\":\"%s\",\"reqTime\":\"%s\"}", escort_protocol::CMD_START_TASK_REP,
			result, pReqStartTask_->usReqSeq, szTaskId, szExistDeviceId, szExistGuarder,
			pReqStartTask_->szRequestTime);
		std::string strReplyUtf = AnsiToUtf8(szReply);

    //m_pRepMsgProducer->send("test_reply", strReplyUtf, false);

		size_t nLen = strReplyUtf.size();
		unsigned char * pReplyData = new unsigned char[nLen + 1];
		memcpy_s(pReplyData, nLen + 1, strReplyUtf.c_str(), nLen);
		pReplyData[nLen] = '\0';
		encryptMessage(pReplyData, 0, nLen, kRepSafeKey);
		m_pRepMsgProducer->send(strDestURI, pReplyData, nLen, false);
		char szLog[512] = { 0 };
		sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]deviceId=%s, guarder=%s, target=%s, destination=%s, "
			"reqSeq=%hu, reqDatetime=%s, taskType=%hu, taskLimit=%hu, taskId=%s, retcode=%d\r\n", __FUNCTION__,
			__LINE__, pReqStartTask_->szDeviceId, pReqStartTask_->szGuarderId, pReqStartTask_->szTarget,
			pReqStartTask_->szDestination, pReqStartTask_->usReqSeq, pReqStartTask_->szRequestTime, 
			pReqStartTask_->usTaskType, pReqStartTask_->usDistanceLimit, szTaskId, result);
		writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		delete[] pReplyData;
		pReplyData = NULL;
	}
}

void expand_protocol::ExpandProtocolService::handleTaskStop(expand_protocol::RequestStopTask * pReqStopTask_,
	std::string strDest_)
{
	if (pReqStopTask_ && strlen(pReqStopTask_->szTaskId)) {
		std::string strTaskId = pReqStopTask_->szTaskId;
		char szDeviceId[20] = { 0 };
		char szGuarderId[20] = { 0 };
		char szFactoryId[4] = { 0 };
		int result;
		unsigned long ulReqTime = makeDatetime(pReqStopTask_->szRequestTime);
		unsigned long ulNow = (unsigned long)time(NULL);
		if (abs((double)ulNow - ulReqTime) <= 90.0) {
			pthread_mutex_lock(&m_mutex4TaskList);
			if (!m_taskList.empty()) {
				expand_protocol::EscortTaskList::iterator iter = m_taskList.find(strTaskId);
				if (iter != m_taskList.end()) {
					EscortTask * pTask = iter->second;
					if (pTask) {
						if (pTask->nTaskFlee == 1) {
							result = escort_protocol::ERROR_TASK_IN_WARN_SITUATION;
						}
						else {
							result = escort_protocol::ERROR_NO;
							strcpy_s(szDeviceId, sizeof(szDeviceId), pTask->szDeviceId);
							strcpy_s(szFactoryId, sizeof(szFactoryId), pTask->szFactoryId);
							strcpy_s(szGuarderId, sizeof(szGuarderId), pTask->szGuarder);
							delete pTask;
							pTask = NULL;
							m_taskList.erase(iter);
						}
					}
				}
				else {
					result = escort_protocol::ERROR_TASK_NOT_EXISTS;
				}
			}
			else {
				result = escort_protocol::ERROR_TASK_NOT_EXISTS;
			}
			pthread_mutex_unlock(&m_mutex4TaskList);
		}
		else {
			result = escort_protocol::ERROR_REQUEST_TIME_EXPIRED;
		}
		if (strlen(szGuarderId)) {
			std::string strGuarder = szGuarderId;
			pthread_mutex_lock(&m_mutex4GuarderList);
			expand_protocol::EscortGuarderList::iterator iter = m_guarderList.find(strGuarder);
			if (iter != m_guarderList.end()) {
				Guarder * pGuarder = iter->second;
				if (pGuarder) {
					pGuarder->usState = STATE_GUARDER_FREE;
					memset(pGuarder->szTaskId, 0, sizeof(pGuarder->szTaskId));
					memset(pGuarder->szTaskStartTime, 0, sizeof(pGuarder->szTaskStartTime));
					memset(pGuarder->szBindDevice, 0, sizeof(pGuarder->szBindDevice));
				}
			}
			pthread_mutex_unlock(&m_mutex4GuarderList);
		}
		if (strlen(szDeviceId)) {
			std::string strDeviceId = szDeviceId;
			pthread_mutex_lock(&m_mutex4DeviceList);
			expand_protocol::EscortDeviceList::iterator iter = m_deviceList.find(strDeviceId);
			if (iter != m_deviceList.end()) {
				WristletDevice * pDevice = iter->second;
				if (pDevice) {
					memset(pDevice->szBindGuard, 0, sizeof(pDevice->szBindGuard));
					pDevice->ulBindTime = 0;
					pDevice->deviceBasic.nStatus = DEV_ONLINE;
					if (pDevice->deviceBasic.nBattery < BATTERY_THRESHOLD) {
						pDevice->deviceBasic.nStatus += DEV_LOWPOWER;
					}
					if (pDevice->deviceBasic.nLooseStatus == 1) {
						pDevice->deviceBasic.nStatus += DEV_LOOSE;
					}
				}
			}
			pthread_mutex_unlock(&m_mutex4DeviceList);
		}
		if (result == escort_protocol::ERROR_NO) {
			char szTaskMsg[512] = { 0 };
			sprintf_s(szTaskMsg, sizeof(szTaskMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,"
				"\"sequence\":%u,\"datetime\":\"%s\",\"report\":[{\"subType\":%d,\"taskId\":\"%s\","
				"\"closeType\":0}]}", MSG_SUB_REPORT, getNextInteractSequence(), pReqStopTask_->szRequestTime,
				SUB_REPORT_TASK_CLOSE, pReqStopTask_->szTaskId);
			sendInteractionMessage(szTaskMsg, strlen(szTaskMsg));
			char szUnbindMsg[512] = { 0 };
			sprintf_s(szUnbindMsg, sizeof(szUnbindMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,"
				"\"sequence\":%u,\"datetime\":\"%s\",\"report\":[{\"subType\":%d,\"factoryId\":\"%s\","
				"\"deviceId\":\"%s\",\"guarder\":\"%s\"}]}", MSG_SUB_REPORT, getNextInteractSequence(),
				pReqStopTask_->szRequestTime, SUB_REPORT_DEVICE_UNBIND, szFactoryId, szDeviceId, szGuarderId);
			sendInteractionMessage(szUnbindMsg, strlen(szUnbindMsg));
		}

		char szReply[512] = { 0 };
		std::string strDestURI = strDest_;
		if (strDestURI.empty()) {
			strDestURI = DEFAULT_REPLY_DEST;
		}
		sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retCode\":%d,\"seq\":%u,\"reqTime\":\"%s\","
			"\"taskId\":\"%s\",\"guarder\":\"%s\",\"deviceId\":\"%s\"}", escort_protocol::CMD_STOP_TASK_REP,
			result, pReqStopTask_->usReqSeq, pReqStopTask_->szRequestTime, pReqStopTask_->szTaskId,
			szGuarderId, szDeviceId);
		std::string strReplyUtf = AnsiToUtf8(szReply);
		size_t nLen = strReplyUtf.size();
		unsigned char * pReplyData = new unsigned char[nLen + 1];
		memcpy_s(pReplyData, nLen + 1, strReplyUtf.c_str(), nLen);
		pReplyData[nLen] = '\0';
		encryptMessage(pReplyData, 0, nLen, kRepSafeKey);
		m_pRepMsgProducer->send(strDestURI, pReplyData, nLen, false);
		char szLog[512] = { 0 };
		sprintf_s(szLog, sizeof(szLog), "[Broker]%s[%d]taskId=%s, deviceId=%s, guarderId=%s, datetime=%s, "
			"seq=%hu\r\n", __FUNCTION__, __LINE__, pReqStopTask_->szTaskId, szDeviceId, szGuarderId,
			pReqStopTask_->szRequestTime, pReqStopTask_->usReqSeq);
		writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		delete[] pReplyData;
		pReplyData = NULL;
	}
}

void expand_protocol::ExpandProtocolService::loadOrgList()
{
	char szDatetime[20] = { 0 };
	formatDatetime((unsigned long)time(NULL), szDatetime, sizeof(szDatetime));
	char szOrgMsg[512] = { 0 };
	sprintf_s(szOrgMsg, sizeof(szOrgMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%u,"
		"\"datetime\":\"%s\",\"getType\":%d,\"param1\":\"%s\",\"param2\":\"%s\",\"param3\":\"%d\"}",
		MSG_SUB_GETINFO, getNextInteractSequence(), szDatetime, BUFFER_ORG, "", "", 0);
	sendInteractionMessage(szOrgMsg, strlen(szOrgMsg));
}

void expand_protocol::ExpandProtocolService::loadDeviceList()
{
	char szDatetime[20] = { 0 };
	formatDatetime((unsigned long)time(NULL), szDatetime, sizeof(szDatetime));
	char szOrgMsg[512] = { 0 };
	sprintf_s(szOrgMsg, sizeof(szOrgMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%u,"
		"\"datetime\":\"%s\",\"getType\":%d,\"param1\":\"%s\",\"param2\":\"%s\",\"param3\":\"%d\"}",
		MSG_SUB_GETINFO, getNextInteractSequence(), szDatetime, BUFFER_DEVICE, "", "", 0);
	sendInteractionMessage(szOrgMsg, strlen(szOrgMsg));
}

void expand_protocol::ExpandProtocolService::loadGuarderList()
{
	char szDatetime[20] = { 0 };
	formatDatetime((unsigned long)time(NULL), szDatetime, sizeof(szDatetime));
	char szOrgMsg[512] = { 0 };
	sprintf_s(szOrgMsg, sizeof(szOrgMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%u,"
		"\"datetime\":\"%s\",\"getType\":%d,\"param1\":\"%s\",\"param2\":\"%s\",\"param3\":\"%d\"}",
		MSG_SUB_GETINFO, getNextInteractSequence(), szDatetime, BUFFER_GUARDER, "", "", 0);
	sendInteractionMessage(szOrgMsg, strlen(szOrgMsg));
}

void expand_protocol::ExpandProtocolService::loadTaskList()
{
	char szDatetime[20] = { 0 };
	formatDatetime((unsigned long)time(NULL), szDatetime, sizeof(szDatetime));
	char szOrgMsg[512] = { 0 };
	sprintf_s(szOrgMsg, sizeof(szOrgMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%u,"
		"\"datetime\":\"%s\",\"getType\":%d,\"param1\":\"%s\",\"param2\":\"%s\",\"param3\":\"%d\"}",
		MSG_SUB_GETINFO, getNextInteractSequence(), szDatetime, BUFFER_TASK, "", "", 0);
	sendInteractionMessage(szOrgMsg, strlen(szOrgMsg));
}

bool expand_protocol::ExpandProtocolService::getLoadOrgList()
{
	bool result = false;
	pthread_mutex_lock(&m_mutex4LoadOrgList);
	result = m_bLoadOrgList;
	pthread_mutex_unlock(&m_mutex4LoadOrgList);
	return result;
}

void expand_protocol::ExpandProtocolService::setLoadOrgList(bool bVal_)
{
	pthread_mutex_lock(&m_mutex4LoadOrgList);
	m_bLoadOrgList = bVal_;
	pthread_mutex_unlock(&m_mutex4LoadOrgList);
}

bool expand_protocol::ExpandProtocolService::getLoadGuarderList()
{
	bool result = false;
	pthread_mutex_lock(&m_mutex4LoadGuarderList);
	result = m_bLoadGuarderList;
	pthread_mutex_unlock(&m_mutex4LoadGuarderList);
	return result;
}

void expand_protocol::ExpandProtocolService::setLoadGuarderList(bool bVal_)
{
	pthread_mutex_lock(&m_mutex4LoadGuarderList);
	m_bLoadGuarderList = bVal_;
	pthread_mutex_unlock(&m_mutex4LoadGuarderList);
}

bool expand_protocol::ExpandProtocolService::getLoadDeviceList()
{
	bool result = false;
	pthread_mutex_lock(&m_mutex4DeviceList);
	result = m_bLoadDeviceList;
	pthread_mutex_unlock(&m_mutex4DeviceList);
	return result;
}

void expand_protocol::ExpandProtocolService::setLoadDeviceList(bool bVal_)
{
	pthread_mutex_lock(&m_mutex4DeviceList);
	m_bLoadDeviceList = bVal_;
	pthread_mutex_unlock(&m_mutex4DeviceList);
}

bool expand_protocol::ExpandProtocolService::getLoadTaskList()
{
	bool result = false;
	pthread_mutex_lock(&m_mutex4TaskList);
	result = m_bLoadTaskList;
	pthread_mutex_unlock(&m_mutex4TaskList);
	return result;
}

void expand_protocol::ExpandProtocolService::setLoadTaskList(bool bVal_)
{
	pthread_mutex_lock(&m_mutex4LoadTaskList);
	m_bLoadTaskList = bVal_;
	pthread_mutex_unlock(&m_mutex4LoadTaskList);
}

void expand_protocol::ExpandProtocolService::clearOrgList()
{
	pthread_mutex_lock(&m_mutex4OrgList);
	if (!m_orgList.empty()) {
		expand_protocol::EscortOrganizationList::iterator iter = m_orgList.begin();
		while (iter != m_orgList.end()) {
			OrganizationEx * pOrg = iter->second;
			if (pOrg) {
				delete pOrg;
				pOrg = NULL;
			}
			iter = m_orgList.erase(iter);
		}
	}
	pthread_mutex_unlock(&m_mutex4OrgList);
}

void expand_protocol::ExpandProtocolService::clearGuarderList()
{
	pthread_mutex_lock(&m_mutex4GuarderList);
	if (!m_guarderList.empty()) {
		expand_protocol::EscortGuarderList::iterator iter = m_guarderList.begin();
		while (iter != m_guarderList.end()) {
			Guarder * pGuarder = iter->second;
			if (pGuarder) {
				delete pGuarder;
				pGuarder = NULL;
			}
			iter = m_guarderList.erase(iter);
		}
	}
	pthread_mutex_unlock(&m_mutex4GuarderList);
}

void expand_protocol::ExpandProtocolService::clearDeviceList()
{
	pthread_mutex_lock(&m_mutex4DeviceList);
	if (!m_deviceList.empty()) {
		expand_protocol::EscortDeviceList::iterator iter = m_deviceList.begin();
		while (iter != m_deviceList.end()) {
			WristletDevice * pDevice = iter->second;
			if (pDevice) {
				delete pDevice;
				pDevice = NULL;
			}
			iter = m_deviceList.erase(iter);
		}
	}
	pthread_mutex_unlock(&m_mutex4DeviceList);
}

void expand_protocol::ExpandProtocolService::clearTaskList()
{
	pthread_mutex_lock(&m_mutex4TaskList);
	if (!m_taskList.empty()) {
		expand_protocol::EscortTaskList::iterator iter = m_taskList.begin();
		while (iter != m_taskList.end()) {
			EscortTask * pTask = iter->second;
			if (pTask) {
				delete pTask;
				pTask = NULL;
			}
			iter = m_taskList.erase(iter);
		}
	}
	pthread_mutex_unlock(&m_mutex4TaskList);
}

void expand_protocol::ExpandProtocolService::reloadOrgList(bool bFlag_)
{
	pthread_mutex_lock(&m_mutex4OrgList);
	if (!m_orgList.empty()) {
		std::map<std::string, OrganizationEx *>::iterator iter = m_orgList.begin();
		std::map<std::string, OrganizationEx *>::iterator iter_end = m_orgList.end();
		if (bFlag_) {
			for (; iter != iter_end; iter++) {
				OrganizationEx * pOrg = iter->second;
				if (pOrg) {
					pOrg->childList.clear();
				}
			}
			iter = m_orgList.begin();
		}
		for (; iter != iter_end; iter++) {
			OrganizationEx * pOrg = iter->second;
			if (pOrg) {
				std::string strOrgId = pOrg->org.szOrgId;
				if (strlen(pOrg->org.szParentOrgId)) {
					std::string strParentOrgId = pOrg->org.szParentOrgId;
					std::map<std::string, OrganizationEx *>::iterator it = m_orgList.find(strParentOrgId);
					if (it != iter_end) {
						OrganizationEx * pParentOrg = it->second;
						if (pParentOrg) {
							if (pParentOrg->childList.count(strOrgId) == 0) {
								pParentOrg->childList.emplace(strOrgId);
							}
						}
					}
				}
			}
		}
	}
	pthread_mutex_unlock(&m_mutex4OrgList);
}

void expand_protocol::ExpandProtocolService::findOrgChild(std::string strOrgId_,
	std::set<std::string> & childList_)
{
	std::map<std::string, OrganizationEx *>::iterator iter = m_orgList.find(strOrgId_);
	if (iter != m_orgList.end()) {
		OrganizationEx * pOrg = iter->second;
		if (pOrg) {
			if (!pOrg->childList.empty()) {
				std::set<std::string>::iterator it = pOrg->childList.begin();
				std::set<std::string>::iterator it_end = pOrg->childList.end();
				for (; it != it_end; ++it) {
					std::string strChildOrgId = *it;
					if (childList_.count(strChildOrgId) == 0) {
						childList_.emplace(strChildOrgId);
					}
					findOrgChild(strChildOrgId, childList_);
				}
			}
		}
	}
}

unsigned int expand_protocol::ExpandProtocolService::getNextInteractSequence()
{
	unsigned int result = 0;
	pthread_mutex_lock(&m_mutex4InteractSequence);
	result = m_uiInteractSequence++;
	pthread_mutex_unlock(&m_mutex4InteractSequence);
	return result;
}

void expand_protocol::ExpandProtocolService::sendInteractionMessage(const char * pMsgData_,
	unsigned int uiMsgDataLen_)
{
	if (pMsgData_ && uiMsgDataLen_) {
		unsigned char * pFrameData = new unsigned char[uiMsgDataLen_ + 1];
		memcpy_s(pFrameData, uiMsgDataLen_ + 1, pMsgData_, uiMsgDataLen_);
		pFrameData[uiMsgDataLen_] = '\0';
		zmsg_t * msg = zmsg_new();
		zmsg_addmem(msg, pFrameData, uiMsgDataLen_);
		zmsg_send(&msg, m_interactor);
		delete[] pFrameData;
		pFrameData = NULL;
	}
}

void expand_protocol::ExpandProtocolService::formatDatetime(unsigned long ulDateTime_,
	char * pDatetime_, unsigned int uiLen_)
{
	tm tm_time;
	time_t srcTime = ulDateTime_;
	localtime_s(&tm_time, &srcTime);
	char szDatetime[20] = { 0 };
	sprintf_s(szDatetime, sizeof(szDatetime), "%04d%02d%02d%02d%02d%02d", tm_time.tm_year + 1900,
		tm_time.tm_mon + 1, tm_time.tm_mday, tm_time.tm_hour, tm_time.tm_min, tm_time.tm_sec);
	size_t nLen = strlen(szDatetime);
	if (pDatetime_ && uiLen_ >= nLen) {
		strcpy_s(pDatetime_, uiLen_, szDatetime);
	}
}

unsigned long expand_protocol::ExpandProtocolService::makeDatetime(const char * pStrDatetime_)
{
	if (pStrDatetime_) {
		struct tm tm_curr;
		sscanf_s(pStrDatetime_, "%04d%02d%02d%02d%02d%02d", &tm_curr.tm_year, &tm_curr.tm_mon,
			&tm_curr.tm_mday, &tm_curr.tm_hour, &tm_curr.tm_min, &tm_curr.tm_sec);
		tm_curr.tm_year -= 1900;
		tm_curr.tm_mon -= 1;
		return (unsigned long)mktime(&tm_curr);
	}
	return 0;
}

void expand_protocol::ExpandProtocolService::generateTaskId(char * pTaskId_, unsigned int uiLen_)
{
	pthread_mutex_lock(&m_mutex4TaskId);
	char szTaskId[16] = { 0 };
	sprintf_s(szTaskId, sizeof(szTaskId), "%x%04x", (unsigned int)time(NULL), (rand() % 10000));
	pthread_mutex_unlock(&m_mutex4TaskId);
	if (pTaskId_ && uiLen_ >= strlen(szTaskId)) {
		strcpy_s(pTaskId_, uiLen_, szTaskId);
	}
}

void expand_protocol::consumerMsgCb(std::string strDest_, unsigned char * pMsgData_, unsigned int uiDataLen_,
	void * pUserData_)
{
	expand_protocol::ExpandProtocolService * pService = (expand_protocol::ExpandProtocolService *)pUserData_;
	if (pService) {
		expand_protocol::ConsumerMessage  * pConsumerMsg = new expand_protocol::ConsumerMessage();
		pConsumerMsg->strDestURI = strDest_;
		pConsumerMsg->pData = new unsigned char[uiDataLen_ + 1];
		memcpy_s(pConsumerMsg->pData, uiDataLen_ + 1, pMsgData_, uiDataLen_);
		pConsumerMsg->pData[uiDataLen_] = '\0';
		pConsumerMsg->uiDataLen = uiDataLen_;
		if (!pService->addConsumerMessage(pConsumerMsg)) {
			if (pConsumerMsg) {
				if (pConsumerMsg->pData) {
					delete[] pConsumerMsg->pData;
					pConsumerMsg->pData = NULL;
				}
				delete pConsumerMsg;
				pConsumerMsg = NULL;
			}
		}
	}
}

void * expand_protocol::startDealConsumerMessageThread(void * param_)
{
	expand_protocol::ExpandProtocolService * pService = (expand_protocol::ExpandProtocolService *)param_;
	if (pService) {
		pService->dealConsumerMessage();
	}
	pthread_exit(NULL);
	return NULL;
}

void * expand_protocol::startDealInteractMessageThread(void * param_)
{
	expand_protocol::ExpandProtocolService * pService = (expand_protocol::ExpandProtocolService *)param_;
	if (pService) {
		pService->dealInteractMessage();
	}
	pthread_exit(NULL);
	return NULL;
}

void * expand_protocol::startDealTopicMessageThread(void * param_)
{
	expand_protocol::ExpandProtocolService * pService = (expand_protocol::ExpandProtocolService *)param_;
	if (pService) {
		pService->dealTopicMessage();
	}
	pthread_exit(NULL);
	return NULL;
}

void * expand_protocol::startDealLogThread(void * param_)
{
	expand_protocol::ExpandProtocolService * pService = (expand_protocol::ExpandProtocolService *)param_;
	if (pService) {
		pService->dealLog();
	}
	pthread_exit(NULL);
	return NULL;
}

void * expand_protocol::startNetworkThread(void * param_)
{
	expand_protocol::ExpandProtocolService * pService = (expand_protocol::ExpandProtocolService *)param_;
	if (pService) {
		pService->dealNetwork();
	}
	pthread_exit(NULL);
	return NULL;
}

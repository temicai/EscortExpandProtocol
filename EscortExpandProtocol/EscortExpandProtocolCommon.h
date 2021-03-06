#ifndef ESCORT_EXPAND_PROTOCOL_COMMON_H
#define ESCORT_EXPAND_PROTOCOL_COMMON_H

#define DEFAULT_LISTEN_DEST "ES_EXPAND_REQUEST"
#define DEFAULT_REPLY_DEST "ES_EXPAND_REPLY"
#define DEFAULT_PUBLISH_DEST "ES_PUBLISH"

const unsigned short kReqSafeKey = 0xea;
const unsigned short kRepSafeKey = 0xeb;
const unsigned short kPubSafeKey = 0xec;

namespace escort_protocol
{
	enum eCommandType {
		CMD_START_TASK_REQ = 1, 
		CMD_STOP_TASK_REQ = 2,
		CMD_PUB_DEVICE_ONLINE = 100,
		CMD_PUB_DEVICE_OFFLINE = 101,
		CMD_PUB_DEVICE_LOCATE = 110, 
		CMD_PUB_DEVICE_ALARM = 111,
		CMD_PUB_TASK_START = 120,
		CMD_PUB_TASK_STOP = 121,
		CMD_START_TASK_REP = 201,
		CMD_STOP_TASK_REP = 202, 
	};

	enum eErrorCode
	{
		ERROR_NO = 0,
		ERROR_DEVICE_IS_OCCUPIED = 1,
		ERROR_DEVICE_IS_INVALID = 2,
		ERROR_DEVICE_IS_OFFLINE = 3,
		ERROR_DEVICE_LIST_NOT_LOAD = 10,
		ERROR_GUARDER_IS_DUTY = 11,
		ERROR_GUARDER_IS_INVALID = 12,
		ERROR_GUARDER_ALREADY_BIND_DEVICE = 13,
		ERROR_GUARDER_ALREADY_ON_DUTY = 14,
		ERROR_GUARDER_LIST_NOT_LOAD = 20,
		ERROR_TASK_IN_WARN_SITUATION = 21,
		ERROR_TASK_NOT_EXISTS = 22,
    ERROR_REQUEST_TIME_EXPIRED = 99,
	};

	enum eTaskType
	{
		TASK_TYPE_ESCORT = 0,
		TASK_TYPE_MEIDICAL = 1,
		TASK_TYPE_IDENTITY = 2,
	};

  enum eAlarmType
  {
    E_ALARM_TYPE_DEVICE_LOWPOWER = 1,		//低电量告警
    E_ALARM_TYPE_DEVICE_FLEE = 3,				//设备脱管告警
    E_ALARM_TYPE_DEVICE_FENCE = 4,				//设备围栏告警
    E_ALARM_TYPE_DEVICE_LOOSE = 5,				//设备宽松
  };

  enum eLocateType
  {
    E_LOCATE_GPS = 1,
    E_LOCATE_LBS = 2,
    E_LOCATE_APP = 3,
  };


}


#endif
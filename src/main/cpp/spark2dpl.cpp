// spark2dpl.cpp : Defines the entry point for the console application.
//
#include "stdafx.h"
using namespace std;

#define DATE_FORMAT "YYYY-MM-DD HH24:MI:SS"
#define TIMESTAMP_FORMAT "YYYY-MM-DD HH24:MI:SS.FF3"
#define LOAD_UNIT 10000

//**********************************************************************************
// Table Attribute Define Class
//**********************************************************************************
class TableAttr
{
public:
	text	*tableOwner;		// table owner(nearly: Connect User)
	text	*tableName;			// table name
	ub2		columnCnt;			// count of columns on the table 
	text	*defaultDateMask;	// default date format
	ub1		nolog;				// specified nolog
	ub4		transferSize;		// 
	ub2		recordSize;			//
	ub1		parallel;			//
	ub2		skipIndex;			//
	ub2		inputData;			//
	public:TableAttr()
	{
		tableOwner		= nullptr;
		tableName		= nullptr;
		columnCnt		= (ub2)0;
		defaultDateMask = (text*)DATE_FORMAT;
		nolog			= (ub1)1;
		transferSize	= (ub4)1024*256;
		recordSize		= (ub2)0;
		parallel		= (ub1)1;
		skipIndex		= (ub2)OCI_DIRPATH_INDEX_MAINT_SKIP_ALL;
		inputData		= (ub2)OCI_DIRPATH_INPUT_TEXT;
	}
};
//**********************************************************************************
// Column Attribute Define Class
//**********************************************************************************
class ColumnAttr
{
public:
	char	columnName[30 + 1];	// (from StructField.name)
	char	columnType[1+1];	// (from user_tab_columns.data_type)
	ub4		columnPos;			// column position
	ub4		columnSize;			// (from user_tab_columns.data_length)
	ub4		externalType;		// (input data type)
	OraText	*dateFormat;		// (date/timestamp format)
	ColumnAttr()
	{
		memset(columnName, '\0', sizeof(columnName));
		memset(columnType, '\0', sizeof(columnType));
		columnSize = 0;
		columnPos = 0;
		externalType = SQLT_CHR;
		dateFormat = NULL;
	}
};

//**********************************************************************************
// Oracle user_tab_columns Info Class
//**********************************************************************************
class UserTabColumns
{
public:
	char	columnName[30 + 1];	// user_tab_columns.column_name
	char	columnType[1  + 1];	// user_tab_columns.data_type
	ub4		columnSize;			// user_tab_columns.data_length
	UserTabColumns()
	{
		memset(columnName, '\0', sizeof(columnName));
		memset(columnType, '\0', sizeof(columnType));
		columnSize = 0;
	}
};

//**********************************************************************************
// main class
//**********************************************************************************
class Spark2dpl
{
public:
		ifstream			*infile;
private:
		//----------------------------------------
		// JNI variables
		//----------------------------------------
	JNIEnv				*jnienv;
		jobject				jobjThis;
		jobject				jobjIter;
		jobject				jobjStrTyp;
		jobject				jobjProp;
		jobject				jobjLog4j;
		jmethodID			jmtidInfo;
		jmethodID			jmtidErr;
		jmethodID			jmtidIterNext;
		jmethodID			jmtidIterHasNext;
		jmethodID			jmtidRowGet;
		jmethodID			jmtidToString;
		//----------------------------------------
		// OCI Handles
		//----------------------------------------
		OCIEnv				*envhp;
		OCIError			*errhp;
		OCISvcCtx			*svchp;
		OCIStmt				*stmtp;
		OCIDirPathCtx		*dpchp;
		OCIDirPathColArray	*dpcap;
		OCIDirPathStream	*dpstp;
		OCIParam			*colLstDesc;
		//----------------------------------------
		// Inner Define
		//----------------------------------------
		TableAttr			*tblAttr;
		ColumnAttr			*colAttr;
		int					dirPathColCnt;
		int					dirPathRowCnt;
		ub4					allocSize;
		char				*records;
		//----------------------------------------
		// Oracle DataBase Environment
		//----------------------------------------
		char				username[30 + 1];
		char				password[30 + 1];
		char				dbname[30 + 1];
		char				tablename[30 + 1];
	/************************************************************
	 * Constructor
	 ***********************************************************/
	private:Spark2dpl() {}
	public:Spark2dpl
	(
		JNIEnv	*envj,
		jobject	thisj,
		jobject	iterj,
		jobject	sttpj,
		jobject	propj,
		jobject	log4j
	)
	{
//		cout << "*** Spark2dpl constructor start" << endl;
		infile = nullptr;
		jnienv = envj;
		jobjThis = thisj;
		jobjIter = iterj;
		jobjStrTyp = sttpj;
		jobjProp = propj;
		jobjLog4j = log4j;

		jmtidInfo = nullptr;
		jmtidErr = nullptr;
		jmtidIterNext = nullptr;
		jmtidIterHasNext = nullptr;
		jmtidRowGet = nullptr;
		jmtidToString =nullptr;

		envhp = nullptr;
		errhp = nullptr;
		svchp = nullptr;
		stmtp = nullptr;
		dpchp = nullptr;
		dpcap = nullptr;
		dpstp = nullptr;
		colLstDesc = nullptr;

		tblAttr = new TableAttr();
		colAttr = nullptr;
		dirPathColCnt = 0;
		dirPathRowCnt = 0;
		records = nullptr;

		memset(username, '\0', sizeof(username));
		memset(password, '\0', sizeof(password));
		memset(dbname,   '\0', sizeof(dbname));
		memset(tablename,'\0', sizeof(tablename));
	}
	/************************************************************
	 * Destructor
	 ***********************************************************/
	public:~Spark2dpl()
	{
		//----------------------------------------
		// Cleanup Resources
		//----------------------------------------
		sword status = OCI_SUCCESS;
		// Cleanup Input Record Array 
		if (this->records)
		{
			//cout << "Cleanup Input Record Array" << endl;
			delete [] this->records;
		}
		// Cleanup Table Define Attributes
		if (this->tblAttr)
		{
			//cout << "Cleanup Table Define Attributes" << endl;
			delete this->tblAttr;
		}
		// Cleanup Column Define Attributes
		if (this->colAttr)
		{
			//cout << "Cleanup Column Define Attributes" << endl;
			delete [] this->colAttr;
		}
		// Cleanup Stmt Handle
		if (this->stmtp)
		{
			//cout << "Cleanup Stmt Handle" << endl;
			if (status = OCIHandleFree(this->stmtp, OCI_HTYPE_STMT)) checkerr(OCI_HTYPE_ERROR, status);
		}
		// Cleanup Login-Session
		if (this->svchp)
		{
			//cout << "Cleanup Login-Session" << endl;
			if (status = OCILogoff(this->svchp, this->errhp)) checkerr(OCI_HTYPE_ERROR, status);
		}
		// Cleanup Direct-Path Column Array
		if (this->dpcap)
		{
			//cout << "Cleanup Direct-Path Column Array" << endl;
			if (status = OCIHandleFree(this->dpcap, OCI_HTYPE_DIRPATH_COLUMN_ARRAY)) checkerr(OCI_HTYPE_ERROR, status);
		}
		// Cleanup Direct-Path Stream
		if (this->dpstp)
		{
			//cout << "Cleanup Direct-Path Stream" << endl;
			if (status = OCIHandleFree(this->dpstp, OCI_HTYPE_DIRPATH_STREAM)) checkerr(OCI_HTYPE_ERROR, status);
		}
		// Cleanup Direct-Path Context
		if (this->dpchp)
		{
			//cout << "Cleanup Direct-Path Context" << endl;
			if (status = OCIHandleFree(this->dpchp, OCI_HTYPE_DIRPATH_CTX)) checkerr(OCI_HTYPE_ERROR, status);
		}
		// Cleanup OCI Error Handle
		if (this->errhp)
		{
			//cout << "Cleanup OCI ErrorHandle" << endl;
			if (status = OCIHandleFree(this->errhp, OCI_HTYPE_ERROR)) checkerr(OCI_HTYPE_ENV, status);
		}
		//// OCI Terminate
		//if (this->envhp)
		//{
		//	cout << "OCITerminate" << endl;
		//	(void)OCITerminate(OCI_DEFAULT);
		//}
	}

	/**************************************************************************
	* Output OCI Error Infomations.
	**************************************************************************/
	private:void errprint(
		ub4     htype,
		sb4    *errcodep
	){
		char errbuf[512];
		memset(errbuf, '\0', sizeof(errbuf));
		if (this->errhp)
		{
			sb4  errcode;
			if (errcodep == (sb4 *)0)
				errcodep = &errcode;
			(void)OCIErrorGet(
					(dvoid *)this->errhp,
					(ub4)1, 
					(text *)NULL, 
					errcodep,
					(text *)errbuf, 
					(ub4) sizeof(errbuf), 
					OCI_HTYPE_ERROR
				);
			cout << "errbuf:" << errbuf << endl;
			log4j_error(1,errbuf);
		}
	}

	/**************************************************************************
	* Check OCI Error
	**************************************************************************/
	private:void checkerr(
		ub4		htype,
		sword	status
	){
		sb4 errcode = 0;
		switch (status)
		{
		case OCI_SUCCESS:
			break;
		case OCI_SUCCESS_WITH_INFO:
			cout << "INFO - OCI_SUCCESS_WITH_INFO status=" << status << endl;
			this->errprint(htype, &errcode);
			break;
		case OCI_NEED_DATA:
			cout << "Error - OCI_NEED_DATA" << endl;
			break;
		case OCI_NO_DATA:
			cout << "Error - OCI_NODATA" << endl;
			break;
		case OCI_ERROR:
			cout << "Error - OCI_ERROR status=" << status << endl;
			this->errprint(htype, &errcode);
			break;
		case OCI_INVALID_HANDLE:
			cout << "Error - OCI_INVALID_HANDLE" << endl;
			break;
		case OCI_STILL_EXECUTING:
			cout << "Error - OCI_STILL_EXECUTE" << endl;
			break;
		case OCI_CONTINUE:
			cout << "Error - OCI_CONTINUE" << endl;
			break;
		default:
			break;
		}
	}
	/**************************************************************************
	* Log Output(log4j)-Common
	**************************************************************************/
	private:void log4j_print(const char *logVal, jmethodID mtlog)
	{
		if (logVal!=nullptr)
		{
			if (this->jnienv!=nullptr)
			{
				//cout << "NewStringUTF start." << endl;
				jstring message = this->jnienv->NewStringUTF(logVal);
				if (!(this->jnienv->ExceptionCheck()))
				{
					if (this->jobjLog4j!=nullptr)
					{
						(void)this->jnienv->CallVoidMethod(
							this->jobjLog4j,
							mtlog,
							message
						);
					}
				}else {
					//cout << "NewStringUTF failed." << logVal << endl;
					this->jnienv->ExceptionClear();
				}
			}
			else 
			{
				cout << logVal << endl;
			}
		}
	}

	/**************************************************************************
	* Log Output(log4j)-Error
	**************************************************************************/
	private:void log4j_error(int num, ...)
	{
		va_list vlst;
		va_start(vlst, num);
		stringstream logVal;
		for (int i = 0; i < num; i++)
		{
			char *arg = va_arg(vlst, char *);
			if (arg) logVal << arg << " ";
		}
		va_end(vlst);
		//cout << "log4j_error:" << logVal.str() << endl;
		this->log4j_print(logVal.str().c_str(), this->jmtidErr);
	}

	/**************************************************************************
	* Log Output(log4j)-Info
	**************************************************************************/
	private:void log4j_info(int num, ...)
	{
		va_list vlst;
		va_start(vlst, num);
		stringstream logVal;
		for (int i = 0; i < num; i++)
		{
			char *arg = va_arg(vlst, char *);
			if (arg) logVal << arg << " ";
		}
		va_end(vlst);
		this->log4j_print(logVal.str().c_str(), this->jmtidInfo);
	}

	/**************************************************************************
	 * Convert jstring to char array with MS932-Encoding.
	 **************************************************************************/
	private:const char* GetStringMS932Chars(jstring strj, int *pLen)
	{
		if (strj == NULL) return NULL;
		jthrowable ej = this->jnienv->ExceptionOccurred();
		// Clear Exception
		if (ej != NULL) this->jnienv->ExceptionClear(); 
		int len = 0;
		char *sjis = NULL;
		jclass     clsj = NULL;
		jbyteArray arrj = NULL;
		jmethodID  mj = NULL;
		jstring encj = this->jnienv->NewStringUTF("MS932");
		if (encj == NULL) goto END;
		clsj = this->jnienv->GetObjectClass(strj);
		if (clsj == NULL) goto END;
		mj = this->jnienv->GetMethodID(clsj, "getBytes", "(Ljava/lang/String;)[B");
		if (mj == NULL) goto END;
		arrj = (jbyteArray)this->jnienv->CallObjectMethod(strj, mj, encj);
		if (arrj == NULL) goto END;
		len = this->jnienv->GetArrayLength(arrj);
		if (len<0) goto END;
		sjis = (char*)malloc(len + 1);
		if (sjis == NULL) {
			//env->FatalError("malloc");
			goto END;
		}
		this->jnienv->GetByteArrayRegion(arrj, 0, len, (jbyte*)sjis);
		sjis[len] = '\0';
		if (pLen != NULL) *pLen = len;
		// Throw the Exception
		if (ej != NULL) this->jnienv->Throw(ej); 
END:
		this->jnienv->DeleteLocalRef(ej);
		this->jnienv->DeleteLocalRef(encj);
		this->jnienv->DeleteLocalRef(clsj);
		this->jnienv->DeleteLocalRef(arrj);
		return sjis;
	}

	/**************************************************************************
	* Release char array with MS932-Encoding.
	**************************************************************************/
	private:void ReleaseStringMS932Chars(jstring strj, const char *sjis)
	{
		free((void*)sjis);
	}

	/**************************************************************************
	* Initialize OCI
	**************************************************************************/
	private:sword initializeOCI()
	{
//		cout << "initializeOCI START" << endl;
		_ASSERT_EXPR(this->username, "this->username is nullptr");
		_ASSERT_EXPR(this->password, "this->password is nullptr");
		_ASSERT_EXPR(this->dbname, "this->dbname is nullptr");

		sword status = OCI_SUCCESS;
		try {
			// Create OCIEnvironment
			//this->log4j_info(1, "OCIEnvCreate start");
			if (status = OCIEnvCreate(
//			if (status = OCIEnvNlsCreate(
				(OCIEnv **)&(this->envhp),
				(ub4)OCI_THREADED,
				(void *)0,
				0,
				0,
				0,
				(size_t)0,
				(void **)0
				//(ub2)932,
				//(ub2)OCI_UTF16ID
			)
				) {
				throw exception("OCIEnvCreate - error : ");
			}

			// Allocate OCI Error Handle
			//this->log4j_info(1, "OCIHandleAlloc(OCIErrorhandle) start");
			if (status = OCIHandleAlloc(
				this->envhp,
				(dvoid **)&(this->errhp),
				OCI_HTYPE_ERROR,
				(size_t)0,
				(void **)0
			)
				) {
				checkerr(OCI_HTYPE_ERROR, status);
				throw exception("OCIHandleAlloc(OCIError) - error : ");
			}

			// Create Login Session(Oracle DB COnnect)
			//this->log4j_info(1, "OCILogon2 start");
			//this->log4j_info(2, "username=",this->username );
			//this->log4j_info(2, "password=",this->password);
			//this->log4j_info(2, "dbname=",this->dbname);
			if (status = OCILogon2(
				(OCIEnv *)this->envhp,
				(OCIError *)this->errhp,
				(OCISvcCtx **)&(this->svchp),
				(OraText *)this->username,
				(ub4)strlen(this->username),
				(OraText *)this->password,
				(ub4)strlen(this->password),
				(OraText *)this->dbname,
				(ub4)strlen(this->dbname),
				OCI_DEFAULT
				)
			) {
				checkerr(OCI_HTYPE_SVCCTX, status);
				throw exception("OCILogon2 - error : ");
			}
//			cout << "OCILogon2 END." << endl;
		}
		catch (const exception &ex)
		{
			log4j_error(1, ex.what());
		}
	   return status;
	}

	/**************************************************************************
	* Ready the Direct-Path-Load
	**************************************************************************/
	private:sword readyDirectPathLoad()
	{
		sword		status	 = OCI_SUCCESS;
		ColumnAttr	*pcol	 = this->colAttr;	// Column Define Attr Pointer
		OCIParam	*colDesc = nullptr;			// Column Descriptor
		try
		{
			// Create Direct-Path Context
			if (status = OCIHandleAlloc(
				this->envhp,
				(dvoid **)&(this->dpchp),
				OCI_HTYPE_DIRPATH_CTX,
				(size_t)0,
				(void **)0
			)
				) {
				checkerr(OCI_HTYPE_DIRPATH_CTX, status);
				throw exception("OCIHandleAlloc(OCI_HTYPE_DIRPATH_CTX) - error : ");
			}

			// Setting Direct-Path Context Attribute
			vector<tuple<void *, ub4, ub2, string>> dpctxAttrs;
			dpctxAttrs.push_back(make_tuple(this->tablename,				(ub4)strlen(this->tablename),	OCI_ATTR_NAME						,"OCI_ATTR_NAME"));
			dpctxAttrs.push_back(make_tuple(this->username,					(ub4)strlen(this->username),	OCI_ATTR_SCHEMA_NAME				,"OCI_ATTR_SCHEMA_NAME"));
			dpctxAttrs.push_back(make_tuple(&(this->tblAttr->inputData),	(ub4)0,							OCI_ATTR_DIRPATH_INPUT				,"OCI_ATTR_DIRPATH_INPUT"));
			dpctxAttrs.push_back(make_tuple(&(this->tblAttr->columnCnt),	(ub4)0,							OCI_ATTR_NUM_COLS					,"OCI_ATTR_NUM_COLS"));
			dpctxAttrs.push_back(make_tuple(&(this->tblAttr->nolog),		(ub4)0,							OCI_ATTR_DIRPATH_NOLOG				,"OCI_ATTR_DIRPATH_NOLOG"));
			dpctxAttrs.push_back(make_tuple(&(this->tblAttr->parallel),		(ub4)0,							OCI_ATTR_DIRPATH_PARALLEL			,"OCI_ATTR_DIRPATH_PARALLEL"));
			dpctxAttrs.push_back(make_tuple(&(this->tblAttr->skipIndex),	(ub4)0,							OCI_ATTR_DIRPATH_SKIPINDEX_METHOD	,"OCI_ATTR_DIRPATH_SKIPINDEX_METHOD"));
			dpctxAttrs.push_back(make_tuple(&(this->tblAttr->transferSize), (ub4)0,							OCI_ATTR_BUF_SIZE					,"OCI_ATTR_BUF_SIZE"));

			for (tuple<void *, ub4, ub2, string> &tpl : dpctxAttrs)
			{
				void *pattr = get<0>(tpl);
				ub4 attrSize = get<1>(tpl);
				if (status = OCIAttrSet(
					this->dpchp,
					OCI_HTYPE_DIRPATH_CTX,
					pattr,
					attrSize,
					get<2>(tpl),
					this->errhp)
				) {
					checkerr(OCI_HTYPE_DIRPATH_CTX, status);
					stringstream ss;
					ss << "OCIAttrSet() - error : " << get<3>(tpl);
					throw exception(ss.str().c_str());
				}
			}

			// Get the Column Parameter List
			if (status = OCIAttrGet(
								(const void *)this->dpchp,
								(ub4)OCI_HTYPE_DIRPATH_CTX,
								(void *)&(this->colLstDesc),
								(ub4)0,
								OCI_ATTR_LIST_COLUMNS,
								this->errhp
				)
			){
				checkerr(OCI_HTYPE_DIRPATH_CTX, status);
				throw exception("OCIAttrGet(OCI_ATTR_LIST_COLUMNS) - error : ");
			}

			// Loop:Setting Column Describe
			for (ub4 col = 0; col < this->tblAttr->columnCnt; col++, pcol++)
			{
				stringstream idx;
				idx << col << ":" << pcol->columnName;
				// Get the Column Parameter
				if (status = OCIParamGet(
					(const void *)this->colLstDesc,
					(ub4)OCI_DTYPE_PARAM,
					this->errhp,
					(void **)&colDesc,
					(ub4)col + 1
					)
				) {
					checkerr(OCI_DTYPE_PARAM, status);
					throw exception("OCIAttrGet(OCI_DTYPE_PARAM) - error : ");
				}
				// Set the Column position.
				pcol->columnPos = col;

				// Setting the Direct-Path Column Array's Attributes.
				vector<tuple<void *, ub4, ub4, string>> dpColAttrs;
				dpColAttrs.push_back(make_tuple(pcol->columnName, strlen(pcol->columnName), OCI_ATTR_NAME, "columnName"));
				dpColAttrs.push_back(make_tuple(&(pcol->externalType), 0, OCI_ATTR_DATA_TYPE, "externalType"));
				dpColAttrs.push_back(make_tuple(&(pcol->columnSize), 0, OCI_ATTR_DATA_SIZE, "columnSize"));
				dpColAttrs.push_back(make_tuple(pcol->dateFormat, 0, OCI_ATTR_DATEFORMAT, "dateFormat"));

				for (tuple<void *, ub4, ub4, string> tpl : dpColAttrs)
				{
					void	*pvattr = get<0>(tpl);
					ub4		szattr = get<1>(tpl);
					ub4		kdattr = get<2>(tpl);
					string	nmattr = get<3>(tpl);

					if (!pvattr) break;

					if (status = OCIAttrSet(
						(void *)colDesc,
						(ub4)OCI_DTYPE_PARAM,
						(void *)pvattr,
						(ub4)szattr,
						(ub4)kdattr,
						this->errhp
					)
						) {
						checkerr(OCI_DTYPE_PARAM, status);
						throw exception("OCIAttrSet(OCI_DTYPE_PARAM) - error : ");
					}
				}// for loop terminate

				// Cleanup Column Descriptor
				if (status = OCIDescriptorFree(
										(void *)colDesc,
										(ub4)OCI_DTYPE_PARAM
				)
					) {
					checkerr(OCI_DTYPE_PARAM, status);
					throw exception("OCIDescriptorFree - error : ");
				}
			}// for loop terminate

			// Ready Direct-Path load Interface
			if (status = OCIDirPathPrepare(
								this->dpchp,
								this->svchp,
								this->errhp
							)
			){
				checkerr(OCI_HTYPE_DIRPATH_CTX, status);
				throw exception("OCIDirPathPrepare - error : ");
			}

			// Create Direct-Path Column Array
			if (status = OCIHandleAlloc(
				this->dpchp,
				(dvoid **)&(this->dpcap),
				OCI_HTYPE_DIRPATH_COLUMN_ARRAY,
				(size_t)0,
				(void **)0
			)
				) {
				checkerr(OCI_HTYPE_DIRPATH_COLUMN_ARRAY, status);
				throw exception("OCIHandleAlloc(OCI_HTYPE_DIRPATH_COLUMN_ARRAY) - error : ");
			}

			// Create Direct-Path Stream
			if (status = OCIHandleAlloc(
				this->dpchp,
				(dvoid **)&(this->dpstp),
				OCI_HTYPE_DIRPATH_STREAM,
				(size_t)0,
				(void **)0
			)
				) {
				checkerr(OCI_HTYPE_DIRPATH_STREAM, status);
				throw exception("OCIHandleAlloc(OCI_HTYPE_DIRPATH_STREAM) - error : ");
			}

			// Get the Direct-Path Column Array's Row Count
			if (status = OCIAttrGet(
				(const void *)this->dpcap,
				(ub4)OCI_HTYPE_DIRPATH_COLUMN_ARRAY,
				(void *)&(this->dirPathRowCnt),
				(ub4)0,
				OCI_ATTR_NUM_ROWS,
				this->errhp
			)
				) {
				checkerr(OCI_HTYPE_DIRPATH_CTX, status);
				throw exception("OCIAttrGet(OCI_ATTR_NUM_COLS) - error : ");
			}

			// Get the Direct-Path Column Array's Column Count
			if (status = OCIAttrGet(
								(const void *)this->dpcap,
								(ub4)OCI_HTYPE_DIRPATH_COLUMN_ARRAY,
								(void *)&(this->dirPathColCnt),
								(ub4)0,
								OCI_ATTR_NUM_COLS,
								this->errhp
			)
				) {
				checkerr(OCI_HTYPE_DIRPATH_CTX, status);
				throw exception("OCIAttrGet(OCI_ATTR_NUM_COLS) - error : ");
			}

		}catch (const exception &ex) {
			log4j_error(1, ex.what());
		}
		return status;
	}

	/**************************************************************************
	 * Analyze Input Meta-Info(JNI:StructType)
	 **************************************************************************/
	private:bool getMetaInfoStructType()
	{
		bool result = false;
		jboolean isCopy = 0;
		jthrowable ej = nullptr;		// java.lang.Throwable Object
		jclass jclsIter = nullptr;		// scala.collection.Iterator Class-Object
		jclass jclsStTp = nullptr;		// org.apache.spark.sql.types.StructType Class-Object
		jclass jclsStFd = nullptr;		// org.apache.spark.sql.types.StructField Class-Object
		jclass jclsDtTp = nullptr;		// org.apache.spark.sql.types.DataType Class-Object
		jobject jobjStFd = nullptr;		// org.apache.spark.sql.types.StructField Object
		jobject jobjDtTp = nullptr;		// org.apache.spark.sql.types.DataType Object
		jmethodID jmtidSFname = 0;

		try{
			// Initialize Exception Object
			ej = this->jnienv->ExceptionOccurred();
			if (ej != nullptr) this->jnienv->ExceptionClear();

			// Get scala.collection.Iterator-Class Object
			jclsIter = this->jnienv->GetObjectClass(this->jobjIter);
			if (!jclsIter) throw exception("Get scala.collection.Iterator-Class Object - failed.");

			// Get scala.collection.Iterator#next-Method ID
			this->jmtidIterNext = this->jnienv->GetMethodID(
												jclsIter,
												"next", 
												"()Ljava/lang/Object;");
			if (!this->jmtidIterNext) throw exception("Get scala.collection.Iterator#next-Method ID - failed.");

			// Get scala.collection.Iterator#hasNext-Method ID
			this->jmtidIterHasNext = this->jnienv->GetMethodID(
													jclsIter,
													"hasNext", 
													"()Z");
			if (!this->jmtidIterHasNext) throw exception("Get scala.collection.Iterator#hasNext-Method ID - failed.");

			// Get org.apache.spark.sql.StructType-Class Object
			jclsStTp = this->jnienv->GetObjectClass(this->jobjStrTyp);
			if (!jclsStTp) throw exception("Get org.apache.spark.sql.StructType - Class Object - failed.");

			// Get org.apache.spark.sql.StructType#size-Method ID
			jmethodID jmtidSTsize = this->jnienv->GetMethodID(
													jclsStTp,
													"size", 
													"()I");
			if (!jmtidSTsize) throw exception("Get org.apache.spark.sql.StructType#size-Method ID - failed.");

			// Get the Column Count.
			this->tblAttr->columnCnt = (int)this->jnienv->CallIntMethod(this->jobjStrTyp, jmtidSTsize);
			if (!this->tblAttr->columnCnt) throw exception("Get the Column Count - failed.");

			// Get org.apache.spark.sql.types.StructType#apply-Method ID
			jmethodID jmtidSTApply = this->jnienv->GetMethodID(
													jclsStTp,
													"apply", 
													"(I)Lorg/apache/spark/sql/types/StructField;");
			if (!jmtidSTApply) throw exception("Get org.apache.spark.sql.types.StructType#apply-Method ID - failed.");

			// Allocate Column Define Class Arrays
			this->colAttr = new ColumnAttr[this->tblAttr->columnCnt];

			// Setthing Column Name to Column Define Class Arrays
			for (int col = 0; col < this->tblAttr->columnCnt; col++)
			{
				// Get the org.apache.spark.sql.types.StructField-Object
				const jobject jobjStrFld = this->jnienv->CallObjectMethod(
															this->jobjStrTyp,
															jmtidSTApply,
															col);
				if (!jobjStrFld) throw exception("Get the org.apache.spark.sql.types.StructField-Object - failed.");

				// Get the org.apache.spark.sql.StructField-Class Object
				if (!jclsStFd)
				{
					jclsStFd = this->jnienv->GetObjectClass(jobjStrFld);
					if (!jclsStFd) throw exception("Get the org.apache.spark.sql.StructField-Class Object - failed.");
				}

				// Get the org.apache.spark.sql.StructField#nam-Method ID
				if (!jmtidSFname)
				{
					jmtidSFname = this->jnienv->GetMethodID(
														jclsStFd,
														"name",
														"()Ljava/lang/String;");
					if (!jmtidSFname) throw exception("Get the org.apache.spark.sql.StructField#name-Method ID - failed.");
				}

				// Get The ColumnName(StructField#name())
				const jstring jstrColName = (jstring)this->jnienv->CallObjectMethod(jobjStrFld, jmtidSFname);
				if (!jstrColName) throw exception("Get The ColumnName(StructField#name()) - failed.");

				// Convert String object to const char array.
				const char *pArgs = this->jnienv->GetStringUTFChars(jstrColName, &isCopy);
				if (!pArgs) throw exception("Get The ColumnName(GetStringUTFChars) - failed.");

				// Setting Column Define Array-columnName
				char *pColName = (this->colAttr + col)->columnName;
				memcpy(pColName, pArgs, strlen(pArgs));

				// Upper case
				for (int pos = 0; pos < strlen(pColName); pos++)
				{
					if (islower(*(pColName + pos))) *(pColName + pos) = toupper(*(pColName + pos));
				}

				// Cleanup const char array & Java objects.
				this->jnienv->ReleaseStringUTFChars(jstrColName, pArgs);
				this->jnienv->DeleteLocalRef(jstrColName);
				this->jnienv->DeleteLocalRef(jobjStrFld);
			}
			// process successed.
			result=true;
		}catch (const exception &ex) {
			this->log4j_error(1, ex.what());
			if (this->jnienv->ExceptionCheck())
			{
				ej = this->jnienv->ExceptionOccurred();
				if (ej != nullptr) this->jnienv->Throw(ej);
			}
		}
		if(ej) this->jnienv->DeleteLocalRef(ej);
		// Delete Local-Reference java objects.
		if (jclsIter) this->jnienv->DeleteLocalRef(jclsIter);
		if (jclsStTp) this->jnienv->DeleteLocalRef(jclsStTp);
		if (jclsStFd) this->jnienv->DeleteLocalRef(jclsStFd);
		if (jclsDtTp) this->jnienv->DeleteLocalRef(jclsDtTp);
		return result;
	}

	/**************************************************************************
	* Get The Oracle Environment
	**************************************************************************/
	private:bool getOracleEnvs()
	{
		jstring		jstrKey = nullptr;
		jstring		jstrVal = nullptr;
		jclass		jclsLog4j = nullptr;
		jclass		jclsProp = nullptr;
		jmethodID	jmtidGet = nullptr;
		jboolean	isCopy = JNI_FALSE;
		jthrowable	ej = nullptr;
		bool		result = false;

		char *mapping[4][2] = {
			{"user"				, this->username	},
			{"password"			, this->password	},
			{"sid"				, this->dbname		},
			{"tableName"		, this->tablename	}
		};

		// Initialize Java Exception
		ej = this->jnienv->ExceptionOccurred();
		if (ej) this->jnienv->ExceptionClear();

		try{
//			cout << "Get the log4j Class-Object" << endl;
			// Get the log4j Class-Object
			jclsLog4j = (jclass)this->jnienv->GetObjectClass(this->jobjLog4j);
			if (!jclsLog4j) throw exception("Get the log4j Class-Object - failed.");

			// Get the log4j#info Method-ID
			this->jmtidInfo = (jmethodID)this->jnienv->GetMethodID(jclsLog4j,"info","(Ljava/lang/Object;)V");
			if (!this->jmtidInfo) throw exception("Get the log4j#info Method-ID - failed.");

			// Get the log4j#error Method-ID
			this->jmtidErr = (jmethodID)this->jnienv->GetMethodID(jclsLog4j, "error", "(Ljava/lang/Object;)V");
			if (!this->jmtidInfo) throw exception("Get the log4j#error Method-ID - failed.");

			// Get the Property Class-Object
			jclsProp = (jclass)this->jnienv->GetObjectClass(this->jobjProp);
			if (!jclsProp) throw exception("Get the Property Class-Object - failed.");

			// Get the Propety#get Method-ID
			jmtidGet = (jmethodID)this->jnienv->GetMethodID(jclsProp, "get", "(Ljava/lang/Object;)Ljava/lang/Object;");
			if (!jmtidGet) throw exception("Get the Property#get Method-ID - failed.");

//			cout << "Loop:Get the Oracle environments. start" << endl;
			stringstream err;
			// Loop:Get the Oracle environments.
			for (int i = 0; i < (sizeof(mapping) / sizeof(mapping[0])); i++)
			{
				// Get the key for execute Property#get method.
				jstrKey = nullptr;
				jstrKey = (jstring)this->jnienv->NewStringUTF(mapping[i][0]);
				if (!jstrKey) {
					err << "Get the key for execute Property#get method. key=[" << mapping[i][0] << "]";
					throw exception((char *)err.str().c_str());
				}

				// Get the value from Property-Object.
				jstrVal = nullptr;
				jstrVal = (jstring)this->jnienv->CallObjectMethod(jobjProp, jmtidGet, jstrKey);
				if (!jstrVal) {
					err << "Get the value from Property-Object. key=[" << mapping[i][0] << "]";
					throw exception((char *)err.str().c_str());
				}

				// Convert String-Object to const char array.
				const char *pVal = this->jnienv->GetStringUTFChars(jstrVal, &isCopy);
				if (!pVal) {
					err << "Convert String-Object to const char array. key=[" << mapping[i][0] << "]";
					throw exception(err.str().c_str());
				}

				// Setting Oracle environment.
				memcpy(mapping[i][1], pVal, strlen(pVal) );

				// Cleanup the const char array and java String objects.
				this->jnienv->ReleaseStringUTFChars(jstrVal, pVal);
				this->jnienv->DeleteLocalRef(jstrVal);
				this->jnienv->DeleteLocalRef(jstrKey);
			}

			// Setting Table Define Attribute(table owner & table name)
			this->tblAttr->tableOwner = (text *)this->username;
			this->tblAttr->tableName = (text *)this->tablename;

			// process succeed
			result = true;
		}
		catch (const exception &ex) {
			this->log4j_error(1, ex.what());
			if (this->jnienv->ExceptionCheck())
			{
				result = false;
				ej = this->jnienv->ExceptionOccurred();
				if (ej) this->jnienv->Throw(ej);
			}
		}
		// Delete Local-Reference java objects.
		if (jstrKey) this->jnienv->DeleteLocalRef(jstrKey);
		if (jstrVal) this->jnienv->DeleteLocalRef(jstrVal);
		if (jclsLog4j) this->jnienv->DeleteLocalRef(jclsLog4j);
		if (jclsProp) this->jnienv->DeleteLocalRef(jclsProp);

		return result;
	}

	/**************************************************************************
	 * Analize Header Infomations(File)
	 **************************************************************************/
	private:bool getHeaderInfoFromFile()
	{
		bool result = false;
		return result;
	}

	/**************************************************************************
	* Analize Header Infomation
	**************************************************************************/
	private:bool getHeaderInfo()
	{
		bool result = false;

		if (this->jnienv!=nullptr)
		{
		   //this->log4j_info(1,"Call getMetaInfoStructTyp.");
		   result = getMetaInfoStructType();
		}
		else
		{
			//this->log4j_info(1, "Call getHeaderInfoFromFile.");
			result = getHeaderInfoFromFile();
		}

		return result;
	}

	/**************************************************************************
	 * Get the Oracle Meta Infomations(user_tab_columns)
	 **************************************************************************/
	private:sword getOracleMetaInfo()
	{
	   sword status = OCI_SUCCESS;
	   ub4 colcnt = 3;
	   char *stmt = "SELECT														 \
					utc.COLUMN_NAME,											 \
					(															 \
						CASE													 \
							WHEN utc.DATA_TYPE = 'DATE'  THEN 'D'				 \
							WHEN utc.DATA_TYPE LIKE 'TIMESTAMP%' THEN 'T'		 \
							WHEN utc.DATA_TYPE = 'NUMBER'  THEN 'N'				 \
							ELSE 'S'											 \
						END														 \
					) DATA_TYPE,												 \
					(															 \
						CASE													 \
							WHEN utc.DATA_TYPE = 'DATE'			 THEN 20		 \
							WHEN utc.DATA_TYPE LIKE 'TIMESTAMP%' THEN 23		 \
							WHEN utc.DATA_TYPE = 'NUMBER' THEN DATA_PRECISION	 \
							ELSE DATA_LENGTH									 \
						END														 \
					) DATA_LENGTH												 \
				FROM															 \
					USER_TAB_COLUMNS	utc										 \
					LEFT OUTER JOIN												 \
						(SELECT													 \
							ucc.TABLE_NAME,										 \
							ucc.COLUMN_NAME,									 \
							ucc.CONSTRAINT_NAME,								 \
							ucn.CONSTRAINT_TYPE									 \
						 FROM													 \
							USER_CONS_COLUMNS  ucc								 \
							INNER JOIN USER_CONSTRAINTS  ucn					 \
							ON													 \
								ucc.CONSTRAINT_NAME = ucn.CONSTRAINT_NAME		 \
						 WHERE													 \
							ucn.CONSTRAINT_TYPE = 'P'							 \
						) ucs													 \
						ON														 \
								utc.TABLE_NAME = ucs.TABLE_NAME(+)				 \
							AND													 \
								utc.COLUMN_NAME = ucs.COLUMN_NAME(+)			 \
				WHERE															 \
						utc.TABLE_NAME=:1										 \
				ORDER BY														 \
					utc.COLUMN_ID												 \
		";
		// 各種ハンドルを割り当てる(文ハンドル)
		if(status = OCIHandleAlloc(
				   this->envhp,
				   (dvoid **)&(this->stmtp),
				   OCI_HTYPE_STMT,
				   (size_t)0,
				   (void **)0)
		){
			checkerr(OCI_HTYPE_STMT, status);
			return OCI_ERROR;
		}

		// 実行するSQL文またはPL/SQL文を準備する
		if(status = OCIStmtPrepare(
				   this->stmtp,
				   this->errhp,
				   (OraText*)stmt,
				   (ub4)strlen(stmt),
				   OCI_NTV_SYNTAX,
				   OCI_DEFAULT
			   )
		){
			checkerr(OCI_HTYPE_STMT, status);
			return OCI_ERROR;
		}

		_ASSERT_EXPR(this->tablename, "this->tablename is nullptr");

		// 条件の設定
		OCIBind  *bnd = (OCIBind *)0;
		if (status = OCIBindByPos(
				   this->stmtp,
				   &bnd,
				   this->errhp,
				   1,
				   (dvoid *)this->tablename,
				   (sword)strlen(this->tablename),
				   SQLT_CHR,
				   (dvoid *)0,
				   (ub2 *)0,
				   (ub2 *)0,
				   (ub4)0,
				   (ub4 *)0,
				   OCI_DEFAULT
			   )
		){
			checkerr(OCI_HTYPE_STMT, status);
			return OCI_ERROR;
		}

		// 選択リスト内の項目を型と出力データ・バッファに関連付ける
		UserTabColumns *metainf = new UserTabColumns[this->tblAttr->columnCnt];

		OCIDefine** dfnp = (OCIDefine **)calloc(this->tblAttr->columnCnt, sizeof(OCIDefine *));
		memset(dfnp, '\0', sizeof(OCIDefine *) * this->tblAttr->columnCnt);

		void *hostList[3] = {
				   metainf[0].columnName ,
				   metainf[0].columnType,
				   &(metainf[0].columnSize)
		};
		sb4 hostSizeList[3] = {
				   sizeof(metainf[0].columnName) - 1,
				   sizeof(metainf[0].columnType) - 1,
				   sizeof(metainf[0].columnSize)
		};

		vector<tuple<void *, sb4, ub2>> hosts;
		hosts.push_back(make_tuple(metainf[0].columnName	, sizeof(metainf[0].columnName) - 1	,	SQLT_CHR));
		hosts.push_back(make_tuple(metainf[0].columnType	, sizeof(metainf[0].columnType) - 1	,	SQLT_CHR));
		hosts.push_back(make_tuple(&(metainf[0].columnSize)	, sizeof(metainf[0].columnSize)		,	SQLT_INT));

		// 結果格納用ホスト変数の設定
		for (unsigned int col = 0; col < colcnt; col++)
		{
			tuple<void *, sb4, ub2> tpl = hosts[col];
			// 結果格納用ホスト変数の設定
			if (status = OCIDefineByPos(
								this->stmtp,
								&(dfnp[col]),
								this->errhp,
								col + 1,
								get<0>(tpl),
								get<1>(tpl),
								get<2>(tpl),
								0,
								0,
								0,
								OCI_DEFAULT
							)
			) {
				checkerr(OCI_HTYPE_STMT, status);
				return OCI_ERROR;
			}

			/* 定義した各値ごとにスキップパラメータを指定する */
			if (status = OCIDefineArrayOfStruct(
						dfnp[col],
						this->errhp,
						(sb4)sizeof(metainf[0]),
						0,
						(ub4)0,
						(ub4)0
				   )
			) {
				checkerr(OCI_HTYPE_STMT, status);
				return OCI_ERROR;
			}
		}

		// 準備済みのSQL文を実行する
		if(status = OCIStmtExecute(
			   this->svchp,
			   this->stmtp,
			   this->errhp,
			   this->tblAttr->columnCnt,
			   0,
			   NULL,
			   NULL,
			   OCI_DEFAULT
		   )
		){
			checkerr(OCI_HTYPE_STMT, status);
			return OCI_ERROR;
		}

		_ASSERT_EXPR(this->tblAttr->columnCnt, "this->tblAttr->columnCnt is zero");
		_ASSERT_EXPR(this->colAttr, "this->colAttr is nullptr");

	   // Set Oracle meta info to Column Define Attribute array
	   for (int col = 0; col < this->tblAttr->columnCnt; col++)
	   {
		   //cout << col << ":" << (metainf + col)->columnName << ":" << (metainf + col)->columnType << ":" << (metainf + col)->columnSize << endl;
		   ColumnAttr *pcol = this->colAttr + col;
		   for (int col2 = 0; col2 < this->tblAttr->columnCnt; col2++)
		   {
			   const UserTabColumns *pcol2 = (metainf + col2);
			   if (strncmp(pcol->columnName, pcol2->columnName,strlen(pcol->columnName)) == 0)
			   {
				   // Set the column-size
				   pcol->columnSize = pcol2->columnSize;
				   if (pcol2->columnType[0] == 'S')
				   {
					   pcol->columnSize = (ub4)(pcol->columnSize * 1.5);
				   }

				   // Set the column-type
				   pcol->columnType[0] = pcol2->columnType[0];
				   // Set the date-format
				   if (pcol->columnType[0] == 'D')
				   {
					   // DATE Type
					   pcol->dateFormat = (OraText *)DATE_FORMAT;
				   }
				   else if (pcol->columnType[0] == 'T')
				   {
					   // TimeStamp Type
					   pcol->dateFormat = (OraText *)TIMESTAMP_FORMAT;
				   }
				   break;
			   }
			}
		   // Add Column Max size.
		   this->allocSize += pcol->columnSize;
		   // Add Separator char(\0) size.
		   this->allocSize++;
		}

		// Allocate Input Record Array 
		this->records = new char[this->allocSize * LOAD_UNIT];
		if (this->records == nullptr)
		{
			stringstream exceptMsg;
			exceptMsg << "memory alloc failed. size=" << (this->allocSize * LOAD_UNIT);
			this->log4j_error(1, exceptMsg.str().c_str());
			return false;
		}

		// Set the transfer size.
		this->tblAttr->transferSize = this->allocSize * 1000;

		return status;
	}

	/**************************************************************************
	* Convert Iterator<Row> to char array
	**************************************************************************/
	private:bool convertIter2CharArray(int *pRcdCnt)
	{
		bool		result	= false;
		jthrowable	ej		= nullptr;
		jclass		jclsRow	= nullptr;
		jclass		jclsCol = nullptr;
		jobject		jobjRow = nullptr;
		jobject		jobjCol = nullptr;
		jboolean	isCopy = JNI_FALSE;
		size_t		collen = 0;
		char		*ptCol = this->records;

		// Clear the record-count
		(*pRcdCnt) = 0;

		// Clear the Input Record Array 
		memset(this->records, '\0', sizeof(this->records));

		// Initialize Exception Object
		ej = this->jnienv->ExceptionOccurred();
		if (ej != nullptr) this->jnienv->ExceptionClear();

		try {
			for (int row = 0; row < LOAD_UNIT; row++)
			{
				// Check the more than Object in Iterator(with Iterator#hasNext)
				jboolean hasNext = this->jnienv->CallBooleanMethod(this->jobjIter, this->jmtidIterHasNext);
				if (!hasNext) break;

				// Get the org.apache.spark.sql.Row Object(with Iterator#next)
				jobjRow = this->jnienv->CallObjectMethod(this->jobjIter, this->jmtidIterNext);
				if (!jobjRow) throw exception("Get the org.apache.spark.sql.Row Object(with Iterator#next) - failed.");

				// Get the org.apache.spark.sql.Row Class-Object(first only)
				if (!jclsRow)
				{
					jclsRow = this->jnienv->GetObjectClass(jobjRow);
					if (!jclsRow) throw exception("Get the org.apache.spark.sql.Row Class-Object(first only) - failed.");
				}

				// Get the org.apache.spark.sql.Row#get Method-ID(first only)
				if (!this->jmtidRowGet)
				{
					this->jmtidRowGet = this->jnienv->GetMethodID(
																jclsRow,
																"get",
																"(I)Ljava/lang/Object;"
						);
					if (!this->jmtidRowGet) throw exception("Get the org.apache.spark.sql.Row#get Method-ID(first only) - failed.");
				}

				// Loop:Get the Column Object & Edit Input Record Array
				for (int col = 0; col < (this->tblAttr->columnCnt); col++)
				{
					// Get the org.apache.spark.sql.Column Object from Row Object
					jobjCol = nullptr;
					jobjCol = this->jnienv->CallObjectMethod(jobjRow, this->jmtidRowGet, (jint)col);
					if (jobjCol==nullptr) throw exception("Get the org.apache.spark.sql.Column Object from Row Object - failed.");

					// Get the org.apache.spark.sql.Column Class-Object
					if (!jclsCol)
					{
						jclsCol = this->jnienv->GetObjectClass(jobjCol);
						if (!jclsCol) throw exception("Get the org.apache.spark.sql.Column Class-Object - failed.");
					}

					// Get the org.apache.spark.sql.Column#toString Method-ID(first only)
					if (!this->jmtidToString)
					{
						this->jmtidToString = this->jnienv->GetMethodID(
														jclsCol,
														"toString",
														"()Ljava/lang/String;"
						);
						if (!this->jmtidToString) throw exception("Get the org.apache.spark.sql.Column#toString Method-ID(first only) - failed.");
					}

					ub4 colSize = this->colAttr[col].columnSize;
					char dataType = this->colAttr[col].columnType[0];

					// Get The Data-Value from Column Object(with toString)
					jstring jstrVal = (jstring)this->jnienv->CallObjectMethod(jobjCol, this->jmtidToString, (jint)col);
					if (!jstrVal) {
						stringstream idx;
						idx << "Get The Data-Value from Column Object(with toString) - failed. [";
						idx << col << "]:[" << this->colAttr[col].columnName << "]";
						throw exception(idx.str().c_str());
					}

					// Convert String Object to const char array.
					const char *pVal = this->GetStringMS932Chars(jstrVal, 0);
					//const char *pVal = this->jnienv->GetStringUTFChars(jstrVal, &isCopy);
					if (!pVal)
					{
						// val is null
						;
					}else {
						// Cut the mill time if dataType eq 'Date'
						if (dataType == 'D')
						{
							char *ptrTs = strchr((char *)pVal, '.');
							if (ptrTs) {
								*ptrTs = '\0';
								*(ptrTs+1) = '\0';
							}
						}
						// get column length
						collen = strlen(pVal);
//						cout << "colName=" << this->colAttr[col].columnName << ",colSize=" << this->colAttr[col].columnSize << endl;
//						cout << "strlen(pVal)=" << strlen(pVal) << "[" << pVal << "]" << endl;
						memcpy(ptCol, pVal, collen);
						// column termminate
						*(ptCol + collen) = '\0';

						// Adjust Current Pointer for Terminate
						ptCol += collen;

						// Cleanup char array
						//this->jnienv->ReleaseStringUTFChars(jstrVal, pVal);
						this->ReleaseStringMS932Chars(jstrVal, pVal);
					}
					// Adjust Current Pointer for Next Column Start-position
					ptCol++;

					// Cleanup java objects.
					this->jnienv->DeleteLocalRef(jobjCol);
					this->jnienv->DeleteLocalRef(jstrVal);


				}// for loop terminate

				// Increment Record Count
				(*pRcdCnt)++;

				// Cleanup java objects.
				this->jnienv->DeleteLocalRef(jobjRow);

			}
			// process succeed.
			result = true;

		}catch (const exception &ex) {
			this->log4j_error(1, ex.what());
			if (this->jnienv->ExceptionCheck())
			{
				result = false;
				ej = this->jnienv->ExceptionOccurred();
				if (ej) this->jnienv->Throw(ej);
			}
		}
		// Delete Local-Reference java objects.
		if (ej) this->jnienv->DeleteLocalRef(ej);
		if (jclsRow) this->jnienv->DeleteLocalRef(jclsRow);
		if (jobjRow) this->jnienv->DeleteLocalRef(jobjRow);
		if (jclsCol) this->jnienv->DeleteLocalRef(jclsCol);
		if (jobjCol) this->jnienv->DeleteLocalRef(jobjCol);

		return result;
	}

	/**************************************************************************
	* Load Data Into DataBase With Direct-Path-Load
	**************************************************************************/
	private:bool execDataLoad(int rcdCnt)
	{
		bool			result		 = false;
		bool			done		 = false;
		bool			leftover	 = false;
		int				step		 = 0;
		int				dirPathRows	 = this->dirPathRowCnt;
		int				dirPathCols	 = this->dirPathColCnt;
		int				loadedRows	 = 0;
		int				totalRows	 = 0;
		ub4				hType		 = 0;
		ub4				rowOffset	 = 0;
		ub4				curLoadCnt	 = this->dirPathRowCnt;
//		ub4				clen		 = 0;
		ub1				cflg		 = 0;
		ub1				*cval		 = (ub1 *)this->records;
//		ColumnAttr		*colp		 = this->colAttr;
		stringstream	errMsg;
		size_t			collen		 = 0;
		sword			status		 = OCI_SUCCESS;

		// step enums
		enum steps{
			STEP_0_RESET,
			STEP_1_FIX_LOAD_CNT,
			STEP_2_SET_DATA_FIELD,
			STEP_3_CONVERT_STREAM,
			STEP_4_LOADING_STREAM,
			STEP_5_ADJUST_OFFSET,
			STEP_6_SUCCESS_COMPLETE,
			STEP_9_ERROR=9
		};

		// init step
		step = STEP_1_FIX_LOAD_CNT;

		// loop:Direct-Path-Load
		while (!done)
		{
			switch (step)
			{
			case STEP_0_RESET:
				//----------------------------------------------------------------------
				// Reset the Direct-Path Column Array & Stream
				//----------------------------------------------------------------------
				if (status = OCIDirPathColArrayReset(
					this->dpcap,
					this->errhp
				)
					) {
					errMsg << "OCIDirPathColArrayReset - failed.";
					hType = OCI_HTYPE_DIRPATH_COLUMN_ARRAY;
					step = STEP_9_ERROR;
					break;
				}
				if (status = OCIDirPathStreamReset(
					this->dpstp,
					this->errhp
				)
					) {
					errMsg << "OCIDirPathStreamReset - failed.";
					hType = OCI_HTYPE_DIRPATH_STREAM;
					step = STEP_9_ERROR;
					break;
				}
				step = STEP_1_FIX_LOAD_CNT;
				break;
			case STEP_1_FIX_LOAD_CNT:
				//----------------------------------------------------------------------
				// Fix the Loading record count.
				//----------------------------------------------------------------------
				if (totalRows + dirPathRows > rcdCnt)
				{
					curLoadCnt = rcdCnt - totalRows;
				}else {
					curLoadCnt = dirPathRows;
				}
				// Add current load cnt to total rows.
				totalRows += curLoadCnt;
				if (curLoadCnt == 0)
				{
					step = STEP_6_SUCCESS_COMPLETE;
				}else {
					step = STEP_2_SET_DATA_FIELD;
				}
				// Clear the offset for Direct-Path Column Array
				rowOffset = 0;
				break;
			case STEP_2_SET_DATA_FIELD:
				//----------------------------------------------------------------------
				// Set the Direct-Path Column Array from Input Record Array
				//----------------------------------------------------------------------
				step = STEP_3_CONVERT_STREAM;
				for (ub4 row = 0; row < curLoadCnt; row++)
				{
					ColumnAttr *colp = this->colAttr;
					for (int col = 0; col < dirPathCols; col++)
					{
						cflg = OCI_DIRPATH_COL_COMPLETE;
						collen = strlen((char *)cval);
//						cout << "[" << row << "-" << col << "]:" << colp->columnName << ":colSz" << colp->columnSize << ":collen" << collen << ":[" << cval << "]" <<endl;
						if (colp->columnSize < collen)
						{
							errMsg << "ColumnSize over. ROW=" << row << ",COLUMN_NAME=[" << colp->columnName << "],COLUMN_SIZE=" << colp->columnSize << ",DATA_SIZE=" << collen;
							hType = 0;
							step = STEP_9_ERROR;
							break;
						}else if (collen == 0) {
							cflg = OCI_DIRPATH_COL_NULL;
						}

						if (status = OCIDirPathColArrayEntrySet(
														this->dpcap,
														this->errhp,
														row,
														colp->columnPos,
														cval,
														(ub4)collen,
														cflg
													)
						){
							errMsg << "OCIDirPathColArrayEntrySet - failed. ROW=" << row << ",COLUMN_NAME=[" << colp->columnName << "],COLUMN_SIZE=" << colp->columnSize << ",DATA_VALUE=[" << cval << "]";
							hType = OCI_HTYPE_DIRPATH_COLUMN_ARRAY;
							step = STEP_9_ERROR;
							break;
						}
						// Add the value length(for next column)
						cval += collen;
						cval++;
						colp++;
					}
					// break the error
					if (step == STEP_9_ERROR) break;
				}
				break;
			case STEP_3_CONVERT_STREAM:
				//----------------------------------------------------------------------
				// Convert the Direct-Path Column Array to Direct-Path Stream
				//----------------------------------------------------------------------
				leftover = false;
				status = OCIDirPathColArrayToStream(
					this->dpcap,
					this->dpchp,
					this->dpstp,
					this->errhp,
					curLoadCnt,
					rowOffset
				);
				if (status == OCI_SUCCESS) 
				{
					step = STEP_4_LOADING_STREAM;
				}
				else if(status==OCI_NEED_DATA || status ==OCI_CONTINUE)
				{
					step = STEP_4_LOADING_STREAM;
					leftover = true;
				}
				else
				{
					errMsg << "OCIDirPathColArrayToStream - failed. ";
					hType = OCI_HTYPE_DIRPATH_COLUMN_ARRAY;
					step = STEP_9_ERROR;
				}
				break;
			case STEP_4_LOADING_STREAM:
				//----------------------------------------------------------------------
				// Load Direct-Path Stream
				//----------------------------------------------------------------------
				status = OCIDirPathLoadStream(
					this->dpchp,
					this->dpstp,
					this->errhp
				);
				if (status == OCI_SUCCESS)
				{
					// Reset for next rows when load succeess
					step = STEP_0_RESET;
				}
				else if (status == OCI_NEED_DATA || status == OCI_CONTINUE || leftover)
				{
					// 
					step = STEP_5_ADJUST_OFFSET;
				}
				else
				{
					errMsg << "OCIDirPathLoadStream - failed. ";
					hType = OCI_HTYPE_DIRPATH_STREAM;
					step = STEP_9_ERROR;
				}
				break;
			case STEP_5_ADJUST_OFFSET:
				//----------------------------------------------------------------------
				// Adjust Offset for Load(leftover)
				//----------------------------------------------------------------------
				step = STEP_3_CONVERT_STREAM;
				if (status = OCIAttrGet(
										(const void *)this->dpcap,
										(ub4)OCI_HTYPE_DIRPATH_COLUMN_ARRAY,
										(void *)&(loadedRows),
										(ub4)0,
										OCI_ATTR_ROW_COUNT,
										this->errhp
									)
				){
					errMsg << "OCIAttrGet(OCI_ATTR_ROW_COUNT) - failed. ";
					hType = OCI_HTYPE_DIRPATH_COLUMN_ARRAY;
					step = STEP_9_ERROR;
					break;
				}
				// Add the offset to loaded row count.
				rowOffset += loadedRows;
				if (status = OCIDirPathStreamReset(
					this->dpstp,
					this->errhp
				)
					) {
					errMsg << "OCIDirPathStreamReset - failed.";
					hType = OCI_HTYPE_DIRPATH_STREAM;
					step = STEP_9_ERROR;
				}
				break;
			case STEP_6_SUCCESS_COMPLETE:
				//----------------------------------------------------------------------
				// Process Succeed.
				//----------------------------------------------------------------------
				done = true;
				result = true;
				break;
			case STEP_9_ERROR:
			default:
				//----------------------------------------------------------------------
				// Process Failed
				//----------------------------------------------------------------------
				this->checkerr(hType, status);
				this->log4j_error(1, errMsg.str().c_str());
				done = true;
				result = false;
				break;
			}
		}

		return result;
	}

	/**************************************************************************
	 * メイン処理実行
	 **************************************************************************/
	public:jboolean execute()
	{
		jboolean	result	= JNI_FALSE;
		sword		status	= OCI_SUCCESS;
		int			recCnt	= 0;
		int			totalCnt = 0;

		try
		{
			// Get the Oracle Environment
			if (!this->getOracleEnvs())
			{
				throw exception("Get the Oracle Environment - failed.");
			}
			// OCI Initialize
//			cout << "OCI Initialize start" << endl;
			this->log4j_info(1, "OCI Initialize start");
			if (this->initializeOCI())
			{
				throw exception("OCI Initialize - failed.");
			}

			// Analize Header Infomation
//			cout << "getHeaderInfo start" << endl;
			//this->log4j_info(1, "getHeaderInfo start");
			if (!this->getHeaderInfo())
			{
				throw exception("Analize Header Infomation - failed.");
			}

			//　Get the Oracle Meta Infomations(user_tab_columns)
			//this->log4j_info(1, "getOracleMetaInfo start");
			if (this->getOracleMetaInfo())
			{
				throw exception("Get the Oracle Meta Infomations(user_tab_columns) - failed.");
			}

			// Ready Direct-Path Load
			this->log4j_info(1, "readyDirectPathLoad start");
			if (this->readyDirectPathLoad())
			{
				throw exception("Ready Direct-Path Load - faild.");
			}

			// Loop:exec Direct-Path load
			while (true)
			{
				// Convert Iterator<Row> to char array
				//this->log4j_info(1, "convertIter2CharArray start");
				if (!this->convertIter2CharArray(&recCnt)){
					throw exception("Convert Iterator<Row> to char array - failed.");
				}

				if (recCnt == 0) break;

				// Load Data Into DataBase With Direct-Path-Load
				//this->log4j_info(1, "execDataLoad start");
				if (!this->execDataLoad(recCnt)) {
					throw exception("Load Data Into DataBase With Direct-Path-Load - failed.");
				}

				totalCnt += recCnt;
				stringstream cntVal;
				cntVal << totalCnt;
				this->log4j_info(3, "total record cnt", cntVal.str().c_str(), "load success");
			}// for loop terminate

			// Commit the Direct-Path-Load
			//this->log4j_info(1, "OCIDirPathFinish start");
			if(status=OCIDirPathFinish(
								this->dpchp,
								this->errhp
							)
			){
				this->checkerr(OCI_HTYPE_DIRPATH_CTX, status);
				throw exception("OCIDirPathFinish - failed.");
			}
			//this->log4j_info(1, "OCIDirPathFinish end");
			result = JNI_TRUE;
		}
		catch (exception &ex)
		{
			this->log4j_error(1,ex.what());
			result = false;
		}
		return result;
	}
};

/**************************************************************************
* Main
**************************************************************************/
int main(int argc, char** args)
{
	int result = 0;
	ifstream fin;

	if (argc < 2)
	{
		cout << "Usage: " << args[0] << " [fileName]" << endl;
		result = 1;
	}
	else
	{
		fin.open(args[1]);

		Spark2dpl *phdls = new Spark2dpl(nullptr,nullptr,nullptr,nullptr,nullptr,nullptr);

		phdls->infile = &fin;

		if (phdls->execute()) result = 1;

		fin.close();

		delete phdls;
	}

	return result;
}

/*****************************************************************************************************************
* Class:     Spark2dpl
* Method:    load
* Signature: (Lscala/collection/Iterator;Lorg/apache/spark/sql/types/StructType;Ljava/util/Properties;Lorg/apache/log4j/Logger;)Z
******************************************************************************************************************/
JNIEXPORT jboolean JNICALL Java_Spark2dpl_load
(
	JNIEnv	*envj,
	jobject	thisj, 
	jobject	iterj, 
	jobject	sttpj, 
	jobject	propj, 
	jobject	log4j
)
{
	// Assertion the input arguments
	_ASSERT_EXPR(envj,"envj is nullptr");
	_ASSERT_EXPR(thisj, "thisj is nullptr");
	_ASSERT_EXPR(iterj, "iterj is nullptr");
	_ASSERT_EXPR(sttpj, "sttpj is nullptr");
	_ASSERT_EXPR(propj, "propj is nullptr");
	_ASSERT_EXPR(log4j, "log4j is nullptr");

	// Create Instance Spark2dpl
	Spark2dpl *phdls = new Spark2dpl(envj, thisj, iterj, sttpj, propj, log4j);

	// Execute Spark2dpl
	jboolean result = phdls->execute();

	// Cleanup Spark2dpl
	delete phdls;

	return result;
}

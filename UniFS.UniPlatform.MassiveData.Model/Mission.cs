using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace UniFS.UniPlatform.MassiveData.Model
{
    /// <summary>
    /// 任务Model映射模型
    /// </summary>
    public class Mission
    {
        /// <summary>
        /// 任务唯一标识
        /// </summary>
        string taskId;
        public string TaskId
        {
            get { return taskId; }
        }

        /// <summary>
        /// 将要执行的SQL代码段
        /// </summary>
        string execSql;
        public string ExecSql
        {
            get { return execSql; }
        }

        string dbConnCode;
        public string DbConnCode
        {
            get { return dbConnCode; }
        }

        string dbLinkStr;
        public string DbLinkStr
        {
            get { return dbLinkStr; }
        }

        string taskCode;
        public string TaskCode
        {
            get { return taskCode; }
        }
        /// <summary>
        /// 执行状态
        /// </summary>
        StatusCodeEnum statusCode;
        public StatusCodeEnum StatusCode
        {
            get { return statusCode == null ? StatusCodeEnum.QUEUEING : statusCode; }
            set { statusCode = value; }
            //set { statusCode = String.IsNullOrEmpty(Enum.GetName(typeof(StatusCodeEnum), value)) ? statusCode : value; }
        }
        /// <summary>
        /// 执行状态Enum取值范围
        /// 1-正在排队 2-写文件并执行 3-执行结束-成功 4-执行结束-失败
        /// </summary>
        public enum StatusCodeEnum { QUEUEING, EXECUTING, SUCCESS, FAIL };


        public Mission(string taskid, string execsql, string dbconncode, string dblinkstr ,string taskcode)
        {
            this.taskId = taskid;
            this.execSql = execsql;
            this.dbConnCode = dbconncode;
            this.dbLinkStr = dblinkstr;
            this.taskCode = taskcode;
        }


    }
}

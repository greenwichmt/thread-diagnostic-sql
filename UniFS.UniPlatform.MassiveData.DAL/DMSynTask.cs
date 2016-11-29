using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SysDev.Utility.Security;
using System.Configuration;
using SysDev.Utility.Data;
using Oracle.DataAccess.Client;
using System.Data;
using UniFS.UniPlatform.MassiveData.Model;

namespace UniFS.UniPlatform.MassiveData.DAL
{
    /// <summary>
    /// 同步SQL任务的类
    /// </summary>
    public sealed class DMSynTask
    {
        /// <summary>
        /// 同步归并SQL
        /// 1.执行Pocedure生成任务到dm_exec_task
        /// 2.获取dm_exec_task里的任务到DataTable中
        /// 3.将其放到missionList里
        /// </summary>
        public static void SynTaskMethod(List<Mission> missionList, DMSynTaskParam.TASKTYPE tasktype)
        {
            try
            {
                DataTable dt = null;
                if (tasktype == DMSynTaskParam.TASKTYPE.SYNCHRON)
                {
                    ORAHelper.DoProcedure("P_DM_EXEC_TASK_SYN", null, DMCodeParam.ConnDecryptStr);
                    string synSqlAll = DMSynTaskParam.SynSql + " and parent_task_id is null";
                    dt = ORAHelper.Query(synSqlAll, DMCodeParam.ConnDecryptStr).Tables[0];
                }
                else if (tasktype == DMSynTaskParam.TASKTYPE.GROUP)
                {
                    ORAHelper.DoProcedure("P_DM_GET_GROUP", null, DMCodeParam.ConnDecryptStr);
                    string synSqlAll = DMSynTaskParam.GroupSql + " and parent_task_id is null";
                    dt = ORAHelper.Query(synSqlAll, DMCodeParam.ConnDecryptStr).Tables[0];
                }

                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    int count = 0;
                    count = (from missionObj in missionList
                             where
                                 missionObj.TaskId == dt.Rows[i]["TASK_ID"].ToString()
                             select missionObj).Count();
                    //for (int item = 0; item < missionList.Count; item++)
                    //{
                    //    count += missionList[item].TaskId == dt.Rows[i]["TASK_ID"].ToString() ? 1 : 0;
                    //}
                    if (count <= 0)
                    {
                        Mission missionItem = new Mission(dt.Rows[i]["TASK_ID"].ToString(), dt.Rows[i]["EXEC_SQL"].ToString(), dt.Rows[i]["DB_CONN_CODE"].ToString(), dt.Rows[i]["DB_LINK_STR"].ToString(), dt.Rows[i]["TASK_CODE"].ToString());
                        missionList.Add(missionItem);
                    }
                }
                SynTaskUpdate(tasktype.ToString(), "", "", "");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
            }

        }

        public static void SynDeriveTaskMethod(string synSql, string taskid, List<Mission> missionList)
        {
            try
            {
                string synSqlDerive = synSql + " and parent_task_id = :TASK_ID";
                DataSet ds = ORAHelper.Query(synSqlDerive, DMCodeParam.ConnDecryptStr, new OracleParameter(":TASK_ID", taskid));
                DataTable dt = ds.Tables[0];

                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    int count = 0;
                    count = (from missionObj in missionList
                             where
                                 missionObj.TaskId == dt.Rows[i]["TASK_ID"].ToString()
                             select missionObj).Count();
                    //for (int item = 0; item < missionList.Count; item++)
                    //{
                    //    count += missionList[item].TaskId == dt.Rows[i]["TASK_ID"].ToString() ? 1 : 0;
                    //}
                    if (count <= 0)
                    {
                        Mission missionItem = new Mission(dt.Rows[i]["TASK_ID"].ToString(),
                            dt.Rows[i]["EXEC_SQL"].ToString(),
                            dt.Rows[i]["DB_CONN_CODE"].ToString(), dt.Rows[i]["DB_LINK_STR"].ToString(),
                            dt.Rows[i]["TASK_CODE"].ToString());
                        missionList.Add(missionItem);
                        SynTaskUpdate("6", dt.Rows[i]["TASK_ID"].ToString(), "被父任务触发", "");
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
            }

        }
        /// <summary>
        /// 更新目标表的生成状态
        /// </summary>
        /// <param name="statuscode">状态码</param>
        /// <param name="taskid">任务编号</param>
        /// <param name="loginfo">日志信息</param>
        /// <param name="tablename">目标表名</param>
        public static void SynTaskUpdate(string statuscode, string taskid, string loginfo, string tablename)
        {
            OracleParameter[] parameters =
            {
                new OracleParameter("V_STATUS_CODE",OracleDbType.Varchar2,5),
                new OracleParameter("V_TASK_ID",OracleDbType.Varchar2,20),
                new OracleParameter("V_LOG_INFO",OracleDbType.Varchar2,2000),
                new OracleParameter("V_TABLE_NAME",OracleDbType.Varchar2,20)
            };
            parameters[0].Value = statuscode;
            parameters[1].Value = taskid;
            parameters[2].Value = loginfo;
            parameters[3].Value = tablename;
            ORAHelper.DoProcedure("P_DM_EXEC_TASK_UPDATE", parameters, DMCodeParam.ConnDecryptStr);
        }
    }
}

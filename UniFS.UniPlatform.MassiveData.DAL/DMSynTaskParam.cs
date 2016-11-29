using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using UniFS.UniPlatform.MassiveData.Model;

namespace UniFS.UniPlatform.MassiveData.DAL
{
    public class DMSynTaskParam
    {
        private readonly static string synSql = @"select task_id, exec_sql, db_conn_code, db_link_str, task_code, status_code
                                                    from dm_exec_task t
                                                    where t.status_code = '1'
                                                    and task_id is not null
                                                    and exec_sql is not null
                                                    and db_conn_code is not null
                                                    and db_link_str is not null ";
        /// <summary>
        /// 同步任务SQL
        /// </summary>
        public static string SynSql
        {
            get { return synSql + " and task_type = '001' "; }
        }
        /// <summary>
        /// 归并任务SQL
        /// </summary>
        public static string GroupSql
        {
            get { return synSql + " and task_type = '002' "; }
        }
        public readonly static List<Mission> missionList = new List<Mission>();

        public readonly static List<Mission> missionList2 = new List<Mission>();
        public enum TASKTYPE { SYNCHRON, GROUP }
    }
}

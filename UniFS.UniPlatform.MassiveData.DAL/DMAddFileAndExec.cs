using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using UniFS.UniPlatform.MassiveData.Model;
using System.IO;
using System.Threading;
using Oracle.DataAccess.Client;
using SysDev.Utility.Data;

namespace UniFS.UniPlatform.MassiveData.DAL
{
    public class DMAddFileAndExec
    {
        /// <summary>
        /// 0执行成功-1执行失败-2调用SQLPlus程序执行失败-3读取日志失败-4生成文件失败-5重复创建文件错误
        /// </summary>
        public enum SqlplusResultEnum { ORASUCCESS, ORAERROR, PROCESSERROR, LOGERROR, FILEERROR, FILERECREATE, NOEXECUTE };
        ///<summary>
        /// 生成完整路径->写入文件->执行文件->根据返回正确与否绘制前台Form
        /// </summary>
        /// <param name="runmission">要执行的任务对象</param>
        public static SqlplusResultEnum AddFileExecute(Mission runmission, DMSynTaskParam.TASKTYPE tasktype)
        {
            try
            {
                return CreatePathFile(runmission, tasktype);
            }
            catch (ThreadAbortException)
            {
                Console.WriteLine("+++++线程(" + Thread.CurrentThread.Name + ")停止执行当前任务(" + runmission.TaskId + ")+++++");
                return SqlplusResultEnum.NOEXECUTE;
            }
        }
        /// <summary>
        /// 创建文件路径和SQL文件
        /// </summary>
        /// <param name="mission">任务对象</param>
        public static SqlplusResultEnum CreatePathFile(Mission mission, DMSynTaskParam.TASKTYPE tasktype)
        {
            try
            {
                //获取和设置当前目录（即该进程从中启动的目录）的完全限定路径。
                string path1 = System.Environment.CurrentDirectory;
                string path2 = tasktype == DMSynTaskParam.TASKTYPE.SYNCHRON ? "DMSynFile" : "DMGroupFile";
                string path3 = String.Format("{0}\\{1}\\{2}", DateTime.Now.Year, DateTime.Now.Month.ToString().PadLeft(2, '0'), DateTime.Now.Day.ToString().PadLeft(2, '0'));
                //目录格式 ..\DMSqlFile\2016\06\28\taskid_20160628.txt
                string newpath = System.IO.Path.Combine(path1, path2, path3);
                System.IO.Directory.CreateDirectory(newpath);

                TimeSpan timespan = DateTime.Now - new DateTime(1970, 1, 1);
                string filename = String.Format("{0}_{1}.txt", mission.TaskId, Math.Ceiling(timespan.TotalSeconds).ToString());
                string fullpath = System.IO.Path.Combine(newpath, filename);
                return WriteFile(fullpath, mission);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
                return SqlplusResultEnum.FILERECREATE;
            }
        }

        /// <summary>
        /// 写入要执行的SQL文件
        /// </summary>
        /// <param name="fullpath">文件完整路径</param>
        /// <param name="mission">任务对象</param>
        public static SqlplusResultEnum WriteFile(string fullpath, Mission mission)
        {
            try
            {
                using (FileStream fs = new FileStream(fullpath, FileMode.OpenOrCreate, FileAccess.Write))
                {
                    using (StreamWriter sw = new StreamWriter(fs, Encoding.Default))
                    {
                        sw.WriteLine("set timing on;");
                        sw.WriteLine("set trimspool on;");
                        sw.WriteLine("set trimout on;");
                        string logpath = fullpath.Substring(0, fullpath.Length - 4) + ".log";
                        sw.WriteLine("spool " + logpath + ";");
                        sw.WriteLine(String.Empty);
                        sw.WriteLine("select TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') SQL_BEGIN from dual;");
                        sw.WriteLine(mission.ExecSql);
                        sw.WriteLine("/");
                        sw.WriteLine("select TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') SQL_END from dual;");
                        sw.WriteLine("spool off;");
                        sw.WriteLine("quit;");
                        sw.WriteLine("exit");
                        sw.Close();
                        fs.Close();
                        return ExecSQLFile(fullpath, logpath, mission);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
                return SqlplusResultEnum.FILEERROR;
            }
        }
        /// <summary>
        /// 调用sqlplus程序执行脚本文件
        /// </summary>
        /// <param name="fullpath">脚本文件路径</param>
        /// <param name="logpath">生成日志路径</param>
        /// <param name="mission">任务实例</param>
        /// <returns></returns>
        private static SqlplusResultEnum ExecSQLFile(string fullpath, string logpath, Mission mission)
        {
            try
            {
                using (System.Diagnostics.Process processInstance = new System.Diagnostics.Process())
                {
                    processInstance.StartInfo = new System.Diagnostics.ProcessStartInfo();
                    processInstance.StartInfo.FileName = "sqlplus";
                    processInstance.StartInfo.Arguments = String.Format("{0} @{1}", mission.DbLinkStr, fullpath);
                    processInstance.StartInfo.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;
                    //processInstance.StartInfo.UseShellExecute = false;
                    //processInstance.StartInfo.CreateNoWindow = true;
                    processInstance.Start();
                    //无限期等待关联进程退出
                    processInstance.WaitForExit();
                    processInstance.Close();
                    processInstance.Dispose();
                }
                return CheckLogSucceed(logpath, mission);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
                return SqlplusResultEnum.PROCESSERROR;
            }
        }

        private static SqlplusResultEnum CheckLogSucceed(string logpath, Mission mission)
        {
            try
            {
                using (FileStream fs = new FileStream(logpath, FileMode.Open, FileAccess.Read))
                {
                    StreamReader sr = new StreamReader(fs, Encoding.Default);
                    string readcontent = sr.ReadToEnd().Trim();
                    sr.Close();
                    if (readcontent.IndexOf("ORA-") < 0)
                    {
                        return SqlplusResultEnum.ORASUCCESS;
                    }
                    else
                    {
                        string loginfo = "脚本错误--" + (String.IsNullOrWhiteSpace(readcontent) ? "执行产生空日志" : readcontent.Substring(0, readcontent.Length >= 2000 ? 1950 : readcontent.Length));
                        DMSynTask.SynTaskUpdate("4", mission.TaskId, loginfo, "");
                        return SqlplusResultEnum.ORAERROR;
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
                return SqlplusResultEnum.LOGERROR;
            }
        }
    }
}

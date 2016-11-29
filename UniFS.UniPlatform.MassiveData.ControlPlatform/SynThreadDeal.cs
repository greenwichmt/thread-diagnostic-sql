using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using UniFS.UniPlatform.MassiveData.Model;
using UniFS.UniPlatform.MassiveData.DAL;
using Oracle.DataAccess.Client;
using SysDev.Utility.Data;

namespace UniFS.UniPlatform.MassiveData.ControlPlatform
{
    sealed class SynThreadDeal
    {
        #region 类线程字段
        internal readonly static List<Thread> threadList = new List<Thread>();
        #endregion

        #region 类委托及事件字段
        /// <summary>
        /// 读取任务委托，每读取一条任务进行的操作
        /// </summary>
        /// <param name="mission">任务实例</param>
        internal delegate void MissionHandler(Mission mission);
        /// <summary>
        /// 执行任务委托，没执行一条任务进行的操作
        /// </summary>
        /// <param name="mission">任务实例</param>
        internal delegate void MissionExecHandler(Mission mission);
        /// <summary>
        /// 任务完成后委托，当任务完成后操作
        /// </summary>
        /// <param name="mission">任务实例</param>
        internal delegate void MissionCompleteHandler(Mission mission);
        /// <summary>
        /// 任务完成后委托事件
        /// </summary>
        internal static event MissionCompleteHandler MissionComplete;
        /// <summary>
        /// 任务失败后委托，当任务失败后操作
        /// </summary>
        /// <param name="mission">任务实例</param>
        internal delegate void MissionFailHandler(Mission mission);

        internal static event MissionFailHandler MissionFail;
        #endregion

        /// <summary>
        /// 开始从数据库读取任务线程
        /// </summary>
        /// <param name="readMissionHandler">读取任务后执行操作</param>
        internal static void StartReadMission(MissionHandler readMissionHandler)
        {
            Thread readMissionThread = new Thread(() =>
             {
                 DoReadMission(readMissionHandler);
             });
            threadList.Add(readMissionThread);
            readMissionThread.Start();  //开始从数据库读取任务的线程
        }

        /// <summary>
        /// 开始执行任务线程
        /// </summary>
        /// <param name="missionExecHandler">执行任务时执行操作</param>
        internal static void StartExecMission(MissionExecHandler missionExecHandler)
        {
            Thread execMissionThread = new Thread(() =>
               {
                   AddFileExecMethod(missionExecHandler);
               });
            threadList.Add(execMissionThread);
            execMissionThread.Start();  //开始线程
        }

        /// <summary>
        /// 从数据库读取任务操作方法
        /// </summary>
        /// <param name="readMissionHandler">读取任务后执行操作</param>
        private static void DoReadMission(MissionHandler readMissionHandler)
        {
            try
            {
                while (true)
                {
                    lock (DMSynTaskParam.missionList)//锁任务集合对象
                    {
                        DMSynTask.SynTaskMethod(DMSynTaskParam.missionList, DMSynTaskParam.TASKTYPE.SYNCHRON);//同步任务到missionList中
                        if (readMissionHandler != null)
                        {
                            //foreach (Mission mission in DMSynTaskParam.missionList)
                            //    readMissionHandler(mission);
                            for (int i = 0; i < DMSynTaskParam.missionList.Count; i++)
                                readMissionHandler(DMSynTaskParam.missionList[i]);//绘制任务到前台界面
                        }
                        //Console.WriteLine(DMSynTaskParam.missionList.ToString());
                    }
                    Thread.Sleep(10000);//每10秒同步任务到missionList中
                }
            }
            catch (ThreadAbortException)
            {
                Console.WriteLine("=====线程(" + Thread.CurrentThread.Name + ")停止同步任务到missionList中=====");
            }
        }

        /// <summary>
        /// 执行任务方法
        /// </summary>
        /// <param name="missionExecHandler">执行任务时执行操作</param>
        private static void AddFileExecMethod(MissionExecHandler missionExecHandler)
        {
            try
            {
                while (true)
                {
                    lock (DMSynTaskParam.missionList)    //锁任务集合
                    {
                        int curCount = (from missionObj in DMSynTaskParam.missionList
                                        where missionObj.StatusCode == Mission.StatusCodeEnum.EXECUTING
                                        select missionObj).Count();     //查询有多少任务正在执行
                        //可执行任务数
                        string maxTaskCount = DMCodeParam.GetStaticParam("CONTROL_SYN_TASK_MAX") ?? "0";
                        int restCount = (Int32.Parse(maxTaskCount) - curCount) > 0 ? (Int32.Parse(maxTaskCount) - curCount) : 0;
                        restCount = restCount > DMSynTaskParam.missionList.Count ? DMSynTaskParam.missionList.Count : restCount;
                        if (restCount > 0)
                        {
                            //Linq查询可执行任务实例
                            Mission[] missions = (from missionObj in DMSynTaskParam.missionList
                                                  where missionObj.StatusCode == Mission.StatusCodeEnum.QUEUEING
                                                  select missionObj).Take<Mission>(restCount).ToArray<Mission>();
                            foreach (Mission mission in missions)
                            {
                                mission.StatusCode = Mission.StatusCodeEnum.EXECUTING;  //把任务实例状态设置为EXECUTING正在执行
                                Mission runMission = mission;
                                if (missionExecHandler != null)     //执行任务时执行操作
                                {
                                    missionExecHandler(mission);
                                }
                                //新建线程真正执行同步数据操作
                                Thread runMissionThread = new Thread(() =>
                                {
                                    DoExecuteSql(runMission);
                                });
                                runMissionThread.Name = runMission.TaskId;
                                threadList.Add(runMissionThread);
                                runMissionThread.Start();
                            }
                            //循环一轮删除missionList里非排队状态的mission;
                            //lock (DMSynTaskParam.missionList)
                            //{
                            //    //if (DMSynTaskParam.missionList.Contains(runMission)) { DMSynTaskParam.missionList.Remove(runMission); }
                            //    for (int i = DMSynTaskParam.missionList.Count - 1; i > 0; i--)
                            //    {
                            //        if (DMSynTaskParam.missionList[i].StatusCode != Mission.StatusCodeEnum.QUEUEING)
                            //        {
                            //            DMSynTaskParam.missionList.RemoveAt(i);
                            //        }
                            //    }
                            //    Console.WriteLine("这轮循环后missionList里剩下个数："+DMSynTaskParam.missionList.Count);
                            //}
                            Thread.Sleep(1000);
                        }
                    }
                    Thread.Sleep(3000);
                }
            }
            catch (ThreadAbortException)
            {
                Console.WriteLine("+++++线程(" + Thread.CurrentThread.Name + ")停止检测未执行的任务+++++");
            }
        }

        private static void DoExecuteSql(Mission runMission)
        {
            //真正执行sql文件
            DMAddFileAndExec.SqlplusResultEnum resultEnum = DMAddFileAndExec.AddFileExecute(runMission, DMSynTaskParam.TASKTYPE.SYNCHRON);
            //只有返回Enum结果为ORASUCCESS才在前台绘制成功，否则绘制失败
            lock (DMSynTaskParam.missionList)
            {
                if (resultEnum == DMAddFileAndExec.SqlplusResultEnum.ORASUCCESS)
                {
                    DMSynTask.SynDeriveTaskMethod(DMSynTaskParam.SynSql, runMission.TaskId, DMSynTaskParam.missionList);   //读取子任务
                    runMission.StatusCode = Mission.StatusCodeEnum.SUCCESS;
                    if (MissionComplete != null)
                    {
                        MissionComplete(runMission);
                    }
                    DMSynTask.SynTaskUpdate("3", runMission.TaskId, "成功", runMission.TaskCode);
                }
                else
                {
                    runMission.StatusCode = Mission.StatusCodeEnum.FAIL;
                    if (MissionFail != null)
                    {
                        MissionFail(runMission);
                    }
                    if (resultEnum != DMAddFileAndExec.SqlplusResultEnum.ORAERROR)
                    {
                        DMSynTask.SynTaskUpdate("4", runMission.TaskId, "程序错误--未生成文件或日志", runMission.TaskCode);
                    }
                }
                DMSynTaskParam.missionList.Remove(runMission);
            }
        }
        /// <summary>
        /// 终止所有线程并清空线程List
        /// </summary>
        public static void AbortAllThread()
        {
            foreach (Thread thread in threadList)
            {
                try
                {
                    thread.Abort();
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.StackTrace);
                }
            }
            threadList.Clear();
        }
    }
}

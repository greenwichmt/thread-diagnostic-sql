using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using UniFS.UniPlatform.MassiveData.DAL;
using System.Windows.Forms;
using System.Diagnostics;

namespace UniFS.UniPlatform.MassiveData.ControlPlatform
{
    public class testTask
    {
        public static void Main(string[] args)
        {
            try
            {
                //设置应用程序异常处理方式：强制ThreadException处理，忽略配置文件
                Application.SetUnhandledExceptionMode(UnhandledExceptionMode.CatchException);
                //处理UI主线程异常
                Application.ThreadException += new System.Threading.ThreadExceptionEventHandler(Application_ThreadException);
                //处理非UI当前域线程异常
                AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(CurrentDomain_UnhandledException);
                #region 应用程序的主入口
                if (Process.GetProcessesByName(Process.GetCurrentProcess().ProcessName).Length > 1)
                {
                    MessageBox.Show("程序已经运行了一个实例，该程序只允许有一个实例！");
                    return;
                }
                DMCodeParam.FetchParamMethod();//取全局配置
                Application.EnableVisualStyles();
                Application.SetCompatibleTextRenderingDefault(false);
                Application.Run(new MissionForm());
                #endregion
            }
            catch (Exception e)
            {
                string exceptionMessage = GetExceptionMsg(e, "Main函数出错");
                MessageBox.Show(exceptionMessage, "系统错误", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }

        private static void Application_ThreadException(object sender, System.Threading.ThreadExceptionEventArgs e)
        {
            String UIExceptionMessage = GetExceptionMsg(e.Exception, string.Empty);
            MessageBox.Show(UIExceptionMessage, "系统主线程错误", MessageBoxButtons.OK, MessageBoxIcon.Error);
        }

        private static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            String DomainExceptionMessage = GetExceptionMsg(e.ExceptionObject as Exception, string.Empty);
            MessageBox.Show(DomainExceptionMessage, "系统当前域错误", MessageBoxButtons.OK, MessageBoxIcon.Error);
        }
        /// <summary>
        /// 自定义返回异常消息
        /// </summary>
        /// <param name="ex">异常对象</param>
        /// <param name="backString">备用异常消息</param>
        /// <returns>异常消息string</returns>
        static string GetExceptionMsg(Exception ex, String backString)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("****************************异常文本****************************");
            sb.AppendLine("【出现时间】：" + DateTime.Now.ToString());
            if (ex != null)
            {
                string exTypeStr = "【异常类型】：" + ex.GetType().Name;
                string exMessageStr = "【异常信息】：" + ex.Message;
                string exStackTraceStr = "【堆栈调用】：" + ex.StackTrace;
                sb.AppendLine(exTypeStr);
                sb.AppendLine(exMessageStr);
                sb.AppendLine(exStackTraceStr);
                SaveLogManager.SaveLog("DMSynGroup", backString + exTypeStr + exMessageStr + exStackTraceStr);
            }
            else
            {
                string exNullStr = "【未处理异常】：" + backString;
                sb.AppendLine(exNullStr);
                SaveLogManager.SaveLog("DMSynGroup", exNullStr);
            }
            sb.AppendLine("***************************************************************");
            return sb.ToString();
        }
    }
}

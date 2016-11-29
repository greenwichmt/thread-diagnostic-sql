using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Oracle.DataAccess.Client;
using UniFS.UniPlatform.MassiveData.DAL;

namespace UniFS.UniPlatform.MassiveData.ControlPlatform
{
    public class SaveLogManager
    {
        public static void SaveLog(string exType, string exMessage)
        {
            OracleConnection conn = new OracleConnection(DMCodeParam.ConnDecryptStr);
            try
            {
                conn.Open();
                OracleTransaction tran = conn.BeginTransaction();
                String insertSql = "INSERT INTO DM_LOG_DISPATCHER SELECT S_LOG_ID.NEXTVAL,'" + exType + "','" + exMessage + "',SYSDATE FROM DUAL";
                OracleCommand cmd = conn.CreateCommand();
                cmd.CommandText = insertSql;
                cmd.ExecuteNonQuery();
                tran.Commit();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace.ToString());
            }
            finally
            {
                conn.Close();
            }
        }
    }
}

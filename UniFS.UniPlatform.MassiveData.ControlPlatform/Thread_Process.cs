using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Data;
using Oracle.DataAccess;
using Oracle.DataAccess.Client;
using SysDev.Utility.Data;
using UniFS.UniPlatform.MassiveData.ControlPlatform.App_Code;
using System.IO;
using System.Diagnostics;//引用执行cmd文件


namespace UniFS.UniPlatform.MassiveData.ControlPlatform
{
                 
    public class Thread_Process
    {
        public delegate void FileCreateHandler(string fileName);//定义一个委托
        public event FileCreateHandler FileCreate;//定义事件

        public delegate void AddSubFormHandler();//定义一个委托
        public event AddSubFormHandler AddTabSubFrom;

        string fileName;

        public void ScanDatabase()
        {
            string oraSqlStr;//定义一个字符串型的数组
            string strString;

            //打开数据库
            DBConnection.OpenConnection();
            strString = DBConnection.GetSyncSql();
            DataSet ds = DBConnection.SQLQuery(strString);//生成数据集

            DataTable dt=ds.Tables[0];
               
            foreach(DataRow row in dt.Rows)
            {
                if (!string.IsNullOrWhiteSpace(row["EXE_SQL"].ToString()))
                {  
                    WriteSqlFile(row["TABLE_NAME"].ToString(), row["EXE_SQL"].ToString(),true);
                }
                

                if (!string.IsNullOrWhiteSpace(row["db_info"].ToString()))
                {
                    oraSqlStr = "sqlplus.exe " + row["db_info"].ToString() + " @";//+row["TABLE_NAME"].ToString()+".txt"
                    fileName = WriteSqlFile(row["TABLE_NAME"].ToString(), oraSqlStr,false);

                    if (FileCreate != null && AddTabSubFrom != null)
                    {
                        AddTabSubFrom(); //生成一个tabcontrol下的子窗体
                        FileCreate(fileName);
                        
                    }
                       
                }
                    
            }

        }  
           
         /// <summary>
         /// 写文本文件
         /// </summary>
        public string WriteSqlFile(string tableName, string value, bool bool_len)
        {
            
               string filePath = string.Format(@"{0}\{1}\{2}\{3}", 
               System.Environment.CurrentDirectory,
               DateTime.Now.Year, DateTime.Now.ToString("yyyyMM"), 
               DateTime.Now.ToString("yyyyMMdd"));


            if (!Directory.Exists(filePath))
            {
                Directory.CreateDirectory(filePath);
            }

            if (bool_len == true)
            {
                //value.Substring();
                value = value.Replace("$PATH", filePath);
                filePath += "\\" + tableName + ".txt";
            }
            else
            {
                value += filePath + "\\" + tableName + ".txt";
                filePath += "\\" + tableName + ".cmd";
            }
            FileStream f = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite);
            StreamWriter sw = new StreamWriter(f, Encoding.Default);

            sw.WriteLine(value);
            sw.Flush();
            sw.Close();
            f.Close();

            return filePath;

        }



    }
}

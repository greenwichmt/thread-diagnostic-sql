using System;
using SysDev.Utility.Security;
using SysDev.Utility.Data;
using System.Data;
using System.Collections.Generic;
using System.Configuration;

namespace UniFS.UniPlatform.MassiveData.DAL
{
    public class DMCodeParam
    {
        /// <summary>
        /// 设置查询sql字段
        /// </summary>
        private readonly static string staticPramSql = "SELECT param_code,param_value FROM DM_SYS_PARAM_DETAIL where static_flag='1'";
        private static Dictionary<string, string> paramDictionary = new Dictionary<string, string>();

        private static Dictionary<string, string> ParamDictionary
        {
            get { return DMCodeParam.paramDictionary; }
        }
        /// <summary>
        /// 数据库连接字符串-已解密
        /// </summary>
        private readonly static string connDecryptStr = Security.Decrypt(ConfigurationManager.AppSettings[ConstParam.DATASHOWCONN]);

        public static string ConnDecryptStr
        {
            get { return DMCodeParam.connDecryptStr; }
        }

        /// <summary>
        /// 线程方法：查询数据库并将其添加到Dictionary中作为全局配置参数保存
        /// </summary>
        public static void FetchParamMethod()
        {
            try
            {
                DataSet ds = ORAHelper.Query(staticPramSql, connDecryptStr);
                DataTable dt = ds.Tables[0];
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    paramDictionary[dt.Rows[i]["PARAM_CODE"].ToString()] = dt.Rows[i]["PARAM_VALUE"].ToString();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
            }
        }
        /// <summary>
        /// 查询全局配置参数，默认为null
        /// </summary>
        /// <param name="key">要查询的键值</param>
        /// <returns></returns>
        public static string GetStaticParam(string key)
        {
            return paramDictionary.ContainsKey(key) ? paramDictionary[key] : null;
        }
    }
}

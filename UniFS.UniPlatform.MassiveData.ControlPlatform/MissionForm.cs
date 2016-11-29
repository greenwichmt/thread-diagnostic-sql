using System;
using System.Data;
using System.Threading;
using System.Windows.Forms;
using SysDev.Utility.Data;
using UniFS.UniPlatform.MassiveData.DAL;
using UniFS.UniPlatform.MassiveData.Model;

namespace UniFS.UniPlatform.MassiveData.ControlPlatform
{
    public partial class MissionForm : Form
    {
        public MissionForm()
        {
            InitializeComponent();
        }


        private void MissionForm_Load(object sender, EventArgs e)
        {
            SynThreadDeal.MissionComplete += OnMissionComplete;
            SynThreadDeal.MissionFail += OnMissionFail;
            GroupThreadDeal.MissionComplete += OnMissionComplete2;
            GroupThreadDeal.MissionFail += OnMissionFail2;
        }

        #region 同步页tab
        protected void OnMissionStart(Mission mission)
        {
            //数据更新，UI暂时挂起，直到EndUpdate绘制控件，可以有效避免闪烁并大大提高加载速度
            //this.taskListView.BeginUpdate();
            listView1.Invoke(new Action(() =>
            {
                if (!this.listView1.Items.ContainsKey(mission.TaskId))
                {
                    ListViewItem lvi = new ListViewItem(mission.TaskId);
                    lvi.Name = mission.TaskId;
                    lvi.SubItems.Add(mission.DbConnCode);
                    lvi.SubItems.Add(mission.StatusCode.ToString());
                    lvi.SubItems.Add(mission.TaskCode);
                    lvi.SubItems.Add("-");
                    lvi.SubItems.Add("-");
                    listView1.Items.Add(lvi);
                    lvi.EnsureVisible();
                }
                if (listView1.Items.Count > 100) listView1.Items[0].Remove();
            }));
        }

        protected void OnMissionExec(Mission mission)
        {
            listView1.Invoke(new Action(() =>
            {
                try
                {
                    this.listView1.Items[mission.TaskId].SubItems[2].Text = Mission.StatusCodeEnum.EXECUTING.ToString();
                    this.listView1.Items[mission.TaskId].SubItems[4].Text = System.DateTime.Now.ToString().Substring(5);
                }
                catch { }
            }));
        }

        protected void OnMissionComplete(Mission mission)
        {
            listView1.Invoke(new Action(() =>
            {
                try
                {
                    this.listView1.Items[mission.TaskId].SubItems[2].Text = Mission.StatusCodeEnum.SUCCESS.ToString();
                    this.listView1.Items[mission.TaskId].SubItems[5].Text = System.DateTime.Now.ToString().Substring(5);
                }
                catch { }
            }));
        }

        protected void OnMissionFail(Mission mission)
        {
            listView1.Invoke(new Action(() =>
            {
                try
                {
                    this.listView1.Items[mission.TaskId].SubItems[2].Text = Mission.StatusCodeEnum.FAIL.ToString();
                    this.listView1.Items[mission.TaskId].SubItems[5].Text = System.DateTime.Now.ToString().Substring(5);
                }
                catch { }
            }));
        }

        //开启线程
        private void button1_Click(object sender, EventArgs e)
        {
            button1.Enabled = false;
            SynThreadDeal.StartReadMission(OnMissionStart);
            SynThreadDeal.StartExecMission(OnMissionExec);
            button2.Enabled = true;
        }

        //停止线程
        private void button2_Click(object sender, EventArgs e)
        {
            button2.Enabled = false;
            SynThreadDeal.AbortAllThread();
            button1.Enabled = true;
        }
        #endregion


        #region 归并页tab
        protected void OnMissionStart2(Mission mission)
        {
            listView2.Invoke(new Action(() =>
            {
                if (!this.listView2.Items.ContainsKey(mission.TaskId))
                {
                    ListViewItem lvi = new ListViewItem(mission.TaskId);
                    lvi.Name = mission.TaskId;
                    lvi.SubItems.Add(mission.DbConnCode);
                    lvi.SubItems.Add(mission.StatusCode.ToString());
                    lvi.SubItems.Add(mission.TaskCode);
                    lvi.SubItems.Add("-");
                    lvi.SubItems.Add("-");
                    listView2.Items.Add(lvi);
                    lvi.EnsureVisible();
                }
                if (listView2.Items.Count > 100) listView2.Items[0].Remove();
            }));
        }

        protected void OnMissionExec2(Mission mission)
        {
            listView2.Invoke(new Action(() =>
            {
                try
                {
                    this.listView2.Items[mission.TaskId].SubItems[2].Text = Mission.StatusCodeEnum.EXECUTING.ToString();
                    this.listView2.Items[mission.TaskId].SubItems[4].Text = System.DateTime.Now.ToString().Substring(5);
                }
                catch { }
            }));
        }

        protected void OnMissionComplete2(Mission mission)
        {
            listView2.Invoke(new Action(() =>
            {
                try
                {
                    this.listView2.Items[mission.TaskId].SubItems[2].Text = Mission.StatusCodeEnum.SUCCESS.ToString();
                    this.listView2.Items[mission.TaskId].SubItems[5].Text = System.DateTime.Now.ToString().Substring(5);
                }
                catch { }
            }));
        }

        protected void OnMissionFail2(Mission mission)
        {
            listView2.Invoke(new Action(() =>
            {
                try
                {
                    this.listView2.Items[mission.TaskId].SubItems[2].Text = Mission.StatusCodeEnum.FAIL.ToString();
                    this.listView2.Items[mission.TaskId].SubItems[5].Text = System.DateTime.Now.ToString().Substring(5);
                }
                catch { }
            }));
        }

        //开启线程
        private void button3_Click(object sender, EventArgs e)
        {
            button3.Enabled = false;
            GroupThreadDeal.StartReadMission(OnMissionStart2);
            GroupThreadDeal.StartExecMission(OnMissionExec2);
            button4.Enabled = true;
        }

        //停止线程
        private void button4_Click(object sender, EventArgs e)
        {
            button4.Enabled = false;
            GroupThreadDeal.AbortAllThread();
            button3.Enabled = true;
        }
        #endregion

        private void MissionForm_FormClosing(object sender, FormClosingEventArgs e)
        {
            DialogResult dr = MessageBox.Show("终止所有线程并退出程序？", "提示消息：", MessageBoxButtons.OKCancel, MessageBoxIcon.Warning);
            if (dr == DialogResult.OK)
            {
                SynThreadDeal.AbortAllThread();
                GroupThreadDeal.AbortAllThread();
                this.Dispose(true);
                Application.Exit();
            }
            else
            {
                e.Cancel = true;
                this.WindowState = FormWindowState.Minimized;
                this.Visible = true;
            }
        }

        private void button5_Click(object sender, EventArgs e)
        {
            while (listView3.Items.Count > 0) { listView3.Items[0].Remove(); }
            String sql1 = "";
            String sql_template = @" union select task_id col1,
                                          task_code col2,
                                          t.status_code || '-' ||
                                          (case status_code
                                            when '4' then
                                             SUBSTR(T.MEMO, 1, 5) ||
                                             SUBSTR(T.MEMO,
                                                    regexp_instr(t.memo,'ORA-[[:digit:]]{5}:'))
                                            else
                                             t.memo
                                          end) col3,
                                          finish_time col4
                                     from DM_EXEC_TASK T
                                    WHERE t.status_code not in ('1', '3') ";
            if (checkBox1.Checked) sql1 += sql_template + " and task_type='001' ";
            if (checkBox2.Checked) sql1 += sql_template + " and task_type='002' ";
            if (checkBox3.Checked)
            {
                sql1 += @" union
                         select log_id col1, log_type col2, log_info col3, create_time col4
                           from DM_LOG_DISPATCHER t ";
            }
            //无查询项则退出
            if (String.IsNullOrWhiteSpace(sql1)) return;

            sql1 = sql1.Trim().Substring(6);
            int shownum = 0;
            try
            {
                int parsenum = Int32.Parse(textBox1.Text);
                shownum = parsenum < 1 || parsenum > 500 ? 20 : parsenum;
            }
            catch { shownum = 20; }
            //Console.WriteLine(shownum);
            String sql = "select * from (select * from (" + sql1 + ") order by col1 desc,col4 desc) where rownum<=" + shownum;
            DataTable dt = ORAHelper.Query(sql, DMCodeParam.ConnDecryptStr).Tables[0];
            if (dt.Rows.Count > 0)
            {
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    ListViewItem lvi = new ListViewItem(dt.Rows[i][0].ToString());
                    lvi.SubItems.Add(dt.Rows[i][1].ToString());
                    lvi.SubItems.Add(dt.Rows[i][2].ToString());
                    lvi.SubItems.Add(dt.Rows[i][3].ToString());
                    listView3.Items.Add(lvi);
                }
            }

        }

        private void button6_Click(object sender, EventArgs e)
        {
            String input_task_id = textBox2.Text.Trim();
            if (String.IsNullOrEmpty(input_task_id)) { MessageBox.Show("任务编号不能为空！"); return; }
            String queryexists = "select * from DM_EXEC_TASK t where t.task_id='" + input_task_id + "'";
            DataSet ds = ORAHelper.Query(queryexists,DMCodeParam.ConnDecryptStr);
            //Console.WriteLine(ds.Tables[0].Rows.Count);
            if (ds.Tables[0].Rows.Count <= 0)
            {
                MessageBox.Show("您输入的任务编号\"" + input_task_id + "\"不存在！");
                return;
            }
            String updatesql = "update DM_EXEC_TASK t set t.status_code='1',t.memo='client-reset' where t.task_id='" + input_task_id + "'";
            ORAHelper.ExecuteScalar(updatesql, DMCodeParam.ConnDecryptStr);
            MessageBox.Show("您输入的任务编号\"" + input_task_id + "\"已重新排队等待执行！");
        }
    }
}

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using Oracle.ManagedDataAccess.Client;
using System.Data.SqlClient;
using System.Configuration;


namespace Practice1
{
    public partial class Form1 : Form
    {
        Timer formClose = new Timer();
        public static string DefaultConnectionName = "strCon";
        public static string connString = System.Configuration.ConfigurationManager.ConnectionStrings[DefaultConnectionName].ConnectionString;
        string con = "Data Source=(DESCRIPTION =(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST = 192.168.3.55)(PORT = 1521)))(CONNECT_DATA =(SERVICE_NAME = PROD)));User ID=appsro;Password=appsro;";
        DataTable receiving = new DataTable();
        DataTable osipos_pricelist = new DataTable();
        DataTable general = new DataTable();
        DataTable pricelist = new DataTable();
        DataTable imo_items = new DataTable();
        DataTable vendoritems = new DataTable();
        string datenow = DateTime.Now.Date.ToShortDateString();

        public Form1()
        {
            InitializeComponent();
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            
        }

        private void RECEIVING_GET()
        {
            OracleConnection con2 = new OracleConnection(con);
            con2.Open();

            OracleCommand cmd = new OracleCommand();
            cmd.Connection = con2;
            cmd.CommandText = @"SELECT 
                            DISTINCT ( msib.segment1 ) AS ebsbarcode,
                            (SELECT mcb.segment1 
                                FROM 
                                    apps.mtl_category_sets_tl mcst, 
                                    apps.mtl_item_categories mic, 
                                    apps.mtl_categories_b_kfv mcb 
                                WHERE
                                1 = 1 AND 
                            mic.category_set_id = mcst.category_set_id AND mic.category_id = mcb.category_id AND 
                            mic.inventory_item_id = msib.inventory_item_id AND 
                            mic.organization_id = rsl.to_organization_id AND
                            mcst.category_set_name = 'CH Legacy Or Alternate Barcode') legacybarcode, rvhv.CREATION_DATE, rvhv.receipt_num,rvhv.shipment_num,rsl.shipment_line_id,rsl.shipment_header_id,rsl.line_num,rsl.quantity_shipped, rsl.quantity_received , msib.attribute1 , msib.attribute2 , msib.attribute3 , rsl.item_id, rsl.shipment_line_status_code, rsl.to_organization_id FROM rcv_vrc_hds_v rvhv,rcv_shipment_lines rsl,mtl_system_items_b msib WHERE rvhv.shipment_header_id = rsl.shipment_header_id AND 
                            msib.inventory_item_id = rsl.item_id AND
                            rsl.to_organization_id = msib.organization_id  AND 
                            rvhv.creation_date BETWEEN TRUNC(SYSDATE) -5 AND TRUNC(SYSDATE) +1 AND 
                            rsl.shipment_line_status_code IN ('FULLY RECEIVED','PARTIALLY RECEIVED' )
                            AND ROWNUM <= 100000
";
            cmd.CommandType = CommandType.Text;
            OracleDataReader dr = cmd.ExecuteReader();

            //while (dr.Read())
            //{
            //    string c1 = dr.GetValue(0).ToString();
            //    string c2 = dr.GetValue(1).ToString();
            //    string c3 = dr.GetValue(2).ToString();
            //    string c4 = dr.GetValue(3).ToString();
            //    string c5 = dr.GetValue(4).ToString();
            //    string c6 = dr.GetValue(5).ToString();
            //    string c7 = dr.GetValue(6).ToString();
            //    string c8 = dr.GetValue(7).ToString();
            //    string c9 = dr.GetValue(8).ToString();
            //    string c10 = dr.GetValue(9).ToString();
            //    string c11 = dr.GetValue(10).ToString();
            //    string c12 = dr.GetValue(11).ToString();
            //    string c13 = dr.GetValue(12).ToString();
            //    string c14 = dr.GetValue(13).ToString();
            //    string c15 = dr.GetValue(14).ToString();
            //    string c16 = dr.GetValue(15).ToString();
            //    dgreceiving.Rows.Add(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16);
            //}

            receiving.Load(dr);
            con2.Close();
        }

        private void OSIPOS_PRICELIST_GET()
        {
            OracleConnection con2 = new OracleConnection(con);
            con2.Open();
            
            OracleCommand cmd = new OracleCommand();
            cmd.Connection = con2;
            cmd.CommandText = @"SELECT 
                                   item_id,
                                   selling_price,
                                   price_list_name,
                                   store_id,
                                   TO_CHAR(active_from,'YYYY-MM-DD'),
                                   TO_CHAR(active_till,'YYYY-MM-DD'),
                                   TO_CHAR(created_date,'YYYY-MM-DD')
                                FROM 
                                   osipos_store_item_price_list
                                WHERE
                                   created_date BETWEEN TRUNC(SYSDATE) -5 AND TRUNC(SYSDATE) +1";
            cmd.CommandType = CommandType.Text;

            OracleDataReader dr = cmd.ExecuteReader();
            osipos_pricelist.Load(dr);
            con2.Close();
        }

        private void GENERAL_GET()
        {
            OracleConnection con2 = new OracleConnection(con);
            con2.Open();

            OracleCommand cmd = new OracleCommand();
            cmd.Connection = con2;
            cmd.CommandText = @"SELECT
                                    DISTINCT ( msib.segment1 ) AS ebsbarcode,
                                    (SELECT mcb.segment1 From apps.mtl_category_sets_tl mcst,apps.mtl_item_categories mic,apps.mtl_categories_b_kfv mcb  Where 1 = 1 AND mic.category_set_id = mcst.category_set_id AND mic.category_id = mcb.category_id AND mic.inventory_item_id = msib.inventory_item_id  AND mic.organization_id = osipl.store_id AND mcst.category_set_name = 'CH Legacy Or Alternate Barcode') legacybarcode,
                                    msib.attribute1,
                                    msib.attribute2,
                                    msib.attribute3,
                                    osipl.selling_price,
                                    osipl.item_id,
                                    osipl.store_id,
                                    TO_CHAR(osipl.created_date,'YYYY-MM-DD'),
                                    TO_CHAR(osipl.active_from,'YYYY-MM-DD'),
                                    TO_CHAR(osipl.transmission_date,'YYYY-MM-DD')
                                FROM 
                                    apps.mtl_system_items_b msib, osipos_store_item_price_list osipl        
                                WHERE 
                                    msib.inventory_item_id = osipl.item_id AND msib.organization_id = osipl.store_id  AND TO_CHAR(osipl.active_from,'YYYY-MM-DD') <= TO_CHAR(SYSDATE, 'YYYY-MM-DD')  AND osipl.created_date BETWEEN TRUNC(SYSDATE) -5 AND TRUNC(SYSDATE) +1 ";
            cmd.CommandType = CommandType.Text;
            OracleDataReader dr = cmd.ExecuteReader();
            general.Load(dr);
            con2.Close();
        }

        private void PRICELIST_TBL()
        {
            OracleConnection con2 = new OracleConnection(con);
            con2.Open();

            OracleCommand cmd = new OracleCommand();
            cmd.Connection = con2;
            cmd.CommandText = "SELECT DISTINCT(pl.barcode),pl.price_list_name,msi.attribute1,msi.attribute2,msi.attribute3,  pl.SELLING_PRICE , msi.inventory_item_id, pl.creation_date, pl.batch_number, pl.start_date  FROM xxch_price_list_load_t_copy pl,mtl_system_items_b msi  Where pl.barcode = msi.SEGMENT1 AND msi.organization_id = '87' AND pl.STATUS = 'Approved'  AND pl.creation_date BETWEEN TRUNC(SYSDATE)-5 AND TRUNC(SYSDATE) +1";
            cmd.CommandType = CommandType.Text;
            OracleDataReader dr = cmd.ExecuteReader();
            pricelist.Load(dr);
            con2.Close();
        }

        private void IMO_PRICELIST_TBL()
        {
            OracleConnection con2 = new OracleConnection(con);
            con2.Open();

            OracleCommand cmd = new OracleCommand();
            cmd.Connection = con2;
            cmd.CommandText = @"SELECT
                                    msi.inventory_item_id, 
                                    msi.organization_id,
                                    msi.segment1,
                                    msi.attribute1,
                                    msi.attribute2,
                                    msi.attribute3,
                                    msi.primary_uom_code,
                                    msi.primary_unit_of_measure,
                                    msi.INVENTORY_ITEM_STATUS_CODE,
                                    msi.item_type,
                                    (SELECT mcb.segment1 From mtl_category_sets_tl mcst,mtl_item_categories mic,mtl_categories_b_kfv mcb Where 1 = 1 And mic.category_set_id = mcst.category_set_id AND mic.category_id = mcb.category_id AND mic.inventory_item_id = msi.inventory_item_id AND mic.organization_id = msi.organization_id AND mcst.category_set_name = 'CH Legacy Or Alternate Barcode') legacybarcode
                                FROM
                                    mtl_system_items_b msi
                                WHERE
                                    msi.organization_id = '87' AND
                                    msi.INVENTORY_ITEM_STATUS_CODE='Active'";
            cmd.CommandType = CommandType.Text;
            OracleDataReader dr = cmd.ExecuteReader();
            System.Data.DataColumn newColumn = new System.Data.DataColumn("Foo", typeof(System.Int64));
            newColumn.DefaultValue = null;
            imo_items.Columns.Add(newColumn);
            imo_items.Load(dr);
            con2.Close();
        }

        private void VENDOR_ITEMS()
        {
            OracleConnection con2 = new OracleConnection(con);
            con2.Open();

            OracleCommand cmd = new OracleCommand();
            cmd.Connection = con2;
            cmd.CommandText = @"SELECT
                                    ap.vendor_id,
                                    ap.vendor_name,
                                    ap.segment1 AS vendorid,
                                    msib.segment1 AS ebsbarcode,
                                    msib.inventory_item_id AS item_id,
                                    msib.organization_id AS orgid,
                                    msib.item_type AS itemtype,
                                    msib.inventory_item_status_code AS itemstatus
                                FROM
                                    mtl_system_items_b msib,
                                    ap_suppliers ap,
                                    mtl_item_categories_v micv
                                WHERE
                                    msib.inventory_item_id = micv.inventory_item_id AND
                                    msib.organization_id = micv.organization_id AND
                                    ap.segment1 = micv.segment2 AND
                                    msib.organization_id = '87' AND
                                    msib.inventory_item_status_code = 'Active'";
            cmd.CommandType = CommandType.Text;
            OracleDataReader dr = cmd.ExecuteReader();
            vendoritems.Load(dr);
            
            con2.Close();
        }

        private void button1_Click(object sender, EventArgs e)
        {
           RECEIVING_GET();
           INSERT_DATA("receiving");
           OSIPOS_PRICELIST_GET();
           INSERT_DATA("osipos_pricelist");
           GENERAL_GET();
           INSERT_DATA("general");
           PRICELIST_TBL();
           INSERT_DATA("pricelist");
           IMO_PRICELIST_TBL();
           INSERT_DATA("imo_items");
           VENDOR_ITEMS();
           INSERT_DATA("vendor_items");
           MessageBox.Show("importing data complete!");
        }

        private void btnvendor_Click(object sender, EventArgs e)
        {
            VENDOR_ITEMS();
        }

        private void btnpricelist_Click(object sender, EventArgs e)
        {
            PRICELIST_TBL();
        }

        private void btnitems_Click(object sender, EventArgs e)
        {
            IMO_PRICELIST_TBL();
        }

        private void btnosipospricelist_Click(object sender, EventArgs e)
        {
            OSIPOS_PRICELIST_GET();
        }

        private void btngeneral_Click(object sender, EventArgs e)
        {
            GENERAL_GET();
        }

        private void CLEAR_TABLE(string TABLE_TO_DELETE)
        {
            string q1 = "DELETE FROM receiving_tbl where creationdate BETWEEN DATEADD(dd, -5, DATEDIFF(dd, 0, GETDATE())) AND DATEADD(ms, -3, convert(varchar, DATEADD(DAY, 2, GETDATE()), 23))";
            string q2 = "DELETE FROM osiposstorepricelist where created_date BETWEEN DATEADD(dd, -5, DATEDIFF(dd, 0, GETDATE())) AND DATEADD(ms, -3, convert(varchar, DATEADD(DAY, 2, GETDATE()), 23))";
            string q3 = "DELETE FROM general_tbl where created_date BETWEEN DATEADD(dd, -5, DATEDIFF(dd, 0, GETDATE())) AND DATEADD(ms, -3, convert(varchar, DATEADD(DAY, 2, GETDATE()), 23))";
            string q4 = "DELETE FROM pricelist_tbl where creation_date BETWEEN DATEADD(dd, -5, DATEDIFF(dd, 0, GETDATE())) AND DATEADD(ms, -3, convert(varchar, DATEADD(DAY, 2, GETDATE()), 23))";
            string q5 = "DELETE FROM imo_items_tbl";
            string q6 = "DELETE FROM vendoritems_tbl";
            string qparameter = "";

            switch (TABLE_TO_DELETE)
            {
                case "receiving":
                    qparameter = q1;
                    break;
                case "osipos_pricelist":
                    qparameter = q2;
                    break;
                case "general":
                    qparameter = q3;
                    break;
                case "pricelist":
                    qparameter = q4;
                    break;
                case "imo_items":
                    qparameter = q5;
                    break;
                case "vendor_items":
                    qparameter = q6;
                    break;
                default:
                    Console.WriteLine("Default case");
                    break;
            }

            try
            {
                using (SqlConnection cn = new SqlConnection(connString))
                {
                    cn.Open();
                    using (SqlTransaction Trans = cn.BeginTransaction())
                    {
                        try
                        {
                            SqlCommand cmd = new SqlCommand();
                            cmd = new SqlCommand();
                            cmd.Connection = cn;
                            cmd.Transaction = Trans;
                            cmd.CommandType = CommandType.Text;
                            cmd.Parameters.Clear();
                            cmd.CommandText = qparameter;
                            cmd.ExecuteNonQuery();
                            Trans.Commit();
                            //MessageBox.Show("Data record deleted!", "DB Connection With App.Config", MessageBoxButtons.OK, MessageBoxIcon.Asterisk);
                            cn.Close();
                        }
                        catch (Exception ex)
                        {
                            Trans.Rollback();
                            MessageBox.Show(ex.ToString());
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }

        private void INSERT_DATA(string TABLE_TO_INSERT)
        {
            DataTable tableparam = new DataTable();
            tableparam.Clear();

            string tbl1 = "receiving_tbl";
            string tbl2 = "osiposstorepricelist";
            string tbl3 = "general_tbl";
            string tbl4 = "pricelist_tbl";
            string tbl5 = "imo_items_tbl";
            string tbl6 = "vendoritems_tbl";

            int table_columns = 0;
            string tableparameter = "";
        //DELETE FROM general_tbl where created_date BETWEEN DATEADD(dd, -5, DATEDIFF(dd, 0, GETDATE())) AND DATEADD(ms, -3, convert(varchar, DATEADD(DAY, 2, GETDATE()), 23))
            switch (TABLE_TO_INSERT)
            {
                case "receiving":
                    tableparameter = tbl1;
                    tableparam = receiving;
                    table_columns = 16;
                    break;
                case "osipos_pricelist":
                    tableparameter = tbl2;
                    tableparam = osipos_pricelist;
                    table_columns = 7;
                    break;
                case "general":
                    tableparameter = tbl3;
                    tableparam = general;
                    table_columns = 11;
                    break;
                case "pricelist":
                    tableparameter = tbl4;
                    tableparam = pricelist;
                    table_columns = 10;
                    break;
                case "imo_items":
                    tableparameter = tbl5;
                    tableparam = imo_items;
                    table_columns = 12;
                    break;
                case "vendor_items":
                    tableparameter = tbl6;
                    tableparam = vendoritems;
                    table_columns = 8;
                    break;
                default:
                    Console.WriteLine("Default case");
                    break;
            }

            using (SqlConnection cn = new SqlConnection(connString))
            {
                cn.Open();

                try
                {
                    using (SqlBulkCopy copy = new SqlBulkCopy(cn))
                    {
                        for (int i = 0; i < table_columns; i++)
                        {
                            copy.ColumnMappings.Add(i, i);
                        }
                        copy.DestinationTableName = tableparameter;
                        copy.WriteToServer(tableparam);
                    }
                }
                catch (Exception ex)
                {
                    MessageBox.Show(ex.ToString());
                }

                cn.Close();
            } 
 
        }

        private void Form1_Shown(object sender, EventArgs e)
        {
            //MessageBox.Show(datenow.ToString());
            lbl1.Text = "Loading data from oracle";
            lbl1.Update();
            RECEIVING_GET();
            lbl1.Text = "Saving data MSQQL to receiving_tbl";
            lbl1.Update();
            CLEAR_TABLE("receiving");
            INSERT_DATA("receiving");
            progressBar1.Value = 20;

            lbl1.Text = "Loading data from oracle";
            lbl1.Update();
            OSIPOS_PRICELIST_GET();
            CLEAR_TABLE("osipos_pricelist");
            INSERT_DATA("osipos_pricelist");
            lbl1.Text = "Saving data MSQQL to osiposstorepricelist";
            lbl1.Update();
            progressBar1.Value = progressBar1.Value + 20;

            lbl1.Text = "Loading data from oracle";
            lbl1.Update();
            GENERAL_GET();
            lbl1.Text = "Saving data MSQQL to general_tbl";
            lbl1.Update();
            CLEAR_TABLE("general");
            INSERT_DATA("general");
            progressBar1.Value = progressBar1.Value + 20;

            lbl1.Text = "Loading data from oracle";
            lbl1.Update();
            PRICELIST_TBL();
            lbl1.Text = "Saving data MSQQL to pricelist_tbl";
            lbl1.Update();
            CLEAR_TABLE("pricelist");
            INSERT_DATA("pricelist");
            progressBar1.Value = progressBar1.Value + 20;

            lbl1.Text = "Loading data from oracle";
            lbl1.Update();
            IMO_PRICELIST_TBL();
            lbl1.Text = "Saving data MSQQL to imo_items_tbl";
            lbl1.Update();
            CLEAR_TABLE("imo_items");
            INSERT_DATA("imo_items");
            progressBar1.Value = progressBar1.Value + 20;

            lbl1.Text = "Loading data from oracle";
            lbl1.Update();
            VENDOR_ITEMS();
            lbl1.Text = "Saving data MSQQL to vendoritems";
            lbl1.Update();
            CLEAR_TABLE("vendor_items");
            INSERT_DATA("vendor_items");
            progressBar1.Value = progressBar1.Value + 20;

            lbl1.Text = "Data Successfully Imported!";
            lbl1.Update();

            formClose.Interval = 2000;
            formClose.Tick += new EventHandler(formClose_Tick);
            formClose.Start();

        }

        void formClose_Tick(object sender, EventArgs e)
        {
            formClose.Stop();
            formClose.Tick -= new EventHandler(formClose_Tick);
            Application.Exit();
        }
    }
}
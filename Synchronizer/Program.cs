using AutomatMachine;
using MailManagement;
using Microsoft.Extensions.Configuration;
using SqlManagement;
using Synchronizer.Shippernetix;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Mail;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;

namespace Synchronizer
{
    public class Program
    {
        enum SyncAction
        {
            AllOfThem,
            Structure,
            JobDefinition,
            Defect,
            JobHistory

        }
        public static void SendMail(string subject, string message)
        {
            new MailManager(o => o.Code == "GedenErp")
                .Prepare(new Mail(new MailAddress("shippernetix@gedenlines.com"), null, $"Synchronization Operation 101 / {subject}", $"{message}")
            //.AddTo(new MailAddress("bt@gedenlines.com")))
            .AddTo(new MailAddress("karar@gedenlines.com")))
            .Send((ex) =>
            {
                Console.WriteLine(ex.Message);
            });
        }

        public static void SendMailForDataPackage(string message)
        {
            new MailManager(o => o.Code == "GedenErp")
                .Prepare(new Mail(new MailAddress("shippernetix@gedenlines.com"), null, $"Data Package Status", $"{message}")
                            .AddTo(new MailAddress("bt@gedenlines.com")))
            .Send((ex) =>
            {
                Console.WriteLine(ex.Message);
            });
        }


        public static void Main(string[] args)
        {
            Initialize();

            Service.Start();
            //Job_History.ReCalculateLastJob(connection: new MsSqlConnection(connectionString: "MsSqlConnectionString"), callSign: "9HNK", l1: "19", l2: "1", l3: "1", l4: "1", jobCode: "A", actionToWorkWithLastCompletedJob: null);


            Console.WriteLine();


            //var sql1001 = "select ut1.Jd_CallSign,ut1.Jd_L1,ut1.Jd_L2,ut1.Jd_L3,ut1.Jd_L4,ut1.Jd_JobCode from Job_Definition ut1 "
            //    + " inner join (select top(20)  v.IntType,x.Je_CallSign,x.Je_L1,x.Je_L2,x.Je_L3,x.Je_L4,x.Je_JobCode from(select(t.Je_L1 + '-' + t.Je_L2 + '-' + t.Je_L3 + '-' + t.Je_L4 + '-' + t.Je_JobCode + '-' + CONVERT(varchar, t.Je_JobNo)) as Id, t.* "
            //    + " from Job_History t)x " 
            //    +" left join ViewToGetVesselJobs v on x.Id = v.Id and x.Je_CallSign = v.CallSign "
            //    + " where v.JobType='PLM' and  v.Status = 'NEXT JOB' and v.IsOverdue = 1 and IntType = 'MONTH')y "
            //    + " on ut1.Jd_CallSign = y.Je_CallSign "
            //  +" and ut1.Jd_L1 = y.Je_L1 "
            //  +" and ut1.Jd_L2 = y.Je_L2 "
            //  +" and ut1.Jd_L3 = y.Je_L3 "
            //  +" and ut1.Jd_L4 = y.Je_L4 "
            //  +" and ut1.Jd_JobCode = y.Je_JobCode";

//            var sql1001 = "select  v.LastRuntime,v.IntType,x.Je_CallSign,x.Je_L1,x.Je_L2,x.Je_L3,x.Je_L4,x.Je_JobCode from(select (t.Je_L1+'-'+t.Je_L2+'-'+t.Je_L3+'-'+t.Je_L4+'-'+t.Je_JobCode+'-'+CONVERT(varchar,t.Je_JobNo)) as Id,t.*  from Job_History t)x "
//+ " left join ViewToGetVesselJobs v on x.Id = v.Id and x.Je_CallSign = v.CallSign "
// + " where v.JobType = 'PLM' and v.Status = 'NEXT JOB' and v.IsOverdue = 1 and v.IntType = 'COUNTER'";

//            var connection = new MsSqlConnection(connectionString:"DECK");
//            var bbb = SqlManager.CheckConnection(connection);

            //15.08.2022
            //var overdues = SqlManager.ExecuteQuery(sql: sql1001, connection: connection);

            //foreach (var overdue in overdues)
            //{
            //    var jh = new 
            //    {
            //        CallSign = overdue["Je_CallSign"].ToString(),
            //        L1 = overdue["Je_L1"].ToString(),
            //        L2 = overdue["Je_L2"].ToString(),
            //        L3 = overdue["Je_L3"].ToString(),
            //        L4 = overdue["Je_L4"].ToString(),
            //        JobCode = overdue["Je_JobCode"].ToString(),
            //        LastRuntime = overdue["LastRuntime"].ToString()
            //    };

            //    //var updateSql = "update Job_Definition set Jd_PlanDate='2022-12-01 00:00:00.000' where Jd_CallSign=@CallSign and Jd_L1=@L1 and Jd_L2=@L2 and Jd_L3=@L3  and Jd_L4=@L4 and  Jd_JobCode=@JobCode";
            //    var updateSql = "update Job_Definition set Jd_PlanRuntime=@LastRuntime  where Jd_CallSign=@CallSign and Jd_L1=@L1 and Jd_L2=@L2 and Jd_L3=@L3  and Jd_L4=@L4 and  Jd_JobCode=@JobCode";

            //    var b103 = SqlManager.ExecuteNonQuery(sql:updateSql,parameters:new Dictionary<string, object>() 
            //    {
            //        {"CallSign", jh.CallSign},
            //        {"L1", jh.L1},
            //        {"L2", jh.L2},
            //        {"L3", jh.L3},
            //        {"L4", jh.L4},
            //        {"JobCode", jh.JobCode},
            //        {"LastRuntime", Convert.ToInt32(jh.LastRuntime)+2000 }
            //    },connection:connection)>0;

            //    Console.WriteLine($"{jh.CallSign}-{jh.L1}-{jh.L2}-{jh.L3}-{jh.L4}-{jh.JobCode}");

            //    Console.WriteLine(b103);
            //}


            Console.WriteLine();

            //Job_History.ReCalculateLastJob(new MsSqlConnection(connectionString: "MsSqlConnectionString"), "9BRV","7","3","2","1","C",actionToWorkWithLastCompletedJob:null);
            //Job_History.ReCalculateLastJob(new MsSqlConnection(connectionString: "MsSqlConnectionString"), "9BRV", "3", "19", "1", "1", "A", actionToWorkWithLastCompletedJob: null);

            //var q1000 = "select Je_CallSign,Je_L1,Je_L2,Je_L3,Je_L4,Je_JobCode,Je_JobNo,Je_ConfirmText from Job_History j right join Vessel_Master vm on vm.CallSign = j.Je_CallSign where vm.Active = 1 and j.Je_Status = 'COMPLETED' and(Je_CallSign + '-' + Je_L1 + '-' + Je_L2 + '-' + Je_L3 + '-' + Je_L4 + '-' + Je_JobCode + '-' + CONVERT(varchar(10), Je_JobNo)) not in (select(CallSign + '-' + L1 + '-' + L2 + '-' + L3 + '-' + L4 + '-' + JobCode + '-' + CONVERT(varchar(10), JobNo)) from Job_History_DetailsOfJobDone)";

            //var list = SqlManager.ExecuteQuery(q1000,connection:new MsSqlConnection(connectionString: "MsSqlConnectionString"));

            //var cc = 0;

            //var tList = new List<Task>();

            //foreach (var item in list)
            //{
            //    var t = new Task(()=> 
            //    {
            //        try
            //        {
            //            Console.WriteLine(cc++);

            //            var isExisted = SqlManager.Any("select * from Job_History_DetailsOfJobDone where CallSign=@CallSign and L1=@L1 and L2=@L2 and L3=@L3 and L4=@L4 and JobCode=@JobCode and JobNo=@JobNo", new Dictionary<string, object>()
            //            {
            //                    { "CallSign",item["Je_CallSign"]},
            //                    { "L1",item["Je_L1"]},
            //                    { "L2",item["Je_L2"]},
            //                    { "L3",item["Je_L3"]},
            //                    { "L4",item["Je_L4"]},
            //                    { "JobCode",item["Je_JobCode"]},
            //                    { "JobNo",item["Je_JobNo"]}
            //            },
            //                new MsSqlConnection(connectionString: "MsSqlConnectionString"));
            //            if (!isExisted)
            //            {

            //                var str = SqlManager.ExecuteScalar("select dbo.rtf2txt2(Je_ConfirmText) as DetailsOfJobDone from Job_History where Je_CallSign=@CallSign and Je_L1=@L1 and Je_L2=@L2 and Je_L3=@L3 and Je_L4=@L4 and Je_JobCode=@JobCode and Je_JobNo=@JobNo",
            //                        new Dictionary<string, object>()
            //                        {
            //                                            { "CallSign",item["Je_CallSign"]},
            //                                            { "L1",item["Je_L1"]},
            //                                            { "L2",item["Je_L2"]},
            //                                            { "L3",item["Je_L3"]},
            //                                            { "L4",item["Je_L4"]},
            //                                            { "JobCode",item["Je_JobCode"]},
            //                                            { "JobNo",item["Je_JobNo"]},
            //                        },
            //                        new MsSqlConnection(connectionString: "MsSqlConnectionString"));

            //                Console.WriteLine($"{item["Je_CallSign"]}-{item["Je_L1"]}-{ item["Je_L2"]}-{ item["Je_L3"]}-{ item["Je_L4"]}-{ item["Je_JobCode"]}-{item["Je_JobNo"]}");
            //                Console.WriteLine($"Str : {str}");

            //                var isInserted = SqlManager.ExecuteNonQuery("insert into Job_History_DetailsOfJobDone(CallSign,L1,L2,L3,L4,JobCode,JobNo,RichText,PlainText) values (@CallSign,@L1,@L2,@L3,@L4,@JobCode,@JobNo,@RichText,@PlainText)",
            //                    new Dictionary<string, object>()
            //                    {
            //                    { "CallSign",item["Je_CallSign"]},
            //                    { "L1",item["Je_L1"]},
            //                    { "L2",item["Je_L2"]},
            //                    { "L3",item["Je_L3"]},
            //                    { "L4",item["Je_L4"]},
            //                    { "JobCode",item["Je_JobCode"]},
            //                    { "JobNo",item["Je_JobNo"]},
            //                    { "RichText",item["Je_ConfirmText"]},
            //                    { "PlainText",str},
            //                    },
            //                    new MsSqlConnection(connectionString: "MsSqlConnectionString")) > 0;

            //                if (!isInserted)
            //                    Console.WriteLine(isInserted);
            //            }
            //        }
            //        catch (Exception ex)
            //        {
            //            Console.WriteLine(ex.Message);
            //        }
            //    });

            //    tList.Add(t);

            //    t.Start();
            //}
            ////Evrim Bey İstek End



            //Shippernetix.Job_History.FixFromVesselToOffice("9VLU","13","1","23","1","C","3");

            //ExecuteNonQueryOn(new MsSqlConnection(connectionString: "9VRT"), "delete from Approval where A_CallSign='9VRT' and A_L1='4' and A_L2='7' and A_L3='10' and A_L4='1'");
            //FixSpares("9BRV", "5", "1", "0", new MsSqlConnection(connectionString: "9BRV"), new MsSqlConnection(connectionString: "MsSqlConnectionString"), false);
            //Shippernetix.Job_History.Fix("9VRT", "4","7","10","1", "A", "2", new MsSqlConnection(connectionString: "MsSqlConnectionString"), new MsSqlConnection(connectionString: "9VRT"), true);
            //Shippernetix.Job_History.Fix("9HLN", "4", "1", "4", "1", "A", "1", new MsSqlConnection(connectionString: "9HLN"), new MsSqlConnection(connectionString: "MsSqlConnectionString"), true);
            //Shippernetix.Job_History.FixFromOfficeToVessel("9PTY", "6", "1", "7", "4", "B", "1");
            //Shippernetix.Job_History.FixFromVesselToOffice("9PRD", "16", "1", "1", "1", "A", "14");
            //Shippernetix.Job_History.FixFromOfficeToVessel("9BRV", "2", "3", "1", "1", "B", "8");

            //Shippernetix.Job_History.FixFromOfficeToVessel("9HA2", "4", "3", "1", "4", "C", "4");
            //Shippernetix.Job_History.FixFromOfficeToVessel("9RYL", "2", "3", "9", "3", "D", "1");
            //Shippernetix.Job_History.FixFromOfficeToVessel("9RYL", "5", "2", "6", "1", "Def-1063", "1");
            //Shippernetix.Job_History.FixFromOfficeToVessel("9RYL", "5", "8", "1", "1", "Def-1062", "1");
            //Shippernetix.Job_History.FixFromOfficeToVessel("9RYL", "10", "2", "1", "1", "Def-867", "1");
            //Shippernetix.Job_History.FixFromOfficeToVessel("9RYL", "10", "2", "1", "1", "Def-871", "1");
            //Shippernetix.Job_History.FixFromOfficeToVessel("9RYL", "13", "1", "10", "3", "Def-1036", "1");
            //Shippernetix.Job_History.FixFromOfficeToVessel("9RYL", "14", "6", "1", "8", "D", "1");
            //Shippernetix.Job_History.FixFromOfficeToVessel("9RYL", "14", "6", "1", "8", "E", "1");
            // Shippernetix.Job_History.FixFromOfficeToVessel("9BRV", "2", "3", "9", "2", "A", "21");
            //Shippernetix.Job_History.FixFromOfficeToVessel("9REF", "3", "6", "6", "1", "Def-1130", "1");

            ////NO.1 CYL.  01.01.13.07 - A / 21.01.2022  tarihinde yapılmıştır.
            ////NO.2 CYL.  01.01.13.08 - A / 22.01 2022  tarihinde yapılmıştır.
            ////NO.4 CYL.  01.01.13.10 - A / 26.01.2022  tarihinde yapılmıştır.
            ////NO.5 CYL.  01.01.13.11 - A / 22.01 2022  tarihinde yapılmıştır.
            ////NO.6 CYL.  01.01.13.12 - A / 26.01.2022  tarihinde yapılmıştır.

            //Shippernetix.Job_History.ReCalculateLastJob(new MsSqlConnection(connectionString: "MsSqlConnectionString"), callSign: "9HLN", "1", "1", "13", "7", "A", "2022-01-21 00:00:00.000");

            //Shippernetix.Job_History.ReCalculateLastJob(new MsSqlConnection(connectionString: "MsSqlConnectionString"), callSign: "9HLN", "1", "1", "13", "8", "A", "2022-01-22 00:00:00.000");

            //Shippernetix.Job_History.ReCalculateLastJob(new MsSqlConnection(connectionString: "MsSqlConnectionString"), callSign: "9HLN", "1", "1", "13", "10", "A", "2022-01-26 00:00:00.000");

            //Shippernetix.Job_History.ReCalculateLastJob(new MsSqlConnection(connectionString: "MsSqlConnectionString"), callSign: "9HLN", "1", "1", "13", "11", "A", "2022-01-22 00:00:00.000");

            //Shippernetix.Job_History.ReCalculateLastJob(new MsSqlConnection(connectionString: "MsSqlConnectionString"), callSign: "9VRT", "17", "1", "1", "1", "A", "2022-01-26 00:00:00.000");
            //Shippernetix.Job_History.ReCalculateLastJob(new MsSqlConnection(connectionString: "MsSqlConnectionString"), callSign: "9VRT", l1:"17", l2:"1", l3:"1", l4: "10", jobCode: "A",actionToWorkWithLastCompletedJob:null);
            //Shippernetix.Job_History.ReCalculateLastJob(new MsSqlConnection(connectionString: "MsSqlConnectionString"), callSign: "9HNK", l1:"2", l2:"2", l3:"2", l4: "2", jobCode: "B",actionToWorkWithLastCompletedJob:null);
            //Shippernetix.Job_History.ReCalculateLastJob(new MsSqlConnection(connectionString: "MsSqlConnectionString"), callSign: "9HNK", l1:"2", l2:"2", l3:"2", l4: "2", jobCode: "C",actionToWorkWithLastCompletedJob:null);
            //Shippernetix.Job_History.ReCalculateLastJob(new MsSqlConnection(connectionString: "MsSqlConnectionString"), callSign: "9HNK", l1:"2", l2:"2", l3:"2", l4: "1", jobCode: "A",actionToWorkWithLastCompletedJob:null);
            //Shippernetix.Job_History.ReCalculateLastJob(new MsSqlConnection(connectionString: "MsSqlConnectionString"), callSign: "9HNK", l1:"2", l2:"2", l3:"2", l4: "2", jobCode: "B",actionToWorkWithLastCompletedJob:null);
            //Shippernetix.Job_History.ReCalculateLastJob(new MsSqlConnection(connectionString: "MsSqlConnectionString"), callSign: "9HNK", l1:"2", l2:"2", l3:"5", l4: "1", jobCode: "C",actionToWorkWithLastCompletedJob:null);


            //Defect.FixFromOfficeToVessel("9HNK","Def-");

            //Shippernetix.Job_History.FixFromOfficeToVessel("9BUD", "5", "8", "6", "1", "Def-1968", "1");
            //Shippernetix.Job_History.FixFromOfficeToVessel("9BUD", "5", "8", "6", "1", "Def-2958", "1");
            //Shippernetix.Job_History.FixFromOfficeToVessel("9BUD", "5", "10", "1", "2", "Def-2962", "1");

            //Defect.FixFromOfficeToVessel("9REF", "Def-1130");

            //Shippernetix.Defect.FixDefectsIsHiddenStatus("9HLN",new MsSqlConnection(connectionString: "MsSqlConnectionString"),new MsSqlConnection(connectionString: "9HLN"));

            //Shippernetix.Job_History.ReCalculateLastJob(new MsSqlConnection("MsSqlConnectionString"),"9VRT","3","3","7","1","A");

            //Console.ReadLine();


            //Shippernetix.Job_History.FixFromVesselToOffice("9RYL", "10", "1", "2", "2", "Def-1067", "1");
            //Shippernetix.Job_History.FixFromVesselToOffice("9RYL", "12", "1", "2", "1", "Def-939", "1");
            //Shippernetix.Job_History.FixFromVesselToOffice("9RYL", "13", "1", "29", "1", "A", "1");
            //Shippernetix.Job_History.FixFromVesselToOffice("9RYL", "13", "1", "9", "2", "A", "136");
            //Shippernetix.Job_History.FixFromVesselToOffice("9RYL", "14", "1", "1", "1", "Def-1004", "1");
            //Shippernetix.Job_History.FixFromVesselToOffice("9RYL", "14", "2", "2", "1", "Def-1094", "1");
            //Shippernetix.Job_History.FixFromVesselToOffice("9RYL", "18", "1", "1", "1", "Def-1095", "1");
            //Shippernetix.Job_History.FixFromVesselToOffice("9RYL", "18", "1", "1", "1", "Def-902", "1");
            //Shippernetix.Job_History.FixFromVesselToOffice("9RYL", "2", "3", "13", "1", "Def-1105", "1");
            //Shippernetix.Job_History.FixFromVesselToOffice("9RYL", "2", "3", "13", "2", "Def-1099", "1");
            //Shippernetix.Job_History.FixFromVesselToOffice("9RYL", "3", "14", "2", "1", "Def-1073", "1");
            //Shippernetix.Job_History.FixFromVesselToOffice("9RYL", "3", "3", "1", "5", "A", "1");
            //Shippernetix.Job_History.FixFromVesselToOffice("9RYL", "3", "3", "1", "5", "B", "1");
            //Shippernetix.Job_History.FixFromVesselToOffice("9RYL", "3", "3", "1", "6", "A", "1");
            //Shippernetix.Job_History.FixFromVesselToOffice("9RYL", "3", "3", "7", "1", "Def-1074", "1");
            //Shippernetix.Job_History.FixFromVesselToOffice("9RYL", "3", "3", "7", "2", "Def-1075", "1");
            //Shippernetix.Job_History.FixFromVesselToOffice("9RYL", "3", "4", "7", "1", "Def-1023", "1");
            //Shippernetix.Job_History.FixFromVesselToOffice("9RYL", "5", "2", "5", "1", "Def-1100", "1");
            /////10000001//
            //var q101 = "select z.* from(select y.*,(select max(jh.Je_JobNo) from Job_History jh where jh.Je_CallSign = y.Je_CallSign " +
            //    "and jh.Je_L1 = y.Je_L1 " +
            //    "and jh.Je_L2 = y.Je_L2 " +
            //    "and jh.Je_L3 = y.Je_L3 " +
            //    "and jh.Je_L4 = y.Je_L4 " +
            //    "and jh.Je_JobCode = y.Je_JobCode) as MaxJobNumber " +
            //    "from(select Name, Je_CallSign, Je_L1, Je_L2, Je_L3, Je_L4, Je_JobCode, Je_JobNo, Je_Status " +
            //    "from(select vm.Name, jh1.* from Job_History jh1 right join Vessel_Master vm on vm.CallSign = jh1.Je_CallSign and vm.Active = 1)x " +
            //    "group by Name, Je_CallSign, Je_L1, Je_L2, Je_L3, Je_L4, Je_JobCode, Je_JobNo, Je_Status)y)z " +
            //               " where Je_JobNo = MaxJobNumber and Je_JobNo<>1 and Je_Status = 'COMPLETED' and Je_JobCode not like '%Def%'";

            //var list = SqlManager.ExecuteQuery(q101);

            //var updateQuery1 = "update Job_History set Je_Status = 'NEXT JOB' where Je_CallSign=@CallSign and Je_L1=@L1 and  Je_L2=@L2 and Je_L3=@L3 and Je_L4=@L4 and Je_JobCode=@JobCode and Je_JobNo=@JobNo";
            //var updateQuery2 = "update Job_History set Je_Status = 'COMPLETED' where Je_CallSign=@CallSign and Je_L1=@L1 and  Je_L2=@L2 and Je_L3=@L3 and Je_L4=@L4 and Je_JobCode=@JobCode and Je_JobNo=@JobNo";


            //var isUpdated = false;

            //try
            //{
            //    foreach (var item in list)
            //    {
            //        var jobHistory = new
            //        {
            //            CallSign = item["Je_CallSign"].ToString(),
            //            L1 = item["Je_L1"].ToString(),
            //            L2 = item["Je_L2"].ToString(),
            //            L3 = item["Je_L3"].ToString(),
            //            L4 = item["Je_L4"].ToString(),
            //            JobCode = item["Je_JobCode"].ToString(),
            //            JobNo = item["Je_JobNo"].ToString(),
            //        };

            //        var connection1 = new MsSqlConnection();

            //        isUpdated = SqlManager.ExecuteNonQuery(updateQuery1, new Dictionary<string, object>()
            //            {
            //                {"CallSign",jobHistory.CallSign },
            //                {"L1",jobHistory.L1},
            //                {"L2",jobHistory.L2  },
            //                {"L3",jobHistory.L3  },
            //                {"L4",jobHistory.L4  },
            //                {"JobCode",jobHistory.JobCode  },
            //                {"JobNo",jobHistory.JobNo }
            //            }, connection1) > 0;


            //        if (isUpdated)
            //        {
            //            isUpdated = SqlManager.ExecuteNonQuery(updateQuery2, new Dictionary<string, object>()
            //                {
            //                    {"CallSign",jobHistory.CallSign },
            //                    {"L1",jobHistory.L1},
            //                    {"L2",jobHistory.L2  },
            //                    {"L3",jobHistory.L3  },
            //                    {"L4",jobHistory.L4  },
            //                    {"JobCode",jobHistory.JobCode  },
            //                    {"JobNo",jobHistory.JobNo }
            //                }, connection2) > 0;
            //        }
            //        else
            //        {
            //            throw new Exception("Error");
            //        }
            //    }
            //}
            //catch(Exception ex)
            //{
            //    Console.WriteLine(ex);
            //}
            /////10000001//
            ///
            Console.Write("For Synchronizer enter 1 \nFor Manuel DataPackage Mailer enter 2 \nFor Run Automat DataPackage Mailer Enter 3 \n");
            var status = int.Parse(Console.ReadLine());
            if (status == 1)
            {
                var onlyMail = false;
                Console.WriteLine("Just Send Mail (y / n)");
                string onlyMailStr = "";
                bool wrongOnlyMailStr = false;
                do
                {
                    if (wrongOnlyMailStr)
                    {
                        Console.WriteLine("Please Enter Just 'y' or 'n'");
                    }
                    onlyMailStr = Console.ReadLine();
                    onlyMail = onlyMailStr.Equals("y") ? true : false;
                    wrongOnlyMailStr = true;
                } while (!onlyMailStr.Equals("y") && !onlyMailStr.Equals("n"));


                string vesselCallSign = "";
                foreach (var vessel in Vessel_Master.Vessels)
                {
                    Console.WriteLine($"Vessel = {vessel.Name} ///// Vessel CallSign = {vessel.CallSign}\n");
                }
                Console.WriteLine("Please Enter Vessel Callsign Of The Vessel You Want to synchronize.");

                bool wrongCallSing = false;
                do
                {
                    if (wrongCallSing)
                    {
                        Console.WriteLine("There is No Vessel For This CallSign. Please Enter CallSign Again");
                    }

                    vesselCallSign = Console.ReadLine();
                    wrongCallSing = true;
                } while (!Vessel_Master.Vessels.Any(x => x.CallSign == vesselCallSign));

                SyncAction syncAction = SyncAction.AllOfThem;
                Console.WriteLine("\n\nAction Type Ids ; \n");
                Console.WriteLine("\t Structure = 1 \n");
                Console.WriteLine("\t JobDefination = 2 \n");
                Console.WriteLine("\t Defect = 3 \n");
                Console.WriteLine("\t JobHistory = 4 \n");
                Console.WriteLine("\t For All Of Them  = 0 \n");
                Console.WriteLine("Please Enter The Action Types Id\n");

                int syncActionNumber;
                bool wrongActionNumb = false;
                do
                {
                    if (wrongActionNumb)
                    {
                        Console.WriteLine("Please Enter A Number");
                    }
                    wrongActionNumb = true;
                } while (!int.TryParse(Console.ReadLine(), out syncActionNumber));


                syncAction = (SyncAction)syncActionNumber;

                foreach (var vessel in Vessel_Master.Vessels)
                {
                    if (vessel.CallSign != vesselCallSign)
                        continue;

                    var source = new Side("Office", "MsSqlConnectionString", true);

                    var isSourceConnected = SqlManager.CheckConnection(source.Connection);

                    var target = new Side(vessel.Name, vessel.CallSign, true);

                    var istargetConnected = SqlManager.CheckConnection(target.Connection);



                    if (isSourceConnected && istargetConnected)
                    {
                        var maxDataPackage = SqlManager.ExecuteScalar("select max(CAST(Dr_PacketNo as int)) from Data_Receive where Dr_PrepareOwnerCode=@CallSign", new Dictionary<string, object>()
                    {
                        {"CallSign",vessel.CallSign }
                    }, connection: target.Connection);

                        #region Delete Unwanted job which reaches max job number before previous one is completed

                        var sqlToClear = "select x.*from( " +
                            "select jh.*, " +
                            "(select max(Je_JobNo) from Job_History where Je_CallSign = jh.Je_CallSign and Je_L1 = jh.Je_L1 and Je_L2 = jh.Je_L2 and Je_L3 = jh.Je_L3 and Je_L4 = jh.Je_L4 and Je_JobCode = jh.Je_JobCode) as MaxJobNumber " +
                            "from Job_History jh " +
                            "where jh.Je_CallSign = @CallSign )x where x.Je_JobNo = x.MaxJobNumber - 1 and x.Je_Status <> 'COMPLETED' ";

                        var sourceRecordsToClear = SqlManager.ExecuteQuery(sqlToClear, new Dictionary<string, object>()
                    {
                        {"CallSign",vessel.CallSign }
                    }, source.Connection)
                       .Select(d => new
                       {
                           ShippernetixId = string.Format("{0}-{1}-{2}-{3}-{4}-{5}-{6}", d["Je_CallSign"], d["Je_L1"], d["Je_L2"], d["Je_L3"], d["Je_L4"], d["Je_JobCode"], d["Je_JobNo"]),
                           Je_CallSign = d["Je_CallSign"],
                           Je_L1 = d["Je_L1"],
                           Je_L2 = d["Je_L2"],
                           Je_L3 = d["Je_L3"],
                           Je_L4 = d["Je_L4"],
                           Je_JobCode = d["Je_JobCode"],
                           Je_JobNo = d["Je_JobNo"],
                           MaxJobNumber = d["MaxJobNumber"]
                       })
                    .ToList();

                        foreach (var item in sourceRecordsToClear)
                        {
                            Console.WriteLine("{0} is deleted : {1}", item.MaxJobNumber,
                                SqlManager.ExecuteNonQuery("delete from Job_History where Je_CallSign=@Je_CallSign and Je_L1=@Je_L1 and Je_L2=@Je_L2 and Je_L3=@Je_L3 and Je_L4=@Je_L4 and Je_JobCode=@Je_JobCode and Je_JobNo=@MaxJobNo",
                                new Dictionary<string, object>()
                                {
                                {"Je_CallSign",item.Je_CallSign },
                                {"Je_L1", item.Je_L1},
                                {"Je_L2", item.Je_L2},
                                {"Je_L3", item.Je_L3},
                                {"Je_L4", item.Je_L4},
                                {"Je_JobCode", item.Je_JobCode},
                                {"MaxJobNo",item.MaxJobNumber }
                                }, source.Connection));
                        }


                        var targetRecordsToClear = SqlManager.ExecuteQuery(sqlToClear, new Dictionary<string, object>()
                    {
                        {"CallSign",vessel.CallSign }
                    }, target.Connection)
                                               .Select(d => new
                                               {
                                                   ShippernetixId = string.Format("{0}-{1}-{2}-{3}-{4}-{5}-{6}", d["Je_CallSign"], d["Je_L1"], d["Je_L2"], d["Je_L3"], d["Je_L4"], d["Je_JobCode"], d["Je_JobNo"]),
                                                   Je_CallSign = d["Je_CallSign"],
                                                   Je_L1 = d["Je_L1"],
                                                   Je_L2 = d["Je_L2"],
                                                   Je_L3 = d["Je_L3"],
                                                   Je_L4 = d["Je_L4"],
                                                   Je_JobCode = d["Je_JobCode"],
                                                   Je_JobNo = d["Je_JobNo"],
                                                   MaxJobNumber = d["MaxJobNumber"]
                                               })
                            .ToList();

                        foreach (var item in targetRecordsToClear)
                        {
                            Console.WriteLine("{0} is deleted : {1}", item.MaxJobNumber,
                                    SqlManager.ExecuteNonQuery("delete from Job_History where Je_CallSign=@Je_CallSign and Je_L1=@Je_L1 and Je_L2=@Je_L2 and Je_L3=@Je_L3 and Je_L4=@Je_L4 and Je_JobCode=@Je_JobCode and Je_JobNo=@MaxJobNo",
                                    new Dictionary<string, object>()
                                    {
                                                            {"Je_CallSign",item.Je_CallSign },
                                                            {"Je_L1", item.Je_L1},
                                                            {"Je_L2", item.Je_L2},
                                                            {"Je_L3", item.Je_L3},
                                                            {"Je_L4", item.Je_L4},
                                                            {"Je_JobCode", item.Je_JobCode},
                                                            {"MaxJobNo",item.MaxJobNumber }
                                    }, target.Connection));
                        }

                        #endregion


                        Action<CustomConnection, string, string, string, string, string, string, string> actionForComplete = (cc, callSign, l1, l2, l3, l4, jobCode, jobNo) =>
                        {
                            var q = "update Job_History set Je_Status='COMPLETED' where Je_CallSign=@CallSign and Je_L1=@L1 and  Je_L2=@L2 and Je_L3=@L3 and Je_L4=@L4 and Je_JobCode=@JobCode and Je_JobNo=@JobNo";

                            var isUpdated = SqlManager.ExecuteNonQuery(q,
                                new Dictionary<string, object>()
                                {
                                {"CallSign", callSign},
                                {"L1", l1},
                                {"L2", l2},
                                {"L3", l3},
                                {"L4", l4},
                                {"JobCode", jobCode},
                                {"JobNo", jobNo},
                                },
                                cc);

                            Console.WriteLine("isUpdated: " + isUpdated);
                        };

                        Console.WriteLine("Fixing : From target to source");

                        var queryToFix = "  select z.* from(select y.*," +
                                            "(select max(jh.Je_JobNo) from Job_History jh where jh.Je_CallSign = y.Je_CallSign " +
                                            "and jh.Je_L1 = y.Je_L1 " +
                                            "and jh.Je_L2 = y.Je_L2 " +
                                            "and jh.Je_L3 = y.Je_L3 " +
                                            "and jh.Je_L4 = y.Je_L4 " +
                                            "and jh.Je_JobCode = y.Je_JobCode) as MaxJobNumber " +
                                            "from(select Je_CallSign, Je_L1, Je_L2, Je_L3, Je_L4, Je_JobCode, Je_JobNo, Je_Status " +
                                            "from(select jh1.* from Job_History jh1 where jh1.Je_CallSign = @CallSign)x " +
                                            "group by Je_CallSign, Je_L1, Je_L2, Je_L3, Je_L4, Je_JobCode, Je_JobNo, Je_Status)y)z " +
                                            "where (Je_JobCode not like '%Def%' and Je_JobCode not like '%Dam%') and ((Je_JobNo < MaxJobNumber and Je_Status<>'COMPLETED') or (MaxJobNumber=1 and Je_Status='COMPLETED'))";


                        var sourceList = SqlManager.ExecuteQuery(queryToFix,
                            new Dictionary<string, object>()
                            {
                            { "CallSign",vessel.CallSign }
                            },
                            source.Connection)
                            .Select(d => new
                            {
                                ShippernetixId = string.Format("{0}-{1}-{2}-{3}-{4}-{5}-{6}", d["Je_CallSign"], d["Je_L1"], d["Je_L2"], d["Je_L3"], d["Je_L4"], d["Je_JobCode"], d["Je_JobNo"]),
                                Je_CallSign = d["Je_CallSign"],
                                Je_L1 = d["Je_L1"],
                                Je_L2 = d["Je_L2"],
                                Je_L3 = d["Je_L3"],
                                Je_L4 = d["Je_L4"],
                                Je_JobCode = d["Je_JobCode"],
                                Je_JobNo = d["Je_JobNo"],
                                MaxJobNumber = d["MaxJobNumber"]
                            })
                            .ToList();

                        var counter = 0;
                        foreach (var sourceItem in sourceList)
                        {
                            //actionForComplete(source.Connection,sourceItem.Je_CallSign.ToString(),sourceItem.Je_L1.ToString(), sourceItem.Je_L2.ToString(), sourceItem.Je_L3.ToString(), sourceItem.Je_L4.ToString(), sourceItem.Je_JobCode.ToString(), sourceItem.Je_JobNo.ToString());

                            if (Convert.ToInt32(sourceItem.MaxJobNumber) == 1)
                            {
                                var maxJobNumberForCompleted = Convert.ToInt32(sourceItem.MaxJobNumber);

                                var parameters = new Dictionary<string, object>()
                                {
                                    {"CallSign",sourceItem.Je_CallSign.ToString() },
                                    {"L1", sourceItem.Je_L1.ToString()},
                                    {"L2", sourceItem.Je_L2.ToString()},
                                    {"L3", sourceItem.Je_L3.ToString()},
                                    {"L4", sourceItem.Je_L4.ToString()},
                                    {"JobCode",  sourceItem.Je_JobCode.ToString()},
                                    {"LastCompletedJobNo",maxJobNumberForCompleted}
                                };

                                var updateLastCompletedJobStatusQuery = "update Job_History set Je_Status='NEXT JOB' where Je_CallSign = @CallSign and Je_L1 = @L1 and Je_L2 = @L2 and Je_L3 = @L3 and Je_L4 = @L4 and Je_JobCode = @JobCode and Je_JobNo=@LastCompletedJobNo";

                                var arc = SqlManager.ExecuteNonQuery(updateLastCompletedJobStatusQuery, parameters, source.Connection);

                                Console.WriteLine($"Updating lastCompletedJobStatus({maxJobNumberForCompleted}) as Next Job : {arc > 0}");

                                updateLastCompletedJobStatusQuery = "update Job_History set Je_Status='COMPLETED' where Je_CallSign = @CallSign and Je_L1 = @L1 and Je_L2 = @L2 and Je_L3 = @L3 and Je_L4 = @L4 and Je_JobCode = @JobCode and Je_JobNo=@LastCompletedJobNo";

                                arc = SqlManager.ExecuteNonQuery(updateLastCompletedJobStatusQuery, parameters, source.Connection);

                                Console.WriteLine($"Updating lastCompletedJobStatus({maxJobNumberForCompleted}) as Completed : {arc > 0}");
                            }
                            else
                            {
                                Shippernetix.Job_History.Fix(sourceItem.Je_CallSign.ToString(), sourceItem.Je_L1.ToString(), sourceItem.Je_L2.ToString(), sourceItem.Je_L3.ToString(), sourceItem.Je_L4.ToString(), sourceItem.Je_JobCode.ToString(), sourceItem.Je_JobNo.ToString(), target.Connection, source.Connection, true);
                            }

                            Console.WriteLine(sourceItem.ShippernetixId + " " + counter++);
                        }

                        Console.WriteLine("Fixing : From source to target");

                        var targetList = SqlManager.ExecuteQuery(queryToFix,
                            new Dictionary<string, object>()
                            {
                            { "CallSign",vessel.CallSign }
                            },
                            target.Connection)
                            .Select(d => new
                            {
                                ShippernetixId = string.Format("{0}-{1}-{2}-{3}-{4}-{5}-{6}", d["Je_CallSign"], d["Je_L1"], d["Je_L2"], d["Je_L3"], d["Je_L4"], d["Je_JobCode"], d["Je_JobNo"]),
                                Je_CallSign = d["Je_CallSign"],
                                Je_L1 = d["Je_L1"],
                                Je_L2 = d["Je_L2"],
                                Je_L3 = d["Je_L3"],
                                Je_L4 = d["Je_L4"],
                                Je_JobCode = d["Je_JobCode"],
                                Je_JobNo = d["Je_JobNo"],
                                MaxJobNumber = d["MaxJobNumber"]
                            })
                            .ToList();

                        counter = 0;
                        foreach (var targetItem in targetList)
                        {
                            //actionForComplete(target.Connection, targetItem.Je_CallSign.ToString(), targetItem.Je_L1.ToString(), targetItem.Je_L2.ToString(), targetItem.Je_L3.ToString(), targetItem.Je_L4.ToString(), targetItem.Je_JobCode.ToString(), targetItem.Je_JobNo.ToString());


                            if (Convert.ToInt32(targetItem.MaxJobNumber) == 1)
                            {
                                var maxJobNumberForCompleted = Convert.ToInt32(targetItem.MaxJobNumber);

                                var parameters = new Dictionary<string, object>()
                                {
                                    {"CallSign",targetItem.Je_CallSign.ToString() },
                                    {"L1", targetItem.Je_L1.ToString()},
                                    {"L2", targetItem.Je_L2.ToString()},
                                    {"L3", targetItem.Je_L3.ToString()},
                                    {"L4", targetItem.Je_L4.ToString()},
                                    {"JobCode",  targetItem.Je_JobCode.ToString()},
                                    {"LastCompletedJobNo",maxJobNumberForCompleted}
                                };

                                var updateLastCompletedJobStatusQuery = "update Job_History set Je_Status='NEXT JOB' where Je_CallSign = @CallSign and Je_L1 = @L1 and Je_L2 = @L2 and Je_L3 = @L3 and Je_L4 = @L4 and Je_JobCode = @JobCode and Je_JobNo=@LastCompletedJobNo";

                                var arc = SqlManager.ExecuteNonQuery(updateLastCompletedJobStatusQuery, parameters, target.Connection);

                                Console.WriteLine($"Updating lastCompletedJobStatus({maxJobNumberForCompleted}) as Next Job : {arc > 0}");

                                updateLastCompletedJobStatusQuery = "update Job_History set Je_Status='COMPLETED' where Je_CallSign = @CallSign and Je_L1 = @L1 and Je_L2 = @L2 and Je_L3 = @L3 and Je_L4 = @L4 and Je_JobCode = @JobCode and Je_JobNo=@LastCompletedJobNo";

                                arc = SqlManager.ExecuteNonQuery(updateLastCompletedJobStatusQuery, parameters, target.Connection);

                                Console.WriteLine($"Updating lastCompletedJobStatus({maxJobNumberForCompleted}) as Completed : {arc > 0}");
                            }
                            else
                            {
                                Shippernetix.Job_History.Fix(targetItem.Je_CallSign.ToString(), targetItem.Je_L1.ToString(), targetItem.Je_L2.ToString(), targetItem.Je_L3.ToString(), targetItem.Je_L4.ToString(), targetItem.Je_JobCode.ToString(), targetItem.Je_JobNo.ToString(), source.Connection, target.Connection, true);
                            }
                            Console.WriteLine(targetItem.ShippernetixId + " " + counter++);
                        }
                    }
                    else
                    {
                        Console.WriteLine($"Could Not Connect Office({isSourceConnected}) Or Vessel({istargetConnected}) Database. \n Please Check Connection String And Vpn Configurations");
                    }

                    if (isSourceConnected && istargetConnected)
                    {


                        switch (syncAction)
                        {
                            case SyncAction.AllOfThem:
                                Structure.Sync(source, target, vessel, onlyMail);
                                Job_Definition.Sync(source, target, vessel, onlyMail);
                                Defect.Sync(source, target, vessel, onlyMail);
                                Job_History.Sync(source, target, vessel, onlyMail);
                                break;
                            case SyncAction.Structure:
                                Structure.Sync(source, target, vessel, onlyMail);
                                break;
                            case SyncAction.JobDefinition:
                                Job_Definition.Sync(source, target, vessel, onlyMail);
                                break;
                            case SyncAction.Defect:
                                Defect.Sync(source, target, vessel, onlyMail);
                                break;
                            case SyncAction.JobHistory:
                                Job_History.Sync(source, target, vessel, onlyMail);
                                break;
                            default:
                                Structure.Sync(source, target, vessel, onlyMail);
                                Job_Definition.Sync(source, target, vessel, onlyMail);
                                Defect.Sync(source, target, vessel, onlyMail);
                                Job_History.Sync(source, target, vessel, onlyMail);
                                break;
                        }
                    }
                    else
                        Console.WriteLine("Source({0}) is connected : {1},Target({2}) is Connected : {3}", source.Name, isSourceConnected, target.Name, istargetConnected);
                }
            }
            else if (status == 2)
            {

                DataPackageNumbersMatch();

            }
            else if (status == 3)
            {
                
                Automat aout = new Automat("Package","Package");
                var reg = new Job("Package")
                    .SetInterval(hours:1)
                    .SetContinuous(true)
                    .SetAction(() => 
                    {
                        if(DateTime.Now.Hour == 8)
                        {
                            DataPackageNumbersMatch();
                        }
                        else
                        {
                            Console.WriteLine("Run But No Mail. its work at 08:00 AM Between 09:00 AM");
                        }
                    });

                aout.AddJob(reg);

            }
            else
            {
                Console.WriteLine("Program Is Terminated!!!!!!!!!");
            }

            Console.ReadLine();
        
        }

        public static void DataPackageNumbersMatch()
        {
            var vessels = Vessel_Master.Vessels;
            string mailMessage = $"<p>Date = {DateTime.Now}</p>" +
                "<p>Data Package Transfers Last Numbers  Between The Vessel And The Office</p>" +
                "<table style = 'border: 1px solid;'>" +
                " <thead style = 'border: 1px solid'>" +
                "<tr style = 'border: 1px solid;'>" +
                "<td style = 'border:2px solid; padding: 5px;' > CallSign </td>" +
                "<td style = 'border:2px solid; padding: 5px;'> Name </td>" +
                "<td style = 'border:2px solid; padding: 5px;'> Sent From Office</td>" +
                "<td style = 'border:2px solid; padding: 5px;' > Received To Vessel</td>" +
                "<td style = 'border:2px solid; padding: 5px;' > Difference</td>" +

                "<td style = 'border:2px solid; padding: 5px;'> Sent From Vessel</td>" +
                "<td style = 'border:2px solid; padding: 5px;' > Received To Office</td>" +
                "<td style = 'border:2px solid; padding: 5px;' > Difference</td>" +


                "</tr></thead>" +
                "<tbody>";
            var source = new Side("Office", "MsSqlConnectionString", true);
            foreach (var vessel in vessels)
            {
                var target = new Side(vessel.Name, vessel.CallSign, true);
                var connected = 1;
                int dpReciveVesselnum = 0;
                int dpSendVesselnum = 0;
                int dpReciveOfficenum = 0;
                int dpSendOfficenum = 0;
                if (SqlManager.CheckConnection(target.Connection))
                {
                    var dpReciveVessel = SqlManager.ExecuteQuery(sql: "select MAX(Dr_PacketNo) AS Dr_PacketNo from Data_Receive ", connection: target.Connection);
                    var dpSendVessel = SqlManager.ExecuteQuery(sql: "select MAX(Ds_PacketNo) AS Ds_PacketNo from Data_Send ", connection: target.Connection);
                    var dpReciveOffice = SqlManager.ExecuteQuery(sql: $"select MAX(Dr_PacketNo) AS Dr_PacketNo from Data_Receive WHERE Dr_PrepareOwnerCode = '{vessel.CallSign}' ", connection: source.Connection);
                    var dpSendOffice = SqlManager.ExecuteQuery(sql: $"select MAX(Ds_PacketNo) AS Ds_PacketNo from Data_Send WHERE Ds_ReceiveOwnerCode = '{vessel.CallSign}' ", connection: source.Connection);

                    dpReciveVesselnum = dpReciveVessel != null && dpReciveVessel.Count() > 0 &&  dpReciveVessel.FirstOrDefault()["Dr_PacketNo"] != null  ? int.Parse(dpReciveVessel.FirstOrDefault()["Dr_PacketNo"].ToString()) : 0;
                    dpSendVesselnum = dpSendVessel != null && dpSendVessel.Count() > 0 && dpSendVessel.FirstOrDefault()["Ds_PacketNo"] != null ? int.Parse(dpSendVessel.FirstOrDefault()["Ds_PacketNo"].ToString()) : 0;
                    dpReciveOfficenum = dpReciveOffice != null && dpReciveOffice.Count() > 0 && dpReciveOffice.FirstOrDefault()["Dr_PacketNo"] != null  ? int.Parse(dpReciveOffice.FirstOrDefault()["Dr_PacketNo"].ToString()) : 0;
                    dpSendOfficenum = dpSendOffice != null && dpSendOffice.Count() > 0 && dpSendOffice.FirstOrDefault()["Ds_PacketNo"] != null ? int.Parse(dpSendOffice.FirstOrDefault()["Ds_PacketNo"].ToString()) : 0;
                }
                else
                {
                    connected = 0;
                }
                mailMessage += $"<tr";
                mailMessage += connected == 0 || dpReciveVesselnum == 0 ? " style='background-color: yellow;'" : "";
                mailMessage += $">";
                mailMessage += $"<td style='border: 1px solid; padding: 5px; '>{vessel.CallSign}</td>";
                mailMessage += $"<td style='border: 1px solid; padding: 5px; '>{vessel.Name}</td>";


                mailMessage += $"<td style='border: 1px solid; padding: 5px;";
                mailMessage += dpReciveVesselnum != 0 && dpReciveVesselnum != dpSendOfficenum ? "background-color:red;" : "";
                mailMessage += $"'>{dpSendOfficenum}</td>";

                mailMessage += $"<td style='border: 1px solid; padding: 5px; ";
                mailMessage += dpReciveVesselnum != 0 && dpReciveVesselnum != dpSendOfficenum ? "background-color:red;" : "";
                mailMessage += $"'>{dpReciveVesselnum}</td>";

                mailMessage += $"<td style='border: 1px solid; padding: 5px; '>{dpSendOfficenum - dpReciveVesselnum}</td>";


                mailMessage += $"<td style='border: 1px solid; padding: 5px;";
                mailMessage += dpSendVesselnum != 0 && dpSendVesselnum != dpReciveOfficenum ? "background-color:red;" : "";
                mailMessage += $"'>{dpSendVesselnum}</td>";

                mailMessage += $"<td style='border: 1px solid; padding: 5px; ";
                mailMessage += dpSendVesselnum != 0 && dpSendVesselnum != dpReciveOfficenum ? "background-color:red;" : "";
                mailMessage += $"'>{dpReciveOfficenum}</td>";

                mailMessage += $"<td style='border: 1px solid; padding: 5px; '>{dpSendVesselnum - dpReciveOfficenum}</td>";

                mailMessage += "</tr>";


            }
            mailMessage += "</tbody>" +
                "</table>";

            SendMailForDataPackage(mailMessage);
        }
        static void Initialize()
        {
            var config = new ConfigurationBuilder()
            .AddUserSecrets(typeof(Program).Assembly)
            .Build();

            CustomConnection.Start(() =>
            {
                return new Dictionary<string, string>()
                {
                    {
                        "MsSqlConnectionString",
                        config.GetConnectionString("Genel")
                    },
                    {
                        "9BLP",
                         config.GetConnectionString("9BLP")
                    },
                     {
                        "9PTY",
                         config.GetConnectionString("9PTY")
                    },
                    {
                        "9RYL",
                         config.GetConnectionString("9RYL")
                    },
                    {
                        "9HNK",
                         config.GetConnectionString("9HNK")
                    },
                    {
                        "9PRD",
                         config.GetConnectionString("9PRD")
                    },
                    {
                         "9PRK",
                         config.GetConnectionString("9PRK")
                    },
                    {
                        "9VLE",
                        config.GetConnectionString("9VLE")
                    },
                    {
                        "9HA4",
                        config.GetConnectionString("9HA4")
                    },
                    {
                        "9HM2",
                        config.GetConnectionString("9HM2")
                    },
                    {
                        "9REF",
                         config.GetConnectionString("9REF")
                    },
                    {
                        "9BRV",  config.GetConnectionString("9BRV")
                    },
                    {
                        "9HLN",  config.GetConnectionString("9HLN")
                    },
                    {
                        "9VLU",config.GetConnectionString("9VLU")
                    },
                    {
                        "9BUD",config.GetConnectionString("9BUD")
                    },
                    {
                        "9PWR",config.GetConnectionString("9PWR")
                    },
                    {
                        "9HA2",config.GetConnectionString("9HA2")
                    },
                    {
                        "9VRT",config.GetConnectionString("9VRT")
                    },
                    {
                        "9HTU",config.GetConnectionString("9HTU")
                    },
                    {
                        "9PRT",config.GetConnectionString("9PRT")
                    },
                    {
                        "9ANT",config.GetConnectionString("9ANT")
                    },
                    {
                        "9ALF",config.GetConnectionString("9ALF")
                    },
                    {
                        "9ALV",config.GetConnectionString("9ALV")
                    },
                    {
                        "DECK",
                        "Data Source=172.22.23.52; Initial Catalog = GENEL; User Id = sa; Password = '';"
                    }
                };
            });

            new MailManager("mail.gedenlines.com", 25, "shippernetix@gedenlines.com", null, "GedenErp", false);
        }

        public static void FixSpares(string callSign, string group, string product, string item, CustomConnection sourceConnection, CustomConnection targetConnection, bool prepareForVessel)
        {
            var sourceColumnList = SqlManager.SelectColumnNamesForMSSQL("Spares", sourceConnection).Select(d => d["COLUMN_NAME"].ToString()).ToList();

            var targetColumnList = SqlManager.SelectColumnNamesForMSSQL("Spares", targetConnection).Select(d => d["COLUMN_NAME"].ToString()).ToList();

            var sourceColumnListDifferentWithTarget = sourceColumnList.Except(targetColumnList).ToList();

            var spares = new DynamicTable(tableName: "Spares", excludedColumnList: sourceColumnListDifferentWithTarget, customConnection: sourceConnection, parameters: new Dictionary<string, object>()
            {
                {"S_Callsign",callSign },
                {"S_Group", group},
                {"S_Product", product},
                {"S_Item", item},
            })
            .SetUniqueColumns("S_Callsign", "S_Group", "S_Product", "S_Item")
            .PrepareForVesselSide(prepareForVessel)
            .PrepareUpdateAndInsertQueries();

            var affectedRowsCount = SqlManager.ExecuteNonQuery(sql: spares.GetUpdateQueries, parameters: null, targetConnection);
        }

        public static int ExecuteNonQueryOn(MsSqlConnection msSqlConnection, string sql, Dictionary<string, object> parameters = null)
        {
            var arc = SqlManager.ExecuteNonQuery(sql, parameters, msSqlConnection);

            return arc;
        }
    }
}

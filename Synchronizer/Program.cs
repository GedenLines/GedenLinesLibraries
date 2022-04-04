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
using System.Transactions;

namespace Synchronizer
{
    public class Program
    {
        public static void SendMail(string subject,string message)
        {
            new MailManager(o => o.Code == "GedenErp")
                .Prepare(new Mail(new MailAddress("karar@gedenlines.com"), null, $"Synchronization Operation 101 / {subject}", $"{message}")
                            .AddTo(new MailAddress("bt@gedenlines.com")))
            .Send((ex) =>
            {
                Console.WriteLine(ex.Message);
            });
        }

        static void Main(string[] args)
        {
            Initialize();

            //for (int i = 0; i < Vessel_Master.Vessels.Count; i++)
            //{
            //    var vessel = Vessel_Master.Vessels[i];

            //    Automat automat = new Automat(vessel.CallSign,vessel.Name);

            //    automat.AddJob(new Job("Job_History",1000,null,true)
            //        .SetAction(()=> 
            //        {
            //            Console.WriteLine($"{vessel.Name} {vessel.CallSign}");
            //        }));

            //}

            Service.Start();



            ////Evrim Bey İstek
            //var jhCallSign = "9PWR";

            //int jhL1 = 5;

            //int jhL2 = 3;

            //int jhL3 = 2;

            //int jhL4 = 1;

            //var jhJobCode = "G";

            //Shippernetix.Job_History.ReCalculateLastJob(new MsSqlConnection(connectionString: "MsSqlConnectionString"), callSign: jhCallSign, l1: jhL1.ToString(), l2: jhL2.ToString(), l3: jhL3.ToString(), l4: jhL4.ToString(), jobCode: jhJobCode, actionToWorkWithLastCompletedJob: null);
            ////for (int i = 1; i < 15; i++)
            //{
            //    jhL4 = i;

            //    var action = new Action<int>((int maxLastJobNumber) =>
            //    {
            //        //var date = "2021-30-11 00:00:00.000";

            //        //var updateJobHistorySql = $"update Job_History set Je_StartDate='{date}',Je_FinishDate='{date}' where Je_CallSign='{jhCallSign}' and Je_L1='{jhL1}' and Je_L2='{jhL2}' and Je_L3='{jhL3}' and Je_L4='{jhL4}' and Je_JobCode='{jhJobCode}' and Je_JobNo='{maxLastJobNumber}'";

            //        //var arc = SqlManager.ExecuteNonQuery(updateJobHistorySql, null, new MsSqlConnection(connectionString: "MsSqlConnectionString"));

            //        //Console.WriteLine($"{arc > 0}");
            //    });

            //    Shippernetix.Job_History.ReCalculateLastJob(new MsSqlConnection(connectionString: "MsSqlConnectionString"), callSign: jhCallSign, l1: jhL1.ToString(), l2: jhL2.ToString(), l3: jhL3.ToString(), l4: jhL4.ToString(), jobCode: jhJobCode, actionToWorkWithLastCompletedJob: action);

            //    Console.WriteLine(i);
            //}
            ////Evrim Bey İstek End


            //ExecuteNonQueryOn(new MsSqlConnection(connectionString: "9VRT"), "delete from Approval where A_CallSign='9VRT' and A_L1='4' and A_L2='7' and A_L3='10' and A_L4='1'");
            //FixSpares("9BRV", "5", "1", "0", new MsSqlConnection(connectionString: "9BRV"), new MsSqlConnection(connectionString: "MsSqlConnectionString"), false);
            //Shippernetix.Job_History.Fix("9VRT", "4","7","10","1", "A", "2", new MsSqlConnection(connectionString: "MsSqlConnectionString"), new MsSqlConnection(connectionString: "9VRT"), true);
            //Shippernetix.Job_History.Fix("9HLN", "4", "1", "4", "1", "A", "1", new MsSqlConnection(connectionString: "9HLN"), new MsSqlConnection(connectionString: "MsSqlConnectionString"), true);
            //Shippernetix.Job_History.FixFromOfficeToVessel("9PTY", "6", "1", "7", "4", "B", "1");
            //Shippernetix.Job_History.FixFromVesselToOffice("9PRD", "16", "1", "1", "1", "A", "14");
            //Shippernetix.Job_History.FixFromOfficeToVessel("9BRV", "2", "3", "1", "1", "B", "8");
            //Shippernetix.Job_History.FixFromOfficeToVessel("9BRV", "2", "3", "5", "1", "D", "6");
            // Shippernetix.Job_History.FixFromOfficeToVessel("9BRV", "2", "3", "9", "2", "A", "21");
            //Shippernetix.Job_History.FixFromOfficeToVessel("9BRV", "2", "3", "2", "3", "A", "5");

            ////NO.1 CYL.  01.01.13.07 - A / 21.01.2022  tarihinde yapılmıştır.
            ////NO.2 CYL.  01.01.13.08 - A / 22.01 2022  tarihinde yapılmıştır.
            ////NO.4 CYL.  01.01.13.10 - A / 26.01.2022  tarihinde yapılmıştır.
            ////NO.5 CYL.  01.01.13.11 - A / 22.01 2022  tarihinde yapılmıştır.
            ////NO.6 CYL.  01.01.13.12 - A / 26.01.2022  tarihinde yapılmıştır.

            //Shippernetix.Job_History.ReCalculateLastJob(new MsSqlConnection(connectionString: "MsSqlConnectionString"), callSign: "9HLN", "1", "1", "13", "7", "A", "2022-01-21 00:00:00.000");

            //Shippernetix.Job_History.ReCalculateLastJob(new MsSqlConnection(connectionString: "MsSqlConnectionString"), callSign: "9HLN", "1", "1", "13", "8", "A", "2022-01-22 00:00:00.000");

            //Shippernetix.Job_History.ReCalculateLastJob(new MsSqlConnection(connectionString: "MsSqlConnectionString"), callSign: "9HLN", "1", "1", "13", "10", "A", "2022-01-26 00:00:00.000");

            //Shippernetix.Job_History.ReCalculateLastJob(new MsSqlConnection(connectionString: "MsSqlConnectionString"), callSign: "9HLN", "1", "1", "13", "11", "A", "2022-01-22 00:00:00.000");

            //Shippernetix.Job_History.ReCalculateLastJob(new MsSqlConnection(connectionString: "MsSqlConnectionString"), callSign: "9HLN", "1", "1", "13", "12", "A", "2022-01-26 00:00:00.000");


            //Defect.FixFromOfficeToVessel("9HNK","Def-");

            //Shippernetix.Job_History.FixFromOfficeToVessel("9BUD", "5", "8", "6", "1", "Def-1968", "1");
            //Shippernetix.Job_History.FixFromOfficeToVessel("9BUD", "5", "8", "6", "1", "Def-2958", "1");
            //Shippernetix.Job_History.FixFromOfficeToVessel("9BUD", "5", "10", "1", "2", "Def-2962", "1");



            Console.WriteLine("");
            //Shippernetix.Defect.FixDefectsIsHiddenStatus("9HLN",new MsSqlConnection(connectionString: "MsSqlConnectionString"),new MsSqlConnection(connectionString: "9HLN"));

            //Shippernetix.Job_History.ReCalculateLastJob(new MsSqlConnection("MsSqlConnectionString"),"9VRT","3","3","7","1","A");

            //Console.ReadLine();

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

            //        var connection2 = new MsSqlConnection();

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
            


            foreach (var vessel in Vessel_Master.Vessels)
            { 
               if (vessel.CallSign != "9ANT")
                    continue;

                var source = new Side("Office", "MsSqlConnectionString",true);

                var isSourceConnected = SqlManager.CheckConnection(source.Connection);

                var target = new Side(vessel.Name, vessel.CallSign, true);

                var istargetConnected = SqlManager.CheckConnection(target.Connection);

                if (isSourceConnected && istargetConnected)
                {
                    var maxDataPackage = SqlManager.ExecuteScalar("select max(CAST(Dr_PacketNo as int)) from Data_Receive where Dr_PrepareOwnerCode=@CallSign",new Dictionary<string, object>() 
                    {
                        {"CallSign",vessel.CallSign }
                    }, connection:target.Connection);

                    #region Delete Unwanted job which reaches max job number before previous one is completed

                    var sqlToClear = "select x.*from( " +
                        "select jh.*, " +
                        "(select max(Je_JobNo) from Job_History where Je_CallSign = jh.Je_CallSign and Je_L1 = jh.Je_L1 and Je_L2 = jh.Je_L2 and Je_L3 = jh.Je_L3 and Je_L4 = jh.Je_L4 and Je_JobCode = jh.Je_JobCode) as MaxJobNumber " +
                        "from Job_History jh " +
                        "where jh.Je_CallSign = @CallSign )x where x.Je_JobNo = x.MaxJobNumber - 1 and x.Je_Status <> 'COMPLETED' ";

                    var sourceRecordsToClear = SqlManager.ExecuteQuery(sqlToClear,new Dictionary<string, object>() 
                    {
                        {"CallSign",vessel.CallSign }
                    },source.Connection)
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
                        Console.WriteLine("{0} is deleted : {1}",item.MaxJobNumber,
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
                            },source.Connection));
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
                            Shippernetix.Job_History.Fix(sourceItem.Je_CallSign.ToString(), sourceItem.Je_L1.ToString(), sourceItem.Je_L2.ToString(), sourceItem.Je_L3.ToString(), sourceItem.Je_L4.ToString(), sourceItem.Je_JobCode.ToString(), sourceItem.Je_JobNo.ToString(), target.Connection,source.Connection, true);
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
                            Shippernetix.Job_History.Fix(targetItem.Je_CallSign.ToString(), targetItem.Je_L1.ToString(), targetItem.Je_L2.ToString(), targetItem.Je_L3.ToString(), targetItem.Je_L4.ToString(), targetItem.Je_JobCode.ToString(), targetItem.Je_JobNo.ToString(), source.Connection,target.Connection, true);
                        }
                        Console.WriteLine(targetItem.ShippernetixId + " " + counter++);
                    }
                }

                if (isSourceConnected && istargetConnected)
                {
                    var onlyMail = false;

                    //Structure.Sync(source, target, vessel, onlyMail);
                    //Job_Definition.Sync(source, target, vessel, onlyMail);
                    //Defect.Sync(source, target, vessel, onlyMail);
                    Job_History.Sync(source, target, vessel, onlyMail);
                }
                else
                    Console.WriteLine("Source({0}) is connected : {1},Target({2}) is Connected : {3}", source.Name, isSourceConnected, target.Name, istargetConnected);
            }
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

        public static int  ExecuteNonQueryOn(MsSqlConnection msSqlConnection,string sql,Dictionary<string,object> parameters = null)
        {
            var arc = SqlManager.ExecuteNonQuery(sql,parameters,msSqlConnection);

            return arc;
        }
    }
}

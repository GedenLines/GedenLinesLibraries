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
        public static void SendMail(string subject,string message)
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

        public static void Main(string[] args)
        {
            Initialize();

            Service.Start();


            Console.WriteLine();


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

            //Task.WaitAll(tList.ToArray());

            Console.WriteLine("");


            foreach (var vessel in Vessel_Master.Vessels)
            { 
               if (vessel.CallSign != "9RYL")
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

                    Structure.Sync(source, target, vessel, onlyMail);
                    Job_Definition.Sync(source, target, vessel, onlyMail);
                    Defect.Sync(source, target, vessel, onlyMail);
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
                    },
                    {
                        "9ALF",config.GetConnectionString("9ALF")
                    },
                    {
                        "9ALV",config.GetConnectionString("9ALV")
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

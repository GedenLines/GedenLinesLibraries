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
            .AddTo(new MailAddress("bt@gedenlines.com")))
            //.AddTo(new MailAddress("karar@gedenlines.com")))
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

            AutomatService.Start();

            Console.WriteLine();

            Automat aoutTrigger = new Automat("Trigger Checked", "Trigger Checked");
            var regTrigger = new AutomatJob("Trigger Checked")
                .SetInterval(minutes: 30)
                //.SetInterval(seconds:5)
                .SetContinuous(true)
                .SetAction((j) =>
                {
                    TriggerEnableDisableController();
                });

            aoutTrigger.AddJob(regTrigger);


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

                try

                {
                    foreach (var vessel in Vessel_Master.Vessels)
                    {
                        if (vessel.CallSign != vesselCallSign)
                            continue;
                        //if (vessel.CallSign == "9VRD")
                        //    continue;

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

                            Console.WriteLine("queryToFix is working for Source");
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

                            Console.WriteLine("sqlToClear is working for Source");
                            foreach (var item in sourceRecordsToClear)
                            {
                                Console.WriteLine("{0},item.MaxJobNumber : {1} is deleted : {2}", item.ShippernetixId, item.MaxJobNumber,
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

                            Console.WriteLine("sqlToClear is working for Target");
                            foreach (var item in targetRecordsToClear)
                            {
                                Console.WriteLine("{0},item.MaxJobNumber : {1} is deleted : {2}", item.ShippernetixId, item.MaxJobNumber,
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
                catch (Exception exc)
                {

                    Console.WriteLine(exc);
                }
            }
            else if (status == 2)
            {

                DataPackageNumbersMatch();

            }
            else if (status == 3)
            {
                
                Automat aout = new Automat("Package","Package");
                var reg = new AutomatJob("Package")
                    .SetInterval(hours:1)
                    .SetContinuous(true)
                    .SetAction((j) => 
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
                        "9VRD",
                        "Data Source=172.22.23.58; Initial Catalog = GENEL; User Id = sa; Password = '';"
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

        public static void TriggerEnableDisableController()
        {
            var vessels = Vessel_Master.Vessels;
          
            string mailMessage = $"<p>Date = {DateTime.Now}</p>";

            foreach (var vessel in vessels)
            {
                var target = new Side(vessel.Name, vessel.CallSign, true);

                if (SqlManager.CheckConnection(target.Connection))
                {
                    var disabledTriggerQuery = "select * from  sys.triggers where is_disabled=1 and name in('DEFECT_DEL','DEFECT_INS','DEFECT_UPD','DEFECT_UPD_CC','DEFECT_UPD_TO_HIDE','Job_DefDeL','Job_Definition_Ins','Job_Definition_Upd','Job_History_Edit','DELETE_SPARES','SPARE_INSERT')";

                    var results = SqlManager.ExecuteQuery(disabledTriggerQuery,
                        parameters: null,
                        target.Connection)
                        .Select(d => new
                        {
                            Name = d["name"]
                        });

                    Console.WriteLine($"Connecting to {vessel.Name}");

                    if (results.Any())
                    {
                        mailMessage += $"{vessel.CallSign} </br>";
                        mailMessage += $"{vessel.Name} </br>";
                        mailMessage += $"{string.Join(',', results.Select(r => r.Name))}Trigger is Disabled";

                        new MailManager(o => o.Code == "GedenErp")
                                   .Prepare(new Mail(from : new MailAddress("shippernetix@gedenlines.com"),to: null,subject: "Disabled Triggers Controller",body:mailMessage)
                                   //.AddTo(new MailAddress("bt@gedenlines.com")))
                                   .AddTo(new MailAddress("karar@gedenlines.com")))
                                   .Send((ex) =>
                                   {
                                       Console.WriteLine(ex.Message);
                                   });
                    }
                    else
                    {
                        Console.WriteLine("No disabled trigger found");
                    }
                }
            }
        }

        public static int ExecuteNonQueryOn(MsSqlConnection msSqlConnection, string sql, Dictionary<string, object> parameters = null)
        {
            var arc = SqlManager.ExecuteNonQuery(sql, parameters, msSqlConnection);

            return arc;
        }
    }
}

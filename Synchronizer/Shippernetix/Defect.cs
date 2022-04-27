using SqlManagement;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Synchronizer.Shippernetix
{
    public class Defect : ShippernetixDynamicObject
    {
        public Defect(string callSign, string number, List<string> excludedColumnList, CustomConnection customConnection, bool prepareForVesselSide)
        {
            var parameters = new Dictionary<string, object>();

            parameters.Add("Dai_CallSign", callSign);

            parameters.Add("Dai_DamageNo", number);

            Table = new DynamicTable(tableName: "Defects", excludedColumnList: excludedColumnList,customConnection: customConnection, parameters: parameters)
                        .SetUniqueColumns("Dai_CallSign", "Dai_DamageNo")
                        .PrepareForVesselSide(prepareForVesselSide)
                        .PrepareUpdateAndInsertQueries();
        }

        public static void Sync(Side source, Side target, Vessel_Master vessel, bool onlyMail)
        {
            var messageAction = new Func<string, string>(message =>
            {
                Console.WriteLine("\n{0}:\n" + DateTime.Now);

                Console.WriteLine(message);

                return message;
            });

            UpdateDefectsStatusOpenWhenItIsBARIS(source.Connection);
            UpdateDefectsStatusOpenWhenItIsBARIS(target.Connection);


            var sourceColumnList = SqlManager.SelectColumnNamesForMSSQL("Defects", source.Connection).Select(d => d["COLUMN_NAME"].ToString()).ToList();

            var targetColumnList = SqlManager.SelectColumnNamesForMSSQL("Defects", target.Connection).Select(d => d["COLUMN_NAME"].ToString()).ToList();

            var sourceColumnListDifferentWithTarget = sourceColumnList.Except(targetColumnList).ToList();
            var targetColumnListDifferentWithSource = targetColumnList.Except(sourceColumnList).ToList();


            List<string> logMessages = new List<string>() { messageAction("DefectSync is started") };

            var defectGroupsQuery = "select Dai_CallSign,Dai_DamageNo,Dai_Status,Dai_UnitDesc from(select * from Defects where Dai_CallSign = @CallSign)x group by Dai_CallSign, Dai_DamageNo, Dai_Status,Dai_UnitDesc";

            var sourceList = SqlManager.ExecuteQuery(defectGroupsQuery,
                    new Dictionary<string, object>()
                    {
                        { "CallSign",vessel.CallSign }
                    },
                    source.Connection)
                    .Select(d => new
                    {
                        ShippernetixId = string.Format("{0}-{1}", d["Dai_CallSign"], d["Dai_DamageNo"]),
                        Dai_CallSign = d["Dai_CallSign"],
                        Dai_DamageNo = d["Dai_DamageNo"],
                        Dai_Status = d["Dai_Status"],
                        Dai_UnitDesc = d["Dai_UnitDesc"]
                    })
                    .ToList();

            var targetList = SqlManager.ExecuteQuery(defectGroupsQuery,
                new Dictionary<string, object>()
                {
                        { "CallSign",vessel.CallSign }
                },
                target.Connection)
                .Select(d => new
                {
                    ShippernetixId = string.Format("{0}-{1}", d["Dai_CallSign"], d["Dai_DamageNo"]),
                    Dai_CallSign = d["Dai_CallSign"],
                    Dai_DamageNo = d["Dai_DamageNo"],
                    Dai_Status = d["Dai_Status"],
                    Dai_UnitDesc = d["Dai_UnitDesc"]
                })
                .ToList();


            var sourceSample = sourceList.FirstOrDefault(s=>s.Dai_DamageNo.ToString()=="Def-312");
            var targetSample = targetList.FirstOrDefault(s => s.Dai_DamageNo.ToString() == "Def-312");

            var sourceMissing = targetList.Select(t=>t.ShippernetixId).Except(sourceList.Select(s=>s.ShippernetixId)).ToList();
            var targetMissing = sourceList.Select(t => t.ShippernetixId).Except(targetList.Select(s => s.ShippernetixId)).ToList();

            foreach (var sm in sourceMissing)
            {
                var defectToProcess = targetList.FirstOrDefault(s=>s.ShippernetixId==sm);

                var targetStructureSql = "SELECT * FROM Structure WHERE En_CallSign = @CallSign and En_Desc = @Desc";

                var targetStructure = SqlManager.ExecuteQuery(targetStructureSql,
                                    new Dictionary<string, object>()
                                    {
                                            { "CallSign",defectToProcess.Dai_CallSign },
                                        {"Desc",defectToProcess.Dai_UnitDesc }
                                    },
                                    target.Connection)
                                    .Select(d => new
                                    {
                                        ShippernetixId = string.Format("{0}-{1}-{2}-{3}-{4}", d["En_CallSign"], d["En_L1"], d["En_L2"], d["En_L3"], d["En_L4"]),
                                        En_CallSign = d["En_CallSign"],
                                        En_L1 = d["En_L1"],
                                        En_L2 = d["En_L2"],
                                        En_L3 = d["En_L3"],
                                        En_L4 = d["En_L4"],
                                        En_Desc = d["En_Desc"]
                                    })
                                    .ToList().FirstOrDefault();

                if (targetStructure != null)
                {
                    var sourceStructureSql = "SELECT * FROM Structure WHERE En_CallSign = @CallSign and  En_L1 = @L1 and En_L2 = @L2 and En_L3 = @L3 and En_L4 = @L4";

                    var sourceStructure = SqlManager.ExecuteQuery(sourceStructureSql,
                        new Dictionary<string, object>()
                        {
                                { "CallSign",defectToProcess.Dai_CallSign },
                            {"L1",targetStructure.En_L1 },
                            {"L2",targetStructure.En_L2 },
                            {"L3",targetStructure.En_L3 },
                            {"L4",targetStructure.En_L4 }
                        },
                        source.Connection)
                        .Select(d => new
                        {
                            ShippernetixId = string.Format("{0}-{1}-{2}-{3}-{4}", d["En_CallSign"], d["En_L1"], d["En_L2"], d["En_L3"], d["En_L4"]),
                            En_CallSign = d["En_CallSign"],
                            En_L1 = d["En_L1"],
                            En_L2 = d["En_L2"],
                            En_L3 = d["En_L3"],
                            En_L4 = d["En_L4"],
                            En_Desc = d["En_Desc"]
                        })
                        .ToList().FirstOrDefault();

                    var updateTargetStructreSql = "update Structure set En_Desc=@Desc where En_CallSign = @CallSign and  En_L1 = @L1 and En_L2 = @L2 and En_L3 = @L3 and En_L4 = @L4";

                    var ar = SqlManager.ExecuteNonQuery(updateTargetStructreSql, new Dictionary<string, object>()
                    {
                        {"Desc" ,sourceStructure.En_Desc},
                        {"CallSign" ,sourceStructure.En_CallSign},
                        {"L1" ,sourceStructure.En_L1},
                        {"L2" ,sourceStructure.En_L2},
                        {"L3" ,sourceStructure.En_L3},
                        {"L4" ,sourceStructure.En_L4}
                    }, target.Connection);


                    var updateTargetDefectSql = "update Defects set Dai_UnitDesc=@Desc where Dai_CallSign=@CallSign and Dai_DamageNo=@DamageNo";


                    SqlManager.DisableTrigger("DEFECT_UPD", "Defects", target.Connection);
                    SqlManager.DisableTrigger("DEFECT_UPT_CC", "Defects", target.Connection);

                    var ar2 = SqlManager.ExecuteNonQuery(updateTargetDefectSql, new Dictionary<string, object>()
                    {
                        {"Desc",sourceStructure.En_Desc },
                        {"CallSign", defectToProcess.Dai_CallSign},
                        {"DamageNo", defectToProcess.Dai_DamageNo}
                    }, target.Connection);


                    SqlManager.EnableTrigger("DEFECT_UPD", "Defects", target.Connection);
                    SqlManager.EnableTrigger("DEFECT_UPT_CC", "Defects", target.Connection);

                    try
                    {
                        Shippernetix.Job_History.Fix(sourceStructure.En_CallSign.ToString(), sourceStructure.En_L1.ToString(), sourceStructure.En_L2.ToString(), sourceStructure.En_L3.ToString(), sourceStructure.En_L4.ToString(), defectToProcess.Dai_DamageNo.ToString(), "1", new MsSqlConnection(connectionString: defectToProcess.Dai_CallSign.ToString()), new MsSqlConnection(connectionString: "MsSqlConnectionString"), true);

                        var ar3 = SqlManager.ExecuteNonQuery("delete from Job_History where Je_CallSign=@CallSign and Je_L1=@L1 and  Je_L2=@L2 and Je_L3=@L3 and Je_L4=@L4 and Je_JobCode=@JobCode and Je_JobNo>1",
                            new Dictionary<string, object>()
                            {
                                {"CallSign", sourceStructure.En_CallSign },
                                {"L1",sourceStructure.En_L1},
                                {"L2", sourceStructure.En_L2},
                                {"L3", sourceStructure.En_L3},
                                {"L4", sourceStructure.En_L4},
                                {"JobCode", defectToProcess.Dai_DamageNo}
                            }, new MsSqlConnection(connectionString: "MsSqlConnectionString"));
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("12777");
                        Console.WriteLine(ex);
                    }

                }



                //targetColumnListDifferentWithSource.AddRange(new[] {      "Dai_DefDoc1"
                //                                          ,"Dai_DefDoc2"
                //                                          ,"Dai_DefDoc3"
                //                                          ,"Dai_RepDoc1"
                //                                          ,"Dai_RepDoc2"
                //                                          ,"Dai_RepDoc3"
                //                                          ,"Dai_DefImg" });

                var defect = new Defect(defectToProcess.Dai_CallSign.ToString(),
                    defectToProcess.Dai_DamageNo.ToString(),
                    targetColumnListDifferentWithSource,
                    target.Connection,
                    target.PrepareForVesselSide);

                SqlManager.DisableTrigger("DEFECT_INS", "Defects", source.Connection);

                var affectedRowsCount = SqlManager.ExecuteNonQuery(sql: defect.Table.GetInsertQueries, parameters: null, source.Connection);

                SqlManager.EnableTrigger("DEFECT_INS", "Defects", source.Connection);

                if (affectedRowsCount > 0)
                    logMessages.Add(messageAction(string.Format("{0} Transfered From {0} To {1}", defectToProcess.ShippernetixId, source.Name, target.Name)));
                else
                    logMessages.Add(messageAction(string.Format("An error occured while transfering structure : {0}", defectToProcess.ShippernetixId)));

            }

            foreach (var tm in targetMissing)
            {
                var defectToProcess = sourceList.FirstOrDefault(s => s.ShippernetixId == tm);

                var defect = new Defect(defectToProcess.Dai_CallSign.ToString(),
                                    defectToProcess.Dai_DamageNo.ToString(),
                                    sourceColumnListDifferentWithTarget,
                                    source.Connection,
                                    source.PrepareForVesselSide);

                SqlManager.DisableTrigger("DEFECT_INS", "Defects", target.Connection);
                SqlManager.DisableTrigger("DEFECT_INS_CC", "Defects", target.Connection);

                var affectedRowsCount = SqlManager.ExecuteNonQuery(sql: defect.Table.GetInsertQueries, parameters: null, target.Connection);

                SqlManager.EnableTrigger("DEFECT_INS", "Defects", target.Connection);
                SqlManager.EnableTrigger("DEFECT_INS_CC", "Defects", target.Connection);

                if (affectedRowsCount > 0)
                    logMessages.Add(messageAction(string.Format("{0} Transfered From {0} To {1}", defectToProcess.ShippernetixId, source.Name, source.Name)));
                else
                    logMessages.Add(messageAction(string.Format("An error occured while transfering structure : {0}", defectToProcess.ShippernetixId)));
            }

            logMessages.Add(messageAction(string.Format("Fetched {0} records from {1} and {2} records from {3} For {4}({5})",
                                                                                    sourceList.Count(),
                                                                                    source.Name,
                                                                                    targetList.Count(),
                                                                                    target.Name,
                                                                                    vessel.Name,
                                                                                    vessel.CallSign)));

            if (sourceList.Count == 0 || targetList.Count == 0)
                throw new Exception("Could not fetch data...");

            var sourceDifferences = sourceList.Where(ss => ss.Dai_Status.ToString() == "CLOSED").Select(ss => ss.ShippernetixId)
                                .Intersect(targetList.Where(ts => ts.Dai_Status.ToString() != "CLOSED").Select(ts => ts.ShippernetixId))
                                .Select(e => sourceList.FirstOrDefault(s => s.ShippernetixId == e))
                                .ToList();

            logMessages.Add(messageAction(string.Format("Source different from target with {0}", sourceDifferences.Count())));

            logMessages.Add(messageAction(string.Format("Differences : {0}", string.Join(",", sourceDifferences.Select(sd => sd.ShippernetixId)))));

            if(!onlyMail)
            if (sourceDifferences.Any())
            {
                logMessages.Add(messageAction(string.Format("Source different from target with {0} : {1}", sourceDifferences.Count(), string.Join(",", sourceDifferences.Select(sd => sd.ShippernetixId)))));

                foreach (var sourceDifference in sourceDifferences)
                {

                    var defect = new Defect(sourceDifference.Dai_CallSign.ToString(),
                                                        sourceDifference.Dai_DamageNo.ToString(),
                                                        sourceColumnListDifferentWithTarget,
                                                        source.Connection,
                                                        source.PrepareForVesselSide);

                    SqlManager.DisableTrigger("DEFECT_UPD", "Defects", target.Connection);
                    SqlManager.DisableTrigger("DEFECT_UPT_CC", "Defects", target.Connection);

                    var affectedRowsCount = SqlManager.ExecuteNonQuery(sql: defect.Table.GetUpdateQueries, parameters: null, target.Connection);

                    SqlManager.EnableTrigger("DEFECT_UPD", "Defects", target.Connection);
                    SqlManager.EnableTrigger("DEFECT_UPT_CC", "Defects", target.Connection);

                    if (affectedRowsCount > 0)
                        logMessages.Add(messageAction(string.Format("{0} Transfered From {1} To {2}", sourceDifference.ShippernetixId, source.Name, target.Name)));
                    else
                        logMessages.Add(messageAction(string.Format("An error occured while transfering structure : {0}", sourceDifference.ShippernetixId)));
                }
            }

            var targetDifferences = targetList.Where(ts => ts.Dai_Status.ToString() == "VESSEL COMPLETED").Select(ss => ss.ShippernetixId)
                                .Intersect(sourceList.Where(ss => ss.Dai_Status.ToString() == "OPEN").Select(ts => ts.ShippernetixId))
                                 .Select(e => targetList.FirstOrDefault(s => s.ShippernetixId == e))
                                .ToList();


            logMessages.Add(messageAction(string.Format("Target different from source with {0}", targetDifferences.Count())));

            logMessages.Add(messageAction(string.Format("Differences : {0}", string.Join(",", targetDifferences.Select(sd => sd.ShippernetixId)))));

            if(!onlyMail)
            if (targetDifferences.Any())
            {
                logMessages.Add(messageAction(string.Format("Target different from source with {0} : {1}", targetDifferences.Count(), string.Join(",", targetDifferences.Select(td => td.ShippernetixId)))));

                foreach (var targetDifference in targetDifferences)
                {
                    var defect = new Defect(targetDifference.Dai_CallSign.ToString(),
                                targetDifference.Dai_DamageNo.ToString(),
                                targetColumnListDifferentWithSource,
                                target.Connection,
                                target.PrepareForVesselSide);


                    var affectedRowsCount = SqlManager.ExecuteNonQuery(sql: defect.Table.GetUpdateQueries, parameters: null, source.Connection);

                    if (affectedRowsCount > 0)
                        logMessages.Add(messageAction(string.Format("{0} Transfered From {0} To {1}", targetDifference.ShippernetixId, target.Name, source.Name)));
                    else
                        logMessages.Add(messageAction(string.Format("An error occured while transfering structure : {0}", targetDifference.ShippernetixId)));
                }
            }

            SqlManager.EnableTrigger("DEFECT_UPD", "Defects", target.Connection);
            SqlManager.EnableTrigger("DEFECT_UPD", "Defects", source.Connection);

            SqlManager.EnableTrigger("DEFECT_INS", "Defects", target.Connection);
            SqlManager.EnableTrigger("DEFECT_INS", "Defects", source.Connection);

            SqlManager.EnableTrigger("DEFECT_INS_CC", "Defects", target.Connection);
            SqlManager.EnableTrigger("DEFECT_UPT_CC", "Defects", target.Connection);

            Program.SendMail($"Defect/{vessel.Name}({vessel.CallSign})", string.Join("</br></br>", logMessages));
        }

        public static void UpdateDefectsStatusOpenWhenItIsBARIS(CustomConnection connection)
        {
            var sql = "update Defects set Dai_Status='OPEN' where Dai_Status='BARIS' ";

            var arc = SqlManager.ExecuteNonQuery(sql,connection:connection);

            Console.WriteLine("UpdateDefectsStatusOpenWhenItIsBARIS was worked and its result : " + arc);
        }

        public static void FixDefectsIsHiddenStatus(string callSign,CustomConnection source,CustomConnection target)
        {
            var sql = $"select * from Defects where Dai_CallSign='{callSign}' and (Dai_Status='OPEN' or Dai_Status='VESSEL COMPLETED') and Hidden=1";

            var defects = SqlManager.ExecuteQuery(sql,null,source);

            foreach (var defect in defects)
            {
                var updateSql = "update Defects set Hidden=@IsHidden where Dai_CallSign=@CallSign and Dai_DamageNo=@DamageNo";

                var isHidden = Convert.ToBoolean(defect["Hidden"].ToString()) ? 1 : 0;

                var damageNo = defect["Dai_DamageNo"].ToString();

                var arc = SqlManager.ExecuteNonQuery(updateSql, new Dictionary<string, object>()
                {
                    {"IsHidden", isHidden},
                    {"CallSign",callSign },
                    {"DamageNo", damageNo}
                }, connection: target);

                Console.WriteLine("FixDefectsIsHiddenStatus :" + arc + " affected rows count");
            }            
        }

        public static void FixFromOfficeToVessel(string callSign, string defectNumber)
        {
            var officeConnection = new MsSqlConnection(connectionString: "MsSqlConnectionString");
            var vesselConnection = new MsSqlConnection(connectionString: callSign);

            Fix(callSign, defectNumber,officeConnection,vesselConnection);
        }

        public static void FixFromVesselToOffice(string callSign, string defectNumber)
        {
            var officeConnection = new MsSqlConnection(connectionString: "MsSqlConnectionString");
            var vesselConnection = new MsSqlConnection(connectionString: callSign);

            Fix(callSign,defectNumber,vesselConnection,officeConnection);
        }

        public static void Fix(string callSign, string defectNumber, CustomConnection sourceConnection, CustomConnection targetConnection)
        {
            var sourceColumnList = SqlManager.SelectColumnNamesForMSSQL("Defects", sourceConnection).Select(d => d["COLUMN_NAME"].ToString()).ToList();

            var targetColumnList = SqlManager.SelectColumnNamesForMSSQL("Defects", targetConnection).Select(d => d["COLUMN_NAME"].ToString()).ToList();

            var sourceColumnListDifferentWithTarget = sourceColumnList.Except(targetColumnList).ToList();

            var defect = new Defect(callSign, defectNumber, sourceColumnListDifferentWithTarget, sourceConnection, true);

            var affectedRowsCount = SqlManager.ExecuteNonQuery(sql: defect.Table.GetUpdateQueries, parameters: null, targetConnection);

            Console.WriteLine($"{callSign}-{defectNumber} is fixing :  {affectedRowsCount}");
        }
    }
}

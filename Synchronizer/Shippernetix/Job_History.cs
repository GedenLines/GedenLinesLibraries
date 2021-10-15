using SqlManagement;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Synchronizer.Shippernetix
{
    public class Job_History : ShippernetixDynamicObject
    {
        public Job_History(string callSign, string l1, string l2, string l3, string l4, string jobCode, string jobNumber, List<string> excludedColumnList,CustomConnection customConnection, bool prepareForVesselSide)
        {
            var parameters = new Dictionary<string, object>();

            if (!string.IsNullOrEmpty(callSign))
                parameters.Add("Je_CallSign", callSign);


            if (!string.IsNullOrEmpty(l1))
                parameters.Add("Je_L1", l1);


            if (!string.IsNullOrEmpty(l2))
                parameters.Add("Je_L2", l2);


            if (!string.IsNullOrEmpty(l3))
                parameters.Add("Je_L3", l3);


            if (!string.IsNullOrEmpty(l4))
                parameters.Add("Je_L4", l4);


            if (!string.IsNullOrEmpty(jobCode))
                parameters.Add("Je_JobCode", jobCode);


            if (!string.IsNullOrEmpty(jobNumber))
                parameters.Add("Je_JobNo", jobNumber);

            Table = new DynamicTable(tableName: "Job_History", excludedColumnList: excludedColumnList,customConnection:customConnection ,parameters: parameters)
                        .SetUniqueColumns("Je_CallSign", "Je_L1", "Je_L2", "Je_L3", "Je_L4", "Je_JobCode", "Je_JobCode", "Je_JobNo")
                        .PrepareForVesselSide(prepareForVesselSide)
                        .PrepareUpdateAndInsertQueries();
        }

        public static void Sync(Side source, Side target, Vessel_Master vessel,bool onlyMail)
        {
            var messageAction = new Func<string, string>(message =>
            {
                Console.WriteLine("\n{0}:\n", DateTime.Now);

                Console.WriteLine(message);

                return message;
            });

            var sourceColumnList = SqlManager.SelectColumnNamesForMSSQL("Job_History", source.Connection).Select(d => d["COLUMN_NAME"].ToString()).ToList();

            var targetColumnList = SqlManager.SelectColumnNamesForMSSQL("Job_History", target.Connection).Select(d => d["COLUMN_NAME"].ToString()).ToList();

            var sourceColumnListDifferentWithTarget = sourceColumnList.Except(targetColumnList).ToList();
            var targetColumnListDifferentWithSource = targetColumnList.Except(sourceColumnList).ToList();

            List<string> logMessages = new List<string>() { messageAction("Job_History Sync is started") };

            var jobGroupsQuery = "select y.*," +
                "(select max(jh.Je_JobNo) from Job_History jh where jh.Je_CallSign = y.Je_CallSign " +
                "and jh.Je_L1 = y.Je_L1 " +
                "and jh.Je_L2 = y.Je_L2 " +
                "and jh.Je_L3 = y.Je_L3 " +
                "and jh.Je_L4 = y.Je_L4 " +
                "and jh.Je_JobCode = y.Je_JobCode) as MaxJobNumber " +
                "from(select Je_CallSign, Je_L1, Je_L2, Je_L3, Je_L4, Je_JobCode, Je_JobNo, Je_Status " +
                "from(select jh1.* from Job_History jh1 where jh1.Je_CallSign=@CallSign)x " +
                "group by Je_CallSign, Je_L1, Je_L2, Je_L3, Je_L4, Je_JobCode, Je_JobNo, Je_Status)y";

            var sourceList = SqlManager.ExecuteQuery(jobGroupsQuery,
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

            var targetList = SqlManager.ExecuteQuery(jobGroupsQuery,
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

            logMessages.Add(messageAction(string.Format("Fetched {0} records from {1} and {2} records from {3} For {4}({5})",
                                                                        sourceList.Count(),
                                                                        source.Name,
                                                                        targetList.Count(),
                                                                        target.Name,
                                                                        vessel.Name,
                                                                        vessel.CallSign)));

            var sourceDifferences = sourceList.Select(ss => ss.ShippernetixId)
                    .Except(targetList.Select(ts => ts.ShippernetixId))
                    .Select(e => sourceList.FirstOrDefault(s => s.ShippernetixId == e))
                    .ToList();

            var intermediateSourceRecords = sourceDifferences.Where(sd => (int)sd.Je_JobNo < (int)sd.MaxJobNumber).ToList();

            var lastSourceRecords = sourceDifferences.Where(sd => (int)sd.Je_JobNo == (int)sd.MaxJobNumber).ToList();

            logMessages.Add(messageAction(string.Format("<b>Source different from target with {0}</b>", sourceDifferences.Count())));

            logMessages.Add(messageAction(string.Format("<b>Differences</b> : {0}", string.Join(",", sourceDifferences.Select(sd => sd.ShippernetixId)))));

            logMessages.Add(messageAction(string.Format("<b>Intermediate Source Records</b> : {0}", string.Join(",", intermediateSourceRecords.Select(sd => sd.ShippernetixId)))));

            logMessages.Add(messageAction(string.Format("<b>Last Source Record Differences</b> : {0}", string.Join(",", lastSourceRecords.Select(sd => sd.ShippernetixId)))));


            if(!onlyMail)
            if (intermediateSourceRecords.Any())
            {
                foreach (var sourceDifference in intermediateSourceRecords)
                {
                    var jobHistory = new Job_History(sourceDifference.Je_CallSign.ToString(),
                                                        sourceDifference.Je_L1.ToString(),
                                                        sourceDifference.Je_L2.ToString(),
                                                        sourceDifference.Je_L3.ToString(),
                                                        sourceDifference.Je_L4.ToString(),
                                                        sourceDifference.Je_JobCode.ToString(),
                                                        sourceDifference.Je_JobNo.ToString(),
                                                        sourceColumnListDifferentWithTarget,
                                                        source.Connection,
                                                        source.PrepareForVesselSide);

                    var affectedRowsCount = SqlManager.ExecuteNonQuery(sql: jobHistory.Table.GetInsertQueries, parameters: null, target.Connection);

                    if (affectedRowsCount > 0)
                        logMessages.Add(messageAction(string.Format("{0} Transfered From {1} To {2}", sourceDifference.ShippernetixId, source.Name, target.Name)));
                    else
                        logMessages.Add(messageAction(string.Format("An error occured while transfering structure : {0}", sourceDifference.ShippernetixId)));
                }
            }

            if (!onlyMail)
            if (lastSourceRecords.Any())
            {
                foreach (var sourceDifference in lastSourceRecords)
                {
                    var jobHistory = new Job_History(sourceDifference.Je_CallSign.ToString(),
                                                        sourceDifference.Je_L1.ToString(),
                                                        sourceDifference.Je_L2.ToString(),
                                                        sourceDifference.Je_L3.ToString(),
                                                        sourceDifference.Je_L4.ToString(),
                                                        sourceDifference.Je_JobCode.ToString(),
                                                        sourceDifference.Je_JobNo.ToString(),
                                                        sourceColumnListDifferentWithTarget,
                                                        source.Connection,
                                                        source.PrepareForVesselSide);


                    var affectedRowsCount = SqlManager.ExecuteNonQuery(sql: jobHistory.Table.GetInsertQueries, parameters: null, target.Connection);

                    if (affectedRowsCount > 0)
                        logMessages.Add(messageAction(string.Format("{0} Transfered From {1} To {2}", sourceDifference.ShippernetixId, source.Name, target.Name)));
                    else
                        logMessages.Add(messageAction(string.Format("An error occured while transfering structure : {0}", sourceDifference.ShippernetixId)));


                    var previousJobNumber = Convert.ToInt32(sourceDifference.Je_JobNo) - 1;

                    if (previousJobNumber < 1)
                        continue;

                    var previousJobHistory = new Job_History(sourceDifference.Je_CallSign.ToString(),
                                    sourceDifference.Je_L1.ToString(),
                                    sourceDifference.Je_L2.ToString(),
                                    sourceDifference.Je_L3.ToString(),
                                    sourceDifference.Je_L4.ToString(),
                                    sourceDifference.Je_JobCode.ToString(),
                                    previousJobNumber.ToString(),
                                    sourceColumnListDifferentWithTarget,
                                    source.Connection,
                                    source.PrepareForVesselSide);

                    var affectedRowsCount2 = SqlManager.ExecuteNonQuery(sql: jobHistory.Table.GetUpdateQueries, parameters: null, target.Connection);

                    if (affectedRowsCount2 > 0)
                        logMessages.Add(messageAction(string.Format("{0} Updated From {1} To {2}", string.Format("{0}-{1}-{2}-{3}-{4}-{5}-{6}", sourceDifference.Je_CallSign, sourceDifference.Je_L1, sourceDifference.Je_L2, sourceDifference.Je_L3, sourceDifference.Je_L4, sourceDifference.Je_JobCode, sourceDifference.Je_JobNo), source.Name, target.Name)));
                    else
                        logMessages.Add(messageAction(string.Format("An error occured while transfering structure : {0}", sourceDifference.ShippernetixId)));

                }

            }

            var targetDifferences = targetList.Select(ss => ss.ShippernetixId)
                .Except(sourceList.Select(ts => ts.ShippernetixId))
                .Select(e => targetList.FirstOrDefault(s => s.ShippernetixId == e))
                .ToList();


            var intermediateTargetRecords = targetDifferences.Where(td => (int)td.Je_JobNo < (int)td.MaxJobNumber).ToList();

            var lastTargetRecords = targetDifferences.Where(td => (int)td.Je_JobNo == (int)td.MaxJobNumber).ToList();

            logMessages.Add(messageAction(string.Format("<b>Target different from source with {0}</b>", targetDifferences.Count())));

            logMessages.Add(messageAction(string.Format("<b>Differences</b> : {0}", string.Join(",", targetDifferences.Select(sd => sd.ShippernetixId)))));

            logMessages.Add(messageAction(string.Format("<b>Intermediate Target Records</b> : {0}", string.Join(",", intermediateTargetRecords.Select(sd => sd.ShippernetixId)))));

            logMessages.Add(messageAction(string.Format("<b>Last Target Record Differences</b> : {0}", string.Join(",", lastTargetRecords.Select(sd => sd.ShippernetixId)))));

            if (!onlyMail)
            if (intermediateTargetRecords.Any())
            {
                foreach (var targetDifference in intermediateTargetRecords)
                {
                    try
                    {

                        var jobHistory = new Job_History(targetDifference.Je_CallSign.ToString(),
                                                            targetDifference.Je_L1.ToString(),
                                                            targetDifference.Je_L2.ToString(),
                                                            targetDifference.Je_L3.ToString(),
                                                            targetDifference.Je_L4.ToString(),
                                                            targetDifference.Je_JobCode.ToString(),
                                                            targetDifference.Je_JobNo.ToString(),
                                                            targetColumnListDifferentWithSource,
                                                            target.Connection,
                                                            target.PrepareForVesselSide);

                        var affectedRowsCount = SqlManager.ExecuteNonQuery(sql: jobHistory.Table.GetInsertQueries, parameters: null, source.Connection);

                        if (affectedRowsCount > 0)
                            logMessages.Add(messageAction(string.Format("{0} Transfered From {1} To {2}", targetDifference.ShippernetixId, target.Name, source.Name)));
                        else
                            logMessages.Add(messageAction(string.Format("An error occured while transfering structure : {0}", targetDifference.ShippernetixId)));
                    }
                    catch (Exception ex)
                    {

                        continue;
                    }

                }
            }

            if (!onlyMail)
            if (lastTargetRecords.Any())
            {
                foreach (var targetDifference in lastTargetRecords)
                {
                    var jobHistory = new Job_History(targetDifference.Je_CallSign.ToString(),
                                    targetDifference.Je_L1.ToString(),
                                    targetDifference.Je_L2.ToString(),
                                    targetDifference.Je_L3.ToString(),
                                    targetDifference.Je_L4.ToString(),
                                    targetDifference.Je_JobCode.ToString(),
                                    targetDifference.Je_JobNo.ToString(),
                                    targetColumnListDifferentWithSource,
                                    target.Connection,
                                    target.PrepareForVesselSide);

                    var previousJobNumber = Convert.ToInt32(targetDifference.Je_JobNo) - 1;

                    Job_History previousJobHistory = null;

                    if (previousJobNumber > 1)
                        previousJobHistory = new Job_History(targetDifference.Je_CallSign.ToString(),
                                    targetDifference.Je_L1.ToString(),
                                    targetDifference.Je_L2.ToString(),
                                    targetDifference.Je_L3.ToString(),
                                    targetDifference.Je_L4.ToString(),
                                    targetDifference.Je_JobCode.ToString(),
                                    previousJobNumber.ToString(),
                                    targetColumnListDifferentWithSource,
                                    target.Connection,
                                    target.PrepareForVesselSide);


                    var affectedRowsCount = SqlManager.ExecuteNonQuery(sql: jobHistory.Table.GetInsertQueries, parameters: null, source.Connection);

                    if (affectedRowsCount > 0)
                        logMessages.Add(messageAction(string.Format("{0} Transfered From {1} To {2}", targetDifference.ShippernetixId, target.Name, source.Name)));
                    else
                        logMessages.Add(messageAction(string.Format("An error occured while transfering structure : {0}", targetDifference.ShippernetixId)));



                    if (previousJobNumber < 1)
                        continue;


                    var affectedRowsCount2 = SqlManager.ExecuteNonQuery(sql: previousJobHistory.Table.GetUpdateQueries, parameters: null, source.Connection);

                    if (affectedRowsCount2 > 0)
                        logMessages.Add(messageAction(string.Format("{0} Updated From {1} To {2}", string.Format("{0}-{1}-{2}-{3}-{4}-{5}-{6}", targetDifference.Je_CallSign, targetDifference.Je_L1, targetDifference.Je_L2, targetDifference.Je_L3, targetDifference.Je_L4, targetDifference.Je_JobCode, targetDifference.Je_JobNo), target.Name, source.Name)));
                    else
                        logMessages.Add(messageAction(string.Format("An error occured while transfering structure : {0}", targetDifference.ShippernetixId)));
                }
            }

            Program.SendMail($"Job_Definition/{vessel.Name}({vessel.CallSign})", string.Join("</br></br>", logMessages));
        }

        public static void Fix(string callSign, string l1, string l2, string l3, string l4, string jobCode, string jobNumber, CustomConnection sourceConnection,CustomConnection targetConnection,bool prepareForVessel)
        {
            var sourceColumnList = SqlManager.SelectColumnNamesForMSSQL("Job_History", sourceConnection).Select(d => d["COLUMN_NAME"].ToString()).ToList();

            var targetColumnList = SqlManager.SelectColumnNamesForMSSQL("Job_History", targetConnection).Select(d => d["COLUMN_NAME"].ToString()).ToList();

            var sourceColumnListDifferentWithTarget = sourceColumnList.Except(targetColumnList).ToList();
            //var targetColumnListDifferentWithSource = targetColumnList.Except(sourceColumnList).ToList();

            var job_History = new Job_History(callSign,l1,l2,l3,l4,jobCode,jobNumber, sourceColumnListDifferentWithTarget,sourceConnection,prepareForVessel);

            var affectedRowsCount = SqlManager.ExecuteNonQuery(sql: job_History.Table.GetUpdateQueries, parameters: null, targetConnection);
        }

    }
}

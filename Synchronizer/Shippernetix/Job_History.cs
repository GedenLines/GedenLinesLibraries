using SqlManagement;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Synchronizer.Shippernetix
{
    public class Job_History : ShippernetixDynamicObject
    {
        private readonly object LockObject = new object();

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

            lock (LockObject)
            {
                Table = new DynamicTable(tableName: "Job_History", excludedColumnList: excludedColumnList, customConnection: customConnection, parameters: parameters)
                    .SetUniqueColumns("Je_CallSign", "Je_L1", "Je_L2", "Je_L3", "Je_L4", "Je_JobCode", "Je_JobCode", "Je_JobNo")
                    .PrepareForVesselSide(prepareForVesselSide)
                    .PrepareUpdateAndInsertQueries();
            }
        }

        public static void Sync(Side source, Side target, Vessel_Master vessel,bool onlyMail)
        {
            var taskList = new List<Task>();

            var messageAction = new Func<string, string>(message =>
            {
                Console.WriteLine("\n{0}:\n", DateTime.Now);

                Console.WriteLine(message);

                return message;
            });


            FixJobHistoryStatusIfDefectIsClosed(source.Connection);
            FixJobHistoryStatusIfDefectIsClosed(target.Connection);

            SetApprovalInfoAs1ForAlreadyCompletedJobs(target.Connection);

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

            source.Reconnect();

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
                    MaxJobNumber = d["MaxJobNumber"],
                    Je_Status = d["Je_Status"]
                })
                .ToList();

            var list1 = sourceList.Where(s => s.Je_JobCode.ToString().Contains("Def-312")).FirstOrDefault();

            target.Reconnect();

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
                    MaxJobNumber = d["MaxJobNumber"],
                    Je_Status = d["Je_Status"]
                })
                .ToList();

            var list2 = targetList.Where(s => s.Je_JobCode.ToString().Contains("Def-312")).FirstOrDefault();

            logMessages.Add(messageAction(string.Format("Fetched {0} records from {1} and {2} records from {3} For {4}({5})",
                                                                        sourceList.Count(),
                                                                        source.Name,
                                                                        targetList.Count(),
                                                                        target.Name,
                                                                        vessel.Name,
                                                                        vessel.CallSign)));

            if (sourceList.Count == 0 || targetList.Count == 0)
                throw new Exception("Could not fetch data...");

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

            if (lastSourceRecords.Any(r => r.ShippernetixId == "9PRK-19-1-1-1-Def-152-1"))
                Console.WriteLine();

            Task.WhenAll(taskList);

            if (!onlyMail)
               
                if (intermediateSourceRecords.Any())
                {
                    taskList.Clear();

                    foreach (var sourceDifference in intermediateSourceRecords)
                    {

                        Action action = new Action(() =>
                        {
                            source.Reconnect();
                            target.Reconnect();

                            Job_History jobHistory = null;

                            if (Job_History.Any(sourceDifference.Je_CallSign.ToString(),
                                                sourceDifference.Je_L1.ToString(),
                                                sourceDifference.Je_L2.ToString(),
                                                sourceDifference.Je_L3.ToString(),
                                                sourceDifference.Je_L4.ToString(),
                                                sourceDifference.Je_JobCode.ToString(),
                                                sourceDifference.Je_JobNo.ToString(),
                                                source.Connection))
                            {
                                jobHistory = new Job_History(sourceDifference.Je_CallSign.ToString(),
                                    sourceDifference.Je_L1.ToString(),
                                    sourceDifference.Je_L2.ToString(),
                                    sourceDifference.Je_L3.ToString(),
                                    sourceDifference.Je_L4.ToString(),
                                    sourceDifference.Je_JobCode.ToString(),
                                    sourceDifference.Je_JobNo.ToString(),
                                    sourceColumnListDifferentWithTarget,
                                    source.Connection,
                                    source.PrepareForVesselSide);
                            }

                            var affectedRowsCount = SqlManager.ExecuteNonQuery(sql: jobHistory.Table.GetInsertQueries, parameters: null, target.Connection);

                            if (affectedRowsCount > 0)
                                logMessages.Add(messageAction(string.Format("{0} Transfered From {1} To {2}", sourceDifference.ShippernetixId, source.Name, target.Name)));
                            else
                                logMessages.Add(messageAction(string.Format("An error occured while transfering job_History : {0}", sourceDifference.ShippernetixId)));
                        });

                        taskList.Add(new Task(action));
                    }
                }

            taskList.ForEach(t=>t.Start());
            Task.WaitAll(taskList.ToArray());

            if (!onlyMail)
                if (lastSourceRecords.Any())
                {
                    taskList.Clear();

                    foreach (var sourceDifference in lastSourceRecords)
                    {
                        Action action = new Action(() =>
                        {

                            source.Reconnect();
                            target.Reconnect();
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

                            if (jobHistory == null)
                                return;

                            var previousJobNumber = Convert.ToInt32(sourceDifference.Je_JobNo) - 1;

                            previousJobNumber = previousJobNumber == 0 ? 1 : previousJobNumber;


                            Job_History previousJobHistory = null;


                            if (Job_History.Any(sourceDifference.Je_CallSign.ToString(),
                                                sourceDifference.Je_L1.ToString(),
                                                sourceDifference.Je_L2.ToString(),
                                                sourceDifference.Je_L3.ToString(),
                                                sourceDifference.Je_L4.ToString(),
                                                sourceDifference.Je_JobCode.ToString(),
                                                previousJobNumber.ToString(),
                                                source.Connection))
                            {
                                if (previousJobNumber > 0)
                                    previousJobHistory = new Job_History(sourceDifference.Je_CallSign.ToString(),
                                        sourceDifference.Je_L1.ToString(),
                                        sourceDifference.Je_L2.ToString(),
                                        sourceDifference.Je_L3.ToString(),
                                        sourceDifference.Je_L4.ToString(),
                                        sourceDifference.Je_JobCode.ToString(),
                                        previousJobNumber.ToString(),
                                        sourceColumnListDifferentWithTarget,
                                        source.Connection,
                                        source.PrepareForVesselSide);

                            }


                            var affectedRowsCount = SqlManager.ExecuteNonQuery(sql: jobHistory.Table.GetInsertQueries, parameters: null, target.Connection);

                            if (affectedRowsCount > 0)
                                logMessages.Add(messageAction(string.Format("{0} Transfered From {1} To {2}", sourceDifference.ShippernetixId, source.Name, target.Name)));
                            else
                                logMessages.Add(messageAction(string.Format("An error occured while transfering job_History : {0}", sourceDifference.ShippernetixId)));


                            if (previousJobNumber < 1 || previousJobHistory == null)
                                return;


                            var affectedRowsCount2 = SqlManager.ExecuteNonQuery(sql: jobHistory.Table.GetUpdateQueries, parameters: null, target.Connection);

                            if (affectedRowsCount2 > 0)
                                logMessages.Add(messageAction(string.Format("{0} Updated From {1} To {2}", string.Format("{0}-{1}-{2}-{3}-{4}-{5}-{6}", sourceDifference.Je_CallSign, sourceDifference.Je_L1, sourceDifference.Je_L2, sourceDifference.Je_L3, sourceDifference.Je_L4, sourceDifference.Je_JobCode, previousJobNumber), source.Name, target.Name)));
                            else
                                logMessages.Add(messageAction(string.Format("An error occured while transfering job_History : {0}", sourceDifference.ShippernetixId)));
                        });
                        taskList.Add(new Task(action));
                    }

                }

            taskList.ForEach(t => t.Start());
            Task.WaitAll(taskList.ToArray());

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
                    taskList.Clear();

                    foreach (var targetDifference in intermediateTargetRecords)
                    {
                        source.Reconnect();
                        target.Reconnect();

                        try
                        {

                            Job_History jobHistory = null;

                            if (Job_History.Any(targetDifference.Je_CallSign.ToString(),
                                targetDifference.Je_L1.ToString(),
                                targetDifference.Je_L2.ToString(),
                                targetDifference.Je_L3.ToString(),
                                targetDifference.Je_L4.ToString(),
                                targetDifference.Je_JobCode.ToString(),
                                targetDifference.Je_JobNo.ToString(),
                                target.Connection))
                            {
                                jobHistory = new Job_History(targetDifference.Je_CallSign.ToString(),
                                                targetDifference.Je_L1.ToString(),
                                                targetDifference.Je_L2.ToString(),
                                                targetDifference.Je_L3.ToString(),
                                                targetDifference.Je_L4.ToString(),
                                                targetDifference.Je_JobCode.ToString(),
                                                targetDifference.Je_JobNo.ToString(),
                                                targetColumnListDifferentWithSource,
                                                target.Connection,
                                                target.PrepareForVesselSide);
                            }




                            if (jobHistory == null)
                                return;

                            var affectedRowsCount = SqlManager.ExecuteNonQuery(sql: jobHistory.Table.GetInsertQueries, parameters: null, source.Connection);

                            if (affectedRowsCount > 0)
                                logMessages.Add(messageAction(string.Format("{0} Transfered From {1} To {2}", targetDifference.ShippernetixId, target.Name, source.Name)));
                            else
                                logMessages.Add(messageAction(string.Format("An error occured while transfering job_History : {0}", targetDifference.ShippernetixId)));
                        }
                        catch (Exception ex)
                        {

                            Console.WriteLine(targetDifference.ShippernetixId + " " + ex.Message);
                        }

                }
        }

            var size = 10;
            var totalPage = Convert.ToInt32(Math.Ceiling((double)((double)taskList.Count / (double)size)));


            var taskObjects = new List<TaskObject>();

            for (int page = 0; page < totalPage; page++)
            {
                var skip = size * page;

                var tasksToDo = taskList.Skip(skip).Take(size).ToList();

                for (int k = 0; k < tasksToDo.Count(); k++)
                {
                    taskObjects.Add(new TaskObject(tasksToDo[k], page, k));
                }
            }

            var toGroups = taskObjects.GroupBy(to => to.Page);

            foreach (var toGroup in toGroups)
            {
                if(toGroups.Any(g=>g.Key==toGroup.Key + 1))
                {
                    var list = toGroup.ToList();

                    for (int i = 0; i < list.Count(); i++)
                    {
                        var upperPageList = toGroups.Where(g=>g.Key==toGroup.Key+1).FirstOrDefault().ToList();

                        if (i < upperPageList.Count)
                        {
                            var upo = upperPageList[i];

                            list[i].Task.ContinueWith(t=> 
                            {
                                Console.WriteLine("ContinueWith(Page : {0},Index : {1}) ", upo.Page, upo.Index);

                                if (upo.Task.Status != TaskStatus.RanToCompletion)
                                    upo.Task.Start();
                            });
                        }                       
                    }
                }
            }

            taskObjects.Where(to => to.Page == 0 && to.Task.Status!=TaskStatus.RanToCompletion).ToList().ForEach(to => to.Task.Start());

            Task.WaitAll(taskList.ToArray());


            if (!onlyMail)
            if (lastTargetRecords.Any())
            {
                    taskList.Clear();

                    source.Reconnect();
                    target.Reconnect();

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

                        if (jobHistory == null)
                            continue;

                        var previousJobNumber = Convert.ToInt32(targetDifference.Je_JobNo) - 1;

                        previousJobNumber = previousJobNumber == 0 ? 1 : previousJobNumber;

                        Job_History previousJobHistory = null;


                        if(Job_History.Any(targetDifference.Je_CallSign.ToString(),
                            targetDifference.Je_L1.ToString(),
                            targetDifference.Je_L2.ToString(),
                            targetDifference.Je_L3.ToString(),
                            targetDifference.Je_L4.ToString(),
                            targetDifference.Je_JobCode.ToString(),
                            previousJobNumber.ToString(),
                            target.Connection))
                        {
                            if (previousJobNumber > 0)
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
                        }


                        var affectedRowsCount = SqlManager.ExecuteNonQuery(sql: jobHistory.Table.GetInsertQueries, parameters: null, source.Connection);

                        if (affectedRowsCount > 0)
                            logMessages.Add(messageAction(string.Format("{0} Transfered From {1} To {2}", targetDifference.ShippernetixId, target.Name, source.Name)));
                        else
                            logMessages.Add(messageAction(string.Format("An error occured while transfering job_History : {0}", targetDifference.ShippernetixId)));

                        if (previousJobNumber < 1 || previousJobHistory==null)
                            continue;

                        var affectedRowsCount2 = SqlManager.ExecuteNonQuery(sql: previousJobHistory.Table.GetUpdateQueries, parameters: null, source.Connection);

                        if (affectedRowsCount2 > 0)
                            logMessages.Add(messageAction(string.Format("{0} Updated From {1} To {2}", string.Format("{0}-{1}-{2}-{3}-{4}-{5}-{6}", targetDifference.Je_CallSign, targetDifference.Je_L1, targetDifference.Je_L2, targetDifference.Je_L3, targetDifference.Je_L4, targetDifference.Je_JobCode, previousJobNumber), target.Name, source.Name)));
                        else
                            logMessages.Add(messageAction(string.Format("An error occured while transfering job_History : {0}", targetDifference.ShippernetixId)));
                    }
            }


            Program.SendMail($"Job_History/{vessel.Name}({vessel.CallSign})", string.Join("</br></br>", logMessages));
        }

        public static void FixFromOfficeToVessel(string callSign, string l1, string l2, string l3, string l4, string jobCode, string jobNumber )
        {
            var officeConnection = new MsSqlConnection(connectionString: "MsSqlConnectionString");
            var vesselConnection = new MsSqlConnection(connectionString:callSign);

            Fix(callSign,l1,l2,l3,l4,jobCode,jobNumber,officeConnection,vesselConnection,true);
        }

        public static void FixFromVesselToOffice(string callSign, string l1, string l2, string l3, string l4, string jobCode, string jobNumber)
        {
            var officeConnection = new MsSqlConnection(connectionString: "MsSqlConnectionString");
            var vesselConnection = new MsSqlConnection(connectionString: callSign);

            Fix(callSign, l1, l2, l3, l4, jobCode, jobNumber,vesselConnection, officeConnection, true);
        }

        public static void Fix(string callSign, string l1, string l2, string l3, string l4, string jobCode, string jobNumber, CustomConnection sourceConnection,CustomConnection targetConnection,bool prepareForVessel)
        {
            var sourceColumnList = SqlManager.SelectColumnNamesForMSSQL("Job_History", sourceConnection).Select(d => d["COLUMN_NAME"].ToString()).ToList();

            var targetColumnList = SqlManager.SelectColumnNamesForMSSQL("Job_History", targetConnection).Select(d => d["COLUMN_NAME"].ToString()).ToList();

            var sourceColumnListDifferentWithTarget = sourceColumnList.Except(targetColumnList).ToList();
            //var targetColumnListDifferentWithSource = targetColumnList.Except(sourceColumnList).ToList();

            var job_History = new Job_History(callSign,l1,l2,l3,l4,jobCode,jobNumber, sourceColumnListDifferentWithTarget,sourceConnection,prepareForVessel);

            //SqlManager.DisableTrigger("JHIS_ED_CC", "Job_History", targetConnection);
            //SqlManager.DisableTrigger("Job_History_Edit", "Job_History", targetConnection);

            var affectedRowsCount = SqlManager.ExecuteNonQuery(sql: job_History.Table.GetUpdateQueries, parameters: null, targetConnection);
            //var affectedRowsCount = SqlManager.ExecuteNonQuery(sql: job_History.Table.GetInsertQueries, parameters: null, targetConnection);

            //SqlManager.EnableTrigger("JHIS_ED_CC", "Job_History", targetConnection);
            //SqlManager.DisableTrigger("Job_History_Edit", "Job_History", targetConnection);

            Console.WriteLine($"{callSign}-{l1}-{l2}-{l3}-{l4}-{jobCode}-{jobNumber} fixing {affectedRowsCount}");
        }


        public static bool Any(string callSign, string l1, string l2, string l3, string l4, string jobCode, string jobNumber, CustomConnection customConnection) 
        {
            return SqlManager.Any("select * from Job_History where Je_CallSign=@Je_CallSign and Je_L1=@Je_L1 and Je_L2=@Je_L2 and Je_L3=@Je_L3  and Je_L4=@Je_L4 and Je_JobCode=@Je_JobCode and Je_JobNo=@Je_JobNo",
                new Dictionary<string, object>()
                {
                                {"Je_CallSign",callSign},
                                {"Je_L1",l1 },
                                {"Je_L2",l2 },
                                {"Je_L3",l3 },
                                {"Je_L4",l4 },
                                {"Je_JobCode",jobCode },
                                {"Je_JobNo",jobNumber }
                },customConnection);
        }


        public static void ClearDefectsJobHasJobNumberMoreThan1(CustomConnection connection)
        {
            var sql = "delete from Job_History where Je_JobCode like '%Def%' and Je_JobNo>1";

            var arc = SqlManager.ExecuteNonQuery(sql,connection:connection);


            Console.WriteLine("ClearDefectsJobHasJobNumberMoreThan1 function was worked and its result : " + arc);
        }

        public static void FixJobHistoryStatusIfDefectIsClosed(CustomConnection connection)
        {
            var sql = "select Dai_CallSign, Dai_DamageNo,Dai_Corrective,Dai_CloseDate from Job_History jh inner join Defects d on jh.Je_CallSign = d.Dai_CallSign and jh.Je_JobCode = d.Dai_DamageNo where d.Dai_Status = 'CLOSED' and Je_Status = 'NEXT JOB'";

            var listToFix = SqlManager.ExecuteQuery(sql, connection: connection).Select(r => new
            {
                Dai_CallSign = r["Dai_CallSign"]?.ToString(),
                Dai_DamageNo = r["Dai_DamageNo"]?.ToString(),
                Dai_Corrective = r["Dai_Corrective"]?.ToString(),
                Dai_CloseDate = Convert.ToDateTime(r["Dai_CloseDate"]?.ToString()) <= DateTime.MinValue ? new DateTime(year: 1800,month:1,day:1) : Convert.ToDateTime(r["Dai_CloseDate"]?.ToString())
            }) ;

            var updateJobHistorySql = "UPDATE Job_History " +
                "SET Je_Status = 'COMPLETED', Je_StartDate = @Dai_CloseDate, Je_FinishDate = @Dai_CloseDate, Je_ConfirmText = @Dai_Corrective " +
                "FROM Job_History " +
                "WHERE Je_CallSign = @Dai_CallSign and Je_JobCode = @Dai_DamageNo and Je_Status = 'NEXT JOB'";

            foreach (var item in listToFix)
            {
                Console.WriteLine($"{item.Dai_DamageNo} is fixing");
                var arc = SqlManager.ExecuteNonQuery(updateJobHistorySql, new Dictionary<string, object>()
                {
                    {"Dai_CallSign", item.Dai_CallSign},
                    {"Dai_DamageNo", item.Dai_DamageNo},
                    {"Dai_Corrective",item.Dai_Corrective },
                    { "Dai_CloseDate",item.Dai_CloseDate }
                }, connection);
            }

            ClearDefectsJobHasJobNumberMoreThan1(connection);
        }

        public static void ReCalculateLastJob(MsSqlConnection connection, string callSign, string l1, string l2, string l3, string l4, string jobCode,string date)
        {
            var action = new Action<MsSqlConnection, string, string, string, string, string, string, int>((MsSqlConnection connection, string callSign, string l1, string l2, string l3, string l4, string jobCode, int maxCompletedJobNumber) =>
            {
                var updateJobHistorySql = $"update Job_History set Je_StartDate='{date}',Je_FinishDate='{date}' where Je_CallSign='{callSign}' and Je_L1='{l1}' and Je_L2='{l2}' and Je_L3='{l3}' and Je_L4='{l4}' and Je_JobCode='{jobCode}' and Je_JobNo='{maxCompletedJobNumber}'";

                var arc = SqlManager.ExecuteNonQuery(updateJobHistorySql, null, connection);

                Console.WriteLine($"{arc > 0}");
            });

            ReCalculateLastJob(connection, callSign, l1, l2, l3, l4, jobCode, action);
        }

        public static void ReCalculateLastJob(MsSqlConnection connection,string callSign,string l1,string l2,string l3,string l4,string jobCode,
            Action<MsSqlConnection,string,string,string,string,string,string,int> actionToWorkWithLastCompletedJob = null)
        {
            if (string.IsNullOrEmpty(callSign) && string.IsNullOrEmpty(l1) && string.IsNullOrEmpty(l2) && string.IsNullOrEmpty(l3) && string.IsNullOrEmpty(l4) && string.IsNullOrEmpty(jobCode))
                throw new Exception("Please set required parameters");

            var sql = "select distinct jh.Je_Status as Status,max(jh.Je_JobNo) OVER(partition by jh.Je_Status) as MaxJobNo from Job_History jh where jh.Je_CallSign = @CallSign and jh.Je_L1 = @L1 and jh.Je_L2 = @L2 and jh.Je_L3 = @L3 and jh.Je_L4 = @L4 and jh.Je_JobCode = @JobCode ";

            var parameters = new Dictionary<string, object>()
            {
                {"CallSign",callSign },
                {"L1", l1},
                {"L2", l2},
                {"L3", l3},
                {"L4", l4},
                {"JobCode", jobCode}
            };

            var results = SqlManager.ExecuteQuery(sql, parameters, connection);

            var maxJobNumberForCompleted = results.Where(r=>r["Status"].ToString()=="COMPLETED").Select(r=>Convert.ToInt32(r["MaxJobNo"])).FirstOrDefault();

            var maxJobNumberForNextJob = results.Where(r => r["Status"].ToString() == "NEXT JOB").Select(r => Convert.ToInt32(r["MaxJobNo"])).FirstOrDefault();

            if (maxJobNumberForCompleted>=maxJobNumberForNextJob)
                throw new Exception("maxJobNumberForCompleted>=maxJobNumberForNextJob");

            if (actionToWorkWithLastCompletedJob != null)
                actionToWorkWithLastCompletedJob.Invoke(connection,callSign,l1,l2,l3,l4,jobCode,maxJobNumberForCompleted);

            var deleteLastNextJobQuery = "delete from Job_History where Je_CallSign = @CallSign and Je_L1 = @L1 and Je_L2 = @L2 and Je_L3 = @L3 and Je_L4 = @L4 and Je_JobCode = @JobCode and Je_JobNo=@LastNextJobNo";

            parameters.Add("LastNextJobNo",maxJobNumberForNextJob);
            parameters.Add("LastCompletedJobNo", maxJobNumberForCompleted);

            var arc = SqlManager.ExecuteNonQuery(deleteLastNextJobQuery,parameters,connection);

            Console.WriteLine($"Deleting maxJobNumberForNextJob : {maxJobNumberForNextJob} : {arc > 0}");

            var updateLastCompletedJobStatusQuery= "update Job_History set Je_Status='NEXT JOB' where Je_CallSign = @CallSign and Je_L1 = @L1 and Je_L2 = @L2 and Je_L3 = @L3 and Je_L4 = @L4 and Je_JobCode = @JobCode and Je_JobNo=@LastCompletedJobNo";

            arc = SqlManager.ExecuteNonQuery(updateLastCompletedJobStatusQuery, parameters, connection);

            Console.WriteLine($"Updating lastCompletedJobStatus({maxJobNumberForCompleted}) as Next Job : {arc>0}");

            updateLastCompletedJobStatusQuery = "update Job_History set Je_Status='COMPLETED' where Je_CallSign = @CallSign and Je_L1 = @L1 and Je_L2 = @L2 and Je_L3 = @L3 and Je_L4 = @L4 and Je_JobCode = @JobCode and Je_JobNo=@LastCompletedJobNo";

            arc = SqlManager.ExecuteNonQuery(updateLastCompletedJobStatusQuery, parameters, connection);

            Console.WriteLine($"Updating lastCompletedJobStatus({maxJobNumberForCompleted}) as Completed : {arc>0}");
        }


        public static void SetApprovalInfoAs1ForAlreadyCompletedJobs(CustomConnection connection)
        {
            //var sql = "update jh set jh.Je_Approval = 1 " +
            //            "from Job_History jh inner join " +
            //            "(select Je_Approval,Je_CallSign,Je_L1,Je_L2,Je_L3,Je_L4,Je_JobCode,Je_JobNo,max(Je_JobNo) OVER(PARTITION by Je_CallSign, Je_L1, Je_L2, Je_L3, Je_L4, Je_JobCode) as maxJob " +
            //            "from Job_History " +
            //            ")x " +
            //            "on jh.Je_CallSign = x.Je_CallSign and jh.Je_L1 = x.Je_L1 and jh.Je_L2 = x.Je_L2 and jh.Je_L3 = x.Je_L3 and jh.Je_L4 = x.Je_L4 and jh.Je_JobCode = x.Je_JobCode and jh.Je_JobNo = x.Je_JobNo " +
            //            "where jh.Je_JobNo<> x.maxJob ";

            //var arc = SqlManager.ExecuteNonQuery(sql:sql,connection:connection);

            //Console.WriteLine($"SetApprovalInfoAs1ForAlreadyCompletedJobs : " + arc);
        }
    }

    public class TaskObject
    {
        public int Index { get; set; }

        public int Page { get; set; }
        public Task Task { get; set; }

        public TaskObject(Task task,int page,int index)
        {
            Task = task;

            Page = page;

            Index = index;
        }
    }
}

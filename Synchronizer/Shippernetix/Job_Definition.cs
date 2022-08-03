using SqlManagement;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Synchronizer.Shippernetix
{
    public class Job_Definition : ShippernetixDynamicObject
    {
        public Job_Definition(CustomConnection customConnection,string callSign, string l1, string l2, string l3, string l4, string jobCode, bool prepareForVesselSide)
        {
            var parameters = new Dictionary<string, object>();

            if (!string.IsNullOrEmpty(callSign))
                parameters.Add("Jd_CallSign", callSign);

            if (!string.IsNullOrEmpty(l1))
                parameters.Add("Jd_L1", l1);

            if (!string.IsNullOrEmpty(l2))
                parameters.Add("Jd_L2", l2);

            if (!string.IsNullOrEmpty(l3))
                parameters.Add("Jd_L3", l3);

            if (!string.IsNullOrEmpty(l4))
                parameters.Add("Jd_L4", l4);

            if (!string.IsNullOrEmpty(jobCode))
                parameters.Add("Jd_JobCode", jobCode);

            Table = new DynamicTable(tableName: "Job_Definition",
                customConnection:customConnection,
                parameters: parameters)
                .SetUniqueColumns("Jd_CallSign", "Jd_L1", "Jd_L2", "Jd_L3", "Jd_L4", "Jd_JobCode")
                .PrepareForVesselSide(prepareForVesselSide)
                .PrepareUpdateAndInsertQueries();
        }
        public static void Sync(Side source, Side target, Vessel_Master vessel, bool onlyMail)
        {
            var messageAction = new Func<string, string>(message =>
            {
                Console.WriteLine("\n{0}:\n", DateTime.Now);

                Console.WriteLine(message);

                return message;
            });

            List<string> logMessages = new List<string>() { messageAction("Job_Definition Sync is started") };

            var jobDefinitionGroupsQuery = "select Jd_CallSign,Jd_L1,Jd_L2,Jd_L3,Jd_L4,Jd_JobCode from Job_Definition where Jd_CallSign = @CallSign group by Jd_CallSign, Jd_L1, Jd_L2, Jd_L3, Jd_L4, Jd_JobCode";

            var sourceList = SqlManager.ExecuteQuery(jobDefinitionGroupsQuery,
                new Dictionary<string, object>()
                {
                    { "CallSign",vessel.CallSign }
                },
                source.Connection)
                .Select(d => new
                {
                    ShippernetixId = string.Format("{0}-{1}-{2}-{3}-{4}-{5}", d["Jd_CallSign"], d["Jd_L1"], d["Jd_L2"], d["Jd_L3"], d["Jd_L4"], d["Jd_JobCode"]),
                    Jd_CallSign = d["Jd_CallSign"],
                    Jd_L1 = d["Jd_L1"],
                    Jd_L2 = d["Jd_L2"],
                    Jd_L3 = d["Jd_L3"],
                    Jd_L4 = d["Jd_L4"],
                    Jd_JobCode = d["Jd_JobCode"]
                })
                .ToList();


            var list1 = sourceList.Where(s => s.ShippernetixId == "9HA2-19-1-1-1-C").ToList();

            var targetList = SqlManager.ExecuteQuery(jobDefinitionGroupsQuery,
                                    new Dictionary<string, object>()
                                    {
                                            { "CallSign",vessel.CallSign }
                                    },
                                    target.Connection)
                                     .Select(d => new
                                     {
                                         ShippernetixId = string.Format("{0}-{1}-{2}-{3}-{4}-{5}", d["Jd_CallSign"], d["Jd_L1"], d["Jd_L2"], d["Jd_L3"], d["Jd_L4"], d["Jd_JobCode"]),
                                         Jd_CallSign = d["Jd_CallSign"],
                                         Jd_L1 = d["Jd_L1"],
                                         Jd_L2 = d["Jd_L2"],
                                         Jd_L3 = d["Jd_L3"],
                                         Jd_L4 = d["Jd_L4"],
                                         Jd_JobCode = d["Jd_JobCode"]
                                     })
                                    .ToList();

            var list2 = sourceList.Where(s => s.ShippernetixId == "9HA2-19-1-1-1-C").ToList();

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

            logMessages.Add(messageAction(string.Format("Source different from target with {0}", sourceDifferences.Count())));

            logMessages.Add(messageAction(string.Format("Differences : {0}",string.Join("," ,sourceDifferences.Select(sd=>sd.ShippernetixId)))));


            if(!onlyMail)
            if (sourceDifferences.Any())
            {

                logMessages.Add(messageAction(string.Format("Source different from target with {0}", sourceDifferences.Count())));

                foreach (var sourceDifference in sourceDifferences)
                {
                    var jobDefinition = new Job_Definition(source.Connection, 
                                                        sourceDifference.Jd_CallSign.ToString(),
                                                        sourceDifference.Jd_L1.ToString(),
                                                        sourceDifference.Jd_L2.ToString(),
                                                        sourceDifference.Jd_L3.ToString(),
                                                        sourceDifference.Jd_L4.ToString(),
                                                        sourceDifference.Jd_JobCode.ToString(),
                                                        source.PrepareForVesselSide);

                    var affectedRowsCount = SqlManager.ExecuteNonQuery(sql: jobDefinition.Table.GetInsertQueries, parameters: null, target.Connection);

                    if (affectedRowsCount > 0)
                        logMessages.Add(messageAction(string.Format("{0} Transfered From {1} To {2}", sourceDifference.ShippernetixId, source.Name, target.Name)));
                    else
                        logMessages.Add(messageAction(string.Format("An error occured while transfering job definition : {0}", sourceDifference.ShippernetixId)));
                }
            }


            var targetDifferences = targetList.Select(ss => ss.ShippernetixId)
                                                .Except(sourceList.Select(ts => ts.ShippernetixId))
                                                .Select(e => targetList.FirstOrDefault(s => s.ShippernetixId == e))
                                                .ToList();


            logMessages.Add(messageAction(string.Format("Target different from source with {0}", targetDifferences.Count())));

            logMessages.Add(messageAction(string.Format("Differences : {0}", string.Join(",", targetDifferences.Select(sd => sd.ShippernetixId)))));

            if(!onlyMail)
            if (targetDifferences.Any())
            {
                foreach (var targetDifference in targetDifferences)
                {
                    var jobDefinition = new Job_Definition(target.Connection, 
                                targetDifference.Jd_CallSign.ToString(),
                                targetDifference.Jd_L1.ToString(),
                                targetDifference.Jd_L2.ToString(),
                                targetDifference.Jd_L3.ToString(),
                                targetDifference.Jd_L4.ToString(),
                                targetDifference.Jd_JobCode.ToString(),
                                target.PrepareForVesselSide);


                        var affectedRowsCount = SqlManager.ExecuteNonQuery(sql: jobDefinition.Table.GetInsertQueries, parameters: null, source.Connection);

                        if (affectedRowsCount > 0)
                            logMessages.Add(messageAction(string.Format("{0} Transfered From {1} To {2}", targetDifference.ShippernetixId, target.Name, source.Name)));
                        else
                            logMessages.Add(messageAction(string.Format("An error occured while transfering job definition : {0}", targetDifference.ShippernetixId)));
                    }
            }

            Program.SendMail($"Job_Definition/{vessel.Name}({vessel.CallSign})", string.Join("</br>", logMessages));

        }
    }
}

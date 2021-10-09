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
        public Job_History(string callSign, string l1, string l2, string l3, string l4, string jobCode, string jobNumber)
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

            Table = new DynamicTable(tableName: "Job_History", parameters: parameters)
                        .SetUniqueColumns("Je_CallSign", "Je_L1", "Je_L2", "Je_L3", "Je_L4", "Je_JobCode", "Je_JobCode", "Je_JobNo")
                        .PrepareUpdateAndInsertQueries();
        }

        public static void Sync(Side source, Side target, Vessel_Master vessel)
        {
            var messageAction = new Func<string, string>(message =>
            {
                Console.WriteLine("\n{0}:\n" + DateTime.Now);

                Console.WriteLine(message);

                return message;
            });

            var sourceColumnList = SqlManager.SelectColumnNamesForMSSQL("Job_History", source.Connection).Select(d => d["COLUMN_NAME"].ToString()).ToList();

            var targetColumnList = SqlManager.SelectColumnNamesForMSSQL("Job_History", target.Connection).Select(d => d["COLUMN_NAME"].ToString()).ToList();

            var sourceColumnListDifferentWithTarget = sourceColumnList.Except(targetColumnList).ToList();
            var targetColumnListDifferentWithSource = targetColumnList.Except(sourceColumnList).ToList();

            List<string> logMessages = new List<string>() { messageAction("Job_History is started") };

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

            var sourceDifferences2 = sourceList.Where(ss => targetList.Any(tl => tl.ShippernetixId == ss.ShippernetixId && tl.MaxJobNumber != ss.MaxJobNumber))
                    .ToList();


            var targetDifferences = targetList.Select(ss => ss.ShippernetixId)
                    .Except(sourceList.Select(ts => ts.ShippernetixId))
                    .Select(e => targetList.FirstOrDefault(s => s.ShippernetixId == e))
                    .ToList();

            var targetDifferences2 = targetList.Where(ss => sourceList.Any(tl => tl.ShippernetixId == ss.ShippernetixId && tl.MaxJobNumber != ss.MaxJobNumber))
        .ToList();


        }

    }

}

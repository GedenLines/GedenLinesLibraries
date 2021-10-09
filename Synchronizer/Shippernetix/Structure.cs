using SqlManagement;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Synchronizer.Shippernetix
{
    public class Structure : ShippernetixDynamicObject
    {
        public Structure(string callSign, string l1, string l2, string l3, string l4)
        {
            var parameters = new Dictionary<string, object>();

            if (!string.IsNullOrEmpty(callSign))
                parameters.Add("En_CallSign", callSign);

            if (!string.IsNullOrEmpty(l1))
                parameters.Add("En_L1", l1);

            if (!string.IsNullOrEmpty(l2))
                parameters.Add("En_L2", l2);

            if (!string.IsNullOrEmpty(l3))
                parameters.Add("En_L3", l3);

            if (!string.IsNullOrEmpty(l4))
                parameters.Add("En_L4", l4);

            Table = new DynamicTable(tableName: "Structure", parameters: parameters)
                .SetUniqueColumns("En_CallSign", "En_L1", "En_L2", "En_L3", "En_L4")
                .PrepareUpdateAndInsertQueries();
        }

        public static void Sync(Side source, Side target)
        {
            var messageAction = new Func<string, string>(message =>
             {
                 Console.WriteLine("\n{0}:\n",DateTime.Now);

                 Console.WriteLine(message);

                 return message;
             });

            List<string> logMessages = new List<string>() { messageAction("Structure Sync is started") };

            var structureGroupsQuery = "select En_CallSign, En_L1, En_L2, En_L3, En_L4 from (select * from Structure where En_CallSign=@CallSign) x group by En_CallSign, En_L1, En_L2, En_L3, En_L4";

            foreach (var vessel in Vessel_Master.Vessels)
            {
                var sourceStructures = SqlManager.ExecuteQuery(structureGroupsQuery,
                    new Dictionary<string, object>()
                    {
                        { "CallSign",vessel.CallSign }
                    },
                    source.Connection)
                    .Select(d => new
                    {
                        ShippernetixId = string.Format("{0}-{1}-{2}-{3}-{4}", d["En_CallSign"], d["En_L1"], d["En_L2"], d["En_L3"], d["En_L4"]),
                        En_CallSign = d["En_CallSign"],
                        En_L1 = d["En_L1"],
                        En_L2 = d["En_L2"],
                        En_L3 = d["En_L3"],
                        En_L4 = d["En_L4"]
                    })
                    .ToList();

                

                var targetStructures = SqlManager.ExecuteQuery(structureGroupsQuery,
                                        new Dictionary<string, object>()
                                        {
                                            { "CallSign",vessel.CallSign }
                                        },
                                        target.Connection)
                                        .Select(d => new
                                        {
                                            ShippernetixId = string.Format("{0}-{1}-{2}-{3}-{4}", d["En_CallSign"], d["En_L1"], d["En_L2"], d["En_L3"], d["En_L4"]),
                                            En_CallSign = d["En_CallSign"],
                                            En_L1 = d["En_L1"],
                                            En_L2 = d["En_L2"],
                                            En_L3 = d["En_L3"],
                                            En_L4 = d["En_L4"]
                                        })
                                        .ToList();

                logMessages.Add(messageAction(string.Format("Fetched {0} records from {1} and {2} records from {3} For {4}({5})",
                                                                                                        sourceStructures.Count(),
                                                                                                        source.Name,
                                                                                                        targetStructures.Count(),
                                                                                                        target.Name,
                                                                                                        vessel.Name,
                                                                                                        vessel.CallSign)));

                var sourceDifferences = sourceStructures.Select(ss => ss.ShippernetixId)
                                                    .Except(targetStructures.Select(ts => ts.ShippernetixId))
                                                    .Select(e => sourceStructures.FirstOrDefault(s => s.ShippernetixId == e))
                                                    .ToList();

                if (sourceDifferences.Any())
                {

                    logMessages.Add(messageAction(string.Format("Source different from target with {0}", sourceDifferences.Count())));

                    foreach (var sourceDifference in sourceDifferences)
                    {
                        var structure = new Structure(sourceDifference.En_CallSign.ToString(),
                                                            sourceDifference.En_L1.ToString(),
                                                            sourceDifference.En_L2.ToString(),
                                                            sourceDifference.En_L3.ToString(),
                                                            sourceDifference.En_L4.ToString());

                        Task task = new Task(()=> 
                        {
                            var affectedRowsCount = SqlManager.ExecuteNonQuery(sql:structure.Table.GetInsertQueries,parameters:null,target.Connection);

                            if (affectedRowsCount > 0)
                                logMessages.Add(messageAction(string.Format("{0} Transfered From {1} To {2}", sourceDifference.ShippernetixId, source.Name, target.Name)));
                            else
                                logMessages.Add(messageAction(string.Format("An error occured while transfering structure : {0}", sourceDifference.ShippernetixId)));
                        });


                        task.Start();
                    }
                }


                var targetDifferences = targetStructures.Select(ss => ss.ShippernetixId)
                                    .Except(sourceStructures.Select(ts => ts.ShippernetixId))
                                     .Select(e => targetStructures.FirstOrDefault(s => s.ShippernetixId == e))
                                    .ToList();

                if (targetDifferences.Any())
                {
                    foreach (var targetDifference in targetDifferences)
                    {
                        var structure = new Structure(targetDifference.En_CallSign.ToString(),
                                    targetDifference.En_L1.ToString(),
                                    targetDifference.En_L2.ToString(),
                                    targetDifference.En_L3.ToString(),
                                    targetDifference.En_L4.ToString());

                        Task task = new Task(() =>
                        {
                            var affectedRowsCount = SqlManager.ExecuteNonQuery(sql: structure.Table.GetInsertQueries, parameters: null, source.Connection);

                            if (affectedRowsCount > 0)
                                logMessages.Add(messageAction(string.Format("{0} Transfered From {1} To {2}", targetDifference.ShippernetixId, target.Name, source.Name)));
                            else
                                logMessages.Add(messageAction(string.Format("An error occured while transfering structure : {0}", targetDifference.ShippernetixId)));
                        });


                        task.Start();
                    }
                }
            }

        }
    }
}

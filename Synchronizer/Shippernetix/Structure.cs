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
        public Structure(CustomConnection customConnection,string callSign, string l1, string l2, string l3, string l4, List<string> excludedColumnList, bool prepareForVesselSide)
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

            Table = new DynamicTable(tableName: "Structure", excludedColumnList: excludedColumnList,customConnection:customConnection ,parameters: parameters)
                .SetUniqueColumns("En_CallSign", "En_L1", "En_L2", "En_L3", "En_L4")
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

            var sourceColumnList = SqlManager.SelectColumnNamesForMSSQL("Structure", source.Connection).Select(d => d["COLUMN_NAME"].ToString()).ToList();

            var targetColumnList = SqlManager.SelectColumnNamesForMSSQL("Structure", target.Connection).Select(d => d["COLUMN_NAME"].ToString()).ToList();

            var sourceColumnListDifferentWithTarget = sourceColumnList.Except(targetColumnList).ToList();
            var targetColumnListDifferentWithSource = targetColumnList.Except(sourceColumnList).ToList();

            List<string> logMessages = new List<string>() { messageAction("Structure Sync is started") };

            var structureGroupsQuery = "select En_CallSign, En_L1, En_L2, En_L3, En_L4 from (select * from Structure where En_CallSign=@CallSign) x group by En_CallSign, En_L1, En_L2, En_L3, En_L4";


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

            var list1 = sourceStructures.Where(s => s.ShippernetixId == "9HA2-19-1-1-1").ToList();

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

            var list2 = targetStructures.Where(s => s.ShippernetixId == "9HA2-19-1-1-1").ToList();

            logMessages.Add(messageAction(string.Format("Fetched {0} records from {1} and {2} records from {3} For {4}({5})",
                                                                                                    sourceStructures.Count(),
                                                                                                    source.Name,
                                                                                                    targetStructures.Count(),
                                                                                                    target.Name,
                                                                                                    vessel.Name,
                                                                                                    vessel.CallSign)));


            if (sourceStructures.Count == 0 || targetStructures.Count == 0)
                throw new Exception("Could not fetch data...");

            var sourceDifferences = sourceStructures.Select(ss => ss.ShippernetixId)
                                                .Except(targetStructures.Select(ts => ts.ShippernetixId))
                                                .Select(e => sourceStructures.FirstOrDefault(s => s.ShippernetixId == e))
                                                .ToList();

            logMessages.Add(messageAction(string.Format("Source different from target with {0}", sourceDifferences.Count())));

            logMessages.Add(messageAction(string.Format("Differences : {0}", string.Join(",", sourceDifferences.Select(sd => sd.ShippernetixId)))));


            if(!onlyMail)
            if (sourceDifferences.Any())
            {

                logMessages.Add(messageAction(string.Format("Source different from target with {0}", sourceDifferences.Count())));

                foreach (var sourceDifference in sourceDifferences)
                {
                    var structure = new Structure(source.Connection, 
                                                        sourceDifference.En_CallSign.ToString(),
                                                        sourceDifference.En_L1.ToString(),
                                                        sourceDifference.En_L2.ToString(),
                                                        sourceDifference.En_L3.ToString(),
                                                        sourceDifference.En_L4.ToString(),
                                                        sourceColumnListDifferentWithTarget,
                                                        source.PrepareForVesselSide);

                    var affectedRowsCount = SqlManager.ExecuteNonQuery(sql: structure.Table.GetInsertQueries, parameters: null, target.Connection);

                    if (affectedRowsCount > 0)
                        logMessages.Add(messageAction(string.Format("{0} Transfered From {1} To {2}", sourceDifference.ShippernetixId, source.Name, target.Name)));
                    else
                        logMessages.Add(messageAction(string.Format("An error occured while transfering structure : {0}", sourceDifference.ShippernetixId)));
                }
            }


            var targetDifferences = targetStructures.Select(ss => ss.ShippernetixId)
                                .Except(sourceStructures.Select(ts => ts.ShippernetixId))
                                 .Select(e => targetStructures.FirstOrDefault(s => s.ShippernetixId == e))
                                .ToList();


            logMessages.Add(messageAction(string.Format("Target different from source with {0}", targetDifferences.Count())));

            logMessages.Add(messageAction(string.Format("Differences : {0}", string.Join(",", targetDifferences.Select(sd => sd.ShippernetixId)))));

            if(!onlyMail)
            if (targetDifferences.Any())
            {
                foreach (var targetDifference in targetDifferences)
                {
                    var structure = new Structure(target.Connection,
                                targetDifference.En_CallSign.ToString(),
                                targetDifference.En_L1.ToString(),
                                targetDifference.En_L2.ToString(),
                                targetDifference.En_L3.ToString(),
                                targetDifference.En_L4.ToString(),
                                targetColumnListDifferentWithSource,
                                target.PrepareForVesselSide);

                    var affectedRowsCount = SqlManager.ExecuteNonQuery(sql: structure.Table.GetInsertQueries, parameters: null, source.Connection);

                    if (affectedRowsCount > 0)
                        logMessages.Add(messageAction(string.Format("{0} Transfered From {1} To {2}", targetDifference.ShippernetixId, target.Name, source.Name)));
                    else
                        logMessages.Add(messageAction(string.Format("An error occured while transfering structure : {0}", targetDifference.ShippernetixId)));
                }
            }

            Program.SendMail($"Structure/{vessel.Name}({vessel.CallSign})", string.Join("</br></br>", logMessages));
        }
    }
}

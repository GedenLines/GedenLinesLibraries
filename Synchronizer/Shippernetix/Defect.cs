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
        public Defect(string callSign, string number,List<string> excludedColumnList,bool prepareForVesselSide)
        {
            var parameters = new Dictionary<string, object>();
            
            parameters.Add("Dai_CallSign", callSign);

            parameters.Add("Dai_DamageNo", number);

            Table = new DynamicTable(tableName: "Defects",excludedColumnList: excludedColumnList, parameters: parameters)
                        .SetUniqueColumns("Dai_CallSign", "Dai_DamageNo")
                        .PrepareForVesselSide(prepareForVesselSide)
                        .PrepareUpdateAndInsertQueries();
        }

        public static void Sync(Side source, Side target,Vessel_Master vessel)
        {
            var messageAction = new Func<string, string>(message =>
            {
                Console.WriteLine("\n{0}:\n" + DateTime.Now);

                Console.WriteLine(message);

                return message;
            });

            var sourceColumnList = SqlManager.SelectColumnNamesForMSSQL("Defects", source.Connection).Select(d=>d["COLUMN_NAME"].ToString()).ToList();

            var targetColumnList = SqlManager.SelectColumnNamesForMSSQL("Defects", target.Connection).Select(d => d["COLUMN_NAME"].ToString()).ToList();

            var sourceColumnListDifferentWithTarget = sourceColumnList.Except(targetColumnList).ToList();
            var targetColumnListDifferentWithSource = targetColumnList.Except(sourceColumnList).ToList();


            List<string> logMessages = new List<string>() { messageAction("DefectSync is started") };

            var defectGroupsQuery = "select Dai_CallSign,Dai_DamageNo,Dai_Status from(select * from Defects where Dai_CallSign = @CallSign)x group by Dai_CallSign, Dai_DamageNo, Dai_Status";

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
                        Dai_Status = d["Dai_Status"]
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
                    Dai_Status = d["Dai_Status"]
                })
                .ToList();

            logMessages.Add(messageAction(string.Format("Fetched {0} records from {1} and {2} records from {3} For {4}({5})",
                                                                                    sourceList.Count(),
                                                                                    source.Name,
                                                                                    targetList.Count(),
                                                                                    target.Name,
                                                                                    vessel.Name,
                                                                                    vessel.CallSign)));

            var sourceDifferences = sourceList.Where(ss => ss.Dai_Status.ToString() == "CLOSED").Select(ss => ss.ShippernetixId)
                                .Intersect(targetList.Where(ts => ts.Dai_Status.ToString() != "CLOSED").Select(ts => ts.ShippernetixId))
                                .Select(e => sourceList.FirstOrDefault(s => s.ShippernetixId == e))
                                .ToList();

            if (sourceDifferences.Any())
            {
                logMessages.Add(messageAction(string.Format("Source different from target with {0} : {1}", sourceDifferences.Count(), string.Join(",",sourceDifferences.Select(sd=>sd.ShippernetixId)))));

                foreach (var sourceDifference in sourceDifferences)
                {

                    var defect = new Defect(sourceDifference.Dai_CallSign.ToString(),
                                                        sourceDifference.Dai_DamageNo.ToString(),
                                                        sourceColumnListDifferentWithTarget,
                                                        source.PrepareForVesselSide);


                    var affectedRowsCount = SqlManager.ExecuteNonQuery(sql: defect.Table.GetUpdateQueries, parameters: null, target.Connection);

                    if (affectedRowsCount > 0)
                        logMessages.Add(messageAction(string.Format("{0} Transfered From {0} To {1}", sourceDifference.ShippernetixId, source.Name, target.Name)));
                    else
                        logMessages.Add(messageAction(string.Format("An error occured while transfering structure : {0}", sourceDifference.ShippernetixId)));
                }
            }

            var targetDifferences = targetList.Where(ts => ts.Dai_Status.ToString() == "VESSEL COMPLETED").Select(ss => ss.ShippernetixId)
                                .Intersect(sourceList.Where(ss => ss.Dai_Status.ToString() == "OPEN").Select(ts => ts.ShippernetixId))
                                 .Select(e => targetList.FirstOrDefault(s => s.ShippernetixId == e))
                                .ToList();

            if (targetDifferences.Any())
            {
                logMessages.Add(messageAction(string.Format("Target different from source with {0} : {1}", targetDifferences.Count(), string.Join(",", targetDifferences.Select(td => td.ShippernetixId)))));

                foreach (var targetDifference in targetDifferences)
                {
                    var defect = new Defect(targetDifference.Dai_CallSign.ToString(),
                                targetDifference.Dai_DamageNo.ToString(),
                                targetColumnListDifferentWithSource,
                                target.PrepareForVesselSide);


                    var affectedRowsCount = SqlManager.ExecuteNonQuery(sql: defect.Table.GetUpdateQueries, parameters: null, source.Connection);

                    if (affectedRowsCount > 0)
                        logMessages.Add(messageAction(string.Format("{0} Transfered From {0} To {1}", targetDifference.ShippernetixId, target.Name, source.Name)));
                    else
                        logMessages.Add(messageAction(string.Format("An error occured while transfering structure : {0}", targetDifference.ShippernetixId)));
                }
            }

            }
        }
}

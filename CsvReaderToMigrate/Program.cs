using System;
using SqlManagement;
using FileManagement;
using System.Collections.Generic;
using FileManagement.FileType;
using System.Linq;

namespace CsvReaderToMigrate
{
    class Program
    {
        static void Main(string[] args)
        {
            CustomConnection.Start(() =>
            {
                return new Dictionary<string, string>()
                {
                    {
                        "MsSqlConnectionString",
                        "Server=BRIDGE; Database=GENEL; User id=sa;Password=;Application Name=Genel;Max Pool Size=50;MultipleActiveResultSets=true"
                    },
                };
            });
            

            var connection = new MsSqlConnection(connectionString: "MsSqlConnectionString");


            if (!SqlManager.CheckConnection(connection))
                throw new Exception("Db Connection Problem Occured");

            //var structures = GetStructuresByVessel("9HNK", "ALL", "ALL", "ALL", "ALL", connection);
            //CloneStructureTo(structures, "9SGR", connection, true);



            //var a = 1;
            //var structures = GetStructuresByVessel("9HM2", "16", "ALL", "ALL","ALL", connection);
            //CloneStructureTo(structures, "9ANT", connection,true);

            //structures = GetStructuresByVessel("9VRT", "5", "3", "2", "ALL", connection);
            //CloneStructureTo(structures, "9ANT", connection,true);

            //structures = GetStructuresByVessel("9HA4", "2", "1", "ALL", null, connection);
            //CloneStructureTo(structures, "9ANT", connection, true);
            //structures = GetStructuresByVessel("9HA4", "2", "2", "ALL", null, connection);
            //CloneStructureTo(structures, "9ANT", connection, true);
            //structures = GetStructuresByVessel("9HA4", "2", "3", "ALL", null, connection);
            //CloneStructureTo(structures, "9ANT", connection, true);
            //structures = GetStructuresByVessel("9HA4", "2", "4", "ALL", null, connection);
            //CloneStructureTo(structures, "9ANT", connection, true);
            //structures = GetStructuresByVessel("9HA4", "2", "5", "ALL", null, connection);
            //CloneStructureTo(structures, "9ANT", connection, true);

            try
            {

                //var sql = "select m.Name,jd.Jd_CallSign,jd.Jd_L1,jd.Jd_L2,jd.Jd_L3,jd.Jd_L4,jd.Jd_JobCode,jd.Jd_JobType,jd.Jd_JobTitle,jd.Jd_Desc,Jd_IntType,Jd_Int,Jd_RuntimeSourceCode,Jd_DepCode from Job_Definition jd left join Vessel_Master m on jd.Jd_CallSign=m.CallSign where jd.Jd_CallSign='9VLE'";

                //var rowsOfValueDescriptions = SqlManager.ExecuteQuery(sql: sql, connection: connection);

                //var counter = rowsOfValueDescriptions.Count;

                //foreach (var row in rowsOfValueDescriptions)
                //{
                //    try
                //    {
                //        Console.WriteLine($"{row["Jd_CallSign"]}-{ row["Jd_L1"] }-{row["Jd_L2"]}-{row["Jd_L3"]}-{row["Jd_L4"]}-{row["Jd_JobCode"] }");

                //        var inserted = SqlManager.ExecuteNonQuery(sql: "insert into TEMP_VALUE_JOB_DESC select '"+ row["Name"] + "',Jd_CallSign,Jd_L1,Jd_L2,Jd_L3,Jd_L4,Jd_JobCode,Jd_JobType,Jd_JobTitle,Jd_Desc,dbo.rtf2txt2(Jd_Desc) as RtxToPlain,Jd_IntType,Jd_Int,Jd_RuntimeSourceCode,Jd_DepCode from Job_Definition where Jd_CallSign=@Jd_CallSign and Jd_L1=@Jd_L1 and Jd_L2=@Jd_L2 and Jd_L3=@Jd_L3 and Jd_L4=@Jd_L4 and Jd_JobCode=@Jd_JobCode", 
                //            parameters:new Dictionary<string, object>() 
                //            {
                //                {"Jd_CallSign",row["Jd_CallSign"]},
                //                {"Jd_L1",row["Jd_L1"] },
                //                {"Jd_L2", row["Jd_L2"]},
                //                {"Jd_L3", row["Jd_L3"]},
                //                {"Jd_L4", row["Jd_L4"]},
                //                {"Jd_JobCode", row["Jd_JobCode"] }
                //            },connection: connection);

                //        if (inserted != 1)
                //        {
                //            inserted = SqlManager.ExecuteNonQuery(sql: "insert into TEMP_VALUE_JOB_DESC select '" + row["Name"] + "',Jd_CallSign,Jd_L1,Jd_L2,Jd_L3,Jd_L4,Jd_JobCode,Jd_JobType,Jd_JobTitle,Jd_Desc,null as RtxToPlain,Jd_IntType,Jd_Int,Jd_RuntimeSourceCode,Jd_DepCode from Job_Definition where Jd_CallSign=@Jd_CallSign and Jd_L1=@Jd_L1 and Jd_L2=@Jd_L2 and Jd_L3=@Jd_L3 and Jd_L4=@Jd_L4 and Jd_JobCode=@Jd_JobCode",
                //            parameters: new Dictionary<string, object>()
                //            {
                //                {"Jd_CallSign",row["Jd_CallSign"]},
                //                {"Jd_L1",row["Jd_L1"] },
                //                {"Jd_L2", row["Jd_L2"]},
                //                {"Jd_L3", row["Jd_L3"]},
                //                {"Jd_L4", row["Jd_L4"]},
                //                {"Jd_JobCode", row["Jd_JobCode"] }
                //            }, connection: connection);
                //        }

                //    }
                //    catch (Exception ex)
                //    {

                //    }

                //    Console.WriteLine("Counter : " + counter--);
                //}




                //CloneStructureTo(structures: new List<Structure>()
                //{
                //    new Structure("9PRK","5","3","2","1")
                //},
                //ToCallSign: "9PRT",
                //connection: connection,
                //cloneJobDefinitions: true);

                //CloneStructureTo(structures: new List<Structure>()
                //{
                //    new Structure("9HM2","14","6","1","6")
                //},
                //ToCallSign: "9BRV",
                //connection: connection,
                //cloneJobDefinitions: true);

                //CloneStructureTo(structures: new List<Structure>()
                //{
                //    new Structure("9HM2","14","6","1","6")
                //},
                //ToCallSign: "9HNK",
                //connection: connection,
                //cloneJobDefinitions: true);


                //foreach (var toCallSign in new string[] { "9PRD", "9PRT", "9HA2", "9HLN", "9HN2", "9HNK", "9VLE","9VRT","9HA4","9BUD" })
                //{

                //    //CloneStructureTo("9PRK", toCallSign, connection, new Dictionary<string, object>()
                //    //{
                //    //    { "En_CallSign", "9PRK" },
                //    //    { "En_L1",6},
                //    //    { "En_L2",0},
                //    //    { "En_L3",0},
                //    //    { "En_L4",0}
                //    //});

                //    //CloneStructureTo("9PRK", toCallSign, connection, new Dictionary<string, object>()
                //    //{
                //    //    { "En_CallSign", "9PRK" },
                //    //    { "En_L1",6},
                //    //    { "En_L2",6},
                //    //    { "En_L3",0},
                //    //    { "En_L4",0}
                //    //});

                //    //CloneStructureTo("9PRK", toCallSign, connection, new Dictionary<string, object>()
                //    //{
                //    //    { "En_CallSign", "9PRK" },
                //    //    { "En_L1",6},
                //    //    { "En_L2",6},
                //    //    { "En_L3",1},
                //    //    { "En_L4",0}
                //    //});

                //    //CloneStructureTo("9PRK", toCallSign, connection,new Dictionary<string, object>() 
                //    //{
                //    //    { "En_CallSign", "9PRK" },
                //    //    { "En_L1",6},
                //    //    { "En_L2",6},
                //    //    { "En_L3",1},
                //    //    { "En_L4",1}
                //    //});

                //    CloneJobDefinitionTo("9PRK",toCallSign,connection,new Dictionary<string, object>()
                //    {
                //        {"Jd_CallSign", "9PRK"},
                //        {"Jd_L1",6 },
                //        {"Jd_L2", 6},
                //        {"Jd_L3", 1},
                //        {"Jd_L4", 1},
                //        {"Jd_JobCode", "A"}
                //    });

                //    CloneJobDefinitionTo("9PRK", toCallSign, connection, new Dictionary<string, object>()
                //    {
                //        {"Jd_CallSign", "9PRK"},
                //        {"Jd_L1",6 },
                //        {"Jd_L2", 6},
                //        {"Jd_L3", 1},
                //        {"Jd_L4", 1},
                //        {"Jd_JobCode", "B"}
                //    });
                //}

                //var structures2 = GetStructuresByVessel("9VRD", "ALL", "ALL", "ALL", "ALL", connection);
                //CloneStructureTo(structures2, "9VCT", connection, true);


                var newVesselCallSign = "9VRD";

                var vesselCallSignToReplace = "9PWR";

                //var path = CustomFile.CombinePaths(CustomFile.CurrentUserDesktopPath, "ANTONIS.csv");
                //VLCCDF.csv
                var path = CustomFile.CombinePaths(CustomFile.CurrentUserDesktopPath, "VLCCDF.csv");

                var file = new Dsv(path: path);

                var rows = file.Split();

                var parameters = new Dictionary<string, object>();

                foreach (var row in rows)
                {
                    var columns = row.Split(file.Separator,6,StringSplitOptions.RemoveEmptyEntries);

                    if (columns[0].ToLower() == "CallSign".ToLower())
                        continue;

                    var trimAction = new Func<string, string>((str)=> 
                    {
                        return str.Trim(',').Trim();
                    });

                    var callSign = trimAction(columns[0]);
                    var l1 = trimAction(columns[1]);
                    var l2 = trimAction(columns[2]);
                    var l3 = trimAction(columns[3]);
                    var l4 = trimAction(columns[4]);
                    var code = trimAction(columns[5]);

                    //var structures = GetStructuresByVessel(callSign, l1, l2, l3, l4, connection);
                    //CloneStructureTo(structures, newVesselCallSign, connection, true);

                    //for (int i = 1; i < 5; i++)
                    //{
                    //    if (i == 1)
                    //    {
                    //        parameters.Add("En_CallSign", callSign);
                    //        parameters.Add("En_L1", l1);
                    //        parameters.Add("En_L2", 0);
                    //        parameters.Add("En_L3", 0);
                    //        parameters.Add("En_L4", 0);

                    //        CloneStructureTo(callSign, newVesselCallSign, connection, parameters);
                    //        parameters.Clear();
                    //    }
                    //    else if (i == 2)
                    //    {
                    //        if (l2.ToLower() == "ALL".ToLower())
                    //        {
                    //            var selectOtherLColumns = "select En_L2 as L2,En_L3 as L3,En_L4 as L4 from Structure where En_CallSign=@CallSign and En_L1=@L1";

                    //            var collection = SqlManager.ExecuteQuery(sql: selectOtherLColumns, new Dictionary<string, object>()
                    //            {
                    //                {"CallSign",vesselCallSignToReplace },
                    //                {"L1",l1 }
                    //            });

                    //            foreach (var item in collection)
                    //            {
                    //                var _l2 = item["L2"].ToString();
                    //                var _l3 = item["L3"].ToString();
                    //                var _l4 = item["L4"].ToString();

                    //                //if (_l2=="0" && _l3 == "0" && _l4 == "0")
                    //                //    continue;

                    //                parameters.Add("En_CallSign", vesselCallSignToReplace);
                    //                parameters.Add("En_L1", l1);
                    //                parameters.Add("En_L2", _l2);
                    //                parameters.Add("En_L3", _l3);
                    //                parameters.Add("En_L4", _l4);

                    //                CloneStructureTo(vesselCallSignToReplace, newVesselCallSign, connection, parameters);
                    //                parameters.Clear();
                    //            }
                    //        }
                    //        else
                    //        {
                    //            parameters.Add("En_CallSign", callSign);
                    //            parameters.Add("En_L1", l1);
                    //            parameters.Add("En_L2", l2);
                    //            parameters.Add("En_L3", 0);
                    //            parameters.Add("En_L4", 0);

                    //            CloneStructureTo(callSign, newVesselCallSign, connection, parameters);
                    //            parameters.Clear();
                    //        }
                    //    }
                    //    else if (i == 3)
                    //    {
                    //        if (l3.ToLower() == "ALL".ToLower())
                    //        {
                    //            var selectOtherLColumns = "select En_L3 as L3,En_L4 as L4 from Structure where En_CallSign=@CallSign and En_L1=@L1 and En_L2=@L2";

                    //            var collection = SqlManager.ExecuteQuery(sql: selectOtherLColumns, new Dictionary<string, object>()
                    //            {
                    //                {"CallSign",vesselCallSignToReplace },
                    //                {"L1",l1 },
                    //                {"L2",l2 }
                    //            });

                    //            foreach (var item in collection)
                    //            {
                    //                var _l3 = item["L3"].ToString();
                    //                var _l4 = item["L4"].ToString();

                    //                //if (_l3 == "0" && _l4 == "0")
                    //                //    continue;

                    //                parameters.Add("En_CallSign", vesselCallSignToReplace);
                    //                parameters.Add("En_L1", l1);
                    //                parameters.Add("En_L2", l2);
                    //                parameters.Add("En_L3", _l3);
                    //                parameters.Add("En_L4", _l4);

                    //                CloneStructureTo(vesselCallSignToReplace, newVesselCallSign, connection, parameters);
                    //                parameters.Clear();
                    //            }
                    //        }
                    //        else
                    //        {
                    //            parameters.Add("En_CallSign", callSign);
                    //            parameters.Add("En_L1", l1);
                    //            parameters.Add("En_L2", l2);
                    //            parameters.Add("En_L3", l3);
                    //            parameters.Add("En_L4", 0);

                    //            CloneStructureTo(callSign, newVesselCallSign, connection, parameters);
                    //            parameters.Clear();
                    //        }
                    //    }
                    //    else if (i == 4)
                    //    {
                    //        if (l4.ToLower() == "ALL".ToLower())
                    //        {
                    //            var selectOtherLColumns = "select En_L4 as L4 from Structure where En_CallSign=@CallSign and En_L1=@L1 and En_L2=@L2 and En_L3=@L3";

                    //            var collection = SqlManager.ExecuteQuery(sql: selectOtherLColumns, new Dictionary<string, object>()
                    //            {
                    //                {"CallSign",vesselCallSignToReplace },
                    //                {"L1",l1 },
                    //                {"L2",l2 },
                    //                {"L3",l3 }
                    //            });

                    //            foreach (var item in collection)
                    //            {
                    //                var _l4 = item["L4"].ToString();

                    //                //if (_l4 == "0")
                    //                //    continue;

                    //                parameters.Add("En_CallSign", vesselCallSignToReplace);
                    //                parameters.Add("En_L1", l1);
                    //                parameters.Add("En_L2", l2);
                    //                parameters.Add("En_L3", l3);
                    //                parameters.Add("En_L4", _l4);

                    //                CloneStructureTo(vesselCallSignToReplace, newVesselCallSign, connection, parameters);
                    //                parameters.Clear();
                    //            }
                    //        }
                    //        else
                    //        {
                    //            parameters.Add("En_CallSign", callSign);
                    //            parameters.Add("En_L1", l1);
                    //            parameters.Add("En_L2", l2);
                    //            parameters.Add("En_L3", l3);
                    //            parameters.Add("En_L4", l4);

                    //            CloneStructureTo(callSign, newVesselCallSign, connection, parameters);
                    //            parameters.Clear();
                    //        }
                    //    }
                    //}
                }

                //var allJobDefinitionFromSampleVesselSql = "select Jd_L1 as L1,Jd_L2 as L2,Jd_L3 as L3,Jd_L4 as L4,Jd_JobCode as Code " +
                //                    "from Job_Definition " +
                //                    "where Jd_CallSign = @CallSign and Jd_JobType='PLM'";

                //var jobDefinitionsFromSampleVessel = SqlManager.ExecuteQuery(allJobDefinitionFromSampleVesselSql, new Dictionary<string, object>()
                //    {
                //        {"CallSign",vesselCallSignToReplace }
                //    }, connection);

                //foreach (var jd in jobDefinitionsFromSampleVessel)
                //{
                //    parameters.Clear();
                //    parameters.Add("Jd_CallSign", vesselCallSignToReplace);
                //    parameters.Add("Jd_L1", jd["L1"]);
                //    parameters.Add("Jd_L2", jd["L2"]);
                //    parameters.Add("Jd_L3", jd["L3"]);
                //    parameters.Add("Jd_L4", jd["L4"]);
                //    parameters.Add("Jd_JobCode", jd["Code"]);

                //    if(SqlManager.Any("select  * from Structure where En_CallSign=@CallSign and En_L1=@L1 and En_L2=@L2 and En_L3=@L3 and En_L4=@L4",new Dictionary<string, object>() 
                //    {
                //        {"CallSign", newVesselCallSign},
                //        {"L1", jd["L1"]},
                //        {"L2", jd["L2"]},
                //        {"L3", jd["L3"]},
                //        {"L4", jd["L4"]},
                //    }))
                //    {
                //        var shippernetixId = $"{vesselCallSignToReplace}-{jd["L1"]}-{jd["L2"]}-{jd["L3"]}-{jd["L4"]}-{jd["Code"]}";

                //        CloneJobDefinitionTo(vesselCallSignToReplace, newVesselCallSign, connection, parameters);
                //    }
                //}
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                throw ex;
            }
        }


        public static List<Structure> GetStructuresByVessel(string callSign, string l1, string l2, string l3, string l4, CustomConnection connection)
        {
            var action = new Func<string,bool>((str) =>
            {
                return str?.ToLower() == "all";
            });

            var IsNotNull = new Func<string,bool>((str)=> 
            {
                if(string.IsNullOrEmpty(str))
                    throw new Exception();

                return true;
            });

            if (string.IsNullOrEmpty(l1))
                throw new Exception("Please enter L1");

            var getL1All = action(l1);
            var getL2All = action(l2);
            var getL3All = action(l3);
            var getL4All = action(l4);


            var query = "select En_CallSign,En_L1,En_L2,En_L3,En_L4 from Structure where En_CallSign=@CallSign";

            var parameters = new Dictionary<string, object>();

            parameters.Add("CallSign", callSign);


            if (getL1All)
            {

            }
            else if (getL2All)
            {
                query = $"{query} and En_L1=@L1";

                parameters.Add("L1",l1);
            }
            else if (getL3All)
            {
                query = $"{query} and En_L1=@L1 and En_L2=@L2";

                parameters.Add("L1", l1);

                parameters.Add("L2", l2);
            }
            else if(getL4All)
            {
                query = $"{query} and En_L1=@L1 and En_L2=@L2 and En_L3=@L3";

                parameters.Add("L1", l1);

                parameters.Add("L2", l2);

                parameters.Add("L3", l3);
            }
            else
            {
                if (!getL1All)
                {
                    query = $"{query} and En_L1=@L1 and En_L2=@L2 and En_L3=@L3 and En_L4=@L4";

                    if (IsNotNull(l1) && IsNotNull(l2) && IsNotNull(l3) && IsNotNull(l4))
                    {
                        parameters.Add("L1", l1);

                        parameters.Add("L2", l2);

                        parameters.Add("L3", l3);

                        parameters.Add("L4", l4);
                    }
                }
            }

            var structures = SqlManager.ExecuteQuery(query,parameters,connection)
                .Select(r=> new Structure() 
                {
                    En_CallSign = r["En_CallSign"].ToString(),
                    En_L1 = r["En_L1"].ToString(),
                    En_L2 = r["En_L2"].ToString(),
                    En_L3 = r["En_L3"].ToString(),
                    En_L4 = r["En_L4"].ToString()
                }).ToList();


            List<Structure> missingStructures = new List<Structure>();

            if (IsNotNull(l1))
            {
                var list = SqlManager.ExecuteQuery(query, parameters, connection)
                .Select(r => new Structure()
                {
                    En_CallSign = r["En_CallSign"].ToString(),
                    En_L1 = r["En_L1"].ToString(),
                    En_L2 = "0",
                    En_L3 = "0",
                    En_L4 = "0"
                });

                missingStructures.AddRange(list);
            }

            if (IsNotNull(l2))
            {
                var list  = SqlManager.ExecuteQuery(query, parameters, connection)
                  .Select(r => new Structure()
                  {
                      En_CallSign = r["En_CallSign"].ToString(),
                      En_L1 = r["En_L1"].ToString(),
                      En_L2 = r["En_L2"].ToString(),
                      En_L3 = "0",
                      En_L4 = "0"
                  }).ToList();

                missingStructures.AddRange(list);
            }


            if (IsNotNull(l3))
            {
                var list = SqlManager.ExecuteQuery(query, parameters, connection)
                  .Select(r => new Structure()
                  {
                      En_CallSign = r["En_CallSign"].ToString(),
                      En_L1 = r["En_L1"].ToString(),
                      En_L2 = r["En_L2"].ToString(),
                      En_L3 = r["En_L3"].ToString(),
                      En_L4 = "0"
                  }).ToList();

                missingStructures.AddRange(list);
            }

            foreach (var missingStructure in missingStructures)
            {
                if (structures.Any(s=>s.En_CallSign==missingStructure.En_CallSign && s.En_L1==missingStructure.En_L1 && s.En_L2==missingStructure.En_L2 && s.En_L3==missingStructure.En_L3 && s.En_L4==missingStructure.En_L4))
                    continue;

                structures.Add(new Structure()
                {
                    En_CallSign = missingStructure.En_CallSign,
                    En_L1 = missingStructure.En_L1,
                    En_L2 = missingStructure.En_L2,
                    En_L3 = missingStructure.En_L3,
                    En_L4 = missingStructure.En_L4
                });
            }



            return structures;
        }

        public static List<JobDefinition> GetStructureJobDefinitions(Structure structure,CustomConnection connection)
        {
            var parameters = new Dictionary<string, object>();

            parameters.Add("CallSign", structure.En_CallSign);
            parameters.Add("L1", structure.En_L1);
            parameters.Add("L2", structure.En_L2);
            parameters.Add("L3", structure.En_L3);
            parameters.Add("L4", structure.En_L4);
            parameters.Add("jobType", "PLM");

            var results = SqlManager.ExecuteQuery("select * from Job_Definition where Jd_CallSign=@CallSign and Jd_L1=@L1 and Jd_L2=@L2 and Jd_L3=@L3 and Jd_L4=@L4 and Jd_JobType=@jobType", parameters, connection)
                .Select(r=>new JobDefinition() 
                {
                    Jd_CallSign = r["Jd_CallSign"].ToString(),
                    Jd_L1 = r["Jd_L1"].ToString(),
                    Jd_L2 = r["Jd_L2"].ToString(),
                    Jd_L3 = r["Jd_L3"].ToString(),
                    Jd_L4 = r["Jd_L4"].ToString(),
                    Jd_JobCode = r["Jd_JobCode"].ToString()
                }).ToList();

            return results;
        }

        public static void CloneStructureTo(List<Structure> structures, string ToCallSign, CustomConnection connection,bool cloneJobDefinitions = false)
        {
            foreach (var structure in structures)
            {
                var parameters = new Dictionary<string, object>();

                parameters.Add("En_CallSign", structure.En_CallSign);
                parameters.Add("En_L1", structure.En_L1);
                parameters.Add("En_L2", structure.En_L2);
                parameters.Add("En_L3", structure.En_L3);
                parameters.Add("En_L4", structure.En_L4);

                CloneStructureTo(structure.En_CallSign, ToCallSign, connection, parameters);

                if (cloneJobDefinitions)
                {
                    var jds = GetStructureJobDefinitions(structure,connection);

                    foreach (var jd in jds)
                    {
                        parameters.Clear();

                        parameters.Add("Jd_CallSign", jd.Jd_CallSign);
                        parameters.Add("Jd_L1", jd.Jd_L1);
                        parameters.Add("Jd_L2", jd.Jd_L2);
                        parameters.Add("Jd_L3", jd.Jd_L3);
                        parameters.Add("Jd_L4", jd.Jd_L4);
                        parameters.Add("Jd_JobCode", jd.Jd_JobCode);

                        CloneJobDefinitionTo(jd.Jd_CallSign,ToCallSign,connection,parameters);
                    }
                }
            }
        }


        public static void CloneStructureTo(string fromCallSign, string ToCallSign,string l1,string l2,string l3,string l4, CustomConnection connection)
        {
            var parameters = new Dictionary<string, object>();

            parameters.Add("En_CallSign", fromCallSign);

            if (!string.IsNullOrEmpty(l1))
            {
                parameters.Add("En_L1", l1);

                if (!string.IsNullOrEmpty(l2))
                {
                    parameters.Add("En_L2", l2);

                    if (!string.IsNullOrEmpty(l3))
                    {
                        parameters.Add("En_L3", l3);

                        if(!string.IsNullOrEmpty(l4))
                        {
                            parameters.Add("En_L4", l4);
                        }
                        else
                        {
                            parameters.Add("En_L4", 0);
                        }
                    }
                    else
                    {
                        parameters.Add("En_L3", 0);

                        parameters.Add("En_L4", 0);
                    }
                }
                else
                {
                    parameters.Add("En_L2", 0);

                    parameters.Add("En_L3", 0);

                    parameters.Add("En_L4", 0);
                }
            }
            else
            {
                throw new Exception("Please enter L1");
            }

            CloneStructureTo(fromCallSign,ToCallSign,connection,parameters);
        }


        public static void CloneStructureTo(string fromCallSign,string ToCallSign,CustomConnection connection,Dictionary<string,object> parameters)
        {
            var structureTable = new DynamicTable(tableName: "Structure", customConnection: connection, parameters: parameters)
                        .SetUniqueColumns("En_CallSign", "En_L1", "En_L2", "En_L3", "En_L4")
                        .PrepareForVesselSide(true)
                        .PrepareUpdateAndInsertQueries();

            if (structureTable == null || string.IsNullOrEmpty(structureTable.GetInsertQueries))
                return;

            var structureSqlForNewVessel = structureTable.GetInsertQueries.Replace(fromCallSign, ToCallSign);

            var isInserted = false;

            var existingQuery = structureTable.SelectQuery.Replace(fromCallSign,ToCallSign);

            var IsExistAsStructure = SqlManager.Any(sql: existingQuery,null, connection: connection);

            if (!IsExistAsStructure)
            {
                isInserted = SqlManager.ExecuteNonQuery(sql: structureSqlForNewVessel, parameters: null, connection: connection) > 0;

                if (isInserted)
                {
                    Console.WriteLine("Insert process has been completed successfully(Structure)");

                    foreach (var p in parameters)
                    {
                        Console.Write($"From {p.Key}:{p.Value}");
                    }

                    Console.Write($"To {ToCallSign}");
                }
                else
                {
                    Console.WriteLine("An error occured while inserting(Structure)");
                    throw new Exception("An error occured while inserting(Structure)");
                }
            }
        }

        public static void CloneJobDefinitionTo(string fromCallSign,string ToCallSign,CustomConnection connection,Dictionary<string,object> parameters)
        {
            var jobDefinitionTable = new DynamicTable(tableName: "Job_Definition", customConnection: connection, parameters: parameters)
                .SetUniqueColumns("Jd_CallSign", "Jd_L1", "Jd_L2", "Jd_L3", "Jd_L4", "Jd_JobCode")
                .PrepareForVesselSide(true);

            foreach (var row in jobDefinitionTable.Rows)
            {
                foreach (var column in row.Columns)
                {
                    if (column.Name == "Jd_PlanRuntime")
                        column.Value = 0;
                    else if (column.Name == "Jd_PlanDate")
                        column.Value = DateTime.Now;
                }
            }


            jobDefinitionTable.PrepareUpdateAndInsertQueries();

            if (jobDefinitionTable == null || string.IsNullOrEmpty(jobDefinitionTable.GetInsertQueries))
                return;

            var jobDefinitionSqlForNewVessel = jobDefinitionTable.GetInsertQueries.Replace(fromCallSign, ToCallSign);

            var isInserted = false;

            var existingQuery = jobDefinitionTable.SelectQuery.Replace(fromCallSign, ToCallSign);

            var IsExistAsJobDefinition = SqlManager.Any(sql: existingQuery, parameters: null, connection: connection);

            if (!IsExistAsJobDefinition)
            {
                isInserted = SqlManager.ExecuteNonQuery(sql: jobDefinitionSqlForNewVessel, parameters: null, connection: connection) > 0;

                if (isInserted)
                {
                    Console.WriteLine("Insert process has been completed successfully(Job Definition)");

                    foreach (var p in parameters)
                    {
                        Console.Write($"From {p.Key}:{p.Value}");
                    }

                    Console.Write($"To {ToCallSign}");
                }
                else
                {
                    Console.WriteLine("An error occured while inserting(Job Definition)");
                    throw new Exception("An error occured while inserting(Job Definition)");
                }
            }
        }
    }
    
}

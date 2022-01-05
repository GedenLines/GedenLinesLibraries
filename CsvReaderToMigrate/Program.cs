using System;
using SqlManagement;
using FileManagement;
using System.Collections.Generic;
using FileManagement.FileType;

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


            try
            {
                foreach (var toCallSign in new string[] { "9PRD", "9PRT", "9HA2", "9HLN", "9HN2", "9HNK", "9VLE","9VRT","9HA4","9BUD" })
                {

                    //CloneStructureTo("9PRK", toCallSign, connection, new Dictionary<string, object>()
                    //{
                    //    { "En_CallSign", "9PRK" },
                    //    { "En_L1",6},
                    //    { "En_L2",0},
                    //    { "En_L3",0},
                    //    { "En_L4",0}
                    //});

                    //CloneStructureTo("9PRK", toCallSign, connection, new Dictionary<string, object>()
                    //{
                    //    { "En_CallSign", "9PRK" },
                    //    { "En_L1",6},
                    //    { "En_L2",6},
                    //    { "En_L3",0},
                    //    { "En_L4",0}
                    //});

                    //CloneStructureTo("9PRK", toCallSign, connection, new Dictionary<string, object>()
                    //{
                    //    { "En_CallSign", "9PRK" },
                    //    { "En_L1",6},
                    //    { "En_L2",6},
                    //    { "En_L3",1},
                    //    { "En_L4",0}
                    //});

                    //CloneStructureTo("9PRK", toCallSign, connection,new Dictionary<string, object>() 
                    //{
                    //    { "En_CallSign", "9PRK" },
                    //    { "En_L1",6},
                    //    { "En_L2",6},
                    //    { "En_L3",1},
                    //    { "En_L4",1}
                    //});

                    CloneJobDefinitionTo("9PRK",toCallSign,connection,new Dictionary<string, object>()
                    {
                        {"Jd_CallSign", "9PRK"},
                        {"Jd_L1",6 },
                        {"Jd_L2", 6},
                        {"Jd_L3", 1},
                        {"Jd_L4", 1},
                        {"Jd_JobCode", "A"}
                    });

                    CloneJobDefinitionTo("9PRK", toCallSign, connection, new Dictionary<string, object>()
                    {
                        {"Jd_CallSign", "9PRK"},
                        {"Jd_L1",6 },
                        {"Jd_L2", 6},
                        {"Jd_L3", 1},
                        {"Jd_L4", 1},
                        {"Jd_JobCode", "B"}
                    });
                }


                var newVesselCallSign = "9ANT";

                var vesselCallSignToReplace = "9PWR";

                var path = CustomFile.CombinePaths(CustomFile.CurrentUserDesktopPath, "ANTONIS.csv");

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

                    for (int i = 1; i < 5; i++)
                    {
                        if (i == 1)
                        {
                            parameters.Add("En_CallSign", callSign);
                            parameters.Add("En_L1", l1);
                            parameters.Add("En_L2", 0);
                            parameters.Add("En_L3", 0);
                            parameters.Add("En_L4", 0);

                            CloneStructureTo(callSign, newVesselCallSign, connection, parameters);
                            parameters.Clear();
                        }
                        else if (i == 2)
                        {
                            if (l2.ToLower() == "ALL".ToLower())
                            {
                                var selectOtherLColumns = "select En_L2 as L2,En_L3 as L3,En_L4 as L4 from Structure where En_CallSign=@CallSign and En_L1=@L1";

                                var collection = SqlManager.ExecuteQuery(sql: selectOtherLColumns, new Dictionary<string, object>()
                                {
                                    {"CallSign",vesselCallSignToReplace },
                                    {"L1",l1 }
                                });

                                foreach (var item in collection)
                                {
                                    var _l2 = item["L2"].ToString();
                                    var _l3 = item["L3"].ToString();
                                    var _l4 = item["L4"].ToString();

                                    //if (_l2=="0" && _l3 == "0" && _l4 == "0")
                                    //    continue;

                                    parameters.Add("En_CallSign", vesselCallSignToReplace);
                                    parameters.Add("En_L1", l1);
                                    parameters.Add("En_L2", _l2);
                                    parameters.Add("En_L3", _l3);
                                    parameters.Add("En_L4", _l4);

                                    CloneStructureTo(vesselCallSignToReplace, newVesselCallSign, connection, parameters);
                                    parameters.Clear();
                                }
                            }
                            else
                            {
                                parameters.Add("En_CallSign", callSign);
                                parameters.Add("En_L1", l1);
                                parameters.Add("En_L2", l2);
                                parameters.Add("En_L3", 0);
                                parameters.Add("En_L4", 0);

                                CloneStructureTo(callSign, newVesselCallSign, connection, parameters);
                                parameters.Clear();
                            }
                        }
                        else if (i == 3)
                        {
                            if (l3.ToLower() == "ALL".ToLower())
                            {
                                var selectOtherLColumns = "select En_L3 as L3,En_L4 as L4 from Structure where En_CallSign=@CallSign and En_L1=@L1 and En_L2=@L2";

                                var collection = SqlManager.ExecuteQuery(sql: selectOtherLColumns, new Dictionary<string, object>()
                                {
                                    {"CallSign",vesselCallSignToReplace },
                                    {"L1",l1 },
                                    {"L2",l2 }
                                });

                                foreach (var item in collection)
                                {
                                    var _l3 = item["L3"].ToString();
                                    var _l4 = item["L4"].ToString();

                                    //if (_l3 == "0" && _l4 == "0")
                                    //    continue;

                                    parameters.Add("En_CallSign", vesselCallSignToReplace);
                                    parameters.Add("En_L1", l1);
                                    parameters.Add("En_L2", l2);
                                    parameters.Add("En_L3", _l3);
                                    parameters.Add("En_L4", _l4);

                                    CloneStructureTo(vesselCallSignToReplace, newVesselCallSign, connection, parameters);
                                    parameters.Clear();
                                }
                            }
                            else
                            {
                                parameters.Add("En_CallSign", callSign);
                                parameters.Add("En_L1", l1);
                                parameters.Add("En_L2", l2);
                                parameters.Add("En_L3", l3);
                                parameters.Add("En_L4", 0);

                                CloneStructureTo(callSign, newVesselCallSign, connection, parameters);
                                parameters.Clear();
                            }
                        }
                        else if (i == 4)
                        {
                            if (l4.ToLower() == "ALL".ToLower())
                            {
                                var selectOtherLColumns = "select En_L4 as L4 from Structure where En_CallSign=@CallSign and En_L1=@L1 and En_L2=@L2 and En_L3=@L3";

                                var collection = SqlManager.ExecuteQuery(sql: selectOtherLColumns, new Dictionary<string, object>()
                                {
                                    {"CallSign",vesselCallSignToReplace },
                                    {"L1",l1 },
                                    {"L2",l2 },
                                    {"L3",l3 }
                                });

                                foreach (var item in collection)
                                {
                                    var _l4 = item["L4"].ToString();

                                    //if (_l4 == "0")
                                    //    continue;

                                    parameters.Add("En_CallSign", vesselCallSignToReplace);
                                    parameters.Add("En_L1", l1);
                                    parameters.Add("En_L2", l2);
                                    parameters.Add("En_L3", l3);
                                    parameters.Add("En_L4", _l4);

                                    CloneStructureTo(vesselCallSignToReplace, newVesselCallSign, connection, parameters);
                                    parameters.Clear();
                                }
                            }
                            else
                            {
                                parameters.Add("En_CallSign", callSign);
                                parameters.Add("En_L1", l1);
                                parameters.Add("En_L2", l2);
                                parameters.Add("En_L3", l3);
                                parameters.Add("En_L4", l4);

                                CloneStructureTo(callSign, newVesselCallSign, connection, parameters);
                                parameters.Clear();
                            }
                        }
                    }
                }

                var allJobDefinitionFromSampleVesselSql = "select Jd_L1 as L1,Jd_L2 as L2,Jd_L3 as L3,Jd_L4 as L4,Jd_JobCode as Code " +
                                    "from Job_Definition " +
                                    "where Jd_CallSign = @CallSign and Jd_JobType='PLM'";

                var jobDefinitionsFromSampleVessel = SqlManager.ExecuteQuery(allJobDefinitionFromSampleVesselSql, new Dictionary<string, object>()
                    {
                        {"CallSign",vesselCallSignToReplace }
                    }, connection);

                foreach (var jd in jobDefinitionsFromSampleVessel)
                {
                    parameters.Clear();
                    parameters.Add("Jd_CallSign", vesselCallSignToReplace);
                    parameters.Add("Jd_L1", jd["L1"]);
                    parameters.Add("Jd_L2", jd["L2"]);
                    parameters.Add("Jd_L3", jd["L3"]);
                    parameters.Add("Jd_L4", jd["L4"]);
                    parameters.Add("Jd_JobCode", jd["Code"]);

                    if(SqlManager.Any("select  * from Structure where En_CallSign=@CallSign and En_L1=@L1 and En_L2=@L2 and En_L3=@L3 and En_L4=@L4",new Dictionary<string, object>() 
                    {
                        {"CallSign", newVesselCallSign},
                        {"L1", jd["L1"]},
                        {"L2", jd["L2"]},
                        {"L3", jd["L3"]},
                        {"L4", jd["L4"]},
                    }))
                    {
                        var shippernetixId = $"{vesselCallSignToReplace}-{jd["L1"]}-{jd["L2"]}-{jd["L3"]}-{jd["L4"]}-{jd["Code"]}";

                        CloneJobDefinitionTo(vesselCallSignToReplace, newVesselCallSign, connection, parameters);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                throw ex;
            }
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

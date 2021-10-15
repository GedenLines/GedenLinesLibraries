using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlManagement
{
    public class SqlManager
    {
        public static bool CheckConnection(CustomConnection connection = null)
        {
            var isConnected = false;

            Console.WriteLine("Db connection testing...");

            try
            {
                connection = connection ?? new MsSqlConnection();

                connection.Connect();

                Console.WriteLine("Connected to the database successfully");

                isConnected = true;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            finally
            {
                connection.Connection.Close();

                Console.WriteLine("Test is ending...\n");
            }

            return isConnected;
        }

        public static List<Dictionary<string,object>> SelectColumnNamesForMSSQL(string tableName, CustomConnection connection = null)
        {
            var query = "SELECT COLUMN_NAME,IS_NULLABLE,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'" + tableName + "'";

            return ExecuteQuery(query,null,connection);
        }


        public static List<Dictionary<string, object>> ExecuteQuery(string sql, Dictionary<string, object> parameters = null, CustomConnection connection = null)
        {
            var list = new List<Dictionary<string, object>>();

            IDataReader dataReader = null;
            IDbCommand command = null;

            connection = connection ?? new MsSqlConnection();

            using (connection)
            {
                try
                {
                    connection.Connect();

                    command = connection.CreateCommand();

                    command.CommandTimeout = 60;

                    command.CommandText = sql;

                    if (parameters != null)
                    {
                        var pKeys = parameters.Select(p => p.Key).ToList();

                        foreach (var key in pKeys)
                        {
                            if (!sql.Contains(key))
                                parameters.Remove(key);
                        }

                        command.AddParameters(parameters);
                    }

                    dataReader = command.ExecuteReader();

                    while (dataReader.Read())
                    {
                        var dictionary = new Dictionary<string, object>();

                        for (int i = 0; i < dataReader.FieldCount; i++)
                        {
                            dictionary.Add(dataReader.GetName(i), dataReader.GetValue(i) is DBNull ? null : dataReader.GetValue(i));
                        }

                        list.Add(dictionary);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    //throw ex;
                }
                finally
                {
                    if (dataReader != null)
                        dataReader.Close();
                }
            }

            return list;

        }

        public static object ExecuteScalar(string sql, Dictionary<string, object> parameters = null, CustomConnection connection = null)
        {
            object result = null;

            connection = connection ?? new MsSqlConnection();

            IDbCommand command = null;

            using (connection)
            {
                try
                {
                    connection.Connect();

                    command = connection.CreateCommand();

                    command.CommandText = sql;

                    if (parameters != null)
                    {
                        var pKeys = parameters.Select(p => p.Key).ToList();

                        foreach (var key in pKeys)
                        {
                            if (!sql.Contains(key))
                                parameters.Remove(key);
                        }

                        command.AddParameters(parameters);
                    }

                    result = command.ExecuteScalar();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    throw ex;
                }
            }

            return result;
        }

        public static int ExecuteNonQuery(string sql, Dictionary<string, object> parameters = null, CustomConnection connection = null)
        {
            int affectedRowsCount = 0;

            connection = connection ?? new MsSqlConnection();

            IDbCommand command = null;


            using (connection)
            {
                try
                {
                    connection.Connect();

                    command = connection.CreateCommand();

                    command.CommandTimeout = 60;
                    command.CommandText = sql;

                    if (parameters != null)
                    {
                        var pKeys = parameters.Select(p => p.Key).ToList();

                        foreach (var key in pKeys)
                        {
                            if (!sql.Contains(key))
                                parameters.Remove(key);
                        }

                        command.AddParameters(parameters);
                    }

                    affectedRowsCount = command.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    //throw ex;
                }
            }


            return affectedRowsCount;
        }

        public static bool Any(string sql, Dictionary<string, object> parameters = null, CustomConnection connection = null)
        {
            sql = string.Format("select 1 from ({0}) t", sql);

            return Convert.ToInt32(ExecuteScalar(sql, parameters, connection)) == 1;
        }
    }

    public static class SqlManagerExtensions
    {
        public static void AddParameters(this IDbCommand command, Dictionary<string, object> parameters)
        {
            //if (command.Connection.ToString() == "Oracle.ManagedDataAccess.Client.OracleConnection")
            //    (command as OracleCommand).BindByName = true;

            foreach (var pair in parameters)
            {
                var parameter = command.CreateParameter();

                parameter.ParameterName = pair.Key;
                parameter.Value = pair.Value != null && !string.IsNullOrEmpty(pair.Value.ToString()) ? pair.Value : DBNull.Value;

                command.Parameters.Add(parameter);
            }
        }
    }
}

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

            if (string.IsNullOrEmpty(sql))
                return list;

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
                        command.AddParameters(parameters.Where(p => sql.Contains(p.Key)).ToDictionary(p => p.Key, p => p.Value));
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

                    connection.Disconnect();
                }
            }

            return list;

        }

        public static object ExecuteScalar(string sql, Dictionary<string, object> parameters = null, CustomConnection connection = null)
        {
            object result = null;


            if (string.IsNullOrEmpty(sql))
                return result;

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
                        command.AddParameters(parameters.Where(p => sql.Contains(p.Key)).ToDictionary(p => p.Key, p => p.Value));
                    }

                    result = command.ExecuteScalar();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }

            return result;
        }

        public static int ExecuteNonQuery(string sql, Dictionary<string, object> parameters = null, CustomConnection connection = null,bool isStoreProcedure=false)
        {
            int affectedRowsCount = 0;


            if (string.IsNullOrEmpty(sql))
                return affectedRowsCount;

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

                    if (isStoreProcedure)
                    {
                        command.CommandType = CommandType.StoredProcedure;
                    }

                    if (parameters != null)
                    {
                        if (command.CommandType == CommandType.StoredProcedure)
                            command.AddParameters(parameters);
                        else
                            command.AddParameters(parameters.Where(p => sql.Contains(p.Key)).ToDictionary(p => p.Key, p => p.Value));
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

            if (string.IsNullOrEmpty(sql))
                return false;

            sql = string.Format("select 1 from ({0}) t", sql);

            return Convert.ToInt32(ExecuteScalar(sql, parameters, connection)) == 1;
        }

        public static void EnableTrigger(string triggerName,string onTableName,CustomConnection connection=null)
        {
            var query = $"ENABLE TRIGGER {triggerName} ON {onTableName}";

            ExecuteScalar(sql: query, parameters: null, connection: connection);
        }

        public static void DisableTrigger(string triggerName, string onTableName,CustomConnection connection= null)
        {
            var query = $"DISABLE TRIGGER {triggerName} ON {onTableName}";

            ExecuteScalar(sql: query, parameters: null, connection: connection);
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

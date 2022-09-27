using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlManagement
{
    public enum ConnectionType
    {
        Oracle,
        MsSql
    }

    public interface IDbConnection
    {
        ConnectionType ConnectionType { get; set; }

        string ConnectingString { get; set; }

        DbConnection Connection { get; set; }

        IDbConnection SetConnectionString(string connectionString);

        IDbConnection SetConnection(DbConnection connection);

        IDbConnection Connect();

        IDbConnection Disconnect();
    }

    public abstract class CustomConnection : IDbConnection, IDisposable
    {
        public CustomConnection() { }

        public CustomConnection(string provider, string connectionString,ConnectionType connectionType)
        {
            if (!string.IsNullOrEmpty(connectionString))
                SetConnectionString(connectionString);

            ConnectionType = connectionType;

            if (ConnectionType == ConnectionType.Oracle)
                DbProviderFactories.RegisterFactory(provider, OracleClientFactory.Instance);
            else if ((ConnectionType == ConnectionType.MsSql))
                DbProviderFactories.RegisterFactory(provider, SqlClientFactory.Instance);

            Connection = DbProviderFactories.GetFactory(provider)
                                            .CreateConnection();

            Connection.ConnectionString = ConnectingString;

            //Connect();
        }

        public static void Start(Func<Dictionary<string,string>> connectionStringsProvider)
        {
            ConnectionStringPool = connectionStringsProvider();
        }


        public ConnectionType ConnectionType { get; set; }

        public string ConnectingString { get; set; }

        public DbConnection Connection { get; set; }

        public static Dictionary<string,string> ConnectionStringPool { get; set; }

        public IDbConnection SetConnectionString(string connectionStringName)
        {
            try
            {
                //ConnectingString = ConfigurationManager.ConnectionStrings[connectionStringName].ConnectionString;

                ConnectingString = ConnectionStringPool.FirstOrDefault(s => s.Key == connectionStringName).Value;
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return this;
        }

        public IDbConnection SetConnection(DbConnection connection)
        {
            Connection = connection;

            return this;
        }

        public IDbConnection Connect()
        {
            try
            {
                if (Connection.State != ConnectionState.Open)
                {
                    Connection.Open();
                }
            }
            catch (Exception ex)
            {

                Console.WriteLine(ex.Message);
                //throw ex;
            }

            return this;
        }

        public IDbConnection Disconnect()
        {
            try
            {
                Connection.Close();
            }
            catch (Exception ex)
            {
                //throw ex;
            }

            return this;
        }

        public IDbCommand CreateCommand()
        {
            return Connection.CreateCommand();
        }

        public void Dispose()
        {
            Disconnect();
        }
    }


    public class MsSqlConnection : CustomConnection
    {
        public MsSqlConnection(string provider = "System.Data.SqlClient", string connectionString = "MsSqlConnectionString",ConnectionType connectionType = ConnectionType.MsSql)
            : base(provider, connectionString,connectionType)
        {

        }
    }

    public class OracleConnection : CustomConnection
    {
        public OracleConnection(string provider = "Oracle.DataAccess.Client", string connectionString = "OracleConnectionString",ConnectionType connectionType = ConnectionType.Oracle)
             : base(provider, connectionString,connectionType)
        {

        }
    }
}

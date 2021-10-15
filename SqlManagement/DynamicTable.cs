using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlManagement
{
    public class DynamicTable
    {
        public string Name { get; set; }

        private bool IsUniqueColumnsSet { get; set; }

        public bool VesselSide { get; set; }

        public List<string> ExcludedColumnList;

        public List<DynamicRow> Rows = new List<DynamicRow>();

        public DynamicTable(string tableName,List<string> fields = null,List<string> excludedColumnList = null,int? rowCount = null ,CustomConnection customConnection = null,Dictionary<string,object> parameters = null)
        {
            Name = tableName;

            fields = fields == null ? new List<string>() { "*" } : fields;

            ExcludedColumnList = excludedColumnList ?? new List<string>();

            var columnInfos = SqlManager.SelectColumnNamesForMSSQL(tableName, customConnection);

            var columns = new List<DynamicColumn>();

            foreach (var columnInfo in columnInfos)
            {
                var column = new DynamicColumn(null)
                {
                    Name = columnInfo["COLUMN_NAME"].ToString(),
                    DataType = columnInfo["DATA_TYPE"].ToString(),
                    IsNullable = columnInfo["IS_NULLABLE"].ToString() == "YES"
                };

                columns.Add(column);
            }

            string whereString = null;

            if (parameters != null || parameters.Any())
                whereString = string.Format("where {0}", string.Join(" and ", parameters.Select(p => string.Format("{0}='{1}'", p.Key, p.Value))));

            var selectQuery = string.Format("select {0} {1} from {2} {3}", rowCount != null ? string.Format("top ({0})", rowCount.Value) : null, string.Join(",", fields), Name, whereString);


            var rowInfos = SqlManager.ExecuteQuery(selectQuery, parameters, customConnection);


            foreach (var rowInfo in rowInfos)
            {
                var row = new DynamicRow(this);

                foreach (var column in columns)
                {
                    var rowsColumn = new DynamicColumn(row) 
                    { 
                        Name = column.Name,
                        DataType = column.DataType,
                        IsNullable = column.IsNullable,
                        Value = rowInfo[column.Name]
                    };

                    row.AddColumn(rowsColumn);
                }

                Rows.Add(row);
            }
        }

        public DynamicTable SetUniqueColumns(params string[] columnNames)
        {
            if (!Rows.Any())
                throw new Exception("There is no row defined in table");

            foreach (var row in Rows)
            {
                foreach (var column in row.Columns)
                {
                    if (columnNames.Any(cn => cn == column.Name))
                        column.IsUnique = true;
                }
            }

            if (Rows.Any(r => r.Columns.Any(c => c.IsUnique)))
                IsUniqueColumnsSet = true;

            return this;
        }

        public DynamicTable PrepareForVesselSide(bool enable = true)
        {
            VesselSide = enable;

            return this;
        }

        public DynamicTable PrepareUpdateAndInsertQueries()
        {
            return PrepareUpdateQueries()
                .PrepareInsertQueries();
        }

        public DynamicTable PrepareUpdateQueries()
        {
            if (!IsUniqueColumnsSet)
                throw new Exception("At least one column must be set as unique for this table");

            foreach (var row in Rows)
            {
                row.PrepareUpdateQuery();
            }

            return this;
        }

        public DynamicTable PrepareInsertQueries()
        {
            foreach (var row in Rows)
            {
                row.PrepareInsertQuery();
            }

            return this;
        }

        public string GetInsertQueries
        {
            get
            {
                return string.Join(";", Rows.Select(r => r.InsertQuery));
            }
        }

        public string GetUpdateQueries
        {
            get
            {
                return string.Join(";", Rows.Select(r => r.UpdateQuery));
            }
        }
    }

    public class DynamicRow
    {
        //COLUMN_NAME,IS_NULLABLE,DATA_TYPE

        public DynamicTable Table { get; set; }

        public string UpdateQuery { get; set; }

        public string InsertQuery { get; set; }

        private List<DynamicColumn> columns = new List<DynamicColumn>();
        public List<DynamicColumn> Columns
        {
            get
            {
                return columns.Where(c => !c.Row.Table.ExcludedColumnList.Contains(c.Name)).ToList();
            }
        }

        public DynamicRow(DynamicTable table)
        {
            Table = table;
        }

        public DynamicRow AddColumn(DynamicColumn column)
        {
            columns.Add(column);

            return this;
        }

        public DynamicRow PrepareUpdateQuery()
        {
            if (!Columns.Any(c => c.IsUnique))
                throw new Exception("There is no unique column defined in this row");

            var query = "";

            var setString = string.Join(",", Columns.Select(c => string.Format("{0}={1}", c.Name, c.GetValueForDB)));

            var whereString = string.Join(" and ", Columns.Where(c => c.IsUnique).Select(c => string.Format("{0}='{1}'", c.Name, c.Value)));

            query += string.Format("update {0} set {1} where {2}", Table.Name, setString, whereString);

            UpdateQuery = query;

            return this;
        }

        public DynamicRow PrepareInsertQuery()
        {
            //if (!Columns.Any(c => c.IsUnique))
            //    throw new Exception("There is no unique column defined in this row");

            var query = "";

            var valuesString = string.Join(",", Columns.Select(c => c.GetValueForDB));

            query += string.Format("insert into {0} ({1}) values ({2})", Table.Name, string.Join(",", Columns.Select(c => c.Name)), valuesString);

            //Columns.ForEach(c => Console.WriteLine(string.Format("\nName : {0},Type : {1},Value : {2},GetValue : {3}\n", c.Name, c.DataType, c.Value, c.GetValueForDB)));

            InsertQuery = query;

            return this;

        }
    }

    public class DynamicColumn
    {
        public DynamicRow Row { get; set; }

        public string Name { get; set; }

        public string DataType { get; set; }

        public bool IsNullable { get; set; }

        public bool IsUnique { get; set; }

        public object Value { get; set; }

        public string GetValueForDB
        {
            get
            {
                var value = "";

                if (Value == null)
                    return "null";

                

                switch (DataType)
                {
                    case "bit":

                        value = (Value.ToString().ToLower() == "true" ? 1 : 0).ToString();

                        break;

                    case "int":

                        value = Value.ToString();

                        break;

                    case "datetime":
                    case "smalldatetime":

                        value = Row.Table.VesselSide ? string.Format("'{0:yyyy-MM-dd HH:mm:ss}'", Value) : string.Format("'{0:yyyy-dd-MM HH:mm:ss}'", Value);

                        break;

                    default :

                        value = string.Format("'{0}'", Value.ToString().Replace("'","''"));

                        break;
                }

                return value.ToString();
            }
        }

        public DynamicColumn(DynamicRow row)
        {
            Row = row;
        }
    }
}

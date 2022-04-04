using SqlManagement;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SparepartOnShippernetix
{
    public class Sparepart
    {
        public string S_Callsign { get; set; }

        public int S_Group { get; set; }

        public int S_Product { get; set; }

        public int S_Item { get; set; }

        public string S_Desc { get; set; }

        public string S_Unit { get; set; }

        public string S_Maker { get; set; }

        public int S_MinQty { get; set; }

        public bool S_Critical { get; set; }

        public static bool Create(CustomConnection connection,Sparepart sparepart)
        {
            var maxItemNumber = Convert.ToInt32(SqlManager.ExecuteScalar($"select max(S_Item) from Spares where S_Callsign='{sparepart.S_Callsign}' and S_Group={sparepart.S_Group} and S_Product={sparepart.S_Product}", null, connection)) + 1;

            sparepart.S_Item = maxItemNumber;

            var parameters = new Dictionary<string, object>()
            {
                {"S_Callsign", sparepart.S_Callsign},
                {"S_Group",sparepart.S_Group },
                {"S_Product",sparepart.S_Product },
                {"S_Item",sparepart.S_Item },
                {"S_Desc",sparepart.S_Desc },
                {"S_Unit",sparepart.S_Unit },
                {"S_Maker",sparepart.S_Maker },
                {"S_MinQty",sparepart.S_MinQty },
                {"S_Critical",sparepart.S_Critical }
            };

            var isCreated = SqlManager.ExecuteNonQuery("Insert Into Spares(S_Callsign,S_Category,S_Group,S_Product,S_Item,S_Desc,S_Unit,S_Maker,S_MinQty,S_Critical,Active,Send)values(@S_Callsign,'1',@S_Group,@S_Product,@S_Item,@S_Desc,@S_Unit,@S_Maker,@S_MinQty,@S_Critical,'1','0')",
                                                        parameters,
                                                        connection) > 0;

            return isCreated;
        }
    }
}

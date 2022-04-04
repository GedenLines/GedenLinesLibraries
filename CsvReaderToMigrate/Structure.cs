using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CsvReaderToMigrate
{
    public class Structure
    {
        public string En_CallSign { get; set; }
        public string En_L1 { get; set; }
        public string En_L2 { get; set; }
        public string En_L3 { get; set; }
        public string En_L4 { get; set; }

        public Structure() { }

        public Structure(string callSign,string l1,string l2,string l3,string l4) 
        {
            En_CallSign = callSign;

            En_L1 = l1;

            En_L2 = l2;

            En_L3 = l3;

            En_L4 = l4;
        }
    }
}

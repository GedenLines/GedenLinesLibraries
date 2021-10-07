using SqlManagement;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Synchronizer
{
    public class Source : Side
    {
        public Source(string name,string connectionString,bool prepareForVesselSide) : base(name,connectionString,prepareForVesselSide)
        {

        }
    }
}

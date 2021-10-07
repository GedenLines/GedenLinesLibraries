using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Synchronizer
{
    public class Target : Side
    {
        public Target(string name,string connectionString,bool prepareForVesselSide) : base(name,connectionString, prepareForVesselSide)
        {

        }
    }
}

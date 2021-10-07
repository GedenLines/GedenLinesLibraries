using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FileManagement.FileType
{
    /// <summary>
    /// Delimiter-separated values 
    /// </summary>
    public class Dsv : TextFile
    {
        public char Separator { get; private set; }

        public Dsv(string fileName, string extension = ".csv", char separator = ',')
            : base(fileName, extension)
        {
            SetSeparator(separator);
        }

        public Dsv(string path, char separator = ',')
            : base(path)
        {
            SetSeparator(separator);
        }

        public List<string> Split(int columnCount, bool withHeader = true)
        {
            //var list = new List<string>();

            var lines = ReadLineByStreamReader();

            for (int i = 0; i < lines.Count(); i++)
            {
                lines[i] = lines[i] + Separator;
            }

            if (!withHeader)
                lines.RemoveAt(0);

            //var unifiedString = string.Join(Separator.ToString(), lines);

            //var array = unifiedString.Split(new[] { Separator }, StringSplitOptions.None);


            //for (int page = 0; page < (array.Length / columnCount); page++)
            //{
            //    list.Add(string.Join(Separator.ToString(), array.Skip(page * columnCount).Take(columnCount)));
            //}

            //return list;

            return lines;
        }

        public Dsv SetSeparator(char separator)
        {
            Separator = separator;

            return this;
        }
    }
}

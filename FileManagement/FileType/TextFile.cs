using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FileManagement.FileType
{
    public class TextFile : CustomFile
    {
        public TextFile(string fileName, string extension = ".txt")
            : base(fileName, extension)
        {

        }

        public TextFile(string path, bool createIfNotExists) : base(path, createIfNotExists)
        {

        }

        public List<string> ReadLineByStreamReader(Stream stream = null)
        {
            stream = stream ?? CreateMemoryStream();

            var list = new List<string>();

            using (stream)
            {
                using (var reader = new StreamReader(stream))
                {
                    string line;

                    while ((line = reader.ReadLine()) != null)
                    {
                        list.Add(line);
                    }
                }
            }

            return list;
        }

        public void WriteLineByStreamWriter(List<string> lines)
        {
            using (var writer = new StreamWriter(PathToWrite))
            {
                foreach (var line in lines)
                {
                    writer.WriteLine(line);
                }
            }
        }

        public void WriteLineByStreamWriter(List<string> lines, Stream stream = null)
        {
            stream = stream ?? CreateMemoryStream();

            using (stream)
            {
                using (var writer = new StreamWriter(stream))
                {
                    foreach (var line in lines)
                    {
                        writer.WriteLine(line);
                    }
                }
            }
        }

        public void AppendAllText(string path, string contents)
        {
            AppendAllText(path, contents, LogAction);
        }

        public static void AppendAllText(string path, string contents, Action<Exception> logAction)
        {
            try
            {
                if (ReadAllLines(path, logAction).Count() > 0)
                    contents = "\n" + contents;
                    
                System.IO.File.AppendAllText(path, contents);
            }
            catch (Exception ex)
            {
                if (logAction != null)
                    logAction(ex);
            }
        }

        public void AppendAllLines(string path, IEnumerable<string> contents)
        {
            AppendAllLines(path, contents, LogAction);
        }

        public static void AppendAllLines(string path, IEnumerable<string> contents, Action<Exception> logAction)
        {
            try
            {
                if (ReadAllLines(path, logAction).Count() > 0)
                {
                    var newList = new List<string>() { "\n" };

                    newList.AddRange(contents);

                    contents = newList;
                }

                System.IO.File.AppendAllLines(path, contents);
            }
            catch (Exception ex)
            {
                if (logAction != null)
                    logAction(ex);
            }
        }



        public static string[] ReadAllLines(string path, Action<Exception> logAction)
        {
            string[] lines = null;

            try
            {
                lines = System.IO.File.ReadAllLines(path);
            }
            catch (Exception ex)
            {
                if (logAction != null)
                    logAction(ex);
            }

            return lines;
        }
    }
}

using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FileManagement
{
    public class CustomFile
    {
        #region Properties

        public Guid EntityId { get; set; }

        Guid? id { get; set; }
        public Guid Id
        {
            get
            {
                if (id == null)
                    id = Guid.NewGuid();

                return id.Value;
            }
            set
            {
                id = value;
            }
        }

        public string Name { get; private set; }

        public string NameWithExtension
        {
            get
            {
                return string.Format("{0}{1}", Name, Extension);
            }
        }

        public byte[] Data { get; private set; }

        public string DataAsString
        {
            get
            {
                return ByteArrayToString(Data);
            }
        }

        public string Extension { get; private set; }

        public string PathToRead { get; private set; }

        public string PathToWrite { get; private set; }

        public FileInfo FileInfo { get; set; }

        Compressor Compressor { get; set; }

        public IsolatedStorage IsolatedStorage { get; private set; }

        //public Serializer Serializer { get; set; }

        //public void Serialize()
        //{
        //    if (Serializer == null)
        //        SetSerializer();

        //    Serializer.Serialize();
        //}

        //public void Serialize(object value)
        //{
        //    if (Serializer == null)
        //        SetSerializer();

        //    Serializer.JsonString = Serializer.Serialize(value);
        //}

        public Action<Exception> LogAction { get; set; }

        public string Base64String
        {
            get
            {
                return Convert.ToBase64String(Data);
            }
        }

        public double LengthAsGb
        {
            get
            {
                return Convert.ToDouble(string.Format("{0:0.0000}", (double)LengthAsMb / (double)1024));
            }
        }

        public double LengthAsMb
        {
            get
            {
                return Convert.ToDouble(String.Format("{0:0.0000}", (double)LengthAsKb / (double)1024));
            }
        }

        public double LengthAsKb
        {
            get
            {
                return Convert.ToDouble(String.Format("{0:0.0000}", (double)LengthAsByte / (double)1024));
            }
        }

        public double LengthAsByte
        {
            get
            {
                return Convert.ToDouble(String.Format("{0:0.0000}", (double)FileInfo.Length));
            }
        }

        public static string CurrentUserDesktopPath
        {
            get
            {
                return Environment.GetFolderPath(Environment.SpecialFolder.Desktop);
            }
        }

        public static string CurrentProjectBinPath
        {
            get
            {
                return Environment.CurrentDirectory;
            }
        }

        #endregion

        #region Constructor

        public CustomFile() 
        {
        }

        public CustomFile(string path, bool createNewIfNotExists = true)
        {
            if (createNewIfNotExists)
                CreateNewIfNotExists(path);

            SetDataFromPath(path);
        }

        public CustomFile(string fileName, string extension)
        {
            SetName(fileName);

            SetExtension(extension);
        }

        #endregion

        #region Functions

        //public BinaryReader CreateBinaryReader()
        //{
        //    return new BinaryReader(System.IO.File.Open(PathToRead, FileMode.Open));
        //}

        //public BinaryWriter CreateBinaryWriter()
        //{
        //    return new BinaryWriter(System.IO.File.Open(PathToWrite, FileMode.Create));
        //}

        public Stream GetIsolatedStorageFileStream()
        {
            SetIsolatedStorage();

            return IsolatedStorage.StorageFileStream;
        }

        public MemoryStream CreateMemoryStream()
        {
            return new MemoryStream(Data);
        }

        public bool CreateNewIfNotExists(string path)
        {
            try
            {
                if (!System.IO.File.Exists(path))
                {
                    using (FileStream fs = File.Create(path))
                    {
                    }
                }
            }
            catch (Exception ex)
            {
                LogAction?.Invoke(ex);

                return false;
            }

            return true;
        }

        public byte[] ReadAllBytes(string path)
        {
            return ReadAllBytes(PathToRead, LogAction);
        }

        public CustomFile SetDataFromPath(string path)
        {
            try
            {
                //if (!Exists(path))
                //{

                //    System.IO.File.Create(path);

                //    //throw new Exception(string.Format("The file : '{0}' doesnt exists", path));
                //}

                SetPathToRead(path);
                SetPathToWrite(path);

                Data = ReadAllBytes(path);

                SetExtension(System.IO.Path.GetExtension(path));

                SetName(System.IO.Path.GetFileNameWithoutExtension(path));

                FileInfo = new FileInfo(path);
            }
            catch (Exception ex)
            {
                LogAction?.Invoke(ex);
            }

            return this;
        }

        public CustomFile SetData(byte[] data)
        {
            Data = data;

            return this;
        }

        public CustomFile SetData(string text)
        {
            Data = StringToByteArray(text);

            return this;
        }

        public CustomFile SetCompressor()
        {
            Compressor = new Compressor(this);

            return this;
        }

        public CustomFile SetExtension(string extension)
        {
            Extension = extension;

            return this;
        }

        private CustomFile SetIsolatedStorage()
        {
            if (IsolatedStorage != null)
                IsolatedStorage.Dispose();

            IsolatedStorage = new IsolatedStorage(this);

            return this;
        }

        public CustomFile SetLogAction(Action<Exception> logAction)
        {
            LogAction = logAction;

            return this;
        }

        public CustomFile SetName(string name)
        {
            Name = name;

            return this;
        }

        public CustomFile SetPathToRead(string path)
        {
            PathToRead = path;

            return this;
        }

        public CustomFile SetPathToWrite(string path)
        {
            PathToWrite = path;

            return this;
        }

        //public Serializer SetSerializer()
        //{
        //    Serializer = new Serializer(this);

        //    return Serializer;
        //}

        public bool SaveAs(string path, string fileName, string extension, byte[] data = null)
        {
            path = CombinePaths(path, fileName + extension);

            return WriteAllBytes(path, data);
        }

        public bool SaveAs(string path, string fileName, string extension, string text, Encoding encoding = null)
        {
            encoding = encoding ?? Encoding.UTF8;

            return SaveAs(path, fileName, extension, StringToByteArray(text, encoding));
        }

        public bool SaveAs(string path)
        {
            return SaveAs(path, Name, Extension, Data);
        }

        public bool WriteAllBytes(string path = null,byte[] data = null)
        {
            //string.Format("{0}-Compressed.gz", DateTime.Now.ToString("[yyyy-MM-dd HH-mm-ss-fff]")

            path = path ?? PathToWrite;

            data = data ?? Data;

            return WriteAllBytes(path, data, LogAction);
        }
        #endregion

        #region Static Functions

        public static string ByteArrayToString(byte[] data, Encoding encoding = null)
        {
            var result = string.Empty;
            try 
	        {	        
	        	encoding = encoding ?? System.Text.Encoding.UTF8;
    
                result = encoding.GetString(data);
	        }
	        catch (Exception ex)
	        {
		        Console.WriteLine(ex.ToString());
	        }

            return result;
        }

        public static byte[] StringToByteArray(string value, Encoding encoding = null)
        {
            encoding = encoding ?? System.Text.Encoding.UTF8;

            return encoding.GetBytes(value);
        }

        public static string CombinePaths(params string[] paths)
        {
            var combinedPath = System.IO.Path.Combine(paths);

            return combinedPath;
        }

        public static bool CreateIfNotExists(string path,Action<Exception> logAction = null)
        {
            try
            {
                if (!System.IO.File.Exists(path))
                {
                    System.IO.File.Create(path);
                }
            }
            catch (Exception ex)
            {
                logAction?.Invoke(ex);

                return false;
            }

            return true;
        }

        public static bool CreateDirectoryIfNotExists(string path, Action<Exception> logAction = null)
        {
            try
            {
                if (!Directory.Exists(path))
                {
                    Directory.CreateDirectory(path);
                }
            }
            catch (Exception ex)
            {
                logAction?.Invoke(ex);

                return false;
            }

            return true;
        }

        public static List<string> DirectoryGetFiles(string path)
        {
            var list = new List<string>();

            foreach (string file in System.IO.Directory.GetFiles(path))
            {
                list.Add(file);
            }

            return list;
        }

        public static bool Exists(string path)
        {
            return System.IO.File.Exists(path);
        }

        public static string GetFilePathFromCurrentProjectBin(string fileName)
        {
            return CombinePaths(CurrentProjectBinPath, fileName);
        }

        public static string GetFilePathFromCurrentUserDesktop(string fileName)
        {
            return CombinePaths(CurrentUserDesktopPath, fileName);
        }

        public static bool HasInvalidPathChar(string path)
        {
            return System.IO.Path.GetInvalidPathChars().ToList().Any(c => path.Contains(c));
        }

        public static byte[] ReadAllBytes(string path, Action<Exception> logAction)
        {
            byte[] array = null;

            try
            {
                array = System.IO.File.ReadAllBytes(path);
            }
            catch (Exception ex)
            {
                logAction?.Invoke(ex);
            }

            return array;
        }

        public static bool WriteAllBytes(string path, byte[] data, Action<Exception> logAction)
        {
            try
            {
                System.IO.File.WriteAllBytes(path, data);
            }
            catch (Exception ex)
            {
                logAction?.Invoke(ex);

                return false;
            }

            return true;
        }

        public static string SerializeAsJsonFormat(object value)
        {
            return JsonConvert.SerializeObject(value);
        }

        public static object DeserializeAsJsonFormat(string jsonString)
        {
            return JsonConvert.DeserializeObject(jsonString);
        }

        public static T DeserializeAsJsonFormat<T>(string jsonString)
        {
            return JsonConvert.DeserializeObject<T>(jsonString);
        }

        #endregion
    }
}

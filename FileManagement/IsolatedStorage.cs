using System;
using System.Collections.Generic;
using System.IO;
using System.IO.IsolatedStorage;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace FileManagement
{
    public class IsolatedStorage : IDisposable
    {
        public CustomFile File { get; private set; }

        IsolatedStorageFile StorageFile { get; set; }

        public IsolatedStorageFileStream StorageFileStream { get; set; }

        public string IsolatedStoragePath
        {
            get
            {
                return StorageFileStream.GetType()
                                        .GetField("m_FullPath", BindingFlags.Instance | BindingFlags.NonPublic)
                                        .GetValue(StorageFileStream)
                                        .ToString();
            }
        }


        public IsolatedStorage(CustomFile file)
        {
            SetFile(file);

            try
            {
                StorageFile = IsolatedStorageFile.GetUserStoreForDomain();

                StorageFileStream = new IsolatedStorageFileStream(file.NameWithExtension, FileMode.OpenOrCreate, StorageFile);
            }
            catch (Exception ex)
            {
                if (File.LogAction != null)
                    File.LogAction(ex);
            }
        }


        ~IsolatedStorage()
        {
            Dispose();
        }

        public void SetFile(CustomFile file)
        {
            File = file;
        }

        public void Dispose()
        {
            if (StorageFile != null)
                StorageFile.Close();

            if (StorageFileStream != null)
                StorageFileStream.Close();
        }
    }
}

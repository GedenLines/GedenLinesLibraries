﻿namespace FileManagement.FileType
{
    public class JsonFile : CustomFile
    {
        #region Properties
        #endregion

        #region Constructor
        public JsonFile(string fileName, string extension = ".json")
    : base(fileName, extension)
        {

        }

        public JsonFile(string path, bool createNewIfNotExists)
            : base(path, createNewIfNotExists)
        {

        }
        #endregion

        #region Functions
        //public static string WriteFromObject()
        //{
        //    // Create User object.
        //    var user = new User("Bob", 42);

        //    // Create a stream to serialize the object to.
        //    var ms = new MemoryStream();

        //    // Serializer the User object to the stream.
        //    var ser = new DataContractJsonSerializer(typeof(User));
        //    ser.WriteObject(ms, user);
        //    byte[] json = ms.ToArray();
        //    ms.Close();
        //    return Encoding.UTF8.GetString(json, 0, json.Length);
        //}

        //// Deserialize a JSON stream to a User object.
        //public static User ReadToObject(string json)
        //{
        //    var deserializedUser = new User();
        //    var ms = new MemoryStream(Encoding.UTF8.GetBytes(json));
        //    var ser = new DataContractJsonSerializer(deserializedUser.GetType());
        //    deserializedUser = ser.ReadObject(ms) as User;
        //    ms.Close();
        //    return deserializedUser;BinaryFormatter 
        //}


        #endregion
    }
}

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace ServiceBusTest
{
    public static class CryptoHelper
    {
        public static string GetMD5Hash(object instance)
        {
            return GetHash<MD5CryptoServiceProvider>(instance);
        }

        public static string GETSHA512Hash(object instance)
        {
            return GetHash<SHA512CryptoServiceProvider>(instance);
        }

        private static string GetHash<T>(object instance) where T : HashAlgorithm, new()
        {
            T cryptoServiceProvider = new T();
            return ComputeHash(instance, cryptoServiceProvider);
        }

        private static string ComputeHash<T>(object instance, T cryptoServiceProvider) where T : HashAlgorithm, new()
        {
            DataContractSerializer serializer = new DataContractSerializer(instance.GetType());

            using (MemoryStream memoryStream = new MemoryStream())
            {
                serializer.WriteObject(memoryStream, instance);
                cryptoServiceProvider.ComputeHash(memoryStream.ToArray());
                return Convert.ToBase64String(cryptoServiceProvider.Hash);
            }
        }
    }
}

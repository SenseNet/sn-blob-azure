using System;
using System.Collections.Generic;
using System.Linq;

namespace SenseNet.AzureBlobStorage.Tests
{
    public static class TestData
    {
        public static IEnumerable<object[]> BlobData => new List<object[]>
        {
            new object[] { "4f342e08-2abc-4d6e-8c9c-5fc68be35202", 10000 }
        };

        public static IEnumerable<object[]> RandomBlobDataItem
        {
            get
            {
                var index = new Random(0).Next(0, BlobData.Count() - 1);
                return BlobData.Skip(index).Take(1);
            }
        } 
    }
}
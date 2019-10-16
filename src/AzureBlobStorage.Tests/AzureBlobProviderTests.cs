using System;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SenseNet.ContentRepository.Storage.Data;
using Xunit;

namespace SenseNet.BlobStorage.Azure.Tests
{
    public class AzureBlobProviderTests
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["SenseNet.AzureBlobStorage"].ConnectionString;
        private static int _fileId;

        [Fact]
        public async Task WriteTest()
        {
            const int chunkSize = 4096;
            const int contentSize = 64 * 1024;

            PrepareBlob(GetNextFileId(), contentSize, chunkSize,
                out var provider,
                out var context,
                out var blobId,
                out var content);

            Assert.False(provider.BlobExists(blobId));

            var offset = 0;
            while(offset < content.Length)
            {
                var chunk = content.Skip(offset).Take(chunkSize).ToArray();
                await provider.WriteAsync(context, offset, chunk, CancellationToken.None);
                offset += chunk.Length;
            }

            TestNewBlobAndCleanup(provider, blobId, context, contentSize);
        }

        [Fact]
        public async Task WriteAsyncTest()
        {
            const int chunkSize = 4096;
            const int contentSize = 64 * 1024;

            PrepareBlob(GetNextFileId(), contentSize, chunkSize,
                out var provider,
                out var context,
                out var blobId,
                out var content);

            Assert.False(provider.BlobExists(blobId));

            var offset = 0;
            while (offset < content.Length)
            {
                var chunk = content.Skip(offset).Take(chunkSize).ToArray();

                // write chunk asynchronously
                await provider.WriteAsync(context, offset, chunk, CancellationToken.None);
                offset += chunk.Length;
            }

            TestNewBlobAndCleanup(provider, blobId, context, contentSize);
        }

        [Fact]
        public async Task WriteChunkTest_Error_NotWritten()
        {
            const int uploadChunkSize = 500;
            const int wrongBinaryChunkSize = 300;
            const int contentSize = 700;

            PrepareBlob(GetNextFileId(), contentSize, wrongBinaryChunkSize,
                out var provider,
                out var context,
                out var blobId,
                out var content);

            Assert.False(provider.BlobExists(blobId));

            var offset = 0;

            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                while (offset < content.Length)
                {
                    var chunk = content.Skip(offset).Take(uploadChunkSize).ToArray();
                    await provider.WriteAsync(context, offset, chunk, CancellationToken.None);
                    offset += chunk.Length;
                }
            });

            // blob does not exist because of the incorrect chunk size
            Assert.False(provider.BlobExists(blobId));

            Cleanup(provider, blobId);
        }

        [Fact]
        public async Task WriteChunkTest_Error_WrongBlockList()
        {
            const int uploadChunkSize = 400;
            const int wrongBinaryChunkSize = 200;
            const int contentSize = 1000;

            PrepareBlob(GetNextFileId(), contentSize, wrongBinaryChunkSize,
                out var provider,
                out var context,
                out var blobId,
                out var content);

            Assert.False(provider.BlobExists(blobId));

            var offset = 0;

            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                while (offset < content.Length)
                {
                    var chunk = content.Skip(offset).Take(uploadChunkSize).ToArray();
                    await provider.WriteAsync(context, offset, chunk, CancellationToken.None);
                    offset += chunk.Length;
                }
            });

            Cleanup(provider, blobId);
        }

        [Fact]
        public void Write1MTest()
        {
            const int contentSize = 64 * 1024;
            const int chunkSize = 256 * 1024;

            WriteNewBlob(GetNextFileId(), contentSize, chunkSize, 
                out var provider, 
                out var context, 
                out var blobId,
                out var _);
            
            TestNewBlobAndCleanup(provider, blobId, context, contentSize);
        }

        [Fact]
        public void Write10MTest()
        {
            const int contentSize = 10 * 64 * 1024;
            const int chunkSize = 256 * 1024;

            WriteNewBlob(GetNextFileId(), contentSize, chunkSize,
                out var provider,
                out var context,
                out var blobId,
                out var _);

            TestNewBlobAndCleanup(provider, blobId, context, contentSize);
        }

        [Fact]
        public void Write100MTest()
        {
            const int contentSize = 100 * 64 * 1024;
            const int chunkSize = 1024 * 1024;

            WriteNewBlob(GetNextFileId(), contentSize, chunkSize,
                out var provider,
                out var context,
                out var blobId,
                out var _);

            TestNewBlobAndCleanup(provider, blobId, context, contentSize);
        }

        [Fact]
        public void GetStreamForWriteTest()
        {
            const int contentSize = 64 * 1024;
            const int chunkSize = 1024 * 1024;

            WriteNewBlob(GetNextFileId(), contentSize, chunkSize,
                out var provider,
                out var context,
                out var blobId,
                out var content);

            using (var stream = provider.GetStreamForWrite(context))
            {
                stream.Write(content, 0, content.Length);
            }

            Assert.True(provider.BlobExists(blobId));

            Cleanup(provider, blobId);
        }

        [Theory]
        [MemberData(nameof(TestData.RandomBlobDataItem), MemberType = typeof(TestData))]
        public void GetStreamForReadTest(string blobId, long size)
        {
            const int chunkSize = 1024 * 1024;

            WriteNewBlob(GetNextFileId(), (int)size, chunkSize,
                out var provider,
                out var context,
                out var _,
                out var content, blobId);

            using (var stream = provider.GetStreamForRead(context))
            {
                var read = stream.Read(content, 0, content.Length);
                Assert.Equal(size, read);
            }

            Cleanup(provider, blobId);
        }

        [Theory]
        [MemberData(nameof(TestData.RandomBlobDataItem), MemberType = typeof(TestData))]
        public void CloneStreamForReadTest(string blobId, long size)
        {
            const int chunkSize = 1024 * 1024;

            WriteNewBlob(GetNextFileId(), (int)size, chunkSize,
                out var provider,
                out var context,
                out var _,
                out var _, blobId);

            using (var stream = provider.GetStreamForRead(context))
            {
                using (var cloneStream = provider.CloneStream(context, stream))
                {
                    Assert.False(cloneStream == stream);
                    Assert.True(cloneStream.CanRead);
                    Assert.False(cloneStream.CanWrite);
                    Assert.Equal(stream.Length, cloneStream.Length);
                    Assert.Equal(size, stream.Length);
                }
            }
        }

        [Theory]
        [MemberData(nameof(TestData.RandomBlobDataItem), MemberType = typeof(TestData))]
        public void CloneStreamForWriteTest(string blobId, long size)
        {
            const int chunkSize = 1024 * 1024;

            WriteNewBlob(GetNextFileId(), (int)size, chunkSize,
                out var provider,
                out var context,
                out var _,
                out var _, blobId);

            using (var stream = provider.GetStreamForWrite(context))
            {
                using (var cloneStream = provider.CloneStream(context, stream))
                {
                    Assert.False(cloneStream == stream);
                    Assert.False(cloneStream.CanRead);
                    Assert.True(cloneStream.CanWrite);
                }
            }
        }

        [Theory]
        [MemberData(nameof(TestData.BlobData), MemberType = typeof(TestData))]
        public async Task DeleteTest(string blobId, long size)
        {
            const int chunkSize = 1024 * 1024;

            WriteNewBlob(GetNextFileId(), (int)size, chunkSize,
                out var provider,
                out var context,
                out var _,
                out var _, blobId);

            // the blob provider generated a new blob id
            blobId = ((AzureBlobProviderData) context.BlobProviderData).BlobId;

            Assert.True(provider.BlobExists(blobId));

            await provider.DeleteAsync(context, CancellationToken.None);

            Assert.False(provider.BlobExists(blobId));

            // Delete all blobs

            //foreach (var id in provider.GetBlobIds())
            //{
            //    SnTrace.Database.Write($"Deleting blob {id}.");

            //    provider.Delete(new BlobStorageContext(provider)
            //    {
            //        BlobProviderData = new AzureBlobProviderData
            //        {
            //            BlobId = id
            //        }
            //    });
            //}
        }

        [Fact]
        public void ParseDataTest()
        {
            IBlobProvider iProvider = new AzureBlobProvider(ConnectionString);
            var providerData = iProvider.ParseData(
                "{\"BlobId\":\"314a97b1-aaf8-4325-b677-1d25b8353935\",\"ChunkSize\":262144}") as AzureBlobProviderData;

            Assert.NotNull(providerData);
            Assert.Equal("314a97b1-aaf8-4325-b677-1d25b8353935", providerData.BlobId);
            Assert.Equal(262144, providerData.ChunkSize);
        }

        private static void Cleanup(AzureBlobProvider provider, string blobId)
        {
            if (!(provider?.BlobExists(blobId) ?? false))
                return;

            var context = new BlobStorageContext(provider)
            {
                BlobProviderData = new AzureBlobProviderData { BlobId = blobId }
            };
            
            provider.DeleteAsync(context, CancellationToken.None).GetAwaiter().GetResult();
        }

        private static void PrepareBlob(int fileId, int contentSize, int chunkSize, 
            out AzureBlobProvider provider,
            out BlobStorageContext context,
            out string blobId,
            out byte[] content,
            string testBlobId = null)
        {
            content = Encoding.ASCII.GetBytes(Enumerable.Repeat(0, contentSize)
                .SelectMany(n => "A").ToArray());
            //.SelectMany(n => "0123456789ABCDEF").ToArray());

            provider = new AzureBlobProvider(ConnectionString, chunkSize);
            context = new BlobStorageContext(provider)
            {
                FileId = fileId,
                Length = content.Length,
                PropertyTypeId = 0,
                BlobProviderData = new AzureBlobProviderData
                {
                    BlobId = string.IsNullOrEmpty(testBlobId) ? Guid.NewGuid().ToString() : testBlobId
                }
            };

            provider.AllocateAsync(context, CancellationToken.None).GetAwaiter().GetResult();

            blobId = ((AzureBlobProviderData)context.BlobProviderData).BlobId;

            Cleanup(provider, blobId);
        }

        private static void WriteNewBlob(int fileId, int contentSize, int chunkSize,
            out AzureBlobProvider provider,
            out BlobStorageContext context,
            out string blobId,
            out byte[] content,
            string testBlobId = null)
        {
            PrepareBlob(fileId, contentSize, chunkSize,
                out provider,
                out context,
                out blobId,
                out content, testBlobId);

            var offset = 0;
            while (offset < content.Length)
            {
                var chunk = content.Skip(offset).Take(chunkSize).ToArray();
                provider.WriteAsync(context, offset, chunk, CancellationToken.None).GetAwaiter().GetResult();
                offset += chunk.Length;
            }
        }

        // ReSharper disable once ParameterOnlyUsedForPreconditionCheck.Local
        private static void TestNewBlobAndCleanup(AzureBlobProvider provider, string blobId, 
            BlobStorageContext context, int expectedSize)
        {
            Assert.True(provider.BlobExists(blobId));

            using (var stream = provider.GetStreamForRead(context))
            {
                Assert.Equal(expectedSize, stream.Length);
            }

            Cleanup(provider, blobId);
        }

        private static int GetNextFileId()
        {
            Interlocked.Increment(ref _fileId);

            return _fileId;
        }
    }
}
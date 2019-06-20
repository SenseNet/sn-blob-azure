using SenseNet.ContentRepository.Storage;
using SenseNet.Tools;

namespace SenseNet.BlobStorage.Azure
{
    public static class AzureBlobStorageExtensions
    {
        public static IRepositoryBuilder UseAzureBlobStorage(this IRepositoryBuilder builder,
            string connectionString, int chunkSize)
        {
            return builder.UseExternalBlobProvider(new AzureBlobProvider(connectionString, chunkSize));
        }
    }
}

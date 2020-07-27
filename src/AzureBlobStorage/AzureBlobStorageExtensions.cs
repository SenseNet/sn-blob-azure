using SenseNet.BlobStorage.Azure;
using SenseNet.ContentRepository.Storage;
using SenseNet.Tools;

// ReSharper disable once CheckNamespace
namespace SenseNet.Extensions.DependencyInjection
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

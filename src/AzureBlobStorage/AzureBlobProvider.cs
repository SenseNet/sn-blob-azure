﻿using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using SenseNet.ContentRepository.Storage.Data;
using SenseNet.Diagnostics;

namespace SenseNet.AzureBlobStorage
{
    public class AzureBlobProvider: IBlobProvider, IMultiTenantProvider
    {
        private const string ContainerNamePrefix = "snc";
        private const string FileId = "fileId";
        private const string VersionId = "versionId";
        private const string PropertyTypeId = "propertyTypeId";
        private const int DefaultChunkSizeInBytes = 256 * 1024;

        private readonly CloudBlobContainer _container;
        private string _tenantId = string.Empty;

        private string ContainerName => ContainerNamePrefix + _tenantId;
        public int ChunkSize { get; }
        
        private static readonly BlobRequestOptions Options = new BlobRequestOptions
        {
            RetryPolicy = new LinearRetry(TimeSpan.FromMilliseconds(1000), 3), UseTransactionalMD5 = true
        };

        public AzureBlobProvider(string connectionString, int chunkSize = DefaultChunkSizeInBytes)
        {
            ChunkSize = chunkSize;

            var storageAccount = CloudStorageAccount.Parse(connectionString);
            var blobClient = storageAccount.CreateCloudBlobClient();
            _container = blobClient.GetContainerReference(ContainerName);
            _container.CreateIfNotExists();
        }

        private static string NewBlobId()
        {
            return Guid.NewGuid().ToString();
        }
        private CloudBlockBlob GetBlob(string blobId = null)
        {
            if (blobId == null)
                blobId = NewBlobId();
            
            return _container.GetBlockBlobReference(blobId);
        }

        private static void SetBlobMetadata(CloudBlockBlob blob, BlobStorageContext context, bool commitToAzure = true)
        {
            blob.Metadata[FileId] = context.FileId.ToString();
            blob.Metadata[VersionId] = context.VersionId.ToString();
            blob.Metadata[PropertyTypeId] = context.PropertyTypeId.ToString();

            if (commitToAzure)
                blob.SetMetadata(options: Options);
        }

        public bool BlobExists(string blobId)
        {
            var blob = GetBlob(blobId);
            return blob.Exists(Options);
        }

        public IEnumerable<string> GetBlobIds()
        {
            var list = _container.ListBlobs();
            return list.Select(i => i.Uri.Segments[i.Uri.Segments.GetUpperBound(0)]);
        }
        
        #region IBlobProvider

        public void Allocate(BlobStorageContext context)
        {
            SnTrace.Database.Write("AzureBlobProvider.Allocate: {0}", context.BlobProviderData);

            var blobId = ((AzureBlobProviderData)context.BlobProviderData)?.BlobId;

            context.BlobProviderData = new AzureBlobProviderData
            {
                BlobId = blobId ?? NewBlobId(),
                ChunkSize = ChunkSize
            };
        }

        public Stream CloneStream(BlobStorageContext context, Stream stream)
        {
            return stream is CloudBlobStream ? GetStreamForWrite(context) : GetStreamForRead(context);
        }

        public void Delete(BlobStorageContext context)
        {
            using (var op = SnTrace.Database.StartOperation("AzureBlobProvider.Delete: {0}", context.BlobProviderData))
            {
                var providerData = (AzureBlobProviderData)context.BlobProviderData;
                var blob = GetBlob(providerData.BlobId);
                blob.Delete();
                op.Successful = true;
            }
        }

        public Stream GetStreamForRead(BlobStorageContext context)
        {
            SnTrace.Database.Write("AzureBlobProvider.GetStreamForRead: {0}", context.BlobProviderData);
            var providerData = (AzureBlobProviderData)context.BlobProviderData;
            var blob = GetBlob(providerData.BlobId);
            return blob.OpenRead(options: Options);
        }

        public Stream GetStreamForWrite(BlobStorageContext context)
        {
            SnTrace.Database.Write("AzureBlobProvider.GetStreamForWrite: {0}", context.BlobProviderData);
            var providerData = (AzureBlobProviderData)context.BlobProviderData;
            var blob = GetBlob(providerData.BlobId);
            var stream = blob.OpenWrite(options: Options);
            SetBlobMetadata(blob, context, false);
            return stream;
        }

        public object ParseData(string providerData)
        {
            SnTrace.Database.Write("AzureBlobProvider.ParseData: {0}", providerData);
            return BlobStorageContext.DeserializeBlobProviderData<AzureBlobProviderData>(providerData);
        }

        public void Write(BlobStorageContext context, long offset, byte[] buffer)
        {
            var providerData = (AzureBlobProviderData)context.BlobProviderData;

            SnTrace.Database.Write("AzureBlobProvider.Write: {0}", providerData);

            var blob = GetBlob(providerData.BlobId);
            var chunkSize = providerData.ChunkSize;
            var id = (int) (offset / chunkSize) + 1;
            var blockId =
                Convert.ToBase64String(
                    Encoding.UTF8.GetBytes(string.Format(CultureInfo.InvariantCulture, "{0:D6}", id)));

            var blockCount = (int)Math.Ceiling((decimal)context.Length / chunkSize);
            using (var chunkStream = new MemoryStream(buffer))
            {
                blob.PutBlock(blockId, chunkStream, null, null, Options);
            }

            if (id != blockCount)
                return;

            var blockList = Enumerable.Range(1, blockCount).ToList().ConvertAll(block =>
                Convert.ToBase64String(
                    Encoding.UTF8.GetBytes(string.Format(CultureInfo.InvariantCulture, "{0:D6}", block))));

            blob.PutBlockList(blockList, null, Options);

            SetBlobMetadata(blob, context);
        }

        public async Task WriteAsync(BlobStorageContext context, long offset, byte[] buffer)
        {
            var providerData = (AzureBlobProviderData)context.BlobProviderData;

            SnTrace.Database.Write("AzureBlobProvider.Write: {0}", providerData);

            var blob = GetBlob(providerData.BlobId);
            var chunkSize = providerData.ChunkSize;
            var id = (int)(offset / chunkSize) + 1;
            var blockId =
                Convert.ToBase64String(
                    Encoding.UTF8.GetBytes(string.Format(CultureInfo.InvariantCulture, "{0:D6}", id)));

            var blockCount = (int)Math.Ceiling((decimal)context.Length / chunkSize);
            using (var chunkStream = new MemoryStream(buffer))
            {
                await blob.PutBlockAsync(blockId, chunkStream, null, null, Options, null);
            }

            if (id != blockCount)
                return;

            var blockList = Enumerable.Range(1, blockCount).ToList().ConvertAll(block =>
                Convert.ToBase64String(
                    Encoding.UTF8.GetBytes(string.Format(CultureInfo.InvariantCulture, "{0:D6}", block))));

            await blob.PutBlockListAsync(blockList, null, Options, null);

            SetBlobMetadata(blob, context);
        }

        #endregion

        #region IMultiTenantProvider

        public void SetTenantId(string uniqueTenantId)
        {
            _tenantId = uniqueTenantId;
            NameValidator.ValidateContainerName(ContainerName);
        }

        #endregion
    }
}
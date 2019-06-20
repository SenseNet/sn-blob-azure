namespace SenseNet.BlobStorage.Azure
{
    public interface IMultiTenantProvider
    {
        void SetTenantId(string uniqueTenantId);
    }
}
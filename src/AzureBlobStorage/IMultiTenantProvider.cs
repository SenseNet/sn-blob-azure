namespace SenseNet.AzureBlobStorage
{
    public interface IMultiTenantProvider
    {
        void SetTenantId(string uniqueTenantId);
    }
}
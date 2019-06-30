using Microsoft.WindowsAzure.Storage;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ThoughtHaven.Azure.Storage.Tests.Fakes
{
    public class FakeResourceExistenceTesterBase : ResourceExistenceTesterBase<object>
    {
        public Uri TestStorageUri { get; }
        new public ICollection<Uri> KnownToExist => base.KnownToExist;

        public FakeResourceExistenceTesterBase(Uri testStorageUri)
        {
            this.TestStorageUri = testStorageUri;
        }

        public object? CreateIfNotExists_InputResource;
        protected override Task CreateIfNotExists(object resource)
        {
            this.CreateIfNotExists_InputResource = resource;

            return Task.CompletedTask;
        }

        public object? GetStorageUri_InputResource;
        protected override StorageUri GetStorageUri(object resource)
        {
            this.GetStorageUri_InputResource = resource;

            return new StorageUri(this.TestStorageUri);
        }
    }
}
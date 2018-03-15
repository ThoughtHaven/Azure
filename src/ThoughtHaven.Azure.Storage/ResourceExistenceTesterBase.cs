using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;

namespace ThoughtHaven.Azure.Storage
{
    public abstract class ResourceExistenceTesterBase<TResource>
    {
        private static readonly ICollection<Uri> _knownToExist = new List<Uri>();
        protected ICollection<Uri> KnownToExist => _knownToExist;

        public virtual async Task EnsureExists(TResource resource)
        {
            Guard.Null(nameof(resource), resource);

            var primaryUri = this.GetStorageUri(resource).PrimaryUri;

            if (!this.KnownToExist.Contains(primaryUri))
            {
                await this.CreateIfNotExists(resource).ConfigureAwait(false);

                this.KnownToExist.Add(primaryUri);
            }
        }

        protected abstract StorageUri GetStorageUri(TResource resource);
        protected abstract Task CreateIfNotExists(TResource resource);
    }
}
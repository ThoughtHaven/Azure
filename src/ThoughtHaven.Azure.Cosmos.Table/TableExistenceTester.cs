using System;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;

namespace ThoughtHaven.Azure.Cosmos.Table
{
    public class TableExistenceTester : ResourceExistenceTesterBase<CloudTable>
    {
        private readonly TableRequestOptions _options;

        public TableExistenceTester(TableRequestOptions options)
        {
            this._options = options ?? throw new ArgumentNullException(nameof(options));
        }

        protected override StorageUri GetStorageUri(CloudTable table)
        {
            if (table == null) { throw new ArgumentNullException(nameof(table)); }

            return table.StorageUri;
        }

        protected override Task CreateIfNotExists(CloudTable table)
        {
            if (table == null) { throw new ArgumentNullException(nameof(table)); }

            return table.CreateIfNotExistsAsync(this._options, operationContext: null);
        }
    }
}